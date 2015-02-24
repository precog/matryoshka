package slamdata.engine.physical.mongodb

import collection.immutable.ListMap

import slamdata.engine.fp._
import slamdata.engine.fs.Path
import slamdata.engine._
import Workflow._
import slamdata.engine.analysis.fixplate._
import slamdata.engine.std.StdLib._
import slamdata.engine.javascript._

import scalaz._
import Scalaz._
import monocle.Macro._
import monocle.syntax._

sealed trait WorkflowBuilderError extends Error
object WorkflowBuilderError {
  case class InvalidOperation(operation: String, msg: String)
      extends WorkflowBuilderError {
    def message = "Can not perform `" + operation + "`, because " + msg
  }
  case class UnsupportedDistinct(message: String) extends WorkflowBuilderError
  case class UnsupportedJoinCondition(func: Mapping) extends WorkflowBuilderError {
    def message = "Joining with " + func.name + " is not currently supported"
  }
}

sealed trait WorkflowBuilderF[+A]

object WorkflowBuilder {
  import Workflow._
  import ExprOp._
  import IdHandling._

  type WorkflowBuilder = Term[WorkflowBuilderF]
  type Schema = Option[List[String]]

  type Expr = ExprOp \/ JsMacro
  private def exprToJs(expr: Expr) = expr.fold(ExprOp.toJs(_), \/-(_))
  implicit def ExprRenderTree(implicit RJM: RenderTree[JsMacro]) = new RenderTree[Expr] {
      override def render(x: Expr) =
        x.fold(
          op => Terminal(op.toString, List("ExprOp")),
          js => RJM.render(js))
    }

  case class CollectionBuilderF(
    graph: Workflow,
    base: DocVar,
    struct: Schema) extends WorkflowBuilderF[Nothing]
  object CollectionBuilder {
    def apply(graph: Workflow, base: DocVar, struct: Schema) =
      Term[WorkflowBuilderF](new CollectionBuilderF(graph, base, struct))
  }
  case class ShapePreservingBuilderF[A](
    src: A,
    inputs: List[A],
    op: PartialFunction[List[BsonField], WorkflowOp])
      extends WorkflowBuilderF[A]
  {
    import ShapePreservingBuilder._

    override def equals(that: Any) = that match {
      case that @ ShapePreservingBuilderF(src1, inputs1, op1) =>
        src == src1 && inputs == inputs1 && dummyOp(this) == dummyOp(that)
      case _ => false
    }
    override def hashCode = List(src, inputs, dummyOp(this)).hashCode
  }
  object ShapePreservingBuilder {
    def apply(
      src: WorkflowBuilder,
      inputs: List[WorkflowBuilder],
      op: PartialFunction[List[BsonField], WorkflowOp]) =
      Term[WorkflowBuilderF](new ShapePreservingBuilderF(src, inputs, op))

    def dummyOp[A](builder: ShapePreservingBuilderF[A]) =
      builder.op(
        builder.inputs.zipWithIndex.map {
          case (_, index) => BsonField.Name("_" + index)
        })(
        $read(Collection("")))
  }
  case class ValueBuilderF(value: Bson) extends WorkflowBuilderF[Nothing]
  object ValueBuilder {
    def apply(value: Bson) = Term[WorkflowBuilderF](new ValueBuilderF(value))
  }
  case class ExprBuilderF[A](src: A, expr: Expr) extends WorkflowBuilderF[A]
  object ExprBuilder {
    def apply(src: WorkflowBuilder, expr: Expr) =
      Term[WorkflowBuilderF](new ExprBuilderF(src, expr))
  }

  // NB: The shape is more restrictive than $project because we may need to
  //     convert it to a GroupBuilder, and a nested Reshape can be realized with
  //     a chain of DocBuilders, leaving the collapsing to Workflow.coalesce.
  case class DocBuilderF[A](src: A, shape: ListMap[BsonField.Name, Expr])
      extends WorkflowBuilderF[A]
  object DocBuilder {
    def apply(src: WorkflowBuilder, shape: ListMap[BsonField.Name, Expr]) =
      Term[WorkflowBuilderF](new DocBuilderF(src, shape))
  }

  case class ArrayBuilderF[A](src: A, shape: List[Expr])
      extends WorkflowBuilderF[A]
  object ArrayBuilder {
    def apply(src: WorkflowBuilder, shape: List[Expr]) =
      Term[WorkflowBuilderF](new ArrayBuilderF(src, shape))
  }

  sealed trait Contents[+A]
  object Contents {
    case class Expr[A](contents: A) extends Contents[A]
    case class Doc[A](contents: ListMap[BsonField.Name, A]) extends Contents[A]

    implicit def ContentsRenderTree[A](implicit RA: RenderTree[A], RB: RenderTree[ListMap[BsonField.Name, A]]) =
    new RenderTree[Contents[A]] {
      override def render(v: Contents[A]) =
        v match {
          case Expr(a) => NonTerminal("", RA.render(a) :: Nil, "Expr" :: Nil)
          case Doc(b) => NonTerminal("", RB.render(b) :: Nil, "Doc" :: Nil)
        }
    }
  }
  import Contents._

  type GroupValue = ExprOp \/ GroupOp
  type GroupContents = Contents[GroupValue]

  case class GroupId(srcs: List[WorkflowBuilder]) {
    override def toString = hashCode.toHexString
  }

  case class GroupBuilderF[A](
    src: A, keys: List[A], contents: GroupContents, id: GroupId)
      extends WorkflowBuilderF[A]
  object GroupBuilder {
    def apply(
      src: WorkflowBuilder,
      keys: List[WorkflowBuilder],
      contents: GroupContents,
      id: GroupId) =
      Term[WorkflowBuilderF](new GroupBuilderF(src, keys, contents, id))
  }

  sealed trait StructureType
  case object Array extends StructureType
  case object Object extends StructureType

  case class FlatteningBuilderF[A](src: A, typ: StructureType, field: DocVar)
      extends WorkflowBuilderF[A]
  object FlatteningBuilder {
    def apply(src: WorkflowBuilder, typ: StructureType, field: DocVar) =
      Term[WorkflowBuilderF](new FlatteningBuilderF(src, typ, field))
  }

  /**
    Holds a partially-unknown structure. `Expr` entries are unknown and `Doc`
    entries are known. There should be at least one Expr in the list, otherwise
    it should be a DocBuilder.
    */
  case class SpliceBuilderF[A](src: A, structure: List[Contents[Expr]])
      extends WorkflowBuilderF[A]
  object SpliceBuilder {
    def apply(src: WorkflowBuilder, structure: List[Contents[Expr]]) =
      Term[WorkflowBuilderF](new SpliceBuilderF(src, structure))
  }

  private def rewriteObjRefs(
    obj: ListMap[BsonField.Name, GroupValue])(
    f: PartialFunction[DocVar, DocVar]) =
    obj ∘ (_.bimap(_.rewriteRefs(f), _.rewriteRefs(f)))

  private def rewriteGroupRefs(
    contents: GroupContents)(
    f: PartialFunction[DocVar, DocVar]) =
    contents match {
      case Expr(expr) => Expr(expr.bimap(_.rewriteRefs(f), _.rewriteRefs(f)))
      case Doc(doc)   => Doc(rewriteObjRefs(doc)(f))
    }

  private def rewriteDocPrefix(doc: ListMap[BsonField.Name, Expr], base: DocVar) =
    doc ∘ (rewriteExprPrefix(_, base))

  private def rewriteExprPrefix(expr: Expr, base: DocVar): Expr =
    expr.bimap(_.rewriteRefs(prefixBase(base)), base.toJs >>> _)

  type EitherE[X] = Error \/ X
  type M[X] = StateT[EitherE, NameGen, X]
  type MId[X] = State[NameGen, X]

  // Wrappers for results that don't use state:
  def emit[A](a: A): M[A] = lift(\/-(a))
  def fail[A](e: Error): M[A] = lift(-\/(e))
  def lift[A](v: Error \/ A): M[A] =
    StateT[EitherE, NameGen, A](s => v.map(s -> _))

  // Wrappers for results that don't fail:
  def emitSt[A](v: State[NameGen, A]): M[A] =
    StateT[EitherE, NameGen, A](s => \/-(v.run(s)))

  def swapM[A](v: State[NameGen, Error \/ A]): M[A] =
    StateT[EitherE, NameGen, A](s => { val (s1, x) = v.run(s); x.map(s1 -> _) })

  def commonMap[K, A, B](m: ListMap[K, A \/ B])(f: A => Error \/ B): Error \/ (ListMap[K, A] \/ ListMap[K, B]) = {
    val allAs = (m ∘ (_.swap.toOption)).sequenceU
    allAs.map(l => \/-(-\/(l))).getOrElse((m ∘ (_.fold(f, \/.right))).sequenceU.map(\/.right))
  }

  private def commonShape(shape: ListMap[BsonField.Name, Expr]) =
    commonMap(shape)(ExprOp.toJs)

  private def toCollectionBuilder(wb: WorkflowBuilder): M[CollectionBuilderF] =
    wb.unFix match {
      case cb @ CollectionBuilderF(_, _, _) => emit(cb)
      case ValueBuilderF(value) =>
        emit(CollectionBuilderF($pure(value), DocVar.ROOT(), None))
      case ShapePreservingBuilderF(src, inputs, op) =>
        fold1Builders(inputs).fold(
          toCollectionBuilder(src).map {
            case CollectionBuilderF(g, b, s) =>
              CollectionBuilderF(op(Nil)(g), b, s)
          })(
          _.flatMap { case (input, fields) =>
            // NB: This is a bit of a hack to pull shape-preservers ahead of
            //     other operations when possible. Would be better to implement
            //     this optimization more generally in Workflow, but we lose a
            //     lot of information by then.
            inputs.map(_.unFix match {
              case ExprBuilderF(src @ Term(CollectionBuilderF(_, DocVar.ROOT(base), _)), -\/(DocField(field))) => Some((src, base.fold(field)(_ \ field)))
              case _                                       => None
            }).sequence.fold(
              foldBuilders(src, inputs).flatMap { case (wb, base, fields) =>
                fields.map(_.deref).sequence.fold(
                  emitSt(freshName).flatMap(name =>
                    fields.map(f => (DocField(name) \\ f).deref).sequence.fold(
                      sys.error("prefixed ${name}, but still no field"))(
                      op.lift(_).fold(
                        fail[CollectionBuilderF](WorkflowBuilderError.InvalidOperation("filter", "failed to build operation")))(
                        op =>
                        (toCollectionBuilder(src) |@| toCollectionBuilder(DocBuilder(wb, ListMap(name -> -\/(DocVar.ROOT()))))) {
                          case (
                            CollectionBuilderF(_,     _,    srcStruct),
                            CollectionBuilderF(graph, base0, bothStruct)) =>
                            CollectionBuilderF(
                              chain(graph, op),
                              DocField(name),
                              srcStruct)
                        }))))(
                  op.lift(_).fold(
                    fail[CollectionBuilderF](WorkflowBuilderError.InvalidOperation("filter", "failed to build operation")))(
                    op =>
                    (toCollectionBuilder(src) |@| toCollectionBuilder(wb)) {
                      case (
                        CollectionBuilderF(_,     _,    srcStruct),
                        CollectionBuilderF(graph, base0, bothStruct)) =>
                        val g = chain(graph, op)
                        if (srcStruct == bothStruct)
                          CollectionBuilderF(g, base0 \\ base, srcStruct)
                        else
                          CollectionBuilderF(
                            shift(base0 \\ base, srcStruct, g),
                            DocVar.ROOT(),
                            srcStruct)
                    }))
              })(
              _.unzip match {
                case (srcs, fields) =>
                  op.lift(fields).fold(
                    fail[CollectionBuilderF](WorkflowBuilderError.InvalidOperation("filter", "failed to build operation")))(
                    op =>
                    if (src == input)
                      toCollectionBuilder(src).map {
                        case (CollectionBuilderF(g1, b1, s1)) =>
                          CollectionBuilderF(chain(g1, op), b1, s1)
                      }
                      else
                        (toCollectionBuilder(src) |@| toCollectionBuilder(input)) {
                          case (
                            CollectionBuilderF(g1, b1, s1),
                            CollectionBuilderF(graph, _, _)) =>
                            emitSt(Workflow.merge(g1, chain(graph, op)).map {
                              case ((lbase, _), wf) =>
                                CollectionBuilderF(wf, lbase \\ b1, s1)
                            })
                        }.join)
              })
          })
      case ExprBuilderF(src, -\/(d @ DocVar(_, _))) =>
        toCollectionBuilder(src).map {
          case CollectionBuilderF(graph, base, _) =>
            CollectionBuilderF(graph, base \\ d, None)
        }
      case ExprBuilderF(src, expr) => for {
        cb <- toCollectionBuilder(src)
        name <- emitSt(freshName)
      } yield cb match {
        case CollectionBuilderF(graph, base, _) =>
          CollectionBuilderF(
            chain(graph,
              rewriteExprPrefix(expr, base).fold(
                op => $project(Reshape(ListMap(name -> -\/(op)))),
                js => $simpleMap(JsMacro(x => JsCore.Obj(ListMap(name.asText -> js(x))).fix), Nil))),
            DocField(name),
            None)
      }
      case DocBuilderF(src, shape) =>
        workflow(src).flatMap { case (wf, base) =>
          commonShape(rewriteDocPrefix(shape, base)).fold(
            fail(_),
            s => emit(CollectionBuilderF(
              chain(wf,
                s.fold(
                  exprOps => $project(Reshape(exprOps ∘ \/.left)),
                  jsExprs => $simpleMap(JsMacro(x =>
                    Term(JsCore.Obj(jsExprs.map {
                      case (name, expr) => name.asText -> expr(x)
                    }))), Nil))),
              DocVar.ROOT(),
              Some(shape.toList.map(_._1.asText)))))
        }
      case ArrayBuilderF(src, shape) =>
        workflow(src).flatMap { case (wf, base) =>
          lift(shape.map(_.fold(ExprOp.toJs, \/-(_))).sequenceU.map(jsExprs =>
            CollectionBuilderF(
              chain(wf,
                $simpleMap(JsMacro(x =>
                  JsCore.Arr(jsExprs.map(_(base.toJs(x))).toList).fix), Nil)),
              DocVar.ROOT(),
              None)))
        }
      case GroupBuilderF(src, keys, content, _) =>
        foldBuilders(src, keys).flatMap { case (wb, base, fields) =>
          def key(base: DocVar) = keys match {
                case Nil        => -\/(Literal(Bson.Null))
                case key :: Nil => -\/(key.unFix match {
                  // NB: normalize to Null, to ease merging
                  case ValueBuilderF(_)                 => Literal(Bson.Null)
                  case ExprBuilderF(_, -\/(Literal(_))) => Literal(Bson.Null)
                  case _                                => fields.head.rewriteRefs(prefixBase(base))
                })
                case _          => \/-(Reshape(fields.zipWithIndex.map {
                  case (field, index) =>
                    BsonField.Index(index).toName -> -\/(field)
                }.toListMap).rewriteRefs(prefixBase(base)))
          }

          content match {
            case Expr(-\/(expr)) =>
              // NB: This case just winds up a single value, then unwinds it.
              //     It’s effectively a no-op, so we just use the src and expr.
              toCollectionBuilder(ExprBuilder(src, -\/(expr)))
            case Expr(\/-(grouped)) =>
              for {
                cb <- toCollectionBuilder(wb)
                rootName <- emitSt(freshName)
              } yield cb match {
                case CollectionBuilderF(wf, base0, struct) =>
                  CollectionBuilderF(
                    chain(wf,
                      $group(Grouped(ListMap(rootName -> grouped)).rewriteRefs(prefixBase(base0 \\ base)),
                        key(base0))),
                    DocField(rootName),
                    struct)
              }
            case Doc(obj) =>
              val (ungrouped, grouped) =
                obj.foldLeft[(ListMap[BsonField.Name, ExprOp], ListMap[BsonField.Leaf, GroupOp])]((ListMap.empty[BsonField.Name, ExprOp], ListMap.empty[BsonField.Leaf, GroupOp]))((acc, item) =>
                  item match {
                    case (k, -\/(v)) =>
                      ((x: ListMap[BsonField.Name, ExprOp]) => x + (k -> v)).first(acc)
                    case (k, \/-(v)) =>
                      ((x: ListMap[BsonField.Leaf, GroupOp]) => x + (k -> v)).second(acc)
                  })

              workflow(wb).flatMap { case (wf, base0) =>
                emitSt(ungrouped.size match {
                  case 0 =>
                    state[NameGen, Workflow](chain(wf,
                      $group(Grouped(grouped).rewriteRefs(prefixBase(base0 \\ base)), key(base0))))
                  case 1 =>
                    state[NameGen, Workflow](chain(wf,
                      $group(Grouped(
                        obj.transform {
                          case (_, -\/ (v)) => Push(v.rewriteRefs(prefixBase(base0 \\ base)))
                          case (_,  \/-(v)) => v.rewriteRefs(prefixBase(base0 \\ base))
                        }),
                        key(base0)),
                      $unwind(DocField(ungrouped.head._1))))
                  case _ => for {
                    ungroupedName <- freshName
                    groupedName <- freshName
                  } yield
                    chain(wf,
                      $project(Reshape(ListMap(
                        ungroupedName -> \/-(Reshape(ungrouped.map {
                          case (k, v) => k -> -\/(v.rewriteRefs(prefixBase(base0 \\ base)))
                        })),
                        groupedName -> -\/(DocVar.ROOT())))),
                      $group(Grouped(
                        (grouped ∘ (_.rewriteRefs(prefixBase(DocField(groupedName))))) +
                          (ungroupedName -> Push(DocField(ungroupedName)))),
                        key(DocField(groupedName))),
                      $unwind(DocField(ungroupedName)),
                      $project(Reshape(obj.transform {
                        case (k, -\/(_)) => -\/(DocField(ungroupedName \ k))
                        case (k, \/-(_)) => -\/(DocField(k))
                      })))
                }).map(CollectionBuilderF(
                  _,
                  DocVar.ROOT(),
                  Some(obj.toList.map(_._1.asText))))
              }
          }
        }
      case FlatteningBuilderF(src, Array, field) =>
        toCollectionBuilder(src).map {
          case CollectionBuilderF(graph, base, struct) =>
            CollectionBuilderF(chain(graph, $unwind(base \\ field)), base, struct)
        }
      case FlatteningBuilderF(src, Object, field) =>
        toCollectionBuilder(src).map {
          case CollectionBuilderF(graph, base, struct) =>
            import JsCore._
            CollectionBuilderF(
              chain(graph,
                $simpleMap(JsMacro(Predef.identity), List(JsMacro((base \\ field).toJs(_))))),
              base,
              struct)
        }
      case SpliceBuilderF(src, structure) =>
        workflow(src).flatMap { case (wf, base) =>
          lift(
            structure.map {
              case Expr(unknown) =>
                exprToJs(rewriteExprPrefix(unknown, base))
              case Doc(known) =>
                rewriteDocPrefix(known, base).toList.map { case (k, v) =>
                  exprToJs(v).map(k.asText -> _)
                }.sequenceU.map(ms => JsMacro(x => JsCore.Obj(ms.map { case (k, v) => k -> v(x) }.toListMap).fix))
            }.sequenceU.map(srcs =>
              CollectionBuilderF(
                chain(wf,
                  $simpleMap(JsMacro(x => JsCore.Splice(srcs.map(_(x))).fix), Nil)),
                DocVar.ROOT(),
                None)))
        }
    }

  def workflow(wb: WorkflowBuilder): M[(Workflow, DocVar)] =
    toCollectionBuilder(wb).map(x => (x.graph, x.base))

  def shift(base: DocVar, struct: Schema, graph: Workflow) =
    (base, struct) match {
      case (ExprVar, None)         => graph
      case (_,       None)         =>
        chain(graph,
          Workflow.$project(Reshape(ListMap(ExprName -> -\/(base))),
            ExcludeId))
      case (_,       Some(fields)) =>
        chain(graph,
          Workflow.$project(Reshape(fields.map(name =>
            BsonField.Name(name) ->
              -\/(base \ BsonField.Name(name))).toListMap),
            if (fields.exists(_ == IdLabel)) IncludeId else ExcludeId))
    }

  def build(wb: WorkflowBuilder): M[Workflow] =
    toCollectionBuilder(wb).map {
      case CollectionBuilderF(graph, base, struct) =>
        finish(
          if (base == DocVar.ROOT(None)) graph
          else shift(base, struct, graph))
    }

  private def $project(shape: Reshape): WorkflowOp =
    Workflow.$project(
      shape,
      shape.get(IdName).fold[IdHandling](IgnoreId)(κ(IncludeId)))

  def asLiteral(wb: WorkflowBuilder): Option[Bson] = wb.unFix match {
    case ValueBuilderF(value)                 => Some(value)
    case ExprBuilderF(_, -\/(Literal(value))) => Some(value)
    case _                                    => None
  }

  private def fold1Builders(builders: List[WorkflowBuilder]):
      Option[M[(WorkflowBuilder, List[ExprOp])]] =
    builders match {
      case Nil             => None
      case builder :: Nil  => Some(emit((builder, List(DocVar.ROOT()))))
      case Term(ValueBuilderF(bson)) :: rest =>
        fold1Builders(rest).map(_.map { case (builder, fields) =>
          (builder, Literal(bson) +: fields)
        })
      case builder :: rest =>
        Some(rest.foldLeftM[M, (WorkflowBuilder, List[ExprOp])](
          (builder, List(DocVar.ROOT()))) {
          case ((wf, fields), Term(ValueBuilderF(bson))) =>
            emit((wf, fields :+ Literal(bson)))
          case ((wf, fields), x) =>
            merge(wf, x).map { case (lbase, rbase, src) =>
              (src, fields.map(_.rewriteRefs(prefixBase(lbase))) :+ rbase)
            }
        })
    }

  private def foldBuilders(src: WorkflowBuilder, others: List[WorkflowBuilder]) =
    others.foldLeftM[M, (WorkflowBuilder, DocVar, List[DocVar])](
      (src, DocVar.ROOT(), Nil)) {
      case ((wf, base, fields), x) =>
        merge(wf, x).map { case (lbase, rbase, src) =>
          (src, lbase \\ base, fields.map(lbase \\ _) :+ rbase)
        }
    }

  def filter(src: WorkflowBuilder, those: List[WorkflowBuilder], sel: PartialFunction[List[BsonField], Selector]):
      WorkflowBuilder =
    ShapePreservingBuilder(src, those, PartialFunction(fields => $match(sel(fields))))

  def expr1(wb: WorkflowBuilder)(f: ExprOp => ExprOp): WorkflowBuilder =
    wb.unFix match {
      case ShapePreservingBuilderF(src, inputs, op) =>
        ShapePreservingBuilder(expr1(src)(f), inputs, op)
      case GroupBuilderF(wb0, key, Expr(-\/(DocVar.ROOT(None))), id) =>
        GroupBuilder(expr1(wb0)(f), key, Expr(-\/(DocVar.ROOT())), id)
      case GroupBuilderF(wb0, key, Expr(-\/(expr)), id) =>
        GroupBuilder(wb0, key, Expr(-\/(f(expr))), id)
      case ExprBuilderF(wb0, -\/ (expr1)) => ExprBuilder(wb0, -\/(f(expr1)))
      case ExprBuilderF(wb0,  \/-(js1)) =>
        toJs(f(DocVar.ROOT())).fold(
          κ(ExprBuilder(wb, -\/(f(DocVar.ROOT())))),
          js => ExprBuilder(wb0, \/-(js1 >>> js)))
      case _ => ExprBuilder(wb, -\/(f(DocVar.ROOT())))
    }

  def expr2(wb1: WorkflowBuilder, wb2: WorkflowBuilder)(f: (ExprOp, ExprOp) => ExprOp):
      M[WorkflowBuilder] =
    (wb1.unFix, wb2.unFix) match {
      case (_, ValueBuilderF(bson)) => emit(expr1(wb1)(f(_, Literal(bson))))
      case (ValueBuilderF(bson), _) => emit(expr1(wb2)(f(Literal(bson), _)))
      case (ExprBuilderF(src1, -\/(exprOp1)), ExprBuilderF(src2, -\/(exprOp2)))
        if src1 == src2 => emit(ExprBuilder(src1, -\/(f(exprOp1, exprOp2))))
      case (
        ShapePreservingBuilderF(Term(ExprBuilderF(src1, -\/(exprOp1))), inputs1, op1),
        ShapePreservingBuilderF(Term(ExprBuilderF(src2, -\/(exprOp2))), inputs2, op2))
        if src1 == src2 && inputs1 == inputs2 && op1 == op2 =>
        emit(ShapePreservingBuilder(ExprBuilder(src1, -\/(f(exprOp1, exprOp2))), inputs1, op1))
      case _ =>
        merge(wb1, wb2).map { case (lbase, rbase, src) =>
          src.unFix match {
            case ShapePreservingBuilderF(src0, inputs, op) =>
              ShapePreservingBuilder(ExprBuilder(src0, -\/(f(lbase, rbase))), inputs, op)
            case _ => ExprBuilder(src, -\/(f(lbase, rbase)))
          }
        }
    }

  def expr(wbs: List[WorkflowBuilder])
    (f: List[ExprOp] => ExprOp): M[WorkflowBuilder] = {
    fold1Builders(wbs).fold[M[WorkflowBuilder]](
      fail(WorkflowBuilderError.InvalidOperation("expr", "impossible – no arguments")))(
      _.map {
        case (wb, exprs) =>
          wb.unFix match {
            case ShapePreservingBuilderF(src0, inputs, op) =>
              ShapePreservingBuilder(
                ExprBuilder(src0, -\/(f(exprs))),
                inputs,
                op)
            case _ =>
              ExprBuilder(wb, -\/(f(exprs)))
          }
      })
  }

  def jsExpr1(wb: WorkflowBuilder, js: JsMacro): M[WorkflowBuilder] = {
    def handleSrc(wb1: WorkflowBuilder, tmp: BsonField.Name, x: JsMacro):
        Error \/ WorkflowBuilder =
      wb1.unFix match {
        case DocBuilderF(wb1, shape) =>
          shape.map { case (name, expr) => expr.fold(toJs(_), \/.right).map(name.asText -> _) }.toList.sequenceU.map(_.toListMap).map { nameToMacro =>
            val body = JsMacro(x1 => x(JsCore.Obj(nameToMacro ∘ { _(x1) }).fix))
            DocBuilder(wb1, ListMap(tmp -> \/-(body >>> js)))
          }
        case ShapePreservingBuilderF(wb2, inputs, op) =>
          handleSrc(wb2, tmp, x).map(ShapePreservingBuilder(_, inputs, op))
        case _ => \/-(DocBuilder(wb1, ListMap(tmp -> \/-(x >>> js))))
      }

    wb.unFix match {
      case ShapePreservingBuilderF(src, inputs, op) =>
        jsExpr1(src, js).map(ShapePreservingBuilder(_, inputs, op))
      case ExprBuilderF(wb1, -\/ (expr1)) =>
        lift(toJs(expr1).map(js1 => ExprBuilder(wb1, \/-(js1 >>> js))))
      case ExprBuilderF(wb1,  \/-(js1)) =>
        emit(ExprBuilder(wb1, \/-(js1 >>> js)))
      case GroupBuilderF(wb0, key, Expr(-\/(DocVar.ROOT(None))), id) =>
        jsExpr1(wb0, js).map(GroupBuilder(_, key, Expr(-\/(DocVar.ROOT())), id))
      case GroupBuilderF(wb1, key, Expr(-\/(expr)), id) =>
        for {
          x   <- lift(toJs(expr))
          tmp <- emitSt(freshName)
          src <- lift(handleSrc(wb1, tmp, x)): M[WorkflowBuilder]
        } yield GroupBuilder(src, key, Expr(-\/(DocField(tmp))), id)
      case _ => emit(ExprBuilder(wb, \/-(js)))
    }
  }

  def jsExpr2(wb1: WorkflowBuilder, wb2: WorkflowBuilder, js: (Term[JsCore], Term[JsCore]) => Term[JsCore]): M[WorkflowBuilder] =
    (wb1.unFix, wb2.unFix) match {
      case (_, ValueBuilderF(JsCore(lit))) =>
        jsExpr1(wb1, JsMacro(x => js(x, lit)))
      case (ValueBuilderF(JsCore(lit)), _) =>
        jsExpr1(wb2, JsMacro(x => js(lit, x)))
      case _ =>
        merge(wb1, wb2).map { case (lbase, rbase, src) =>
          ExprBuilder(src, \/-(JsMacro(x => js(lbase.toJs(x), rbase.toJs(x)))))
        }
    }

  def makeObject(wb: WorkflowBuilder, name: String): WorkflowBuilder =
    wb.unFix match {
      case ValueBuilderF(value) =>
        ValueBuilder(Bson.Doc(ListMap(name -> value)))
      case GroupBuilderF(src, key, Expr(cont), id) =>
        GroupBuilder(src, key, Doc(ListMap(BsonField.Name(name) -> cont)), id)
      case ExprBuilderF(src, expr) =>
        DocBuilder(src, ListMap(BsonField.Name(name) -> expr))
      case ShapePreservingBuilderF(src, inputs, op) =>
        ShapePreservingBuilder(makeObject(src, name), inputs, op)
      case _ =>
        DocBuilder(wb, ListMap(BsonField.Name(name) -> -\/(DocVar.ROOT())))
    }

  def makeArray(wb: WorkflowBuilder): WorkflowBuilder = wb.unFix match {
    case ValueBuilderF(value) => ValueBuilder(Bson.Arr(List(value)))
    case _ => ArrayBuilder(wb, List(-\/(DocVar.ROOT())))
  }

  def mergeContents[A](c1: Contents[A], c2: Contents[A]):
      M[((DocVar, DocVar), Contents[A])] = {
    def documentize(c: Contents[A]):
        MId[(DocVar, ListMap[BsonField.Name, A])] =
      c match {
        case Doc(d) => state(DocVar.ROOT() -> d)
        case Expr(cont) =>
          freshName.map(name => (DocField(name), ListMap(name -> cont)))
      }

    lazy val doc =
      swapM((documentize(c1) |@| documentize(c2)) {
        case ((lb, lshape), (rb, rshape)) =>
          Reshape.mergeMaps(lshape, rshape).fold[Error \/ ((DocVar, DocVar), Contents[A])](
            -\/(WorkflowBuilderError.InvalidOperation(
              "merging contents",
              "conflicting fields")))(
            map => \/-((lb, rb) -> Doc(map)))
      })
    if (c1 == c2)
      emit((DocVar.ROOT(), DocVar.ROOT()) -> c1)
    else
      (c1, c2) match {
        case (Expr(v), Doc(o)) =>
          o.find { case (_, e) => v == e }.fold(doc) {
            case (lField, _) =>
              emit((DocField(lField), DocVar.ROOT()) -> Doc(o))
          }
        case (Doc(o), Expr(v)) =>
          o.find { case (_, e) => v == e }.fold(doc) {
            case (rField, _) =>
              emit((DocVar.ROOT(), DocField(rField)) -> Doc(o))
          }
        case _ => doc
      }
  }

  trait Combine {
    def apply[A, B](a1: A, a2: A)(f: (A, A) => B): B
    def flip: Combine
  }
  val unflipped: Combine = new Combine { outer =>
    def apply[A, B](a1: A, a2: A)(f: (A, A) => B) = f(a1, a2)
    val flip = new Combine {
      def apply[A, B](a1: A, a2: A)(f: (A, A) => B) = f(a2, a1)
      val flip = outer
    }
  }

  // TODO: handle concating value, expr, or collection with group (#439)
  def objectConcat(wb1: WorkflowBuilder, wb2: WorkflowBuilder):
      M[WorkflowBuilder] = {
    def impl(wb1: WorkflowBuilder, wb2: WorkflowBuilder, combine: Combine): M[WorkflowBuilder] = {
      def delegate = impl(wb2, wb1, combine.flip)

      def mergeGroups(s1: WorkflowBuilder, s2: WorkflowBuilder, c1: GroupContents, c2: GroupContents, keys: List[WorkflowBuilder], id1: GroupId):
          M[((DocVar, DocVar), WorkflowBuilder)] =
        merge(s1, s2).flatMap { case (lbase, rbase, src) =>
          combine(
            rewriteGroupRefs(c1)(prefixBase(lbase)),
            rewriteGroupRefs(c2)(prefixBase(rbase)))(mergeContents).map {
            case ((lb, rb), contents) =>
              combine(lb, rb)((_, _) -> GroupBuilder(src, keys, contents, id1))
          }
        }

      (wb1.unFix, wb2.unFix) match {
        case (ShapePreservingBuilderF(s1, i1, o1), ShapePreservingBuilderF(s2, i2, o2))
            if i1 == i2 && o1 == o2 =>
          impl(s1, s2, combine).map(ShapePreservingBuilder(_, i1, o1))

        case (
          ShapePreservingBuilderF(
            Term(DocBuilderF(_, shape1)), inputs1, op1),
          GroupBuilderF(
            Term(ShapePreservingBuilderF(_, inputs2, op2)),
            Nil, _, id2))
          if inputs1 == inputs2 && op1 == op2 =>
            impl(GroupBuilder(wb1, Nil, Doc(shape1.keys.toList.map(n => n -> -\/(DocField(n))).toListMap), id2), wb2, combine)
        case (
          GroupBuilderF(
          Term(ShapePreservingBuilderF(_, inputs1, op1)),
            Nil, _, _),
          ShapePreservingBuilderF(Term(DocBuilderF(_, _)), inputs2, op2))
          if inputs1 == inputs2 && op1 == op2 => delegate

        case (
          ShapePreservingBuilderF(
            Term(DocBuilderF(_, shape1)), inputs1, op1),
          DocBuilderF(
            Term(GroupBuilderF(
              Term(ShapePreservingBuilderF(_, inputs2, op2)),
              Nil, _, id2)),
           shape2))
          if inputs1 == inputs2 && op1 == op2 =>
            impl(GroupBuilder(wb1, Nil, Doc(shape1.keys.toList.map(n => n -> -\/(DocField(n))).toListMap), id2), wb2, combine)
        case (
          DocBuilderF(
            Term(GroupBuilderF(
              Term(ShapePreservingBuilderF(_, inputs1, op1)),
              Nil, _, _)),
            shape2),
          ShapePreservingBuilderF(Term(DocBuilderF(_, _)), inputs2, op2))
          if inputs1 == inputs2 && op1 == op2 => delegate

        case (ShapePreservingBuilderF(s, i, o), _) =>
          impl(s, wb2, combine).map(ShapePreservingBuilder(_, i, o))
        case (_, ShapePreservingBuilderF(_, _, _)) => delegate

        case (ValueBuilderF(Bson.Doc(map1)), ValueBuilderF(Bson.Doc(map2))) =>
          emit(ValueBuilder(Bson.Doc(combine(map1, map2)(_ ++ _))))

        case (ValueBuilderF(Bson.Doc(map1)), DocBuilderF(s2, shape2)) =>
          emit(DocBuilder(s2,
            combine(
              map1.map { case (k, v) => BsonField.Name(k) -> -\/(Literal(v)) },
              shape2)(_ ++ _)))
        case (DocBuilderF(_, _), ValueBuilderF(Bson.Doc(_))) => delegate

        case (ValueBuilderF(Bson.Doc(map1)), GroupBuilderF(s1, k1, Doc(c2), id2)) =>
          val content = combine(
            map1.map { case (k, v) => BsonField.Name(k) -> -\/(Literal(v)) },
            c2)(_ ++ _)
          emit(GroupBuilder(s1, k1, Doc(content), id2))
        case (GroupBuilderF(_, _, Doc(_), _), ValueBuilderF(_)) => delegate

        case (
          GroupBuilderF(src1, keys, Expr(-\/(DocVar.ROOT(_))), id1),
          GroupBuilderF(src2, _,    Expr(-\/(DocVar.ROOT(_))), id2))
            if id1 == id2 =>
          impl(src1, src2, combine).map(GroupBuilder(_, keys, Expr(-\/(DocVar.ROOT())), id1))

        case (
          GroupBuilderF(s1, keys, c1 @ Doc(_), id1),
          GroupBuilderF(s2, _,    c2 @ Doc(_), id2))
            if id1 == id2 =>
          mergeGroups(s1, s2, c1, c2, keys, id1).map(_._2)

        case (
          GroupBuilderF(s1, keys, c1 @ Doc(d1), id1),
          DocBuilderF(Term(GroupBuilderF(s2, _, c2, id2)), shape2))
            if id1 == id2 =>
          mergeGroups(s1, s2, c1, c2, keys, id1).map { case ((glbase, grbase), g) =>
            DocBuilder(g, combine(
              d1.transform { case (n, _) => -\/(DocField(n)) },
              (shape2 ∘ (rewriteExprPrefix(_, grbase))))(_ ++ _))
          }
        case (
          DocBuilderF(Term(GroupBuilderF(_, k1, _, id1)), _),
          GroupBuilderF(_, k2, Doc(_), id2))
            if id1 == id2 =>
          delegate

        case (
          DocBuilderF(Term(GroupBuilderF(s1, keys, c1, id1)), shape1),
          DocBuilderF(Term(GroupBuilderF(s2, _,    c2, id2)), shape2))
            if id1 == id2 =>
          mergeGroups(s1, s2, c1, c2, keys, id1).flatMap {
            case ((glbase, grbase), g) =>
              emit(DocBuilder(g, combine(
                shape1 ∘ (rewriteExprPrefix(_, glbase)),
                shape2 ∘ (rewriteExprPrefix(_, grbase)))(_ ++ _)))
          }

        case (
          DocBuilderF(s1, shape),
          GroupBuilderF(_, Nil, _, id2)) =>
          impl(
            DocBuilder(GroupBuilder(s1, Nil, Expr(-\/(DocVar.ROOT())), id2), shape),
            wb2,
            combine)
        case (
          GroupBuilderF(_, Nil, _, _),
          DocBuilderF(_, _)) =>
          delegate

        case (
          DocBuilderF(s1, shape),
          DocBuilderF(Term(GroupBuilderF(_, Nil, _, id2)), _)) =>
          impl(
            DocBuilder(GroupBuilder(s1, Nil, Expr(-\/(DocVar.ROOT())), id2), shape),
            wb2,
            combine)
        case (
          DocBuilderF(Term(GroupBuilderF(_, Nil, _, _)), _),
          DocBuilderF(_, _)) =>
          delegate

        case (DocBuilderF(s1, shape1), DocBuilderF(s2, shape2)) =>
          merge(s1, s2).map { case (lbase, rbase, src) =>
            DocBuilder(src, combine(
              rewriteDocPrefix(shape1, lbase),
              rewriteDocPrefix(shape2, rbase))(_ ++ _))
          }

        case (DocBuilderF(src1, shape), ExprBuilderF(src2, expr)) =>
          merge(src1, src2).map { case (left, right, list) =>
            SpliceBuilder(list, combine(
              Doc(rewriteDocPrefix(shape, left)),
              Expr(rewriteExprPrefix(expr, right)))(List(_, _)))
          }
        case (ExprBuilderF(_, _), DocBuilderF(_, _)) => delegate

        case (ExprBuilderF(src1, expr1), ExprBuilderF(src2, expr2)) =>
          merge(src1, src2).map { case (left, right, list) =>
            SpliceBuilder(list, combine(
              Expr(rewriteExprPrefix(expr1, left)),
              Expr(rewriteExprPrefix(expr2, right)))(List(_, _)))
          }

        case (SpliceBuilderF(src1, structure1), DocBuilderF(src2, shape2)) =>
          merge(src1, src2).map { case (left, right, list) =>
            SpliceBuilder(list, combine(
              structure1,
              List(Doc(rewriteDocPrefix(shape2, right))))(_ ++ _))
          }
        case (DocBuilderF(_, _), SpliceBuilderF(_, _)) => delegate

        case (SpliceBuilderF(src1, structure1), CollectionBuilderF(_, _, _)) =>
          merge(src1, wb2).map { case (left, right, list) =>
            SpliceBuilder(list, combine(
              structure1,
              List(Expr(-\/(right))))(_ ++ _))
          }
        case (CollectionBuilderF(_, _, _), SpliceBuilderF(_, _)) => delegate

        case (DocBuilderF(src, shape), _) =>
          merge(src, wb2).map { case (left, right, list) =>
            SpliceBuilder(list, combine(
              Doc(rewriteDocPrefix(shape, left)),
              Expr(-\/(right))) (List(_, _)))
          }
        case (_, DocBuilderF(_, _)) => delegate

        case (CollectionBuilderF(_, _, _), _) =>
          merge(wb1, wb2).map { case (left, right, list) =>
            SpliceBuilder(list,
              combine(Expr(-\/(left)), Expr(-\/(right)))(List(_, _)))
          }
        case (_, CollectionBuilderF(_, _, _)) => delegate

        case _ => fail(WorkflowBuilderError.InvalidOperation(
          "objectConcat",
          "unrecognized shapes:\n" + wb1.show + "\n" + wb2.show))
      }
    }

    impl(wb1, wb2, unflipped)
  }

  def arrayConcat(left: WorkflowBuilder, right: WorkflowBuilder):
      M[WorkflowBuilder] = {
    def impl(wb1: WorkflowBuilder, wb2: WorkflowBuilder, combine: Combine):
        M[WorkflowBuilder] = {
      def delegate = impl(wb2, wb1, combine.flip)

      (wb1.unFix, wb2.unFix) match {
        case (ValueBuilderF(Bson.Arr(seq1)), ValueBuilderF(Bson.Arr(seq2))) =>
          emit(ValueBuilder(Bson.Arr(seq1 ++ seq2)))

        case (ValueBuilderF(Bson.Arr(seq)), ArrayBuilderF(src, shape)) =>
          emit(ArrayBuilder(src,
            combine(seq.map(x => -\/(Literal(x))), shape)(_ ++ _)))
        case (ArrayBuilderF(_, _), ValueBuilderF(Bson.Arr(_))) => delegate

        case (ArrayBuilderF(src1, shape1), ArrayBuilderF(src2, shape2)) =>
          merge(src1, src2).map { case (lbase, rbase, wb) =>
            ArrayBuilder(wb,
              shape1.map(rewriteExprPrefix(_, lbase)) ++
                shape2.map(rewriteExprPrefix(_, rbase)))
          }
        case _ =>
          fail(WorkflowBuilderError.InvalidOperation(
            "arrayConcat",
            "values are not both arrays"))
      }
    }

    impl(left, right, unflipped)
  }

  def flattenObject(wb: WorkflowBuilder): WorkflowBuilder = wb.unFix match {
    case ShapePreservingBuilderF(src, inputs, op) =>
      ShapePreservingBuilder(flattenObject(src), inputs, op)
    case GroupBuilderF(src, keys, Expr(-\/(DocVar.ROOT(None))), id) =>
      GroupBuilder(flattenObject(src), keys, Expr(-\/(DocVar.ROOT())), id)
    case _ => FlatteningBuilder(wb, Object, DocVar.ROOT())
  }

  def flattenArray(wb: WorkflowBuilder): WorkflowBuilder = wb.unFix match {
    case ShapePreservingBuilderF(src, inputs, op) =>
      ShapePreservingBuilder(flattenArray(src), inputs, op)
    case GroupBuilderF(src, keys, Expr(-\/(DocVar.ROOT(None))), id) =>
      GroupBuilder(flattenArray(src), keys, Expr(-\/(DocVar.ROOT())), id)
    case _ => FlatteningBuilder(wb, Array, DocVar.ROOT())
  }

  def projectField(wb: WorkflowBuilder, name: String):
      Error \/ WorkflowBuilder =
    wb.unFix match {
      case ShapePreservingBuilderF(src, inputs, op) =>
        projectField(src, name).map(ShapePreservingBuilder(_, inputs, op))
      case ValueBuilderF(Bson.Doc(fields)) =>
        fields.get(name).fold[Error \/ WorkflowBuilder](
          -\/(WorkflowBuilderError.InvalidOperation(
            "projectField",
            "value does not contain a field ‘" + name + "’.")))(
          x => \/-(ValueBuilder(x)))
      case ValueBuilderF(_) =>
        -\/(WorkflowBuilderError.InvalidOperation(
          "projectField",
          "value is not a document."))
      case GroupBuilderF(wb0, key, Expr(-\/(DocVar.ROOT(None))), id) =>
        projectField(wb0, name).map(GroupBuilder(_, key, Expr(-\/(DocVar.ROOT())), id))
      case GroupBuilderF(wb0, key, Expr(-\/(dv @ DocVar(_, _))), id) =>
        // TODO: check structure of wb0 (#436)
        \/-(GroupBuilder(wb0, key, Expr(-\/(dv \ BsonField.Name(name))), id))
      case GroupBuilderF(wb0, key, Doc(doc), id) =>
        doc.get(BsonField.Name(name)).fold[Error \/ WorkflowBuilder](
          -\/(WorkflowBuilderError.InvalidOperation(
            "projectField",
            "group does not contain a field ‘" + name + "’.")))(
          x => \/-(GroupBuilder(wb0, key, Expr(x), id)))
      case DocBuilderF(wb, doc) =>
        doc.get(BsonField.Name(name)).fold[Error \/ WorkflowBuilder](
          -\/(WorkflowBuilderError.InvalidOperation(
            "projectField",
            "document does not contain a field ‘" + name + "’.")))(
          expr => \/-(ExprBuilder(wb, expr)))
      case ExprBuilderF(wb0,  \/-(js1)) =>
        \/-(ExprBuilder(wb0,
          \/-(JsMacro(base => DocField(BsonField.Name(name)).toJs(js1(base))))))
      case ExprBuilderF(wb, -\/(DocField(field))) =>
        \/-(ExprBuilder(wb, -\/(DocField(field \ BsonField.Name(name)))))
      case _ => \/-(ExprBuilder(wb, -\/(DocField(BsonField.Name(name)))))
    }

  def projectIndex(wb: WorkflowBuilder, index: Int): M[WorkflowBuilder] =
    wb.unFix match {
      case ValueBuilderF(Bson.Arr(elems)) =>
        if (index < elems.length) // UGH!
          emit(ValueBuilder(elems(index)))
        else
          fail(WorkflowBuilderError.InvalidOperation(
            "projectIndex",
            "value does not contain index ‘" + index + "’."))
      case ArrayBuilderF(wb0, elems) =>
        if (index < elems.length) // UGH!
          emit(ExprBuilder(wb0, elems(index)))
        else
          fail(WorkflowBuilderError.InvalidOperation(
            "projectIndex",
            "array does not contain index ‘" + index + "’."))
      case ValueBuilderF(_) =>
        fail(WorkflowBuilderError.InvalidOperation(
          "projectIndex",
          "value is not an array."))
      case DocBuilderF(_, _) =>
        fail(WorkflowBuilderError.InvalidOperation(
          "projectIndex",
          "value is not an array."))
      case _ =>
        jsExpr1(wb, JsMacro(base =>
          JsCore.Access(base, JsCore.Literal(Js.Num(index, false)).fix).fix))
    }

  def groupBy(src: WorkflowBuilder, keys: List[WorkflowBuilder]):
      WorkflowBuilder =
    GroupBuilder(src, keys, Expr(-\/(DocVar.ROOT())), GroupId(src :: keys))

  def reduce(wb: WorkflowBuilder)(f: ExprOp => GroupOp): WorkflowBuilder =
    wb.unFix match {
      case GroupBuilderF(wb0, keys, Expr(-\/(expr)), id) =>
        GroupBuilder(wb0, keys, Expr(\/-(f(expr))), id)
      case ShapePreservingBuilderF(src @ Term(GroupBuilderF(_, _, Expr(-\/(_)), _)), inputs, op) =>
        ShapePreservingBuilder(reduce(src)(f), inputs, op)
      case _ =>
        // NB: the group must be identified with the source collection, not an
        // expression/doc built on it. This is sufficient in the known cases,
        // but we might need to dig for an actual CollectionBuilder to be safe.
        def id(wb: WorkflowBuilder): GroupId = wb.unFix match {
          case ExprBuilderF(src, _)               => id(src)
          case DocBuilderF(src, _)                => id(src)
          case ShapePreservingBuilderF(src, _, _) => id(src)
          case _ => GroupId(List(wb))
        }
        GroupBuilder(wb, Nil, Expr(\/-(f(DocVar.ROOT()))), id(wb))
    }

  def sortBy(
    src: WorkflowBuilder, keys: List[WorkflowBuilder], sortTypes: List[SortType]):
      WorkflowBuilder =
    ShapePreservingBuilder(
      src,
      keys,
      _.zip(sortTypes) match {
        case x :: xs => $sort(NonEmptyList.nel(x, xs))
      })

  def join(left: WorkflowBuilder, right: WorkflowBuilder,
    tpe: slamdata.engine.LogicalPlan.JoinType, comp: Mapping,
    leftKey: WorkflowBuilder, rightKey: JsMacro):
      M[WorkflowBuilder] = {

    import slamdata.engine.LogicalPlan.JoinType
    import slamdata.engine.LogicalPlan.JoinType._
    import Js._

    // Note: these have to match the names used in the logical plan
    val leftField: BsonField.Name = BsonField.Name("left")
    val rightField: BsonField.Name = BsonField.Name("right")

    val nonEmpty: Selector.SelectorExpr = Selector.NotExpr(Selector.Size(0))

    def padEmpty(side: BsonField): ExprOp =
      Cond(
        Eq(Size(DocField(side)), Literal(Bson.Int32(0))),
        Literal(Bson.Arr(List(Bson.Doc(ListMap())))),
        DocField(side))

    def buildProjection(l: ExprOp, r: ExprOp):
        WorkflowOp =
      chain(_,
        $project(Reshape(ListMap(
          leftField -> -\/(l),
          rightField -> -\/(r)))))

    def buildJoin(src: Workflow, tpe: JoinType): Workflow =
      tpe match {
        case FullOuter =>
          chain(src,
            buildProjection(padEmpty(leftField), padEmpty(rightField)))
        case LeftOuter =>
          chain(src,
            $match(Selector.Doc(ListMap(
              leftField.asInstanceOf[BsonField] -> nonEmpty))),
            buildProjection(DocField(leftField), padEmpty(rightField)))
        case RightOuter =>
          chain(src,
            $match(Selector.Doc(ListMap(
              rightField.asInstanceOf[BsonField] -> nonEmpty))),
            buildProjection(padEmpty(leftField), DocField(rightField)))
        case Inner =>
          chain(
            src,
            $match(
              Selector.Doc(ListMap(
                leftField.asInstanceOf[BsonField] -> nonEmpty,
                rightField -> nonEmpty))))
      }

    def rightMap(keyExpr: JsMacro): AnonFunDecl =
      $Map.mapKeyVal(("key", "value"),
        keyExpr(JsCore.Ident("value").fix).toJs,
        AnonObjDecl(List(
          (leftField.asText, AnonElem(Nil)),
          (rightField.asText, AnonElem(List(Ident("value")))))))

    val rightReduce =
      AnonFunDecl(List("key", "values"),
        List(
          VarDef(List(("result",
            AnonObjDecl(List(
              (leftField.asText, AnonElem(Nil)),
              (rightField.asText, AnonElem(Nil))))))),
          Call(Select(Ident("values"), "forEach"),
            List(AnonFunDecl(List("value"),
              // TODO: replace concat here with a more efficient operation
              //      (push or unshift)
              List(
                BinOp("=",
                  Select(Ident("result"), leftField.asText),
                  Call(Select(Select(Ident("result"), leftField.asText), "concat"),
                    List(Select(Ident("value"), leftField.asText)))),
                BinOp("=",
                  Select(Ident("result"), rightField.asText),
                  Call(Select(Select(Ident("result"), rightField.asText), "concat"),
                    List(Select(Ident("value"), rightField.asText)))))))),
          Return(Ident("result"))))

    comp match {
      case relations.Eq =>
        (workflow(DocBuilder(
          reduce(groupBy(left, List(leftKey)))(Push),
          ListMap(
            leftField  -> -\/(DocVar.ROOT()),
            rightField -> -\/(Literal(Bson.Arr(Nil))),
            BsonField.Name("_id") -> -\/(Include)))) |@|
          workflow(right)) { case ((l, _), (r, _)) =>
            CollectionBuilder(
              chain(
                $foldLeft(
                  l,
                  chain(r,
                    $map(rightMap(rightKey), ListMap()),
                    $reduce(rightReduce, ListMap()))),
                buildJoin(_, tpe),
                $unwind(DocField(leftField)),
                $unwind(DocField(rightField))),
              DocVar.ROOT(),
              None)
        }
      case _ => fail(WorkflowBuilderError.UnsupportedJoinCondition(comp))
    }
  }

  def cross(left: WorkflowBuilder, right: WorkflowBuilder) =
    join(left, right,
      slamdata.engine.LogicalPlan.JoinType.Inner, relations.Eq,
      ValueBuilder(Bson.Null), JsMacro(κ(JsCore.Literal(Js.Null).fix)))

  def limit(wb: WorkflowBuilder, count: Long) =
    ShapePreservingBuilder(wb, Nil, { case Nil => $limit(count) })

  def skip(wb: WorkflowBuilder, count: Long) =
    ShapePreservingBuilder(wb, Nil, { case Nil => $skip(count) })

  def squash(wb: WorkflowBuilder): WorkflowBuilder = wb

  def distinctBy(src: WorkflowBuilder, keys: List[WorkflowBuilder]):
      M[WorkflowBuilder] = {
    def sortKeys(op: WorkflowBuilder): M[List[(BsonField, SortType)]] = {
      def isOrdered(op: Workflow): Boolean = op.unFix match {
        case $Sort(_, _)                            => true
        case $Group(_, _, _)                        => false
        case $GeoNear(_, _, _, _, _, _, _, _, _, _) => true
        case $Unwind(_, _)                          => false
        case p: PipelineF[_]                        => isOrdered(p.src)
        case _                                      => false
      }

      // Note: this currently only handles a couple of cases, which are the ones
      // that are generated by the compiler for SQL's distinct keyword, with
      // order by, with or without "synthetic" projections. A more general
      // implementation would rewrite the pipeline to handle additional cases.
      def findSortKeys(wf: Workflow): Error \/ List[(BsonField, SortType)] =
        wf.unFix match {
          case $Sort(_, keys) => \/-(keys.list)
          case $Project(Term($Sort(_, keys)), shape, _) =>
            keys.list.map {
              case (field, sortType) =>
                Reshape.getAll(shape).collectFirst {
                  case (k, dv @ DocVar.ROOT(prefix))
                      if DocVar.ROOT(field).startsWith(dv) =>
                    k \\ field.flatten.drop(prefix.fold(0)(_.flatten.length))
                }.map(_ -> sortType)
            }.sequence.fold[Error \/ List[(BsonField, SortType)]](-\/(WorkflowBuilderError.UnsupportedDistinct("cannot distinct with missing keys: " + wf)))(\/-(_))
          case sp: ShapePreservingF[_] => findSortKeys(sp.src)
          case _ =>
            if (isOrdered(wf))
              -\/(WorkflowBuilderError.UnsupportedDistinct("cannot distinct with unrecognized ordered source: " + wf))
            else \/-(Nil)
        }

      workflow(op).flatMap {
        case (wf, base) => lift(findSortKeys(wf))
      }
    }

    val distinct = foldBuilders(src, keys).map { case (merged, value, fields) =>
      sortKeys(merged).flatMap { sk =>
        val keyPrefix = "__sd_key_"
        val keyProjs = sk.zipWithIndex.map { case ((name, _), index) =>
          BsonField.Name(keyPrefix + index.toString) -> First(DocField(name))
        }
        def findKeys(wb: WorkflowBuilder): Error \/ (ExprOp \/ Reshape) = {
          def emit(keys: List[BsonField.Name]): Error \/ (ExprOp \/ Reshape) =
            \/-(\/-(Reshape(keys.toList.map(n => n -> -\/(DocField(n))).toListMap)))
          wb.unFix match {
            case CollectionBuilderF(_, _, s2) =>
              s2.fold[Error \/ (ExprOp \/ Reshape)](
                -\/(WorkflowBuilderError.UnsupportedDistinct("Cannot distinct with unknown shape (" + s2 + ")")))(
                byFields => emit(byFields.map(BsonField.Name.apply)))
            case DocBuilderF(_, shape) => emit(shape.keys.toList)
            case GroupBuilderF(_, _, Doc(obj), _) => emit(obj.keys.toList)
            case ShapePreservingBuilderF(src, _, _) => findKeys(src)
            case ExprBuilderF(_, _) => \/-(-\/(DocVar.ROOT()))
            case ValueBuilderF(Bson.Doc(shape)) => emit(shape.keys.toList.map(BsonField.Name.apply))
            case _ => -\/(WorkflowBuilderError.UnsupportedDistinct("Cannot distinct with unknown shape (" + keys.head + ")"))
          }
        }
        val groupedBy = fields match {
          case Nil        => \/-(-\/(Literal(Bson.Null)))
          case key :: Nil => key match {
            // If the key is at the document root, we must explicitly
            //  project out the fields so as not to include a meaningless
            // _id in the key:
            case DocVar.ROOT(None) => findKeys(keys.head)
            case _                 => \/-(-\/(key))
          }
          case _          =>  \/-(\/-(Reshape(fields.zipWithIndex.map {
            case (field, index) =>
              BsonField.Index(index).toName -> -\/(field)
          }.toListMap)))
        }

        val group: M[CollectionBuilderF] = for {
          name <- emitSt(freshName)
          gby  <- lift(groupedBy)
          merg <- toCollectionBuilder(merged)
          CollectionBuilderF(graph, base, struct) = merg
        } yield CollectionBuilderF(
          chain(
            graph,
            $group(
              Grouped(ListMap(name -> First(base \\ value) :: keyProjs: _*)),
              gby.bimap(
                _.rewriteRefs(prefixBase(base)),
                _.rewriteRefs(prefixBase(base))))),
          DocField(name),
          struct)

        val keyPairs = sk.zipWithIndex.map { case ((name, sortType), index) =>
          BsonField.Name(keyPrefix + index.toString) -> sortType
        }
        keyPairs.headOption.fold(group) { head =>
          val tail = keyPairs.drop(1)
          group.map(g => g.copy(graph = chain(g.graph, $sort(NonEmptyList(head, tail: _*)))))
        }
      }
    }.join

    distinct.map(Term[WorkflowBuilderF](_))
  }

  private def merge(left: WorkflowBuilder, right: WorkflowBuilder):
      M[(DocVar, DocVar, WorkflowBuilder)] = {
    def delegate =
      merge(right, left).map { case (r, l, merged) => (l, r, merged)}

    (left.unFix, right.unFix) match {
      case (
        ExprBuilderF(src1, -\/(base1 @ DocField(_))),
        ExprBuilderF(src2, -\/(base2 @ DocField(_))))
          if src1 == src2 =>
        emit((base1, base2, src1))

      case _ if left == right => emit((DocVar.ROOT(), DocVar.ROOT(), left))

      case (ValueBuilderF(bson), ExprBuilderF(src, expr)) =>
        mergeContents(Expr(-\/(Literal(bson))), Expr(expr)).map {
          case ((lbase, rbase), cont) =>
            (lbase, rbase,
              cont match {
                case Expr(expr) => ExprBuilder(src, expr)
                case Doc(doc)   => DocBuilder(src, doc)
              })
        }
      case (ExprBuilderF(_, _), ValueBuilderF(_)) => delegate

      case (ValueBuilderF(bson), DocBuilderF(src, shape)) =>
        mergeContents(Expr(-\/(Literal(bson))), Doc(shape)).map {
          case ((lbase, rbase), cont) =>
            (lbase, rbase,
              cont match {
                case Expr(expr) => ExprBuilder(src, expr)
                case Doc(doc)   => DocBuilder(src, doc)
              })
        }
      case (DocBuilderF(_, _), ValueBuilderF(_)) => delegate

      case (ValueBuilderF(bson), _) =>
        mergeContents(Expr(-\/(Literal(bson))), Expr(-\/(DocVar.ROOT()))).map {
          case ((lbase, rbase), cont) =>
            (lbase, rbase,
              cont match {
                case Expr(expr) => ExprBuilder(right, expr)
                case Doc(doc)   => DocBuilder(right, doc)
              })
        }
      case (_, ValueBuilderF(_)) => delegate

      case (ExprBuilderF(src1, expr1), ExprBuilderF(src2, expr2)) if src1 == src2 =>
        mergeContents(Expr(expr1), Expr(expr2)).map {
          case ((lbase, rbase), cont) =>
            (lbase, rbase,
              cont match {
                case Expr(expr) => ExprBuilder(src1, expr)
                case Doc(doc)   => DocBuilder(src1, doc)
              })
        }
      case (ExprBuilderF(src, -\/(base @ DocField(_))), _) if src == right =>
        emit((base, DocVar.ROOT(), right))
      case (_, ExprBuilderF(src, -\/(DocField(_)))) if left == src =>
        delegate

      case (DocBuilderF(src1, shape1), ExprBuilderF(src2, expr2)) if src1 == src2 =>
        mergeContents(Doc(shape1), Expr(expr2)).map {
          case ((lbase, rbase), cont) =>
            (lbase, rbase,
              cont match {
                case Expr(expr) => ExprBuilder(src1, expr)
                case Doc(doc)  => DocBuilder(src1, doc)
              })
        }
      case (ExprBuilderF(src1, _), DocBuilderF(src2, _)) if src1 == src2 =>
        delegate

      case (DocBuilderF(src1, shape1), DocBuilderF(src2, shape2)) =>
        merge(src1, src2).flatMap { case (lbase, rbase, wb) =>
          mergeContents(
            Doc(rewriteDocPrefix(shape1, lbase)),
            Doc(rewriteDocPrefix(shape2, rbase))).map {
            case ((lbase, rbase), cont) =>
              (lbase, rbase,
                cont match {
                  case Expr(expr) => ExprBuilder(wb, expr)
                  case Doc(doc)   => DocBuilder(wb, doc)
                })
          }
        }

      case (SpliceBuilderF(src, structure), _) =>
        merge(src, right).flatMap { case (lbase, rbase, wb) =>
          emitSt(freshName.map(name =>
            (DocVar.ROOT(), DocField(name),
              SpliceBuilder(wb, structure.map {
                case Expr(expr) => Expr(rewriteExprPrefix(expr, lbase))
                case Doc(doc)  => Doc(rewriteDocPrefix(doc, lbase))
              } :+ Doc(ListMap(name -> -\/(rbase)))))))
        }
      case (_, SpliceBuilderF(_, _)) => delegate

      case (ExprBuilderF(src, expr), _) =>
        merge(src, right).flatMap { case (lbase, rbase, wb) =>
          mergeContents(Expr(rewriteExprPrefix(expr, lbase)), Expr(-\/(rbase))).map {
            case ((lbase, rbase), cont) =>
              (lbase, rbase,
                cont match {
                  case Expr(expr) => ExprBuilder(wb, expr)
                  case Doc(doc)   => DocBuilder(wb, doc)
                })
          }
        }
      case (_, ExprBuilderF(src, _)) => delegate

      case (DocBuilderF(src1, shape1), _) =>
        merge(src1, right).flatMap { case (lbase, rbase, wb) =>
          mergeContents(Doc(rewriteDocPrefix(shape1, lbase)), Expr(-\/(rbase))).map {
            case ((lbase, rbase), cont) =>
              (lbase, rbase,
                cont match {
                  case Expr(expr) => ExprBuilder(wb, expr)
                  case Doc(doc)   => DocBuilder(wb, doc)
                })
          }
        }
      case (_, DocBuilderF(_, _)) => delegate

      case (
        FlatteningBuilderF(src0, typ0, field0),
        FlatteningBuilderF(src1, typ1, field1))
          if typ0 == typ1 =>
        merge(src0, src1).map { case (lbase, rbase, wb) =>
          val lfield = lbase \\ field0
          val rfield = rbase \\ field1
          if (lfield == rfield)
            (lbase, rbase, FlatteningBuilder(wb, typ0, lfield))
          else (lbase, rbase, FlatteningBuilder(FlatteningBuilder(wb, typ0, lfield), typ1, rfield))
        }
      case (FlatteningBuilderF(src, typ, field), _) =>
        merge(src, right).flatMap { case (lbase, rbase, wb) =>
          val lfield = lbase \\ field
          if (lfield.startsWith(rbase) || rbase.startsWith(lfield))
            for {
              lName <- emitSt(freshName)
              rName <- emitSt(freshName)
            } yield
              (DocField(lName), DocField(rName),
                FlatteningBuilder(
                  DocBuilder(wb, ListMap(
                    lName -> -\/(lbase),
                    rName -> -\/(rbase))),
                  typ,
                  DocField(lName) \\ field))
          else emit((lbase, rbase, FlatteningBuilder(wb, typ, lfield)))
        }
      case (_, FlatteningBuilderF(_, _, _)) => delegate

      case (
        spb1 @ ShapePreservingBuilderF(src1, inputs1, op1),
        spb2 @ ShapePreservingBuilderF(src2, inputs2, _))
          if inputs1 == inputs2 && ShapePreservingBuilder.dummyOp(spb1) == ShapePreservingBuilder.dummyOp(spb2) =>
        merge(src1, src2).map { case (lbase, rbase, wb) =>
          (lbase, rbase, ShapePreservingBuilder(wb, inputs1, op1))
        }
      case (ShapePreservingBuilderF(src, inputs, op), _) =>
        merge(src, right).map { case (lbase, rbase, wb) =>
          (lbase, rbase, ShapePreservingBuilder(wb, inputs, op))
        }
      case (_, ShapePreservingBuilderF(src, inputs, op)) => delegate

      case (GroupBuilderF(src1, key1, cont1, id1), GroupBuilderF(src2, key2, cont2, id2))
          if id1 == id2 =>
        merge(src1, src2).flatMap { case (lbase, rbase, wb) =>
          mergeContents(rewriteGroupRefs(cont1)(prefixBase(lbase)), rewriteGroupRefs(cont2)(prefixBase(rbase))).map {
            case ((lb, rb), contents) =>
              (lb, rb, GroupBuilder(wb, key1, contents, id1))
          }
        }

      case (ArrayBuilderF(src, shape), _) =>
        merge(src, right).flatMap { case (lbase, rbase, wb) =>
          workflow(ArrayBuilder(wb, shape.map(rewriteExprPrefix(_, lbase)))).flatMap { case (wf, base) =>
            wf.unFix match {
              case $Project(src, Reshape(shape), idx) =>
                emitSt(freshName.map(rName =>
                  (lbase, DocField(rName),
                    CollectionBuilder(
                      chain(src,
                        $project(Reshape(shape + (rName -> -\/(rbase))))),
                      DocVar.ROOT(),
                      None))))
              case _ => fail(PlannerError.InternalError("couldn’t merge array"))
            }
          }
        }
      case (_, ArrayBuilderF(_, _)) => delegate

      case _ =>
        fail(PlannerError.InternalError("failed to merge:\n" + left.show + "\n" + right.show))
    }
  }

  def read(coll: Collection) =
    CollectionBuilder($read(coll), DocVar.ROOT(), None)
  def pure(bson: Bson) = ValueBuilder(bson)

  implicit def WorkflowBuilderRenderTree(implicit RO: RenderTree[Workflow], RE: RenderTree[ExprOp], REx: RenderTree[Expr], RC: RenderTree[GroupContents], RCE: RenderTree[Contents[Expr]]): RenderTree[WorkflowBuilder] = new RenderTree[WorkflowBuilder] {
    def render(v: WorkflowBuilder) = v.unFix match {
      case CollectionBuilderF(graph, base, struct) =>
        NonTerminal("",
          RO.render(graph) ::
            RE.render(base) ::
            Terminal(struct.toString, "CollectionBuilder" :: "Schema" :: Nil) ::
            Nil,
          "CollectionBuilder" :: Nil)
      case spb @ ShapePreservingBuilderF(src, inputs, op) =>
        NonTerminal("",
          render(src) ::
            (inputs.map(render) :+
              Terminal(ShapePreservingBuilder.dummyOp(spb).toString, "ShapePreservingBuilder" :: "Op" :: Nil)),
          "ShapePreservingBuilder" :: Nil)
      case ValueBuilderF(value) =>
        Terminal(value.toString, "ValueBuilder" :: Nil)
      case ExprBuilderF(src, expr) =>
        NonTerminal("",
          render(src) :: REx.render(expr) :: Nil,
          List("ExprBuilder"))
      case DocBuilderF(src, shape) =>
        NonTerminal("",
          render(src) ::
            NonTerminal("",
              shape.toList.map { case (name, expr) => REx.render(expr).relabel(name.asText + " -> " + _) },
              List("DocBuilder", "Shape")) ::
            Nil,
          List("DocBuilder"))
      case ArrayBuilderF(src, shape) =>
        NonTerminal("",
          render(src) ::
            NonTerminal("", shape.map(REx.render), List("ArrayBuilder", "Shape")) ::
            Nil,
          List("ArrayBuilder"))
      case GroupBuilderF(src, keys, content, id) =>
        NonTerminal("",
          render(src) ::
            NonTerminal("", keys.map(render), List("GroupBuilder", "By")) ::
            RC.render(content).copy(nodeType = "GroupBuilder" :: "Content" :: Nil) ::
            Terminal(id.toString, "GroupBuilder" :: "Id" :: Nil) ::
            Nil,
          "GroupBuilder" :: Nil)
      case FlatteningBuilderF(src, typ, field) =>
        NonTerminal("",
          render(src) ::
            Terminal(typ.toString, "FlatteningBuilder" :: "Type" :: Nil) ::
            RE.render(field) ::
            Nil,
          List("FlatteningBuilder"))
      case SpliceBuilderF(src, structure) =>
        NonTerminal("",
          render(src) :: structure.map(RCE.render(_)),
          List("SpliceBuilder"))
    }
  }
}
