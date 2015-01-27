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
  case object CannotObjectConcatExpr extends WorkflowBuilderError {
    def message = "Cannot object concat an expression"
  }
  case object InvalidSortBy extends WorkflowBuilderError {
    def message = "The sort by set has an invalid structure"
  }
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

  type Expr = ExprOp \/ JsMacro

  case class CollectionBuilderF(
    graph: Workflow,
    base: DocVar,
    struct: SchemaChange) extends WorkflowBuilderF[Nothing]
  object CollectionBuilder {
    def apply(graph: Workflow, base: DocVar, struct: SchemaChange) =
      Term[WorkflowBuilderF](new CollectionBuilderF(graph, base, struct))
  }
  case class ShapePreservingBuilderF[A](
    src: A,
    inputs: List[A],
    op: PartialFunction[List[BsonField], WorkflowOp])
      extends WorkflowBuilderF[A]
  object ShapePreservingBuilder {
    def apply(
      src: WorkflowBuilder,
      inputs: List[WorkflowBuilder],
      op: PartialFunction[List[BsonField], WorkflowOp]) =
      Term[WorkflowBuilderF](new ShapePreservingBuilderF(src, inputs, op))

    def dummyOp(builder: ShapePreservingBuilderF[WorkflowBuilder]) =
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

  case class GroupBuilderF[A](
    src: A,
    key: ExprOp \/ Reshape,
    contents: GroupContents,
    id: GroupId)
      extends WorkflowBuilderF[A]
  object GroupBuilder {
    def apply(
      src: WorkflowBuilder,
      key: ExprOp \/ Reshape,
      contents: GroupContents,
      id: GroupId) =
      Term[WorkflowBuilderF](new GroupBuilderF(src, key, contents, id))
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

  case class GroupId(srcs: List[WorkflowBuilder]) {
    override def toString = hashCode.toHexString
  }

  private def rewriteObjRefs(
    obj: ListMap[BsonField.Name, GroupValue])(
    f: PartialFunction[DocVar, DocVar]) =
    obj ∘ (_.fold(
      expr => -\/(expr.rewriteRefs(f)),
      _.rewriteRefs(f) match {
        case g: GroupOp => \/-(g)
        case _          => sys.error("Transformation changed the type – error!")
      }))

  private def rewriteGroupRefs(
    contents: GroupContents)(
    f: PartialFunction[DocVar, DocVar]) =
    contents match {
      case Expr(expr) => Expr(expr.bimap(
        _.rewriteRefs(f),
        _.rewriteRefs(f) match {
          case g: GroupOp => g
          case _ => sys.error("Transformation changed the type -- error!")
        }))
      case Doc(doc) => Doc(rewriteObjRefs(doc)(f))
    }

  private def rewriteDocPrefix(doc: ListMap[BsonField.Name, Expr], base: DocVar) =
    doc ∘ (rewriteExprPrefix(_, base))

  private def rewriteExprPrefix(expr: Expr, base: DocVar): Expr =
    expr.bimap(
      _.rewriteRefs(prefixBase(base)),
      js => base.toJs >>> js)

  def chainBuilder(wb: WorkflowBuilder, base: DocVar, struct: SchemaChange)(f: WorkflowOp) =
    toCollectionBuilder(wb).map {
      case CollectionBuilderF(g, b, _) =>
        CollectionBuilder(
          Term[WorkflowF](Workflow.rewriteRefs(f(g).unFix, prefixBase(b))),
          base,
          struct)
    }

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

  private def commonShape(shape: ListMap[BsonField.Name, Expr]) = commonMap(shape)(ExprOp.toJs)

  private def toCollectionBuilder(wb: WorkflowBuilder): M[CollectionBuilderF] =
    wb.unFix match {
      case cb @ CollectionBuilderF(_, _, _) => emit(cb)
      case ValueBuilderF(value) =>
        emit(CollectionBuilderF($pure(value), DocVar.ROOT(), SchemaChange.Init))
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
                  fail[CollectionBuilderF](WorkflowBuilderError.InvalidOperation("filter", "can’t filter without field")))(
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
                            srcStruct.shift(base0 \\ base).map(t =>
                              chain(g, Workflow.$project(t._1, t._2))).getOrElse(g),
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
            CollectionBuilderF(graph, base \\ d, SchemaChange.Init)
        }
      case ExprBuilderF(src, expr) => for {
        cb <- toCollectionBuilder(src)
        name <- emitSt(freshName)
      } yield cb match {
        case CollectionBuilderF(graph, base, _) =>
          CollectionBuilderF(
            chain(graph,
              rewriteExprPrefix(expr, base).fold(
                op => $project(Reshape.Doc(ListMap(name -> -\/(op)))),
                js => $simpleMap(JsMacro(x => JsCore.Obj(ListMap(name.asText -> js(x))).fix)))),
            DocField(name),
            SchemaChange.Init)
      }
      case DocBuilderF(src, shape) =>
        workflow(src).flatMap { case (wf, base) =>
          commonShape(rewriteDocPrefix(shape, base)).fold(
            fail(_),
            s => emit(CollectionBuilderF(
              chain(wf,
                s.fold(
                  exprOps => $project(Reshape.Doc(exprOps ∘ \/.left)),
                  jsExprs => $simpleMap(JsMacro(x =>
                    Term(JsCore.Obj(jsExprs.map {
                      case (name, expr) => name.asText -> expr(x)
                    })))))),
              DocVar.ROOT(),
              SchemaChange.MakeObject(shape.map {
                case (k, _) => k.asText -> SchemaChange.Init
              }))))
        }
      case GroupBuilderF(src, _, Expr(-\/(expr)), _) =>
        // NB: This case just winds up a single value, then unwinds it. It’s
        //     effectively a no-op, so we just use the src and expr
        toCollectionBuilder(ExprBuilder(src, -\/(expr)))
      case GroupBuilderF(src, key, Expr(\/-(grouped)), _) =>
        for {
          cb <- toCollectionBuilder(src)
          rootName <- emitSt(freshName)
        } yield cb match {
          case CollectionBuilderF(wf, base, struct) =>
            CollectionBuilderF(
              chain(wf,
                $group(Grouped(ListMap(rootName -> grouped)).rewriteRefs(prefixBase(base)),
                  key.bimap(_.rewriteRefs(prefixBase(base)), _.rewriteRefs(prefixBase(base))))),
              DocField(rootName),
              struct)
        }
      case GroupBuilderF(src, key, Doc(obj), _) =>
        val (ungrouped, grouped) =
          obj.foldLeft[(ListMap[BsonField.Name, ExprOp], ListMap[BsonField.Leaf, GroupOp])]((ListMap.empty[BsonField.Name, ExprOp], ListMap.empty[BsonField.Leaf, GroupOp]))((acc, item) =>
            item match {
              case (k, -\/(v)) =>
                ((x: ListMap[BsonField.Name, ExprOp]) => x + (k -> v)).first(acc)
              case (k, \/-(v)) =>
                ((x: ListMap[BsonField.Leaf, GroupOp]) => x + (k -> v)).second(acc)
            })

        workflow(src).flatMap {
          case (wf, base) =>
            emitSt(ungrouped.size match {
              case 0 =>
                state[NameGen, Workflow](chain(wf,
                  $group(Grouped(grouped).rewriteRefs(prefixBase(base)),
                    key.bimap(_.rewriteRefs(prefixBase(base)), _.rewriteRefs(prefixBase(base))))))
              case 1 =>
                state[NameGen, Workflow](chain(wf,
                  $group(Grouped(
                    obj.transform {
                      case (_, -\/ (v)) => Push(v.rewriteRefs(prefixBase(base)))
                      case (_,  \/-(v)) => v.rewriteRefs(prefixBase(base))
                    }),
                    key.bimap(_.rewriteRefs(prefixBase(base)), _.rewriteRefs(prefixBase(base)))),
                  $unwind(DocField(ungrouped.head._1))))
              case _ => for {
                ungroupedName <- freshName
                groupedName <- freshName
              } yield
                chain(wf,
                  $project(Reshape.Doc(ListMap(
                    ungroupedName -> \/-(Reshape.Doc(ungrouped.map {
                      case (k, v) => k -> -\/(v.rewriteRefs(prefixBase(base)))
                    })),
                    groupedName -> -\/(DocVar.ROOT())))),
                  $group(Grouped(
                    (grouped ∘ (_.rewriteRefs(prefixBase(DocField(groupedName))))) +
                      (ungroupedName -> Push(DocField(ungroupedName)))),
                    key.bimap(_.rewriteRefs(prefixBase(DocField(groupedName))), _.rewriteRefs(prefixBase(DocField(groupedName))))),
                  $unwind(DocField(ungroupedName)),
                  $project(Reshape.Doc(obj.transform {
                    case (k, -\/(_)) => -\/(DocField(ungroupedName \ k))
                    case (k, \/-(_)) => -\/(DocField(k))
                  })))
            }).map(CollectionBuilderF(
              _,
              DocVar.ROOT(),
              SchemaChange.MakeObject(obj.map {
                case (k, _) => (k.asText -> SchemaChange.Init)
              })))
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
            val field = base.toJs(JsCore.Ident("value").fix)
            CollectionBuilderF(
              chain(graph,
                $simpleFlatMap(Predef.identity, JsMacro(base.toJs(_)))),
              base,
              struct)
        }
    }

  def workflow(wb: WorkflowBuilder): M[(Workflow, DocVar)] =
    toCollectionBuilder(wb).map(x => (x.graph, x.base))

  def build(wb: WorkflowBuilder): M[Workflow] =
    toCollectionBuilder(wb).flatMap {
      case CollectionBuilderF(graph, base, struct) =>
        base match {
          case DocVar.ROOT(None) => emit(finish(graph))
          case base =>
            build(CollectionBuilder(
              struct.shift(base).map(t =>
                chain(graph, Workflow.$project(t._1, t._2))).getOrElse(graph),
              DocVar.ROOT(),
              SchemaChange.Init))
        }
    }

  private def $project(shape: Reshape): WorkflowOp =
    Workflow.$project(
      shape,
      shape.get(IdName).fold[IdHandling](IgnoreId)(Function.const(IncludeId)))

  def asLiteral(wb: WorkflowBuilder) =
    asExprOp(wb).collect { case (x @ Literal(_)) => x }


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

  def expr1(wb: WorkflowBuilder)(f: ExprOp => ExprOp):
      Error \/ WorkflowBuilder =
    wb.unFix match {
      case ShapePreservingBuilderF(src, inputs, op) =>
        expr1(src)(f).map(ShapePreservingBuilder(_, inputs, op))
      case GroupBuilderF(wb0, key, Expr(-\/(expr)), id) =>
        \/-(GroupBuilder(wb0, key, Expr(-\/(f(expr))), id))
      case ExprBuilderF(wb0, -\/ (expr1)) =>
        \/-(ExprBuilder(wb0, -\/(f(expr1))))
      case ExprBuilderF(wb0,  \/-(js1)) =>
        toJs(f(DocVar.ROOT())).map(js => ExprBuilder(wb0, \/-(js1 >>> js)))
      case _ => \/-(ExprBuilder(wb, -\/(f(DocVar.ROOT()))))
    }

  def expr2(wb1: WorkflowBuilder, wb2: WorkflowBuilder)(f: (ExprOp, ExprOp) => ExprOp):
      M[WorkflowBuilder] =
    (wb1.unFix, wb2.unFix) match {
      case (_, ValueBuilderF(bson)) => lift(expr1(wb1)(f(_, Literal(bson))))
      case (ValueBuilderF(bson), _) => lift(expr1(wb2)(f(Literal(bson), _)))
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

  def makeArray(wb: WorkflowBuilder): M[WorkflowBuilder] = wb.unFix match {
    case ValueBuilderF(value) => emit(ValueBuilder(Bson.Arr(List(value))))
    case _ =>
      toCollectionBuilder(wb).map {
        case CollectionBuilderF(graph, base, struct) =>
          CollectionBuilder(
            chain(graph,
              $project(Reshape.Arr(ListMap(BsonField.Index(0) -> -\/(base))))),
            DocVar.ROOT(),
            struct.makeArray(0))
      }
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

  // TODO: handle concating value, expr, or collection with group (#439)
  def objectConcat(wb1: WorkflowBuilder, wb2: WorkflowBuilder):
      M[WorkflowBuilder] = {
    def delegate = objectConcat(wb2, wb1)
    def mergeGroups(s1: WorkflowBuilder, s2: WorkflowBuilder, c1: GroupContents, c2: GroupContents, k1: ExprOp \/ Reshape, id1: GroupId):
        M[((DocVar, DocVar), WorkflowBuilder)] =
      merge(s1, s2).flatMap { case (lbase, rbase, src) =>
        val key = k1.bimap(_.rewriteRefs(prefixBase(lbase)), _.rewriteRefs(prefixBase(lbase)))
        mergeContents(
          rewriteGroupRefs(c1)(prefixBase(lbase)),
          rewriteGroupRefs(c2)(prefixBase(rbase))).map {
          case ((lb, rb), contents) =>
            (lb, rb) -> GroupBuilder(src, key, contents, id1)
        }
      }

    // TODO: change this to take Maps and only fail if overlapping keys have
    //       conflicting values
    def unlessConflicts[A, B](a: Set[A], b: Set[A], f: => M[B]): M[B] = {
      val keys = a intersect b
      if (keys.isEmpty)
        f
      else
        fail(WorkflowBuilderError.InvalidOperation(
          "objectConcat",
          "conflicting keys: " + keys))
    }

    def generalMerge =
      emitSt(freshId("arg")).flatMap { name =>
        def builderWithUnknowns(
          src: WorkflowBuilder,
          fields: List[Term[JsCore] => Js.Stmt]) =
          // TODO: replace with ExprBuilder using JsMacro
          chainBuilder(src, DocVar.ROOT(), SchemaChange.Init)(
            $map($Map.mapMap(name,
              Js.Call(
                Js.AnonFunDecl(List("rez"),
                  fields.map(_(JsCore.Ident("rez").fix)) :+ Js.Return(Js.Ident("rez"))),
                List(Js.AnonObjDecl(Nil))))))

        def side(wb: WorkflowBuilder, base: DocVar):
            Error \/ ((Term[JsCore] => Js.Stmt) \/ List[BsonField.Name])=
          wb.unFix match {
            case CollectionBuilderF(_, _, _) =>
              \/-(-\/($Reduce.copyAllFields(base.toJs(JsCore.Ident(name).fix))))
            case ValueBuilderF(Bson.Doc(map)) =>
              \/-(\/-(map.keys.map(BsonField.Name(_)).toList))
            case DocBuilderF(_, shape) => \/-(\/-(shape.keys.toList))
            // TODO: Restrict to DocVar, Literal, Let, Cond, and IfNull (see #471)
            case ExprBuilderF(_, _) =>
              \/-(-\/($Reduce.copyAllFields(base.toJs(JsCore.Ident(name).fix))))
            case _ =>
              -\/(WorkflowBuilderError.InvalidOperation(
                "objectConcat",
                "values are not both documents: " + wb))
          }

        merge(wb1, wb2).flatMap { case (left, right, list) =>
          lift((side(wb1, left) |@| side(wb2, right))((_, _) match {
            case (\/-(f1), \/-(f2)) =>
              unlessConflicts(f1.toSet, f2.toSet,
                emit(DocBuilder(list,
                  (f1.map(k => k -> -\/(left \ k)) ++
                    f2.map(k => k -> -\/(right \ k))).toListMap)))
            case (\/-(_), -\/(_)) => delegate
            case (-\/(f1), \/-(f2)) =>
              builderWithUnknowns(
                list,
                List(f1) ++
                  f2.map(k =>
                    (t: Term[JsCore]) => $Reduce.copyOneField(
                      JsMacro(JsCore.Select(_, k.asText).fix),
                      (right \ k).toJs(JsCore.Ident(name).fix))(t)))
            case (-\/(f1), -\/(f2)) =>
              builderWithUnknowns(list, List(f1, f2))
          })).join
        }
      }

    (wb1.unFix, wb2.unFix) match {
      case (ShapePreservingBuilderF(s1, i1, o1), ShapePreservingBuilderF(s2, i2, o2))
          if i1 == i2 && o1 == o2 =>
        objectConcat(s1, s2).map(ShapePreservingBuilder(_, i1, o1))

      case (ShapePreservingBuilderF(s, i, o), _) =>
        objectConcat(s, wb2).map(ShapePreservingBuilder(_, i, o))
      case (_, ShapePreservingBuilderF(_, _, _)) => delegate

      case (ValueBuilderF(Bson.Doc(map1)), ValueBuilderF(Bson.Doc(map2))) =>
        emit(ValueBuilder(Bson.Doc(map1 ++ map2)))

      case (ValueBuilderF(Bson.Doc(map1)), DocBuilderF(s2, shape2)) =>
        unlessConflicts(map1.keySet.map(BsonField.Name(_)), shape2.keySet,
          emit(DocBuilder(s2,
            map1.map { case (k, v) => BsonField.Name(k) -> -\/(Literal(v)) } ++
              shape2)))
      case (DocBuilderF(_, _), ValueBuilderF(Bson.Doc(_))) => delegate

      case (ValueBuilderF(Bson.Doc(map1)), GroupBuilderF(s1, k1, Doc(c2), id2)) =>
        unlessConflicts(map1.keySet.map(BsonField.Name(_)), c2.keySet,
          emit(GroupBuilder(s1, k1, Doc(map1.map { case (k, v) => BsonField.Name(k) -> -\/(Literal(v)) } ++ c2), id2)))
      case (GroupBuilderF(_, _, Doc(_), _), ValueBuilderF(_)) => delegate

      case (
        GroupBuilderF(s1, k1, c1 @ Doc(_), id1),
        GroupBuilderF(s2, k2, c2 @ Doc(_), id2))
          if id1 == id2 =>
        mergeGroups(s1, s2, c1, c2, k1, id1).map(_._2)

      case (
        GroupBuilderF(s1, k1, c1 @ Doc(d1), id1),
        DocBuilderF(Term(GroupBuilderF(s2, k2, c2, id2)), shape2))
          if id1 == id2 =>
        mergeGroups(s1, s2, c1, c2, k1, id1).map { case ((glbase, grbase), g) =>
          val shape = d1.transform { case (n, _) => -\/(DocField(n)) } ++
          (shape2 ∘ (rewriteExprPrefix(_, grbase)))
          DocBuilder(g, shape)
        }
      case (
        DocBuilderF(Term(GroupBuilderF(_, k1, _, id1)), _),
        GroupBuilderF(_, k2, Doc(_), id2))
          if id1 == id2 =>
        delegate

      case (
        DocBuilderF(Term(GroupBuilderF(s1, k1, c1, id1)), shape1),
        DocBuilderF(Term(GroupBuilderF(s2, k2, c2, id2)), shape2))
          if id1 == id2 =>
        unlessConflicts(shape1.keySet, shape2.keySet,
          mergeGroups(s1, s2, c1, c2, k1, id1).flatMap {
            case ((glbase, grbase), g) =>
              val shape = (shape1 ∘ (rewriteExprPrefix(_, glbase))) ++ (shape2 ∘ (rewriteExprPrefix(_, grbase)))
              emit(DocBuilder(g, shape))
          })

      case (
        DocBuilderF(s1, shape),
        GroupBuilderF(_, -\/(Literal(Bson.Null)), _, id2)) =>
        objectConcat(
          DocBuilder(GroupBuilder(s1, -\/(Literal(Bson.Null)), Expr(-\/(DocVar.ROOT())), id2), shape),
          wb2)
      case (
        GroupBuilderF(_, -\/(Literal(Bson.Null)), _, _),
        DocBuilderF(_, _)) =>
        delegate

      case (
        DocBuilderF(s1, shape),
        DocBuilderF(Term(GroupBuilderF(_, -\/(Literal(Bson.Null)), _, id2)), _)) =>
        objectConcat(
          DocBuilder(GroupBuilder(s1, -\/(Literal(Bson.Null)), Expr(-\/(DocVar.ROOT())), id2), shape),
          wb2)
      case (
        DocBuilderF(Term(GroupBuilderF(_, -\/(Literal(Bson.Null)), _, _)), _),
        DocBuilderF(_, _)) =>
        delegate
        
      case (DocBuilderF(s1, shape1), DocBuilderF(s2, shape2)) =>
        unlessConflicts(shape1.keySet, shape2.keySet,
          merge(s1, s2).map { case (lbase, rbase, src) =>
            DocBuilder(src, rewriteDocPrefix(shape1, lbase) ++ rewriteDocPrefix(shape2, rbase))
          })

      // NB: JS-exprs cannot be rolled into $group ops, leading to this somewhat exceptional case
      case (
        GroupBuilderF(s1, k1, c1 @ Doc(d1), id1),
        GroupBuilderF(Term(
          ExprBuilderF(Term(GroupBuilderF(s2, k2, c2, id2)), \/-(expr2))),
          -\/(Literal(Bson.Null)),
          Doc(c2a), id2a))
          if id1 == id2 =>
        mergeGroups(s1, s2, c1, c2, k1, id1).flatMap { case ((glbase, grbase), g) =>
          emitSt(for {
            rName <- freshName
          } yield {
            val doc = DocBuilder(g,
              d1.transform { case (n, _) => -\/(DocField(n)) } +
                (rName -> \/-(grbase.toJs >>> expr2)))
            GroupBuilder(doc, -\/(Literal(Bson.Null)),
              Doc(d1.transform { case (n, _) => -\/(DocField(n)) } ++
                rewriteObjRefs(c2a)(prefixBase(DocField(rName)))),
              id2a)
          })
        }
      case (
        GroupBuilderF(Term(ExprBuilderF(Term(GroupBuilderF(_, _, _, id1)), \/-(_))), -\/(Literal(Bson.Null)), Doc(_), _),
        GroupBuilderF(_, _, Doc(_), id2))
          if id1 == id2 =>
        delegate

      // NB: these cases come up when wildcards are used, and require a more general
      // approach, which we don't know to be correct and efficient in all cases.
      case (CollectionBuilderF(_, _, _), _) => generalMerge
      case (_, CollectionBuilderF(_, _, _)) => generalMerge
      case (ExprBuilderF(_, _), ExprBuilderF(_, _)) => generalMerge

      case _ => fail(WorkflowBuilderError.InvalidOperation(
        "objectConcat",
        "unrecognized shapes:\n" + wb1.show + "\n" + wb2.show))
    }
  }

  def arrayConcat(left: WorkflowBuilder, right: WorkflowBuilder):
      M[WorkflowBuilder] = {
    def convert(base: DocVar, cb: CollectionBuilderF, shift: Int) =
      cb.struct.simplify match {
        case SchemaChange.MakeArray(m) =>
          \/-((
            (m.keys.toSeq.map { index =>
              BsonField.Index(index + shift) -> -\/(base \ BsonField.Index(index))
            }),
            if (shift == 0) m else m.map(t => (t._1 + shift) -> t._2),
            m.keys.max + 1 + shift))
        case _ => -\/(WorkflowBuilderError.CannotObjectConcatExpr)
      }

    (left.unFix, right.unFix) match {
      case (
        cb1 @ CollectionBuilderF(_, _, _),
        cb2 @ CollectionBuilderF(_, _, _)) =>
        merge(left, right).flatMap { case (left, right, list) =>
          for {
            l <- lift(convert(left, cb1, 0))
            (reshape1, array1, shift) = l
            r <- lift(convert(right, cb2, shift))
            (reshape2, array2, _) = r
            rez <- chainBuilder(
              list, DocVar.ROOT(), SchemaChange.MakeArray(array1 ++ array2))(
              $project(Reshape.Arr(ListMap((reshape1 ++ reshape2): _*))))
          } yield rez
        }
      case (ValueBuilderF(Bson.Arr(seq1)), ValueBuilderF(Bson.Arr(seq2))) =>
        emit(ValueBuilder(Bson.Arr(seq1 ++ seq2)))
      case (cb @ CollectionBuilderF(src, base, _), ValueBuilderF(Bson.Arr(seq))) =>
        lift(for {
          l <- convert(base, cb, 0)
          (reshape, array, shift) = l
        } yield CollectionBuilder(
          chain(src,
            $project(Reshape.Arr(ListMap((reshape ++ seq.zipWithIndex.map {
              case (item, index) =>
                (BsonField.Index(index + shift) -> -\/(Literal(item)))
            }): _*)))),
          DocVar.ROOT(),
          SchemaChange.MakeArray(array ++ seq.zipWithIndex.map {
            case (item, index) =>
              // TODO: make the schema for the values more specific (#437)
              (index + shift -> SchemaChange.Init)
          })))
      case (ValueBuilderF(Bson.Arr(seq)), cb @ CollectionBuilderF(src, base, _)) =>
        lift(for {
          r <- convert(base, cb, seq.length)
          (reshape, array, _) = r
        } yield CollectionBuilder(
          chain(src,
            $project(Reshape.Arr(ListMap((seq.zipWithIndex.map {
              case (item, index) =>
                (BsonField.Index(index) -> -\/(Literal(item)))
            } ++ reshape): _*)))),
          DocVar.ROOT(),
          SchemaChange.MakeArray(seq.zipWithIndex.map {
            case (item, index) =>
              // TODO: make the schema for the values more specific (#437)
              (index -> SchemaChange.Init)
          }.toListMap ++ array)))
      case _ =>
        fail(WorkflowBuilderError.InvalidOperation(
          "arrayConcat",
          "values are not both arrays"))
    }
  }

  def flattenObject(wb: WorkflowBuilder): WorkflowBuilder = wb.unFix match {
    case ShapePreservingBuilderF(src, inputs, op) =>
      ShapePreservingBuilder(flattenObject(src), inputs, op)
    case _ => FlatteningBuilder(wb, Object, DocVar.ROOT())
  }

  def flattenArray(wb: WorkflowBuilder): WorkflowBuilder = wb.unFix match {
    case ShapePreservingBuilderF(src, inputs, op) =>
      ShapePreservingBuilder(flattenArray(src), inputs, op)
    case _ => FlatteningBuilder(wb, Array, DocVar.ROOT())
  }

  def projectField(wb: WorkflowBuilder, name: String):
      Error \/ WorkflowBuilder =
    wb.unFix match {
      case ShapePreservingBuilderF(src, inputs, op) =>
        projectField(src, name).map(ShapePreservingBuilder(_, inputs, op))
      case CollectionBuilderF(graph, base, SchemaChange.MakeArray(_)) =>
        -\/(WorkflowBuilderError.InvalidOperation(
          "projectField",
          "can not project a field from an array."))
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
      case ValueBuilderF(_) =>
        fail(WorkflowBuilderError.InvalidOperation(
          "projectIndex",
          "value is not an array."))
      case DocBuilderF(_, _) =>
        fail(WorkflowBuilderError.InvalidOperation(
          "projectIndex",
          "value is not an array."))
      case ExprBuilderF(wb0, expr) =>
        lift(expr.fold(ExprOp.toJs, \/-(_)).map(js =>
          ExprBuilder(wb0,
            \/-(JsMacro(base =>
              JsCore.Access(js(base),
                JsCore.Literal(Js.Num(index, false)).fix).fix)))))
      case _ =>
        emit(ExprBuilder(wb,
          \/-(JsMacro(base =>
            JsCore.Access(base,
              JsCore.Literal(Js.Num(index, false)).fix).fix))))
    }

  def groupBy(src: WorkflowBuilder, keys: List[WorkflowBuilder]):
      M[WorkflowBuilder] =
    foldBuilders(src, keys).map { case (wb, base, fields) =>
      GroupBuilder(
        wb,
        keys match {
          case Nil        => -\/(Literal(Bson.Null))
          case key :: Nil => -\/(key.unFix match {
            // NB: normalize to Null, to ease merging
            case ValueBuilderF(_)                 => Literal(Bson.Null)
            case ExprBuilderF(_, -\/(Literal(_))) => Literal(Bson.Null)
            case _                                => fields.head
          })
          case _          => \/-(Reshape.Arr(fields.zipWithIndex.map {
            case (field, index) =>
              BsonField.Index(index) -> -\/(field)
          }.toListMap))
        },
        Expr(-\/(base)),
        GroupId(wb :: keys))
    }

  def reduce(wb: WorkflowBuilder)(f: ExprOp => GroupOp): WorkflowBuilder =
    wb.unFix match {
      case GroupBuilderF(wb0, key, Expr(-\/(expr)), id) =>
        GroupBuilder(wb0, key, Expr(\/-(f(expr))), id)
      case ShapePreservingBuilderF(src @ Term(GroupBuilderF(_, _, Expr(-\/(_)), _)), inputs, op) =>
        ShapePreservingBuilder(reduce(src)(f), inputs, op)
      case _ =>
        // NB: the group must be identified with the source collection, not an
        // expression/doc built on it. This is sufficient in the known cases,
        // but we might need to dig for an actual CollectionBuilder to be safe.
        val id = wb.unFix match {
          case ExprBuilderF(wb0, _) => GroupId(List(wb0))
          case DocBuilderF(wb0, _) => GroupId(List(wb0))
          case _ => GroupId(List(wb))
        }
        GroupBuilder(wb, -\/(Literal(Bson.Null)), Expr(\/-(f(DocVar.ROOT()))), id)
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
    leftKey: ExprOp, rightKey: JsMacro):
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
        $project(Reshape.Doc(ListMap(
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
        (workflow(left) |@| workflow(right)) {
          case ((l, lbase), (r, rbase)) =>
            CollectionBuilder(
              chain(
                $foldLeft(
                  chain(
                    l,
                    $group(
                      Grouped(ListMap(leftField -> Push(lbase))), -\/(leftKey.rewriteRefs(prefixBase(lbase)))),
                    Workflow.$project(
                      Reshape.Doc(ListMap(
                        leftField -> -\/(DocField(leftField)),
                        rightField -> -\/(Literal(Bson.Arr(Nil))))),
                      IncludeId)),
                  chain(r,
                    $map(rightMap(rightKey)),
                    $reduce(rightReduce))),
                buildJoin(_, tpe),
                $unwind(DocField(leftField)),
                $unwind(DocField(rightField))),
              DocVar.ROOT(),
              SchemaChange.Init)
        }
      case _ => fail(WorkflowBuilderError.UnsupportedJoinCondition(comp))
    }
  }

  def cross(left: WorkflowBuilder, right: WorkflowBuilder) =
    join(left, right,
      slamdata.engine.LogicalPlan.JoinType.Inner, relations.Eq,
      Literal(Bson.Null), JsMacro(Function.const(JsCore.Literal(Js.Null).fix)))

  def limit(wb: WorkflowBuilder, count: Long) =
    ShapePreservingBuilder(wb, Nil, { case Nil => $limit(count) })

  def skip(wb: WorkflowBuilder, count: Long) =
    ShapePreservingBuilder(wb, Nil, { case Nil => $skip(count) })

  def squash(wb: WorkflowBuilder): WorkflowBuilder = wb

  def distinctBy(src: WorkflowBuilder, keys: List[WorkflowBuilder]):
      M[WorkflowBuilder] = {
    def sortKeys(op: WorkflowBuilder): M[List[(BsonField, SortType)]] = {
      object HavingRoot {
        def unapply(shape: Reshape): Option[BsonField] = shape match {
          case Reshape.Doc(map) => map.collect {
            case (name, -\/(op)) if op == DocVar.ROOT() => name
          }.headOption
          case Reshape.Arr(map) => map.collect {
            case (index, -\/(op)) if op == DocVar.ROOT() => index
          }.headOption
        }
      }

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
          case $Project(Term($Sort(_, keys)), HavingRoot(root), _) =>
            \/-(keys.list.map {
              case (field, sortType) => (root \ field) -> sortType
            })
          case $Project(Term($Sort(_, keys)), Reshape.Doc(shape), _) =>
            keys.list.map {
              case (field, sortType) =>
                shape.find {
                  case (_, -\/(DocField(v))) => v == field
                  case _                     => false
                }.map(_._1 -> sortType)
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
        def findKeys(wb: WorkflowBuilder): Error \/ (ExprOp \/ Reshape) =
          wb.unFix match {
            case CollectionBuilderF(g2, b2, s2) =>
              s2.simplify match {
                case SchemaChange.MakeObject(byFields) =>
                  \/-(\/-(Reshape.Arr(
                    byFields.keys.toList.zipWithIndex.map { case (name, index) =>
                      BsonField.Index(index) -> -\/(DocField(BsonField.Name(name)))
                    }.toListMap)))
                case SchemaChange.MakeArray(byFields) =>
                  \/-(\/-(Reshape.Arr(
                    byFields.keys.toList.map { index =>
                      BsonField.Index(index) -> -\/(DocField(BsonField.Index(index)))
                    }.toListMap)))
                case _ =>
                  -\/(WorkflowBuilderError.UnsupportedDistinct("Cannot distinct with unknown shape (" + s2 + ")"))
              }
            case DocBuilderF(ksrc, shape) =>
              \/-(\/-(Reshape.Arr(ListMap(shape.keys.toList.zipWithIndex.map { case (name, index) => BsonField.Index(index) -> -\/(DocField(name)) }: _*))))
            case GroupBuilderF(_, _, Doc(obj), _) =>
              \/-(\/-(Reshape.Arr(ListMap(obj.keys.toList.zipWithIndex.map { case (name, index) => BsonField.Index(index) -> -\/(DocField(name)) }: _*))))
            case ShapePreservingBuilderF(src, _, _) => findKeys(src)
            case ExprBuilderF(_, _) => \/-(-\/(DocVar.ROOT()))
            case _ => -\/(WorkflowBuilderError.UnsupportedDistinct("Cannot distinct with unknown shape (" + keys.head + ")"))
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
          case _          =>  \/-(\/-(Reshape.Arr(fields.zipWithIndex.map {
            case (field, index) =>
              BsonField.Index(index) -> -\/(field)
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

  def asExprOp(wb: WorkflowBuilder): Option[ExprOp] = wb.unFix match {
    case ValueBuilderF(value)         => Some(Literal(value))
    case ExprBuilderF(src, -\/(expr)) => Some(expr)
    case _                            => None
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

      case (_, ExprBuilderF(src, expr)) if left == src =>
        for {
          lName <- emitSt(freshName)
          rName <- emitSt(freshName)
        } yield
          (DocField(lName), DocField(rName),
            DocBuilder(src, ListMap(
              lName -> -\/(DocVar.ROOT()),
              rName -> expr)))
      case (ExprBuilderF(src, _), _) if src == right => delegate

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
        mib1 @ ShapePreservingBuilderF(src1, inputs1, op1),
        mib2 @ ShapePreservingBuilderF(src2, inputs2, _))
          if inputs1 == inputs2 && ShapePreservingBuilder.dummyOp(mib1) == ShapePreservingBuilder.dummyOp(mib2) =>
        merge(src1, src2).map { case (lbase, rbase, wb) =>
          (lbase, rbase, ShapePreservingBuilder(wb, inputs1, op1))
        }
      case (ShapePreservingBuilderF(src, inputs, op), _) =>
        merge(src, right).map { case (lbase, rbase, wb) =>
          (lbase, rbase, ShapePreservingBuilder(wb, inputs, op))
        }
      case (_, ShapePreservingBuilderF(src, inputs, op)) => delegate

      case (GroupBuilderF(src1, key1, cont1, id1), GroupBuilderF(src2, key2, cont2, id2))
          if src1 == src2 && id1 == id2 =>
        mergeContents(cont1, cont2).map {
          case ((lb, rb), contents) =>
            (lb, rb, GroupBuilder(src1, key1, contents, id1))
        }
      case _ =>
        (toCollectionBuilder(left) |@| toCollectionBuilder(right))((_, _) match {
          case (
            CollectionBuilderF(graph1, base1, struct1),
            CollectionBuilderF(graph2, base2, struct2)) =>
            emitSt(Workflow.merge(graph1, graph2).map { case ((lbase, rbase), op) =>
              (lbase \\ base1, rbase \\ base2,
                CollectionBuilder(
                  op,
                  DocVar.ROOT(),
                  if (struct1 == struct2) struct1 else SchemaChange.Init))
            })
        }).join
    }
  }

  def read(coll: Collection) =
    CollectionBuilder($read(coll), DocVar.ROOT(), SchemaChange.Init)
  def pure(bson: Bson) = ValueBuilder(bson)

  implicit def WorkflowBuilderRenderTree(implicit RO: RenderTree[Workflow], RE: RenderTree[ExprOp], RC: RenderTree[GroupContents]): RenderTree[WorkflowBuilder] = new RenderTree[WorkflowBuilder] {
    def renderExpr(x: Expr) =
      x.fold(
        op => Terminal(op.toString, List("ExprBuilder", "ExprOp")),
        js => Terminal(js(JsCore.Ident("_").fix).toJs.render(0), List("ExprBuilder", "Js")))

    def render(v: WorkflowBuilder) = v.unFix match {
      case CollectionBuilderF(graph, base, struct) =>
        NonTerminal("",
          RO.render(graph) ::
            RE.render(base) ::
            Terminal(struct.toString, "CollectionBuilder" :: "SchemaChange" :: Nil) ::
            Nil,
          "CollectionBuilder" :: Nil)
      case mib @ ShapePreservingBuilderF(src, inputs, op) =>
        NonTerminal("",
          render(src) ::
            (inputs.map(render) :+
              Terminal(ShapePreservingBuilder.dummyOp(mib).toString, "ShapePreservingBuilder" :: "Op" :: Nil)),
          "ShapePreservingBuilder" :: Nil)
      case ValueBuilderF(value) =>
        Terminal(value.toString, "ValueBuilder" :: Nil)
      case ExprBuilderF(src, expr) =>
        NonTerminal("",
          render(src) :: renderExpr(expr) :: Nil,
          List("ExprBuilder"))
      case DocBuilderF(src, shape) =>
        NonTerminal("",
          render(src) ::
            NonTerminal("",
              shape.toList.map { case (name, expr) => renderExpr(expr).relabel(name.asText + " -> " + _) },
              List("DocBuilder", "Shape")) ::
            Nil,
          List("DocBuilder"))
      case GroupBuilderF(src, key, content, id) =>
        NonTerminal("",
          render(src) ::
            Terminal(key.toString, "GroupBuilder" :: "By" :: Nil) ::
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
    }
  }
}
