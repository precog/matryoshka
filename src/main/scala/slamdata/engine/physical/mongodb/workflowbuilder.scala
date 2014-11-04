package slamdata.engine.physical.mongodb

import collection.immutable.ListMap

import slamdata.engine.fp._
import slamdata.engine.fs.Path
import slamdata.engine._
import Workflow._
import slamdata.engine.analysis.fixplate._
import slamdata.engine.std.StdLib._

import scalaz._
import Scalaz._
import monocle.Macro._
import monocle.syntax._

sealed trait WorkflowBuilderError extends Error
object WorkflowBuilderError {
  case object CouldNotPatchRoot extends WorkflowBuilderError {
    def message = "Could not patch ROOT"
  }
  case object CannotObjectConcatExpr extends WorkflowBuilderError {
    def message = "Cannot object concat an expression"
  }
  case object CannotArrayConcatExpr extends WorkflowBuilderError {
    def message = "Cannot array concat an expression"
  }
  case object NotGrouped extends WorkflowBuilderError {
    def message = "The pipeline builder has not been grouped by another set, so a group op doesn't make sense"
  }
  case class InvalidGroup[A](op: WorkflowF[A]) extends WorkflowBuilderError {
    def message = "Can not group " + op
  }
  case object InvalidSortBy extends WorkflowBuilderError {
    def message = "The sort by set has an invalid structure"
  }
  case object UnknownStructure extends WorkflowBuilderError {
    def message = "The structure is unknown due to a missing project or group operation"
  }
  case class InvalidOperation(operation: String, msg: String)
      extends WorkflowBuilderError {
    def message = "Can not perform `" + operation + ", because " + msg
  }
  case class UnsupportedDistinct(message: String) extends WorkflowBuilderError
  case class UnsupportedJoinCondition(func: Mapping) extends WorkflowBuilderError {
    def message = "Joining with " + func.name + " is not currently supported"
  }
}

sealed trait WorkflowBuilderF[+A]

/**
 * A `WorkflowBuilder` consists of a graph of operations, a structure, and a
 * base mod for that structure.
 */
object WorkflowBuilder {
  import Workflow._
  import ExprOp._
  import IdHandling._

  type WorkflowBuilder = Term[WorkflowBuilderF]

  case class CollectionBuilderF(
    graph: Workflow,
    base: DocVar,
    struct: SchemaChange) extends WorkflowBuilderF[Nothing]
  object CollectionBuilder {
    def apply(graph: Workflow, base: DocVar, struct: SchemaChange) =
      Term[WorkflowBuilderF](new CollectionBuilderF(graph, base, struct))
  }
  case class ValueBuilderF(value: Bson) extends WorkflowBuilderF[Nothing]
  object ValueBuilder {
    def apply(value: Bson) = Term[WorkflowBuilderF](new ValueBuilderF(value))
  }
  case class ExprBuilderF[A](src: A, expr: ExprOp) extends WorkflowBuilderF[A]
  object ExprBuilder {
    def apply(src: WorkflowBuilder, expr: ExprOp) =
      Term[WorkflowBuilderF](new ExprBuilderF(src, expr))
  }
  // NB: The shape is more restrictive than $project because we may need to
  //     convert it to a GroupBuilder, and a nested Reshape can be realized with
  //     a chain of DocBuilders, leaving the collapsing to Workflow.coalesce.
  case class DocBuilderF[A](src: A, shape: ListMap[BsonField.Name, ExprOp])
      extends WorkflowBuilderF[A]
  object DocBuilder {
    def apply(src: WorkflowBuilder, shape: ListMap[BsonField.Name, ExprOp]) =
      Term[WorkflowBuilderF](new DocBuilderF(src, shape))
  }
  case class GroupBuilderF[A](src: A, key: ExprOp \/ Reshape, contents: GroupContents)
      extends WorkflowBuilderF[A]
  object GroupBuilder {
    def apply(src: WorkflowBuilder, key: ExprOp \/ Reshape, contents: GroupContents) =
      Term[WorkflowBuilderF](new GroupBuilderF(src, key, contents))
  }

  private def rewriteDocRefs(
    doc: ListMap[BsonField.Name, ExprOp])(
    f: PartialFunction[DocVar, DocVar]) =
    doc.transform { case (_, v) => v.rewriteRefs(f) }

  sealed trait GroupContents
  case class FieldContent(content: DocVar) extends GroupContents
  case class PushedContent(content: ExprOp) extends GroupContents
  case class ReducedContent(content: GroupOp) extends GroupContents
  case class ObjectContent(
    content: ListMap[BsonField.Name, ExprOp \/ GroupOp])
      extends GroupContents

  private def rewriteObjRefs(
    obj: ListMap[BsonField.Name, ExprOp \/ GroupOp])(
    f: PartialFunction[DocVar, DocVar]) =
    obj.transform {
      case (_, -\/(expr))    => -\/(expr.rewriteRefs(f))
      case (_, \/-(grouped)) => grouped.rewriteRefs(f) match {
        case g : GroupOp => \/-(g)
        case _ => sys.error("Transformation changed the type -- error!")
      }
    }

  private def rewriteGroupRefs(
    contents: GroupContents)(
    f: PartialFunction[DocVar, DocVar]) =
    contents match {
      case FieldContent(d)         => FieldContent(f(d))
      case PushedContent(expr)     => PushedContent(expr.rewriteRefs(f))
      case ReducedContent(grouped) => grouped.rewriteRefs(f) match {
        case g : GroupOp => ReducedContent(g)
        case _ => sys.error("Transformation changed the type -- error!")
      }
      case ObjectContent(obj)      => ObjectContent(rewriteObjRefs(obj)(f))
    }

  def rewriteRefs(wb: WorkflowBuilder)(f: PartialFunction[DocVar, DocVar]) =
    wb.unFix match {
      case CollectionBuilderF(_, _, _) => wb
      case ValueBuilderF(_)            => wb
      case ExprBuilderF(src, expr)     => ExprBuilder(src, expr.rewriteRefs(f))
      case DocBuilderF(src, doc)       => DocBuilder(src, rewriteDocRefs(doc)(f))
      case GroupBuilderF(src, key, contents) =>
        GroupBuilder(src,
          key.bimap(_.rewriteRefs(f), _.rewriteRefs(f)),
          rewriteGroupRefs(contents)(f))
    }

  def chainBuilder(wb: WorkflowBuilder, base: DocVar, struct: SchemaChange)(f: WorkflowOp) =
    toCollectionBuilder(wb).map(_ match {
      case CollectionBuilderF(g, b, _) =>
        CollectionBuilder(
          Term[WorkflowF](Workflow.rewriteRefs(f(g).unFix, prefixBase(b))),
          base,
          struct)
    })

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

  def emitT[A](v: EitherE[State[NameGen, A]]): M[A] =
    StateT[EitherE, NameGen, A](s => v.map(_.run(s)))
  def swapM[A](v: State[NameGen, Error \/ A]): M[A] =
    StateT[EitherE, NameGen, A](s => { val (s1, x) = v.run(s); x.map(s1 -> _) })

  private def toCollectionBuilder(wb: WorkflowBuilder):
      MId[CollectionBuilderF] =
    wb.unFix match {
      case cb @ CollectionBuilderF(_, _, _) => state(cb)
      case ValueBuilderF(value) =>
        state(CollectionBuilderF($pure(value), DocVar.ROOT(), SchemaChange.Init))
      case ExprBuilderF(src, expr) => for {
        cb <- toCollectionBuilder(src)
        name <- freshName
      } yield cb match {
        case CollectionBuilderF(graph, base, struct) =>
          CollectionBuilderF(
            chain(graph,
              $project(Reshape.Doc(ListMap(name -> -\/(expr.rewriteRefs(prefixBase(base))))))),
            DocField(name),
            struct)
      }
      case DocBuilderF(src, shape) =>
        workflow(src).map { case (wf, base) =>
          CollectionBuilderF(
            chain(wf,
              $project(Reshape.Doc(shape.transform {
                case (_, v) => -\/(v)
              }).rewriteRefs(prefixBase(base)))),
            DocVar.ROOT(),
            SchemaChange.MakeObject(shape.map {
              case (k, _) => k.asText -> SchemaChange.Init
            }))
        }
      case GroupBuilderF(src, _, FieldContent(doc)) =>
        // NB: This case just winds up a single value, then unwinds it. It’s
        //     effectively a no-op, so we just use the src and expr
        toCollectionBuilder(ExprBuilder(src, doc))
      case GroupBuilderF(src, _, PushedContent(expr)) =>
        // NB: This case just winds up a single value, then unwinds it. It’s
        //     effectively a no-op, so we just use the src and expr
        toCollectionBuilder(ExprBuilder(src, expr))
      case GroupBuilderF(src, key, ReducedContent(grouped)) =>
        for {
          cb <- toCollectionBuilder(src)
          rootName <- freshName
        } yield cb match {
          case CollectionBuilderF(wf, base, struct) =>
          CollectionBuilderF(
            chain(wf,
              $group(Grouped(ListMap(rootName -> grouped)).rewriteRefs(prefixBase(base)),
                key.bimap(_.rewriteRefs (prefixBase(base)), _.rewriteRefs(prefixBase(base))))),
            DocField(rootName),
            struct)
        }
      case GroupBuilderF(src, key, ObjectContent(obj)) =>
        val (ungrouped, grouped) =
          obj.foldLeft[(ListMap[BsonField.Name, ExprOp], ListMap[BsonField.Leaf, GroupOp])]((ListMap.empty[BsonField.Name, ExprOp], ListMap.empty[BsonField.Leaf, GroupOp]))((acc, item) =>
            item match {
              case (k, -\/(v)) =>
                ((x: ListMap[BsonField.Name, ExprOp]) => x + (k -> v)).first(acc)
              case (k, \/-(v)) =>
                ((x: ListMap[BsonField.Leaf, GroupOp]) => x + (k -> v)).second(acc)
            })

        workflow(src).flatMap(_ match {
          case (wf, base) =>
            (ungrouped.size match {
              case 0 =>
                state[NameGen, Workflow](chain(wf,
                  $group(Grouped(grouped).rewriteRefs(prefixBase(base)),
                    key.bimap(_.rewriteRefs(prefixBase(base)), _.rewriteRefs(prefixBase(base))))))
              case 1 =>
                state[NameGen, Workflow](chain(wf,
                  $group(Grouped(
                    obj.transform {
                      case (_, -\/(v)) => Push(v)
                      case (_, \/-(v)) => v.rewriteRefs(prefixBase(base))
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
                    grouped.transform { case (_, v) => v.rewriteRefs(prefixBase(DocField(groupedName))) } + (ungroupedName -> Push(DocField(ungroupedName)))),
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
        })
    }

  def workflow(wb: WorkflowBuilder): MId[(Workflow, DocVar)] =
    toCollectionBuilder(wb).map(x => (x.graph, x.base))

  def build(wb: WorkflowBuilder): MId[Workflow] =
    toCollectionBuilder(wb).flatMap(_ match {
      case CollectionBuilderF(graph, base, struct) => base match {
        case DocVar.ROOT(None) => state(finish(graph))
        case base =>
          val g1 = struct.shift(base).map(t => chain(graph, Workflow.$project(t._1, t._2))).getOrElse(graph)
          build(CollectionBuilder(g1, DocVar.ROOT(), struct))
      }
    })

  private def $project(shape: Reshape): WorkflowOp =
    Workflow.$project(
      shape,
      shape.get(IdName).fold[IdHandling](IgnoreId)(Function.const(IncludeId)))

  def asLiteral(wb: WorkflowBuilder) =
    asExprOp(wb).collect { case (x @ Literal(_)) => x }

  def expr1(wb: WorkflowBuilder)(f: DocVar => ExprOp): WorkflowBuilder =
    ExprBuilder(wb, f(DocVar.ROOT()))

  def expr2(
    wb1: WorkflowBuilder, wb2: WorkflowBuilder)(
    f: (DocVar, DocVar) => ExprOp):
      MId[WorkflowBuilder] =
    merge(wb1, wb2) { (lbase, rbase, src) =>
      ExprBuilder(src, f(lbase, rbase))}

  def expr3(
    wb1: WorkflowBuilder, wb2: WorkflowBuilder, wb3: WorkflowBuilder)(
    f: (DocVar, DocVar, DocVar) => ExprOp):
      MId[WorkflowBuilder] = {
    def nest(lname: BsonField.Name, rname: BsonField.Name) =
      (lbase: DocVar, rbase: DocVar, src: WorkflowBuilder) =>
        DocBuilder(src, ListMap(lname -> lbase, rname -> rbase))

    for {
      l      <- freshName
      ll     <- freshName
      lr     <- freshName
      r      <- freshName
      p12    <- merge(wb1, wb2)(nest(ll, lr))
      p123   <- merge(p12, wb3)(nest(l, r))
    } yield
      expr1(p123)(root => f(root \ l \ ll, root \ l \ lr, root \ r))
  }

  def makeObject(wb: WorkflowBuilder, name: String): WorkflowBuilder =
    wb.unFix match {
      case ValueBuilderF(value) =>
        ValueBuilder(Bson.Doc(ListMap(name -> value)))
      case GroupBuilderF(src, key, FieldContent(d)) =>
        GroupBuilder(src, key,
          ObjectContent(ListMap(BsonField.Name(name) -> -\/(d))))
      case GroupBuilderF(src, key, PushedContent(expr)) =>
        GroupBuilder(src, key,
          ObjectContent(ListMap(BsonField.Name(name) -> -\/(expr))))
      case GroupBuilderF(src, key, ReducedContent(grouped)) =>
        GroupBuilder(src, key,
          ObjectContent(ListMap(BsonField.Name(name) -> \/-(grouped))))
      case ExprBuilderF(src, expr) =>
        DocBuilder(src, ListMap(BsonField.Name(name) -> expr))
      case _ =>
        DocBuilder(wb, ListMap(BsonField.Name(name) -> DocVar.ROOT()))
    }

  def makeArray(wb: WorkflowBuilder): MId[WorkflowBuilder] = wb.unFix match {
    case ValueBuilderF(value) => state(ValueBuilder(Bson.Arr(List(value))))
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

  def objectConcat(wb1: WorkflowBuilder, wb2: WorkflowBuilder):
      M[WorkflowBuilder] =
    emitSt(freshId).flatMap { name =>

      def builderWithUnknowns(
        src: WorkflowBuilder,
        fields: List[Js.Expr => Js.Stmt]) =
        chainBuilder(src, DocVar.ROOT(), SchemaChange.Init)(
          $map($Map.mapMap(name,
            Js.Call(
              Js.AnonFunDecl(List("rez"),
                fields.map(_(Js.Ident("rez"))) :+ Js.Return(Js.Ident("rez"))),
              List(Js.AnonObjDecl(Nil))))))

      def side(wb: WorkflowBuilder, base: DocVar) = {
        wb.unFix match {
          case CollectionBuilderF(_, _, _) =>
            \/-(-\/($Reduce.copyAllFields(base.toJs(Js.Ident(name)))))
          case ValueBuilderF(Bson.Doc(map)) =>
            \/-(\/-(map.keys.map(BsonField.Name(_)).toList))
          case DocBuilderF(_, shape) => \/-(\/-(shape.keys.toList))
          case GroupBuilderF(_, _, ObjectContent(obj)) =>
            \/-(\/-(obj.keys.toList))
          // TODO: Restrict to DocVar, Literal, Let, Cond, and IfNull (#436)
          case ExprBuilderF(_, _) =>
            \/-(-\/($Reduce.copyAllFields(base.toJs(Js.Ident(name)))))
          case _ =>
            -\/(WorkflowBuilderError.InvalidOperation(
              "objectConcat",
              "values are not both documents"))
        }
      }

      emitSt(merge(wb1, wb2) { (left, right, list) =>
        lift((side(wb1, left) |@| side(wb2, right))((_, _))).flatMap(_ match {
          case (\/-(f1), \/-(f2)) =>
            if ((f1 intersect f2).isEmpty)
              emit(DocBuilder(list,
                (f1.map(k => k -> left \ k) ++
                  f2.map(k => k -> right \ k)).toListMap))
            else fail[WorkflowBuilder](WorkflowBuilderError.InvalidOperation(
              "objectConcat",
              "conflicting keys"))
          case (\/-(f1), -\/(f2)) =>
            emitSt(builderWithUnknowns(
              list,
              f1.map(k =>
                $Reduce.copyOneField(
                  Js.Access(_, Js.Str(k.asText)),
                  (left \ k).toJs(Js.Ident(name)))) ++
                List(f2)))
          case (-\/(f1), \/-(f2)) =>
            emitSt(builderWithUnknowns(
              list,
              List(f1) ++
                f2.map(k =>
                  $Reduce.copyOneField(
                    Js.Access(_, Js.Str(k.asText)),
                    (right \ k).toJs(Js.Ident(name))))))
          case (-\/(f1), -\/(f2)) =>
            emitSt(builderWithUnknowns(list, List(f1, f2)))
        })
      })}.join

  def arrayConcat(left: WorkflowBuilder, right: WorkflowBuilder):
      M[WorkflowBuilder] = {
    def convert(root: DocVar) = (shift: Int, keys: Seq[Int]) =>
    (keys.map { index =>
      BsonField.Index(index + shift) -> -\/(root \ BsonField.Index(index))
    }): Seq[(BsonField.Index, ExprOp \/ Reshape)]
    def meh(base: DocVar, cb: CollectionBuilderF, shift: Int) =
      cb.struct.simplify match {
        case SchemaChange.MakeArray(m) =>
          \/-((
            convert(base)(shift, m.keys.toSeq),
            if (shift == 0) m else m.map(t => (t._1 + shift) -> t._2),
            m.keys.max + 1 + shift))
        case _ => -\/(WorkflowBuilderError.CannotObjectConcatExpr)
      }

    (left.unFix, right.unFix) match {
      case (
        cb1 @ CollectionBuilderF(_, _, _),
        cb2 @ CollectionBuilderF(_, _, _)) =>
        emitSt(merge(left, right) { (left, right, list) =>
          for {
            l <- lift(meh(left, cb1, 0))
            (reshape1, array1, shift) = l
            r <- lift(meh(right, cb2, shift))
            (reshape2, array2, _) = r
            rez <- emitSt(chainBuilder(
              list, DocVar.ROOT(), SchemaChange.MakeArray(array1 ++ array2))(
              $project(Reshape.Arr(ListMap((reshape1 ++ reshape2): _*)))))
          } yield rez
        }).join
      case (ValueBuilderF(Bson.Arr(seq1)), ValueBuilderF(Bson.Arr(seq2))) =>
        emit(ValueBuilder(Bson.Arr(seq1 ++ seq2)))
      case (cb @ CollectionBuilderF(src, base, _), ValueBuilderF(Bson.Arr(seq))) =>
        lift(for {
          l <- meh(base, cb, 0)
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
          r <- meh(base, cb, seq.length)
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

  def flattenObject(wb: WorkflowBuilder): MId[WorkflowBuilder] =
    toCollectionBuilder(wb).map(_ match {
      case CollectionBuilderF(graph, base, struct) =>
        val field = base.toJs(Js.Ident("value"))
        CollectionBuilder(
          chain(graph,
            $flatMap(
              Js.AnonFunDecl(List("key", "value"),
                List(
                  Js.VarDef(List("rez" -> Js.AnonElem(Nil))),
                  Js.ForIn(Js.Ident("attr"), field,
                    Js.Call(
                      Js.Select(Js.Ident("rez"), "push"),
                      List(
                        Js.AnonElem(List(
                          Js.Call(Js.Ident("ObjectId"), Nil),
                          Js.Access(field, Js.Ident("attr"))))))),
                  Js.Return(Js.Ident("rez")))))),
          DocVar.ROOT(),
          struct)
    })

  def flattenArray(wb: WorkflowBuilder): MId[WorkflowBuilder] =
    toCollectionBuilder(wb).map(_ match {
      case CollectionBuilderF(graph, base, struct) =>
        CollectionBuilder(chain(graph, $unwind(base)), base, struct)
    })

  def projectField(wb: WorkflowBuilder, name: String):
      Error \/ WorkflowBuilder =
    wb.unFix match {
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
      case GroupBuilderF(wb0, key, FieldContent(d)) =>
        // TODO: check structure of wb0 (#436)
        \/-(GroupBuilder(wb0, key, FieldContent(d \ BsonField.Name(name))))
      case ExprBuilderF(wb, DocField(field)) =>
        \/-(ExprBuilder(wb, DocField(field \ BsonField.Name(name))))
      case _ => \/-(ExprBuilder(wb, DocField(BsonField.Name(name))))
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
      case _ => emitSt(toCollectionBuilder(wb).map(_ match {
        // TODO: if the struct is MakeArray, we can see if the index is there
        //       and error if not. (#436)
        case CollectionBuilderF(graph, base, struct) =>
          // TODO: when https://jira.mongodb.org/browse/SERVER-4589 is fixed,
          //       replace with:
          //       ExprBuilder(wb, DocField(BsonField.Index(Index)))
          CollectionBuilder(
            chain(graph,
              $map(
                $Map.mapMap("value",
                  Js.Access(
                    base.toJs(Js.Ident("value")),
                    Js.Num(index, false))))),
            DocVar.ROOT(),
            struct.projectIndex(index))
      }))
    }

  def groupBy(src: WorkflowBuilder, key: WorkflowBuilder):
      MId[WorkflowBuilder] =
    key.unFix match {
      case ValueBuilderF(_) =>
        // NB: If the key is a constant, normalize to Null, to ease merging
        state(GroupBuilder(src, -\/(Literal(Bson.Null)), FieldContent(DocVar.ROOT())))
      case ExprBuilderF(_, Literal(_)) =>
        // NB: Treat this as a constant key, don’t hold onto the key source
        state(GroupBuilder(src, -\/(Literal(Bson.Null)), FieldContent(DocVar.ROOT())))
      case _ =>
        merge(src, key) { (lbase, rbase, wf) =>
          GroupBuilder(
            rewriteRefs(wf)(prefixBase(lbase)),
            -\/(rbase),
            FieldContent(lbase))
        }
    }

  def reduce(wb: WorkflowBuilder)(f: ExprOp => GroupOp): WorkflowBuilder =
    wb.unFix match {
      case GroupBuilderF(wb0, key, FieldContent(d)) =>
        GroupBuilder(wb0, key, ReducedContent(f(d)))
      case GroupBuilderF(wb0, key, PushedContent(expr)) =>
        GroupBuilder(wb0, key, ReducedContent(f(expr)))
      case _ =>
        GroupBuilder(wb, -\/(Literal(Bson.Null)), ReducedContent(f(DocVar.ROOT())))
    }

  def sortBy(
    src: WorkflowBuilder, keys: WorkflowBuilder, sortTypes: List[SortType]):
      M[WorkflowBuilder] =
    emitSt((toCollectionBuilder(src) |@| toCollectionBuilder(keys))((_, _))).flatMap(_ match {
      case (CollectionBuilderF(_, _, struct), CollectionBuilderF(_, _, s2)) =>
        emitSt(merge(src, keys) { (sort, by, list) =>
          (s2.simplify, by) match {
            case (SchemaChange.MakeArray(els), DocVar(_, Some(by)))
                if els.size == sortTypes.length =>
              val sortFields = (els.zip(sortTypes).foldLeft(List.empty[(BsonField, SortType)]) {
                case (acc, ((idx, s), sortType)) =>
                  (by \ BsonField.Index(idx) -> sortType) :: acc
              }).reverse

              sortFields match {
                case Nil =>
                  fail[WorkflowBuilder](WorkflowBuilderError.InvalidSortBy)
                case x :: xs =>
                  emitSt(chainBuilder(
                    list, sort, struct)(
                    $sort(NonEmptyList.nel(x, xs))))
              }
            case _ => fail[WorkflowBuilder](WorkflowBuilderError.InvalidSortBy)
          }
        }).join
    })

  def join(left: WorkflowBuilder, right: WorkflowBuilder,
    tpe: slamdata.engine.LogicalPlan.JoinType, comp: Mapping,
    leftKey: ExprOp, rightKey: Js.Expr => Js.Expr):
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
        Eq(
          Size(DocField(side)),
          Literal(Bson.Int32(0))),
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

    def rightMap(keyExpr: Expr => Expr): AnonFunDecl =
      $Map.mapKeyVal(("key", "value"),
        keyExpr(Ident("value")),
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
        emitSt((workflow(left) |@| workflow(right)){
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
        })
      case _ => fail(WorkflowBuilderError.UnsupportedJoinCondition(comp))
    }
  }

  def cross(left: WorkflowBuilder, right: WorkflowBuilder) =
    join(left, right,
      slamdata.engine.LogicalPlan.JoinType.Inner, relations.Eq,
      Literal(Bson.Null), Function.const(Js.Null))

  def appendOp (wb: WorkflowBuilder, op: WorkflowOp): MId[WorkflowBuilder] =
    toCollectionBuilder(wb).map(_ match {
      case CollectionBuilderF(graph, base, struct) =>
        val (newGraph, newBase) = Workflow.rewrite(op(graph).unFix, base)
        CollectionBuilder(Term(newGraph), newBase, struct)
    })

  def squash(wb: WorkflowBuilder): WorkflowBuilder = wb

  def distinctBy(src: WorkflowBuilder, key: WorkflowBuilder):
      M[WorkflowBuilder] = {
    def sortKeys(op: WorkflowBuilder): Error \/ List[(BsonField, SortType)] = {
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
        case p: PipelineF[_]                        => isOrdered(p.src)
        case _                                      => false
      }

      // Note: this currently only handles a couple of cases, which are the ones
      // that are generated by the compiler for SQL's distinct keyword, with
      // order by, with or without "synthetic" projections. A more general
      // implementation would rewrite the pipeline to handle additional cases.
      op.unFix match {
        case CollectionBuilderF(graph, _, _) => graph.unFix match {
          case $Sort(_, keys) => \/-(keys.list)

          case $Project(Term($Sort(_, keys)), HavingRoot(root), _) =>
            \/-(keys.list.map {
              case (field, sortType) => (root \ field) -> sortType
            })

          case _ =>
            if (isOrdered(graph))
              -\/(WorkflowBuilderError.UnsupportedDistinct("cannot distinct with unrecognized ordered source: " + op))
            else \/-(Nil)
        }
        case GroupBuilderF(_, _, _) => \/-(Nil)
        case DocBuilderF(src, _)    => sortKeys(src)
        case ExprBuilderF(src, _)   => sortKeys(src)
        case ValueBuilderF(_)       => \/-(Nil)
      }
    }

    val distinct = emitSt(merge(src, key) { (value, by, merged) =>
      lift(sortKeys(merged)).flatMap { sk =>
        val keyPrefix = "__sd_key_"
        val keyProjs = sk.zipWithIndex.map { case ((name, _), index) => BsonField.Name(keyPrefix + index.toString) -> First(DocField(name)) }

        val group = for {
          groupedBy <- lift((src.unFix, key.unFix) match {
            case (CollectionBuilderF(g1, b1, s1), CollectionBuilderF(g2, b2, s2)) =>
              (s2.simplify, by) match {
                case (_, DocVar(_, Some(_))) => \/-(-\/(by))
                // If the key is at the document root, we must explicitly
                //  project out the fields so as not to include a meaningless
                // _id in the key:
                case (SchemaChange.MakeObject(byFields), _) =>
                  \/-(\/-(Reshape.Arr(ListMap(
                    byFields.keys.toList.zipWithIndex.map { case (name, index) =>
                      BsonField.Index(index) -> -\/ (by \ BsonField.Name(name))
                    }: _*))))
                case (SchemaChange.MakeArray(byFields), _) =>
                  \/-(\/-(Reshape.Arr(ListMap(
                    byFields.keys.toList.map { index =>
                      BsonField.Index(index) -> -\/(by \ BsonField.Index(index))
                    }: _*))))
                case _ =>
                  -\/(WorkflowBuilderError.UnsupportedDistinct("Cannot distinct with unknown shape (" + s1 + "; " + s2 + "; " + by + ")"))
              }
            case (_, DocBuilderF(ksrc, shape)) =>
              \/-(\/-(Reshape.Arr(ListMap(shape.keys.toList.zipWithIndex.map { case (name, index) => BsonField.Index(index) -> -\/(by \ name) }: _*))))
            case (_, GroupBuilderF(_, _, ObjectContent(obj))) =>
              \/-(\/-(Reshape.Arr(ListMap(obj.keys.toList.zipWithIndex.map { case (name, index) => BsonField.Index(index) -> -\/(by \ name) }: _*))))
            case _ => \/-(-\/(by))
          })

          merg <- emitSt(toCollectionBuilder(merged))
          CollectionBuilderF(graph, base, _) = merg
        } yield chain(
          graph,
          $group(Grouped(ListMap(ExprName -> First(base \\ value) :: keyProjs: _*)), groupedBy))
        if (sk.isEmpty) group
        else {
          val keyPairs = sk.zipWithIndex.map { case ((name, sortType), index) => BsonField.Name(keyPrefix + index.toString) -> sortType }
          keyPairs.headOption.map { head =>
            val tail = keyPairs.drop(1)
            group.flatMap(g => emit(chain(g, $sort(NonEmptyList(head, tail: _*)))))
          }.getOrElse(group)
        }
      }
    }).join

    distinct.flatMap(graph => emitSt(toCollectionBuilder(src).map(_ match {
      case CollectionBuilderF(_, _, struct) =>
        CollectionBuilder(graph, ExprVar, struct)
    })))
  }

  def asExprOp(wb: WorkflowBuilder) = wb.unFix match {
    case ValueBuilderF(value)    => Some(Literal(value))
    case ExprBuilderF(src, expr) => Some(expr)
    case _                       => None
  }

  private def merge[A](
    left: WorkflowBuilder, right: WorkflowBuilder)(
    f: (DocVar, DocVar, WorkflowBuilder) => A):
      MId[A] =
    (left.unFix, right.unFix) match {
      case (DocBuilderF(src1, shape1), DocBuilderF(src2, shape2)) =>
        merge(src1, src2) { (lbase, rbase, wb) =>
          Reshape.mergeMaps(
            rewriteDocRefs(shape1)(prefixBase(lbase)),
            rewriteDocRefs(shape2)(prefixBase(rbase))).fold(
            (toCollectionBuilder(left) |@| toCollectionBuilder(right))((_, _) match {
              case (
                CollectionBuilderF(graph1, base1, _),
                CollectionBuilderF(graph2, base2, _)) =>
                (Workflow.merge(graph1, graph2)).map { case ((lbase, rbase), op) =>
                  f(lbase \\ base1, rbase \\ base2,
                    CollectionBuilder(op, DocVar.ROOT(), SchemaChange.Init))
                }
            }).join)(
            x => state(f(DocVar.ROOT(), DocVar.ROOT(), DocBuilder(wb, x))))
        }.join
      case (
        GroupBuilderF(src1, key1, ObjectContent(obj1)),
        GroupBuilderF(src2, key2, ObjectContent(obj2)))
          if src1 == src2 && key1 == key2 =>
        state(f(
          DocVar.ROOT(), DocVar.ROOT(),
          GroupBuilder(src1, key1, ObjectContent(obj1 ++ obj2))))
      case _ =>
        (toCollectionBuilder(left) |@| toCollectionBuilder(right))((_, _) match {
          case (
            CollectionBuilderF(graph1, base1, _),
            CollectionBuilderF(graph2, base2, _)) =>
            (Workflow.merge(graph1, graph2)).map { case ((lbase, rbase), op) =>
              f(lbase \\ base1, rbase \\ base2,
                CollectionBuilder(op, DocVar.ROOT(), SchemaChange.Init))
            }
        }).join
    }

  def read(coll: Collection) =
    CollectionBuilder($read(coll), DocVar.ROOT(), SchemaChange.Init)
  def pure(bson: Bson) = ValueBuilder(bson)

  implicit def WorkflowBuilderRenderTree(implicit RO: RenderTree[Workflow], RE: RenderTree[ExprOp]): RenderTree[WorkflowBuilder] = new RenderTree[WorkflowBuilder] {
    def render(v: WorkflowBuilder) = v.unFix match {
      case CollectionBuilderF(graph, base, struct) =>
        NonTerminal("",
          RO.render(graph) ::
            RE.render(base) ::
            Terminal(struct.toString, "WorkflowBuilder" :: "SchemaChange" :: Nil) ::
            Nil,
          "CollectionBuilder" :: Nil)
      case ValueBuilderF(value) =>
        Terminal(value.toString, "ValueBuilder" :: Nil)
      case ExprBuilderF(src, expr) =>
        NonTerminal("",
          render(src) ::
            Terminal(expr.toString, "ExprBuilder" :: "ExprOp" :: Nil) ::
            Nil,
          "ExprBuilder" :: Nil)
      case DocBuilderF(src, shape) =>
        NonTerminal("",
          render(src) ::
            Terminal(shape.toString, "DocBuilder" :: "ListMap" :: Nil) ::
            Nil,
          "DocBuilder" :: Nil)
      case GroupBuilderF(src, key, content) =>
        NonTerminal("",
          render(src) ::
            Terminal(key.toString, "GroupBuilder" :: "ExprOp" :: Nil) ::
            Terminal(content.toString, "GroupBuilder" :: "GroupContents" :: Nil) ::
            Nil,
          "GroupBuilder" :: Nil)
    }
  }
}
