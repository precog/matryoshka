/*
 * Copyright 2014 - 2015 SlamData Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package slamdata.engine.physical.mongodb

import collection.immutable.ListMap

import slamdata.engine.fp._
import slamdata.engine.fs.Path
import slamdata.engine._
import slamdata.engine.analysis.fixplate._
import slamdata.engine.std.StdLib._
import slamdata.engine.javascript._

import scalaz._
import Scalaz._

sealed trait WorkflowBuilderError extends Error
object WorkflowBuilderError {
  final case class InvalidOperation(operation: String, msg: String)
      extends WorkflowBuilderError {
    def message = "Can not perform `" + operation + "`, because " + msg
  }
  final case class UnsupportedDistinct(message: String) extends WorkflowBuilderError
  final case class UnsupportedJoinCondition(func: Mapping) extends WorkflowBuilderError {
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

  type Expr = ExprOp \/ JsFn
  private def exprToJs(expr: Expr) = expr.fold(ExprOp.toJs(_), \/-(_))
  implicit def ExprRenderTree(implicit RJM: RenderTree[JsFn]) = new RenderTree[Expr] {
      override def render(x: Expr) =
        x.fold(
          op => Terminal(List("ExprOp"), Some(op.toString)),
          js => RJM.render(js))
    }

  final case class CollectionBuilderF(
    graph: Workflow,
    base: DocVar,
    struct: Schema) extends WorkflowBuilderF[Nothing]
  object CollectionBuilder {
    def apply(graph: Workflow, base: DocVar, struct: Schema) =
      Term[WorkflowBuilderF](new CollectionBuilderF(graph, base, struct))
  }
  final case class ShapePreservingBuilderF[A](
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
        $read(Collection("", "")))
  }
  final case class ValueBuilderF(value: Bson) extends WorkflowBuilderF[Nothing]
  object ValueBuilder {
    def apply(value: Bson) = Term[WorkflowBuilderF](new ValueBuilderF(value))
  }
  final case class ExprBuilderF[A](src: A, expr: Expr) extends WorkflowBuilderF[A]
  object ExprBuilder {
    def apply(src: WorkflowBuilder, expr: Expr) =
      Term[WorkflowBuilderF](new ExprBuilderF(src, expr))
  }

  // NB: The shape is more restrictive than $project because we may need to
  //     convert it to a GroupBuilder, and a nested Reshape can be realized with
  //     a chain of DocBuilders, leaving the collapsing to Workflow.coalesce.
  final case class DocBuilderF[A](src: A, shape: ListMap[BsonField.Name, Expr])
      extends WorkflowBuilderF[A]
  object DocBuilder {
    def apply(src: WorkflowBuilder, shape: ListMap[BsonField.Name, Expr]) =
      Term[WorkflowBuilderF](new DocBuilderF(src, shape))
  }

  final case class ArrayBuilderF[A](src: A, shape: List[Expr])
      extends WorkflowBuilderF[A]
  object ArrayBuilder {
    def apply(src: WorkflowBuilder, shape: List[Expr]) =
      Term[WorkflowBuilderF](new ArrayBuilderF(src, shape))
  }

  sealed trait Contents[+A]
  sealed trait DocContents[+A] extends Contents[A]
  sealed trait ArrayContents[+A] extends Contents[A]
  object Contents {
    final case class Expr[A](contents: A) extends DocContents[A] with ArrayContents[A]
    final case class Doc[A](contents: ListMap[BsonField.Name, A]) extends DocContents[A]
    final case class Array[A](contents: List[A]) extends ArrayContents[A]

    implicit def ContentsRenderTree[A](implicit RA: RenderTree[A], RB: RenderTree[ListMap[BsonField.Name, A]]) =
      new RenderTree[Contents[A]] {
        val nodeType = "Contents" :: Nil

        override def render(v: Contents[A]) =
          v match {
            case Expr(a)   => NonTerminal("Expr" :: nodeType, None, RA.render(a) :: Nil)
            case Doc(b)    => NonTerminal("Doc" :: nodeType, None, RB.render(b) :: Nil)
            case Array(as) => NonTerminal("Array" :: nodeType, None, as.map(RA.render))
          }
      }
  }
  import Contents._

  type GroupValue = ExprOp \/ GroupOp
  type GroupContents = DocContents[GroupValue]

  final case class GroupId(srcs: List[WorkflowBuilder]) {
    override def toString = hashCode.toHexString
  }

  final case class GroupBuilderF[A](
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

  sealed trait StructureType {
    val field: DocVar

    def rewrite(f: DocVar => DocVar): StructureType
  }
  object StructureType {
    final case class Array(field: DocVar) extends StructureType {
      def rewrite(f: DocVar => DocVar) = Array(f(field))
    }
    final case class Object(field: DocVar) extends StructureType {
      def rewrite(f: DocVar => DocVar) = Object(f(field))
    }
  }

  final case class FlatteningBuilderF[A](src: A, fields: Set[StructureType])
      extends WorkflowBuilderF[A]
  object FlatteningBuilder {
    def apply(src: WorkflowBuilder, fields: Set[StructureType]) =
      Term[WorkflowBuilderF](new FlatteningBuilderF(src, fields))
  }

  /**
    Holds a partially-unknown structure. `Expr` entries are unknown and `Doc`
    entries are known. There should be at least one Expr in the list, otherwise
    it should be a DocBuilder.
    */
  final case class SpliceBuilderF[A](src: A, structure: List[DocContents[Expr]])
      extends WorkflowBuilderF[A] {
    def toJs: Error \/ JsFn =
      structure.map {
        case Expr(unknown) => exprToJs(unknown)
        case Doc(known)    => known.toList.map { case (k, v) =>
          exprToJs(v).map(k.asText -> _)
        }.sequenceU.map(ms => JsFn(jsBase, JsCore.Obj(ms.map { case (k, v) => k -> v(jsBase.fix) }.toListMap).fix))
      }.sequenceU.map(srcs =>
        JsFn(jsBase, JsCore.SpliceObjects(srcs.map(_(jsBase.fix))).fix))
  }
  object SpliceBuilder {
    def apply(src: WorkflowBuilder, structure: List[DocContents[Expr]]) =
      Term[WorkflowBuilderF](new SpliceBuilderF(src, structure))
  }

  final case class ArraySpliceBuilderF[A](src: A, structure: List[ArrayContents[Expr]])
      extends WorkflowBuilderF[A] {
    def toJs: Error \/ JsFn =
      structure.map {
        case Expr(unknown) => exprToJs(unknown)
        case Array(known)  => known.map(exprToJs).sequenceU.map(
            ms => JsFn(jsBase, JsCore.Arr(ms.map(_(jsBase.fix))).fix))
      }.sequenceU.map(srcs =>
        JsFn(jsBase, JsCore.SpliceArrays(srcs.map(_(jsBase.fix))).fix))
  }
  object ArraySpliceBuilder {
    def apply(src: WorkflowBuilder, structure: List[ArrayContents[Expr]]) =
      Term[WorkflowBuilderF](new ArraySpliceBuilderF(src, structure))
  }

  implicit def WorkflowBuilderEqualF = new EqualF[WorkflowBuilderF] {
    def equal[A](v1: WorkflowBuilderF[A], v2: WorkflowBuilderF[A])(implicit A: Equal[A]) = (v1, v2) match {
      case (CollectionBuilderF(g1, b1, s1), CollectionBuilderF(g2, b2, s2)) =>
        g1 == g2 && b1 == b2 && s1 === s2
      case (v1 @ ShapePreservingBuilderF(s1, i1, _), v2 @ ShapePreservingBuilderF(s2, i2, _)) =>
        s1 === s2 && i1 === i2 &&  ShapePreservingBuilder.dummyOp(v1) == ShapePreservingBuilder.dummyOp(v2)
      case (ValueBuilderF(v1), ValueBuilderF(v2)) => v1 == v2
      case (ExprBuilderF(s1, e1), ExprBuilderF(s2, e2)) =>
        s1 === s2 && e1 == e2
      case (DocBuilderF(s1, e1), DocBuilderF(s2, e2)) =>
        s1 === s2 && e1 == e2
      case (ArrayBuilderF(s1, e1), ArrayBuilderF(s2, e2)) =>
        s1 === s2 && e1 == e2
      case (GroupBuilderF(s1, k1, c1, i1), GroupBuilderF(s2, k2, c2, i2)) =>
        s1 === s2 && k1 === k2 && c1 == c2 && i1 == i2
      case (FlatteningBuilderF(s1, f1), FlatteningBuilderF(s2, f2)) =>
        s1 === s2 && f1 == f2
      case (SpliceBuilderF(s1, i1), SpliceBuilderF(s2, i2)) =>
        s1 === s2 && i1 == i2
      case (ArraySpliceBuilderF(s1, i1), ArraySpliceBuilderF(s2, i2)) =>
        s1 === s2 && i1 == i2
      case _ => false
    }
  }

  implicit val WorkflowBuilderTraverse = new Traverse[WorkflowBuilderF] {
    def traverseImpl[G[_], A, B](
      fa: WorkflowBuilderF[A])(
      f: A => G[B])(
      implicit G: Applicative[G]):
        G[WorkflowBuilderF[B]] =
    fa match {
      case x @ CollectionBuilderF(_, _, _) => G.point(x)
      case ShapePreservingBuilderF(src, inputs, op) =>
        (f(src) |@| inputs.traverse(f))(ShapePreservingBuilderF(_, _, op))
      case x @ ValueBuilderF(_) => G.point(x)
      case ExprBuilderF(src, expr) => f(src).map(ExprBuilderF(_, expr))
      case DocBuilderF(src, shape) => f(src).map(DocBuilderF(_, shape))
      case ArrayBuilderF(src, shape) => f(src).map(ArrayBuilderF(_, shape))
      case GroupBuilderF(src, keys, contents, id) =>
        (f(src) |@| keys.traverse(f))(GroupBuilderF(_, _, contents, id))
      case FlatteningBuilderF(src, fields) =>
        f(src).map(FlatteningBuilderF(_, fields))
      case SpliceBuilderF(src, structure) =>
        f(src).map(SpliceBuilderF(_, structure))
      case ArraySpliceBuilderF(src, structure) =>
        f(src).map(ArraySpliceBuilderF(_, structure))
    }
  }

  val branchLengthƒ: WorkflowBuilderF[Int] => Int = {
    case CollectionBuilderF(_, _, _) => 0
    case ShapePreservingBuilderF(src, inputs, _) => 1 + src
    case ValueBuilderF(_) => 0
    case ExprBuilderF(src, _) => 1 + src
    case DocBuilderF(src, _) => 1 + src
    case ArrayBuilderF(src, _) => 1 + src
    case GroupBuilderF(src, keys, _, _) => 1 + src
    case FlatteningBuilderF(src, _) => 1 + src
    case SpliceBuilderF(src, _) => 1 + src
    case ArraySpliceBuilderF(src, _) => 1 + src
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

  private val jsBase = JsCore.Ident("__val")

  private def toCollectionBuilder(wb: WorkflowBuilder): M[CollectionBuilderF] = {
    wb.unFix match {
      case cb @ CollectionBuilderF(_, _, _) => emit(cb)
      case ValueBuilderF(value) =>
        emit(CollectionBuilderF($pure(value), DocVar.ROOT(), None))
      case ShapePreservingBuilderF(src, inputs, op) =>
        // At least one argument has no deref (e.g. $$ROOT)
        def case1(src: WorkflowBuilder, input: WorkflowBuilder, op: PartialFunction[List[BsonField], Workflow => Workflow], fields: List[DocVar]): M[CollectionBuilderF] = {
          emitSt(freshName).flatMap(name =>
            fields.map(f => (DocField(name) \\ f).deref).sequence.fold(
              sys.error("prefixed ${name}, but still no field"))(
              op.lift(_).fold(
                fail[CollectionBuilderF](WorkflowBuilderError.InvalidOperation("filter", "failed to build operation")))(
                op =>
                (toCollectionBuilder(src) |@| toCollectionBuilder(DocBuilder(input, ListMap(name -> -\/(DocVar.ROOT()))))) {
                  case (
                    CollectionBuilderF(_, _, srcStruct),
                    CollectionBuilderF(graph, _, _)) =>
                    CollectionBuilderF(
                      chain(graph, op),
                      DocField(name),
                      srcStruct)
                })))
        }
        // Every argument has a deref (so, a BsonField that can be given to the op)
        def case2(src: WorkflowBuilder, input: WorkflowBuilder, base: DocVar, op: PartialFunction[List[BsonField], Workflow => Workflow], fields: List[BsonField]): M[CollectionBuilderF] = {
          ((toCollectionBuilder(src) |@| toCollectionBuilder(input)) {
            case (
              CollectionBuilderF(_, _, srcStruct),
              CollectionBuilderF(graph, base0, bothStruct)) =>
              op.lift(fields.map(f => base0.deref.map(_ \ f).getOrElse(f))).fold[M[CollectionBuilderF]](
                fail[CollectionBuilderF](WorkflowBuilderError.InvalidOperation("filter", "failed to build operation")))(
                { op =>
                  val g = chain(graph, op)
                  if (srcStruct == bothStruct)
                    emit(CollectionBuilderF(g, base0 \\ base, srcStruct))
                  else {
                    val (g1, base1) = shift(base0 \\ base, srcStruct, g)
                    emit(CollectionBuilderF(g1, base1, srcStruct))
                  }
              })
          }).join
        }
        fold1Builders(inputs).fold(
          toCollectionBuilder(src).map {
            case CollectionBuilderF(g, b, s) =>
              CollectionBuilderF(op(Nil)(g), b, s)
          })(
          _.flatMap { case (input, fields) =>
            foldBuilders(src, inputs).flatMap { case (input1, base, fields) =>
              fields.map(_.deref).sequence.fold(
                case1(src, input1, op, fields))(
                case2(src, input1, base, op, _))
            }
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
                js => $simpleMap(NonEmptyList(MapExpr(JsFn(jsBase, JsCore.Obj(ListMap(name.asText -> js(jsBase.fix))).fix))), ListMap()))),
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
                  jsExprs => $simpleMap(NonEmptyList(
                    MapExpr(JsFn(jsBase,
                      Term(JsCore.Obj(jsExprs.map {
                        case (name, expr) => name.asText -> expr(jsBase.fix)
                      }))))),
                    ListMap()))),
              DocVar.ROOT(),
              Some(shape.toList.map(_._1.asText)))))
        }
      case ArrayBuilderF(src, shape) =>
        workflow(src).flatMap { case (wf, base) =>
          lift(shape.map(_.fold(ExprOp.toJs, \/-(_))).sequenceU.map(jsExprs =>
            CollectionBuilderF(
              chain(wf,
                $simpleMap(NonEmptyList(
                  MapExpr(JsFn(jsBase,
                    JsCore.Arr(jsExprs.map(_(base.toJs(jsBase.fix))).toList).fix))),
                  ListMap())),
              DocVar.ROOT(),
              None)))
        }
      case GroupBuilderF(src, keys, content, _) =>
        foldBuilders(src, keys).flatMap { case (wb, base, fields) =>
          def key(base: DocVar) = keys.zip(fields) match {
            case Nil        => -\/(Literal(Bson.Null))
            case (key, field) :: Nil => -\/(key.unFix match {
              // NB: normalize to Null, to ease merging
              case ValueBuilderF(_)                 => Literal(Bson.Null)
              case ExprBuilderF(_, -\/(Literal(_))) => Literal(Bson.Null)
              case _                                => field.rewriteRefs(prefixBase(base))
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
                        (grouped ∘ (_.rewriteRefs(prefixBase(DocField(groupedName) \\ base0)))) +
                          (ungroupedName -> Push(DocField(ungroupedName)))),
                        key(DocField(groupedName) \\ base0)),
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
      case FlatteningBuilderF(src, fields) =>
        toCollectionBuilder(src).map {
          case CollectionBuilderF(graph, base, struct) =>
            CollectionBuilderF(fields.foldRight(graph) {
              case (StructureType.Array(field), acc) => $unwind(base \\ field)(acc)
              case (StructureType.Object(field), acc) =>
                $simpleMap(NonEmptyList(FlatExpr(JsFn(jsBase, (base \\ field).toJs(jsBase.fix)))), ListMap())(acc)
            }, base, struct)
        }
      case sb @ SpliceBuilderF(_, _) =>
        workflow(sb.src).flatMap { case (wf, base) =>
          lift(
            sb.toJs.map { splice =>
              CollectionBuilderF(
                chain(wf,
                  $simpleMap(NonEmptyList(MapExpr(JsFn(jsBase, (base.toJs >>> splice)(jsBase.fix)))), ListMap())),
                DocVar.ROOT(),
                None)
            })
        }
      case sb @ ArraySpliceBuilderF(_, _) =>
        workflow(sb.src).flatMap { case (wf, base) =>
          lift(
            sb.toJs.map { splice =>
              CollectionBuilderF(
                chain(wf,
                  $simpleMap(NonEmptyList(MapExpr(JsFn(jsBase, (base.toJs >>> splice)(jsBase.fix)))), ListMap())),
                DocVar.ROOT(),
                None)
            })
        }
    }
  }

  def workflow(wb: WorkflowBuilder): M[(Workflow, DocVar)] =
    toCollectionBuilder(wb).map(x => (x.graph, x.base))

  def shift(base: DocVar, struct: Schema, graph: Workflow): (Workflow, DocVar) =
    (base, struct) match {
      case (ExprVar, None)         => (graph, ExprVar)
      case (_,       None)         =>
        (chain(graph,
          Workflow.$project(Reshape(ListMap(ExprName -> -\/(base))),
            ExcludeId)),
        ExprVar)
      case (_,       Some(fields)) =>
        (chain(graph,
          Workflow.$project(Reshape(fields.map(name =>
            BsonField.Name(name) ->
              -\/(base \ BsonField.Name(name))).toListMap),
            if (fields.exists(_ == IdLabel)) IncludeId else ExcludeId)),
        DocVar.ROOT())
    }

  def build(wb: WorkflowBuilder): M[Workflow] =
    toCollectionBuilder(wb).map {
      case CollectionBuilderF(graph, base, struct) =>
        finish(
          if (base == DocVar.ROOT(None)) graph
          else shift(base, struct, graph)._1)
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

  private def foldBuilders(src: WorkflowBuilder, others: List[WorkflowBuilder]): M[(WorkflowBuilder, DocVar, List[DocVar])] =
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


  def inlineExprs[R](contents: DocContents[ExprOp \/ R], expr: ExprOp): Option[ExprOp] =
    contents match {
      case Expr(-\/(dv @ DocVar(_, _))) => Some(expr.rewriteRefs(prefixBase(dv)))
      case Expr(-\/(ex)) => Some(expr.mapUp { case DocVar.ROOT(None) => ex })
      case Doc(map) => expr.mapUpM {
        case DocField(field @ BsonField.Name(_)) => map.get(field).flatMap(_.fold(Some(_), κ(None)))
      }
      case _ => None
    }

  def expr1(wb: WorkflowBuilder)(f: ExprOp => ExprOp): M[WorkflowBuilder] =
    expr(List(wb)) { case List(e) => f(e) }

  def expr2(
    wb1: WorkflowBuilder, wb2: WorkflowBuilder)(
    f: (ExprOp, ExprOp) => ExprOp):
      M[WorkflowBuilder] =
    expr(List(wb1, wb2)) { case List(e1, e2) => f(e1, e2) }

  def coalesceSource(src: WorkflowBuilder, expr: ExprOp): WorkflowBuilder = {
    lazy val default = ExprBuilder(src, -\/(expr))
    def inln[R](cont: DocContents[ExprOp \/ R])(f: ExprOp => WorkflowBuilder) =
      inlineExprs(cont, expr).fold(default)(f)

    src.unFix match {
      case ExprBuilderF(wb0, \/-(js1)) =>
        toJs(expr).fold(κ(default), js => ExprBuilder(wb0, \/-(js1 >>> js)))
      case ExprBuilderF(src0, contents) =>
        inln(Expr(contents))(expr => ExprBuilder(src0, -\/(expr)))
      case DocBuilderF(src0, contents) =>
        inln(Doc(contents))(expr => ExprBuilder(src0, -\/(expr)))
      case ShapePreservingBuilderF(src0, inputs, op) =>
        ShapePreservingBuilder(coalesceSource(src0, expr), inputs, op)
      case GroupBuilderF(wb0, key, Expr(-\/(DocVar.ROOT(None))), id) =>
        GroupBuilder(
          coalesceSource(wb0, expr),
          key,
          Expr(-\/(DocVar.ROOT())),
          id)
      case GroupBuilderF(wb0, key, contents, id) =>
        inln(contents)(expr => GroupBuilder(wb0, key, Expr(-\/(expr)), id))
      case _ => default
    }
  }

  def expr(wbs: List[WorkflowBuilder])
    (f: List[ExprOp] => ExprOp): M[WorkflowBuilder] = {
    fold1Builders(wbs).fold[M[WorkflowBuilder]](
      fail(WorkflowBuilderError.InvalidOperation("expr", "impossible – no arguments")))(
      _.map { case (wb, exprs) => coalesceSource(wb, f(exprs)) })
  }

  def jsExpr1(wb: WorkflowBuilder, js: JsFn): Error \/ WorkflowBuilder =
    wb.unFix match {
      case ShapePreservingBuilderF(src, inputs, op) =>
        jsExpr1(src, js).map(ShapePreservingBuilder(_, inputs, op))
      case ExprBuilderF(wb1, -\/ (expr1)) =>
        toJs(expr1).map(js1 => ExprBuilder(wb1, \/-(js1 >>> js)))
      case ExprBuilderF(wb1,  \/-(js1)) =>
        \/-(ExprBuilder(wb1, \/-(js1 >>> js)))
      case GroupBuilderF(wb0, key, Expr(-\/(expr)), id) =>
        ExprOp.toJs(expr).flatMap(
          ex => jsExpr1(wb0, JsFn(jsBase, ex(js(jsBase.fix)))).map(
            GroupBuilder(_, key, Expr(-\/(DocVar.ROOT())), id)))
      case _ => \/-(ExprBuilder(wb, \/-(js)))
    }

  def jsExpr2(wb1: WorkflowBuilder, wb2: WorkflowBuilder, js: (Term[JsCore], Term[JsCore]) => Term[JsCore]): M[WorkflowBuilder] =
    (wb1.unFix, wb2.unFix) match {
      case (_, ValueBuilderF(JsCore(lit))) =>
        lift(jsExpr1(wb1, JsFn(jsBase, js(jsBase.fix, lit))))
      case (ValueBuilderF(JsCore(lit)), _) =>
        lift(jsExpr1(wb2, JsFn(jsBase, js(lit, jsBase.fix))))
      case _ =>
        merge(wb1, wb2).map { case (lbase, rbase, src) =>
          ExprBuilder(src, \/-(JsFn(jsBase, js(lbase.toJs(jsBase.fix), rbase.toJs(jsBase.fix)))))
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

  def mergeContents[A](c1: DocContents[A], c2: DocContents[A]):
      M[((DocVar, DocVar), DocContents[A])] = {
    def documentize(c: DocContents[A]):
        State[NameGen, (DocVar, ListMap[BsonField.Name, A])] =
      c match {
        case Doc(d) => state(DocVar.ROOT() -> d)
        case Expr(cont) =>
          freshName.map(name => (DocField(name), ListMap(name -> cont)))
      }

    lazy val doc =
      swapM((documentize(c1) |@| documentize(c2)) {
        case ((lb, lshape), (rb, rshape)) =>
          Reshape.mergeMaps(lshape, rshape).fold[Error \/ ((DocVar, DocVar), DocContents[A])](
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

        case (ShapePreservingBuilderF(s, i, o), _) =>
          impl(s, wb2, combine).map(ShapePreservingBuilder(_, i, o))
        case (_, ShapePreservingBuilderF(_, _, _)) => delegate

        case (ArrayBuilderF(src1, shape1), ArrayBuilderF(src2, shape2)) =>
          merge(src1, src2).map { case (lbase, rbase, wb) =>
            ArrayBuilder(wb,
              shape1.map(rewriteExprPrefix(_, lbase)) ++
                shape2.map(rewriteExprPrefix(_, rbase)))
          }
        case (ArrayBuilderF(src1, shape1), ExprBuilderF(src2, expr2)) =>
          merge(src1, src2).map { case (left, right, list) =>
            ArraySpliceBuilder(list, combine(
              Array(shape1.map(x => rewriteExprPrefix(x, left))),
              Expr(rewriteExprPrefix(expr2, right)))(List(_, _)))
          }
        case (ExprBuilderF(_, _), ArrayBuilderF(_, _)) => delegate

        case (ValueBuilderF(Bson.Arr(seq1)), ExprBuilderF(src2, expr2)) =>
          emit(ArraySpliceBuilder(src2, combine(
            Array(seq1.map(x => -\/(Literal(x)))),
            Expr(expr2))(List(_, _))))
        case (ExprBuilderF(_, _), ValueBuilderF(Bson.Arr(_))) => delegate

        case (ArraySpliceBuilderF(src1, structure1), ArrayBuilderF(src2, shape2)) =>
          merge(src1, src2).map { case (left, right, list) =>
            ArraySpliceBuilder(list, combine(
              structure1,
              List(Array(shape2.map(rewriteExprPrefix(_, right)))))(_ ++ _))
          }
        case (ArrayBuilderF(_, _), ArraySpliceBuilderF(_, _)) => delegate

        case (ArraySpliceBuilderF(src1, structure1), ExprBuilderF(src2, expr2)) =>
          merge(src1, src2).map { case (left, right, list) =>
            ArraySpliceBuilder(list, combine(
              structure1,
              List(Expr(rewriteExprPrefix(expr2, right))))(_ ++ _))
          }
        case (ExprBuilderF(_, _), ArraySpliceBuilderF(_, _)) => delegate

        case _ =>
          fail(WorkflowBuilderError.InvalidOperation(
            "arrayConcat",
            "values are not both arrays"))
      }
    }

    impl(left, right, unflipped)
  }

  def flattenObject(wb: WorkflowBuilder): M[WorkflowBuilder] = wb.unFix match {
    case ShapePreservingBuilderF(src, inputs, op) =>
      flattenObject(src).map(ShapePreservingBuilder(_, inputs, op))
    case GroupBuilderF(src, keys, Expr(-\/(DocVar.ROOT(None))), id) =>
      flattenObject(src).map(GroupBuilder(_, keys, Expr(-\/(DocVar.ROOT())), id))
    case _ =>
      expr1(wb)(base =>
          Cond(
            And(NonEmptyList(
              Lte(Literal(Bson.Doc(ListMap())), base),
              Lt(base, Literal(Bson.Arr(List()))))),
            base,
            Literal(Bson.Doc(ListMap("" -> Bson.Null))))).map(
        FlatteningBuilder(
          _,
          Set(StructureType.Object(DocVar.ROOT()))))
  }

  def flattenArray(wb: WorkflowBuilder): M[WorkflowBuilder] = wb.unFix match {
    case ShapePreservingBuilderF(src, inputs, op) =>
      flattenArray(src).map(ShapePreservingBuilder(_, inputs, op))
    case GroupBuilderF(src, keys, Expr(-\/(DocVar.ROOT(None))), id) =>
      flattenArray(src).map(GroupBuilder(_, keys, Expr(-\/(DocVar.ROOT())), id))
    case _ =>
      expr1(wb)(base =>
          Cond(
            And(NonEmptyList(
              Lte(Literal(Bson.Arr(List())), base),
              Lt(base, Literal(Bson.Binary(scala.Array[Byte]()))))),
            base,
            Literal(Bson.Arr(List(Bson.Null))))).map(
        FlatteningBuilder(
          _,
          Set(StructureType.Array(DocVar.ROOT()))))
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
          \/-(JsFn(jsBase, DocField(BsonField.Name(name)).toJs(js1(jsBase.fix))))))
      case ExprBuilderF(wb, -\/(DocField(field))) =>
        \/-(ExprBuilder(wb, -\/(DocField(field \ BsonField.Name(name)))))
      case _ => \/-(ExprBuilder(wb, -\/(DocField(BsonField.Name(name)))))
    }

  def projectIndex(wb: WorkflowBuilder, index: Int): Error \/ WorkflowBuilder =
    wb.unFix match {
      case ValueBuilderF(Bson.Arr(elems)) =>
        if (index < elems.length) // UGH!
          \/-(ValueBuilder(elems(index)))
        else
          -\/(WorkflowBuilderError.InvalidOperation(
            "projectIndex",
            "value does not contain index ‘" + index + "’."))
      case ArrayBuilderF(wb0, elems) =>
        if (index < elems.length) // UGH!
          \/-(ExprBuilder(wb0, elems(index)))
        else
          -\/(WorkflowBuilderError.InvalidOperation(
            "projectIndex",
            "array does not contain index ‘" + index + "’."))
      case ValueBuilderF(_) =>
        -\/(WorkflowBuilderError.InvalidOperation(
          "projectIndex",
          "value is not an array."))
      case DocBuilderF(_, _) =>
        -\/(WorkflowBuilderError.InvalidOperation(
          "projectIndex",
          "value is not an array."))
      case _ =>
        jsExpr1(wb, JsFn(jsBase,
          JsCore.Access(jsBase.fix, JsCore.Literal(Js.Num(index, false)).fix).fix))
    }

  def deleteField(wb: WorkflowBuilder, name: String):
      Error \/ WorkflowBuilder =
    wb.unFix match {
      case ShapePreservingBuilderF(src, inputs, op) =>
        deleteField(src, name).map(ShapePreservingBuilder(_, inputs, op))
      case ValueBuilderF(Bson.Doc(fields)) =>
        \/-(ValueBuilder(Bson.Doc(fields - name)))
      case ValueBuilderF(_) =>
        -\/(WorkflowBuilderError.InvalidOperation(
          "deleteField",
          "value is not a document."))
      case GroupBuilderF(wb0, key, Expr(-\/(DocVar.ROOT(None))), id) =>
        deleteField(wb0, name).map(GroupBuilder(_, key, Expr(-\/(DocVar.ROOT())), id))
      case GroupBuilderF(wb0, key, Doc(doc), id) =>
        \/-(GroupBuilder(wb0, key, Doc(doc - BsonField.Name(name)), id))
      case DocBuilderF(wb0, doc) =>
        \/-(DocBuilder(wb0, doc - BsonField.Name(name)))
      case _ => jsExpr1(wb, JsFn(jsBase,
        // FIXME: Need to pull this back up from the top level (#663)
        JsCore.Call(JsCore.Ident("remove").fix,
          List(jsBase.fix, JsCore.Literal(Js.Str(name)).fix)).fix))
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
          case _                                  => GroupId(List(wb))
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

  // TODO: This is an approximation. If we could postpone this decision until
  //      `Workflow.crush`, when we actually have a task (whether aggregation or
  //       mapReduce) in hand, we would know for sure.
  def requiresMapReduce(wb: WorkflowBuilder): Boolean = {
    // TODO: Get rid of this when we functorize WorkflowTask
    def checkTask(wt: WorkflowTask): Boolean = wt match {
      case WorkflowTask.FoldLeftTask(_, _)   => true
      case WorkflowTask.MapReduceTask(_, _)  => true
      case WorkflowTask.PipelineTask(src, _) => checkTask(src)
      case _                                 => false
    }

    workflow(wb).evalZero.fold(
      κ(false),
      wf => checkTask(task(wf._1)))
  }

  def join(left0: WorkflowBuilder, right0: WorkflowBuilder,
    tpe: slamdata.engine.LogicalPlan.JoinType, comp: Mapping,
    leftKey0: WorkflowBuilder, leftJs0: JsFn,
    rightKey0: WorkflowBuilder, rightJs0: JsFn):
      M[WorkflowBuilder] = {

    import slamdata.engine.LogicalPlan.JoinType
    import slamdata.engine.LogicalPlan.JoinType._
    import Js._

    // FIXME: these have to match the names used in the logical plan
    val leftField0: BsonField.Name = BsonField.Name("left")
    val rightField0: BsonField.Name = BsonField.Name("right")

    val (left, right, leftKey, rightKey, leftField, rightField) =
      if (requiresMapReduce(left0) && !requiresMapReduce(right0))
        (right0, left0, rightKey0, leftJs0, rightField0, leftField0)
      else
        (left0, right0, leftKey0, rightJs0, leftField0, rightField0)

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

    def rightMap(keyExpr: JsFn): AnonFunDecl =
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
      ValueBuilder(Bson.Null), JsFn.const(JsCore.Literal(Js.Null).fix),
      ValueBuilder(Bson.Null), JsFn.const(JsCore.Literal(Js.Null).fix))

  def limit(wb: WorkflowBuilder, count: Long) =
    ShapePreservingBuilder(wb, Nil, { case Nil => $limit(count) })

  def skip(wb: WorkflowBuilder, count: Long) =
    ShapePreservingBuilder(wb, Nil, { case Nil => $skip(count) })

  def squash(wb: WorkflowBuilder): WorkflowBuilder = wb

  def distinctBy(src: WorkflowBuilder, keys: List[WorkflowBuilder]):
      M[WorkflowBuilder] = {
    def sortKeys(wf: Workflow): Error \/ List[(BsonField, SortType)] = {
      def isOrdered(wf: Workflow): Boolean = wf.unFix match {
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
      def loop(wf: Workflow): Error \/ List[(BsonField, SortType)] =
        wf.unFix match {
          case $Sort(_, keys) => \/-(keys.list)
          case $Project(Term($Sort(_, keys)), shape, _) =>
            keys.list.map {
              case (field, sortType) =>
                Reshape.getAll(shape).collectFirst {
                  case (k, dv @ DocVar.ROOT(prefix))
                      if DocVar.ROOT(field).startsWith(dv) =>
                    k \\ field.flatten.toList.drop(prefix.fold(0)(_.flatten.size))
                }.map(_ -> sortType)
            }.sequence.fold[Error \/ List[(BsonField, SortType)]](-\/(WorkflowBuilderError.UnsupportedDistinct("cannot distinct with missing keys: " + wf)))(\/-(_))
          case sp: ShapePreservingF[_] => loop(sp.src)
          case _ =>
            if (isOrdered(wf))
              -\/(WorkflowBuilderError.UnsupportedDistinct("cannot distinct with unrecognized ordered source: " + wf))
            else \/-(Nil)
        }

      loop(wf)
    }

    def findKeys(wb: WorkflowBuilder): Option[ExprOp \/ Reshape] = {
      def reshape(keys: Iterable[BsonField.Name]): ExprOp \/ Reshape =
        \/-(Reshape(keys.map(_ -> -\/(Include)).toList.toListMap))
      wb.unFix match {
        case CollectionBuilderF(_, _, s2)       =>
          s2.map(x => reshape(x.map(BsonField.Name)))
        case DocBuilderF(_, shape)              => Some(reshape(shape.keys))
        case GroupBuilderF(_, _, Doc(obj), _)   => Some(reshape(obj.keys))
        case ShapePreservingBuilderF(src, _, _) => findKeys(src)
        case ExprBuilderF(_, _)                 => Some(-\/(DocVar.ROOT()))
        case ValueBuilderF(Bson.Doc(shape))     =>
          Some(reshape(shape.keys.map(BsonField.Name)))
        case _                                  => None
      }
    }

    val keyPrefix = "__sd_key_"

    for {
      t1 <- foldBuilders(src, keys)
      (merged, value, fields) = t1

      b2 <- toCollectionBuilder(merged)
      CollectionBuilderF(graph, base, struct) = b2

      sk <- lift(sortKeys(graph))

      name <- emitSt(freshName)
    } yield {
      val keyProjs = sk.zipWithIndex.map {
        case ((name, _), index) =>
          BsonField.Name(keyPrefix + index.toString) -> First(DocField(name))
      }

      val groupedBy = keys.zip(fields) match {
        case Nil        => Some(-\/(Literal(Bson.Null)))
        case (key, field) :: Nil => field match {
          // If the key is at the document root, we must explicitly
          //  project out the fields so as not to include a meaningless
          // _id in the key:
          case DocVar.ROOT(None) => findKeys(key)
          case _                 => Some(-\/(field))
        }
        case _          => Some(\/-(Reshape(fields.zipWithIndex.map {
          case (field, index) => BsonField.Index(index).toName -> -\/(field)
        }.toListMap)))
      }

      val group = CollectionBuilderF(
          groupedBy.fold(
            chain(
              graph,
              $simpleMap(NonEmptyList(
                MapExpr(JsFn(jsBase,
                  JsCore.Call(JsCore.Ident("remove").fix,
                    List(jsBase.fix, JsCore.Literal(Js.Str("_id")).fix)).fix))),
                ListMap()),
              $group(
                Grouped(ListMap(name -> First(DocVar.ROOT()) :: keyProjs: _*)),
                -\/(DocVar.ROOT()))))(
            gby => chain(
              graph,
              $group(
                Grouped(ListMap(name -> First(base \\ value) :: keyProjs: _*)),
                gby.bimap(
                  _.rewriteRefs(prefixBase(base)),
                  _.rewriteRefs(prefixBase(base)))))),
          DocField(name),
          struct)

      val keyPairs = sk.zipWithIndex.map { case ((name, sortType), index) =>
        BsonField.Name(keyPrefix + index.toString) -> sortType
      }
      val op = keyPairs.toNel.map { keyPairs =>
        group.copy(graph = chain(group.graph, $sort(keyPairs)))
      }.getOrElse(group)

      Term[WorkflowBuilderF](op)
    }
  }

  private def merge(left: WorkflowBuilder, right: WorkflowBuilder):
      M[(DocVar, DocVar, WorkflowBuilder)] = {
    def delegate =
      merge(right, left).map { case (r, l, merged) => (l, r, merged) }

    (left.unFix, right.unFix) match {
      case (
        ExprBuilderF(src1, -\/(base1 @ DocField(_))),
        ExprBuilderF(src2, -\/(base2 @ DocField(_))))
          if src1 === src2 =>
        emit((base1, base2, src1))

      case _ if left === right => emit((DocVar.ROOT(), DocVar.ROOT(), left))

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

      case (ExprBuilderF(src1, expr1), ExprBuilderF(src2, expr2)) if src1 === src2 =>
        mergeContents(Expr(expr1), Expr(expr2)).map {
          case ((lbase, rbase), cont) =>
            (lbase, rbase,
              cont match {
                case Expr(expr) => ExprBuilder(src1, expr)
                case Doc(doc)   => DocBuilder(src1, doc)
              })
        }
      case (ExprBuilderF(src, -\/(base @ DocField(_))), _) if src === right =>
        emit((base, DocVar.ROOT(), right))
      case (_, ExprBuilderF(src, -\/(DocField(_)))) if left === src =>
        delegate

      case (DocBuilderF(src1, shape1), ExprBuilderF(src2, expr2)) if src1 === src2 =>
        mergeContents(Doc(shape1), Expr(expr2)).map {
          case ((lbase, rbase), cont) =>
            (lbase, rbase,
              cont match {
                case Expr(expr) => ExprBuilder(src1, expr)
                case Doc(doc)  => DocBuilder(src1, doc)
              })
        }
      case (ExprBuilderF(src1, _), DocBuilderF(src2, _)) if src1 === src2 =>
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

      case (sb @ SpliceBuilderF(_, _), _) =>
        merge(sb.src, right).flatMap { case (lbase, rbase, wb) =>
          for {
            lName  <- emitSt(freshName)
            rName  <- emitSt(freshName)
            splice <- lift(sb.toJs)
          } yield (DocField(lName), DocField(rName),
            DocBuilder(wb, ListMap(
              lName ->  \/-(lbase.toJs >>> splice),
              rName -> -\/ (rbase))))
        }
      case (_, SpliceBuilderF(_, _)) => delegate

      case (sb @ ArraySpliceBuilderF(_, _), _) =>
        merge(sb.src, right).flatMap { case (lbase, rbase, wb) =>
          for {
            lName  <- emitSt(freshName)
            rName  <- emitSt(freshName)
            splice <- lift(sb.toJs)
          } yield (DocField(lName), DocField(rName),
            DocBuilder(wb, ListMap(
              lName ->  \/-(lbase.toJs >>> splice),
              rName -> -\/ (rbase))))
        }
      case (_, ArraySpliceBuilderF(_, _)) => delegate

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
        FlatteningBuilderF(src0, fields0),
        FlatteningBuilderF(src1, fields1)) =>
        left.cata(branchLengthƒ) cmp right.cata(branchLengthƒ) match {
          case Ordering.LT =>
            merge(left, src1).map { case (lbase, rbase, wb) =>
              (lbase, rbase, FlatteningBuilder(wb, fields1.map(_.rewrite(rbase \\ _))))
            }
          case Ordering.EQ =>
            merge(src0, src1).map { case (lbase, rbase, wb) =>
              val lfields = fields0.map(_.rewrite(lbase \\ _))
              val rfields = fields1.map(_.rewrite(rbase \\ _))
              (lbase, rbase, FlatteningBuilder(wb, lfields union rfields))
            }
          case Ordering.GT =>
            merge(src0, right).map { case (lbase, rbase, wb) =>
              (lbase, rbase, FlatteningBuilder(wb, fields0.map(_.rewrite(lbase \\ _))))
            }
        }
      case (FlatteningBuilderF(src, fields), _) =>
        merge(src, right).flatMap { case (lbase, rbase, wb) =>
          val lfields = fields.map(_.rewrite(lbase \\ _))
          if (lfields.exists(x => x.field.startsWith(rbase) || rbase.startsWith(x.field)))
            for {
              lName <- emitSt(freshName)
              rName <- emitSt(freshName)
            } yield
              (DocField(lName), DocField(rName),
                FlatteningBuilder(
                  DocBuilder(wb, ListMap(
                    lName -> -\/(lbase),
                    rName -> -\/(rbase))),
                  fields.map(_.rewrite(DocField(lName) \\ _))))
          else emit((lbase, rbase, FlatteningBuilder(wb, lfields)))
        }
      case (_, FlatteningBuilderF(_, _)) => delegate

      case (
        spb1 @ ShapePreservingBuilderF(src1, inputs1, op1),
        spb2 @ ShapePreservingBuilderF(src2, inputs2, _))
          if inputs1 === inputs2 && ShapePreservingBuilder.dummyOp(spb1) == ShapePreservingBuilder.dummyOp(spb2) =>
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

  implicit def WorkflowBuilderRenderTree(implicit RO: RenderTree[Workflow], RE: RenderTree[ExprOp], REx: RenderTree[Expr], RG: RenderTree[Contents[GroupValue]], RC: RenderTree[Contents[Expr]]): RenderTree[WorkflowBuilder] = new RenderTree[WorkflowBuilder] {
    val nodeType = "WorkflowBuilder" :: Nil

    def render(v: WorkflowBuilder) = v.unFix match {
      case CollectionBuilderF(graph, base, struct) =>
        NonTerminal("CollectionBuilder" :: nodeType, None,
          RO.render(graph) ::
            RE.render(base) ::
            Terminal("Schema" :: "CollectionBuilder" :: nodeType, Some(struct.toString)) ::
            Nil)
      case spb @ ShapePreservingBuilderF(src, inputs, op) =>
        val nt = "ShapePreservingBuilder" :: nodeType
        NonTerminal(nt, None,
          render(src) ::
            (inputs.map(render) :+
              Terminal("Op" :: nt, Some(ShapePreservingBuilder.dummyOp(spb).toString))))
      case ValueBuilderF(value) =>
        Terminal("ValueBuilder" :: nodeType, Some(value.toString))
      case ExprBuilderF(src, expr) =>
        NonTerminal("ExprBuilder" :: nodeType, None,
          render(src) :: REx.render(expr) :: Nil)
      case DocBuilderF(src, shape) =>
        val nt = "DocBuilder" :: nodeType
        NonTerminal(nt, None,
          render(src) ::
            NonTerminal("Shape" :: nt, None,
              shape.toList.map {
                case (name, expr) =>
                  val t = REx.render(expr)
                  t.copy(label = Some(name.asText + " -> " + t.label.getOrElse("None")))
               }) ::
            Nil)
      case ArrayBuilderF(src, shape) =>
        val nt = "ArrayBuilder" :: nodeType
        NonTerminal(nt, None,
          render(src) ::
            NonTerminal("Shape" :: nt, None, shape.map(REx.render)) ::
            Nil)
      case GroupBuilderF(src, keys, content, id) =>
        val nt = "GroupBuilder" :: nodeType
        NonTerminal(nt, None,
          render(src) ::
            NonTerminal("By" :: nt, None, keys.map(render)) ::
            RG.render(content).copy(nodeType = "Content" :: nt) ::
            Terminal("Id" :: nt, Some(id.toString)) ::
            Nil)
      case FlatteningBuilderF(src, fields) =>
        val nt = "FlatteningBuilder" :: nodeType
        NonTerminal(nt, None,
          render(src) ::
            fields.toList.map(x => RE.render(x.field).copy(nodeType = (x match {
              case StructureType.Array(_) => "Array"
              case StructureType.Object(_) => "Object"
            }) :: nt)))
      case SpliceBuilderF(src, structure) =>
        NonTerminal("SpliceBuilder" :: nodeType, None,
          render(src) :: structure.map(RC.render(_)))
      case ArraySpliceBuilderF(src, structure) =>
        NonTerminal("ArraySpliceBuilder" :: nodeType, None,
          render(src) :: structure.map(RC.render(_)))
    }
  }
}
