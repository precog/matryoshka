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

package quasar.physical.mongodb

import quasar.Predef._
import quasar.{NonTerminal, RenderTree, Terminal}, RenderTree.ops._
import quasar.fp._
import quasar.namegen._
import quasar._, Planner._
import quasar.recursionschemes._, Recursive.ops._
import quasar.fs.Path
import quasar.javascript._
import quasar.std.StdLib._
import quasar.jscore, jscore.{JsCore, JsFn}

import scalaz._, Scalaz._
import shapeless.contrib.scalaz.instances.deriveEqual

sealed trait WorkflowBuilderF[+A]

object WorkflowBuilder {
  import quasar.physical.mongodb.accumulator._
  import quasar.physical.mongodb.expression._
  import Workflow._
  import IdHandling._

  /** A partial description of a query that can be run on an instance of MongoDB */
  type WorkflowBuilder = Fix[WorkflowBuilderF]
  /** If we know what the shape is, represents the list of Fields. */
  type Schema = Option[NonEmptyList[BsonField.Name]]

  /**
   * Either arbitrary javascript expression or Pipeline expression
   * An arbitrary javascript is more powerful but less performant because it's get
   * materialized into a Map/Reduce operation.
   */
  type Expr = JsFn \/ Expression
  private def exprToJs(expr: Expr) = expr.fold(\/-(_), toJs)
  implicit val ExprRenderTree = new RenderTree[Expr] {
    def render(x: Expr) =
      x.fold(
        _.render,
        op => Terminal(List("ExprOp"), Some(op.toString)))
  }

  /**
   * Like ValueBuilder, this is a Leaf node which can be used to construct a more complicated WorkflowBuilder.
   * Takes a value resulting from a Workflow and wraps it in a WorkflowBuilder.
   * For example: If you want to read from MongoDB and then project on a field, the read would be the
   * CollectionBuilder.
   * @param base Name, or names under which the values produced by the src will be found.
   *             It's most often `Root`, or else it's probably a temporary `Field`
   * @param struct In the case of read, it's None. In the case where we are converting a WorkflowBuilder into
   *               a Workflow, we have access to the shape of this Workflow and encode it in `struct`.
   */
  final case class CollectionBuilderF(
    src: Workflow,
    base: Base,
    struct: Schema) extends WorkflowBuilderF[Nothing]
  object CollectionBuilder {
    def apply(graph: Workflow, base: Base, struct: Schema) =
      Fix[WorkflowBuilderF](new CollectionBuilderF(graph, base, struct))
  }

  /** For instance, $match, $skip, $limit, $sort */
  final case class ShapePreservingBuilderF[A](
    src: A,
    inputs: List[A],
    op: PartialFunction[List[BsonField], WorkflowOp])
      extends WorkflowBuilderF[A]
  {
    lazy val dummyOp =
      op(
        inputs.zipWithIndex.map {
          case (_, index) => BsonField.Name("_" + index)
        })(
        // Nb. This read is an arbitrary value that allows us to compare the partial function
        $read(Collection("", "")))

    override def equals(that: scala.Any) = that match {
      case that @ ShapePreservingBuilderF(src1, inputs1, op1) =>
        src == src1 && inputs == inputs1 && dummyOp == that.dummyOp
      case _ => false
    }
    override def hashCode = List(src, inputs, dummyOp).hashCode
  }
  object ShapePreservingBuilder {
    def apply(
      src: WorkflowBuilder,
      inputs: List[WorkflowBuilder],
      op: PartialFunction[List[BsonField], WorkflowOp]) =
      Fix[WorkflowBuilderF](new ShapePreservingBuilderF(src, inputs, op))
  }

  /**
   * A query that produces a constant value.
   */
  final case class ValueBuilderF(value: Bson) extends WorkflowBuilderF[Nothing]
  object ValueBuilder {
    def apply(value: Bson) = Fix[WorkflowBuilderF](new ValueBuilderF(value))
  }

  /**
   * A query that applies an Expression operator to a source (which could be multiple values)
   * You can think of Expression as a function application in MongoDB that accepts values and produces
   * new values. It's kind of like a map.
   * The shape coming out of an ExprBuilder is unknown because of the fact that the Expression can be arbitrary.
   * @param src The values on which to apply the Expression
   * @param expr The expression that procudes a new set of values given a set of values.
   */
  final case class ExprBuilderF[A](src: A, expr: Expr) extends WorkflowBuilderF[A]
  object ExprBuilder {
    def apply(src: WorkflowBuilder, expr: Expr) =
      Fix[WorkflowBuilderF](new ExprBuilderF(src, expr))
  }

  /**
   * Same as an ExprBuilder but contains the shape of the resulting query.
   * The result is a document that maps the field Name to the resulting values
   * from applying the Expression associated with that name.
   * NB: The shape is more restrictive than $project because we may need to
   * convert it to a GroupBuilder, and a nested Reshape can be realized with
   * a chain of DocBuilders, leaving the collapsing to Workflow.coalesce.
   */
  final case class DocBuilderF[A](src: A, shape: ListMap[BsonField.Name, Expr])
      extends WorkflowBuilderF[A]
  object DocBuilder {
    def apply(src: WorkflowBuilder, shape: ListMap[BsonField.Name, Expr]) =
      Fix[WorkflowBuilderF](new DocBuilderF(src, shape))
  }

  final case class ArrayBuilderF[A](src: A, shape: List[Expr])
      extends WorkflowBuilderF[A]
  object ArrayBuilder {
    def apply(src: WorkflowBuilder, shape: List[Expr]) =
      Fix[WorkflowBuilderF](new ArrayBuilderF(src, shape))
  }

  sealed trait Contents[+A]
  sealed trait DocContents[+A] extends Contents[A]
  sealed trait ArrayContents[+A] extends Contents[A]
  object Contents {
    final case class Expr[A](contents: A) extends DocContents[A] with ArrayContents[A]
    final case class Doc[A](contents: ListMap[BsonField.Name, A]) extends DocContents[A]
    final case class Array[A](contents: List[A]) extends ArrayContents[A]

    implicit def ContentsRenderTree[A: RenderTree] =
      new RenderTree[Contents[A]] {
        val nodeType = "Contents" :: Nil

        def render(v: Contents[A]) =
          v match {
            case Expr(a)   => NonTerminal("Expr" :: nodeType, None, a.render :: Nil)
            case Doc(b)    => NonTerminal("Doc" :: nodeType, None, b.render :: Nil)
            case Array(as) => NonTerminal("Array" :: nodeType, None, as.map(_.render))
          }
      }
  }
  import Contents._

  def contentsToBuilder: Contents[Expr] => WorkflowBuilder => WorkflowBuilder = {
    case Expr(expr) => ExprBuilder(_, expr)
    case Doc(doc)   => DocBuilder(_, doc)
    case Array(arr) => ArrayBuilder(_, arr)
  }

  type GroupValue[A] = AccumOp[A] \/ A
  type GroupContents = DocContents[GroupValue[Expression]]

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
      Fix[WorkflowBuilderF](new GroupBuilderF(src, keys, contents, id))
  }

  sealed trait StructureType[A] {
    val field: A
  }
  object StructureType {
    final case class Array[A](field: A) extends StructureType[A]
    final case class Object[A](field: A) extends StructureType[A]

    implicit val StructureTypeTraverse = new Traverse[StructureType] {
      def traverseImpl[G[_], A, B](fa: StructureType[A])(f: A => G[B])(implicit G: Applicative[G]):
          G[StructureType[B]] =
        fa match {
          case Array(field) => f(field).map(Array(_))
          case Object(field) => f(field).map(Object(_))
        }
    }
  }

  final case class FlatteningBuilderF[A](src: A, fields: Set[StructureType[DocVar]])
      extends WorkflowBuilderF[A]
  object FlatteningBuilder {
    def apply(src: WorkflowBuilder, fields: Set[StructureType[DocVar]]) =
      Fix[WorkflowBuilderF](new FlatteningBuilderF(src, fields))
  }

  /**
    Holds a partially-unknown structure. `Expr` entries are unknown and `Doc`
    entries are known. There should be at least one Expr in the list, otherwise
    it should be a DocBuilder.
    */
  final case class SpliceBuilderF[A](src: A, structure: List[DocContents[Expr]])
      extends WorkflowBuilderF[A] {
    def toJs: PlannerError \/ JsFn =
      structure.map {
        case Expr(unknown) => exprToJs(unknown)
        case Doc(known)    => known.toList.map { case (k, v) =>
          exprToJs(v).map(k.asText -> _)
        }.sequenceU.map(ms => JsFn(jsBase, jscore.Obj(ms.map { case (k, v) => jscore.Name(k) -> v(jscore.Ident(jsBase)) }.toListMap)))
      }.sequenceU.map(srcs =>
        JsFn(jsBase, jscore.SpliceObjects(srcs.map(_(jscore.Ident(jsBase))))))
  }
  object SpliceBuilder {
    def apply(src: WorkflowBuilder, structure: List[DocContents[Expr]]) =
      Fix[WorkflowBuilderF](new SpliceBuilderF(src, structure))
  }

  final case class ArraySpliceBuilderF[A](src: A, structure: List[ArrayContents[Expr]])
      extends WorkflowBuilderF[A] {
    def toJs: PlannerError \/ JsFn =
      structure.map {
        case Expr(unknown) => exprToJs(unknown)
        case Array(known)  => known.map(exprToJs).sequenceU.map(
            ms => JsFn(jsBase, jscore.Arr(ms.map(_(jscore.Ident(jsBase))))))
      }.sequenceU.map(srcs =>
        JsFn(jsBase, jscore.SpliceArrays(srcs.map(_(jscore.Ident(jsBase))))))
  }
  object ArraySpliceBuilder {
    def apply(src: WorkflowBuilder, structure: List[ArrayContents[Expr]]) =
      Fix[WorkflowBuilderF](new ArraySpliceBuilderF(src, structure))
  }

  // NB: This instance can’t be derived, because of `dummyOp`.
  implicit def WorkflowBuilderEqualF = new EqualF[WorkflowBuilderF] {
    def equal[A: Equal](v1: WorkflowBuilderF[A], v2: WorkflowBuilderF[A]) = (v1, v2) match {
      case (CollectionBuilderF(g1, b1, s1), CollectionBuilderF(g2, b2, s2)) =>
        g1 == g2 && b1 == b2 && s1 ≟ s2
      case (v1 @ ShapePreservingBuilderF(s1, i1, _), v2 @ ShapePreservingBuilderF(s2, i2, _)) =>
        s1 ≟ s2 && i1 ≟ i2 && v1.dummyOp == v2.dummyOp
      case (ValueBuilderF(v1), ValueBuilderF(v2)) => v1 == v2
      case (ExprBuilderF(s1, e1), ExprBuilderF(s2, e2)) =>
        s1 ≟ s2 && e1 == e2
      case (DocBuilderF(s1, e1), DocBuilderF(s2, e2)) =>
        s1 ≟ s2 && e1 == e2
      case (ArrayBuilderF(s1, e1), ArrayBuilderF(s2, e2)) =>
        s1 ≟ s2 && e1 == e2
      case (GroupBuilderF(s1, k1, c1, i1), GroupBuilderF(s2, k2, c2, i2)) =>
        s1 ≟ s2 && k1 ≟ k2 && c1 == c2 && i1 == i2
      case (FlatteningBuilderF(s1, f1), FlatteningBuilderF(s2, f2)) =>
        s1 ≟ s2 && f1 == f2
      case (SpliceBuilderF(s1, i1), SpliceBuilderF(s2, i2)) =>
        s1 ≟ s2 && i1 == i2
      case (ArraySpliceBuilderF(s1, i1), ArraySpliceBuilderF(s2, i2)) =>
        s1 ≟ s2 && i1 == i2
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

  /** Simplify/coalesce certain shapes, eliminating extra layers that make it
    * harder to pattern match. Should be applied before `objectConcat`,
    * `arrayConcat`, or `merge`.
    */
  val normalizeƒ: WorkflowBuilderF[WorkflowBuilder] => WorkflowBuilder = {
    def collapse(outer: Expr, inner: ListMap[BsonField.Name, Expr]): Option[Expr] = {
      def rewriteExpr(t: Expression)(applyExpr: PartialFunction[ExprOp[Expr], Option[Expr]]): Option[Expr] =
        t.cataM[Option, Expr] { x =>
          applyExpr.lift(x).getOrElse {
            x.sequenceU.fold(
              κ(for {
                op <- x.map(exprToJs).sequenceU.toOption
                js <- toJsSimpleƒ(op).toOption
              } yield -\/(js)),
              {
                case $varF(_) => None
                case op => \/-(Fix(op)).some
              })
          }
        }

      outer.fold(
        js =>
          for {
            xs <- inner.map { case (n, x) =>
                    jscore.Select(jscore.Ident(js.param), n.value) -> exprToJs(x)
                  }.sequenceU.toOption
            expr1 <- Corecursive[Fix].apoM[jscore.JsCoreF, Option, JsCore](js.expr) {
                    case t @ jscore.Access(b, _) if b == jscore.Ident(js.param) =>
                      xs.get(t).map(_(jscore.Ident(js.param)).unFix.map(_.left))
                    case t => t.unFix.map(_.right[JsCore]).some
                  }
          } yield -\/(JsFn(js.param, expr1)),
        expr =>
          rewriteExpr(expr) {
            case $varF(DocVar(_, Some(f))) =>
              f.flatten match {
                case NonEmptyList(h @ BsonField.Name(_)) => inner.get(h)
                case ls =>
                  ls.head match {
                    case n @ BsonField.Name(_) => inner.get(n).flatMap {
                      case \/-($var(DocVar(b, None))) => BsonField(ls.tail).map(f => \/-($var(DocVar(b, Some(f)))))
                      case \/-($var(DocVar(b, Some(f)))) => \/-($var(DocVar(b, Some(BsonField(f.flatten :::> ls.tail))))).some
                      case _ => None
                    }
                    case _ => None
                  }
              }
          })
    }

    def inln(outerExpr: Expr, cont: DocContents[_ \/ Expression]) =
      outerExpr.fold(
        κ(None),
        expr => (cont match {
          case Expr(\/-($var(dv))) =>
            Some(rewriteExprRefs(expr)(prefixBase(dv)))
          case Expr(\/-(ex)) => expr.cataM[Option, Expression] {
            case $varF(DocVar.ROOT(None)) => ex.some
            case $varF(_)                 => None
            case x                        => Fix(x).some
          }
          case Doc(map) => expr.cataM[Option, Expression] {
            case $varF(DocField(field)) =>
              field.flatten.toList match {
                case (name @ BsonField.Name(_)) :: Nil =>
                  map.get(name).flatMap(_.toOption)
                case (name @ BsonField.Name(_)) :: tail =>
                  map.get(name).flatMap {
                    case \/-($var(dv)) => BsonField(tail).map(p => $var(dv \ p))
                    case _ => None
                  }
                case _ => None
              }
            case $varF(_) => None
            case x => Some(Fix(x))
          }
          case _ => None
        }))

    def loop(w: WorkflowBuilderF[WorkflowBuilder]): Option[WorkflowBuilder] = w match {
      case ExprBuilderF(src, \/-($$ROOT)) => src.some
      case ExprBuilderF(src, outerExpr) =>
        src.unFix match {
          case ExprBuilderF(wb0, -\/(js1)) =>
            exprToJs(outerExpr).map(js => ExprBuilder(wb0, -\/(js1 >>> js))).toOption
          case ExprBuilderF(src0, contents) =>
            inln(outerExpr, Expr(contents)).map(expr => ExprBuilder(src0, \/-(expr)))
          case DocBuilderF(src, innerShape) =>
            collapse(outerExpr, innerShape).map(ExprBuilder(src, _))
          case ShapePreservingBuilderF(src0, inputs, op) =>
            ShapePreservingBuilder(
              normalizeƒ(ExprBuilderF(src0, outerExpr)),
              inputs,
              op).some
          case GroupBuilderF(wb0, key, Expr(\/-($var(DocVar.ROOT(None)))), id) =>
            GroupBuilder(
              normalizeƒ(ExprBuilderF(wb0, outerExpr)),
              key,
              Expr(\/-($$ROOT)),
              id).some
          case GroupBuilderF(wb0, key, contents, id) =>
            inln(outerExpr, contents).map(expr => GroupBuilder(normalizeƒ(ExprBuilderF(wb0, \/-(expr))), key, Expr(\/-($$ROOT)), id))
          case _ => None
        }
      case DocBuilderF(Fix(DocBuilderF(src, innerShape)), outerShape) =>
        outerShape.traverse(collapse(_, innerShape)).map(DocBuilder(src, _))
      case DocBuilderF(Fix(ExprBuilderF(src, innerExpr)), outerShape) =>
        outerShape.traverse(inln(_, Expr(innerExpr))).map(exes => DocBuilder(src, exes ∘ (_.right)))
      case DocBuilderF(Fix(ShapePreservingBuilderF(src, inputs, op)), shape) =>
        ShapePreservingBuilder(normalizeƒ(DocBuilderF(src, shape)), inputs, op).some
      case _ => None
    }

    w => loop(w).fold(Fix(w))(x => normalizeƒ(x.unFix))
  }

  def normalize(wb: WorkflowBuilder) = normalizeƒ(wb.unFix)

  private def rewriteObjRefs(
    obj: ListMap[BsonField.Name, GroupValue[Expression]])(
    f: PartialFunction[DocVar, DocVar]) =
    obj ∘ (_.bimap(accumulator.rewriteGroupRefs(_)(f), rewriteExprRefs(_)(f)))

  private def rewriteGroupRefs(
    contents: GroupContents)(
    f: PartialFunction[DocVar, DocVar]) =
    contents match {
      case Expr(expr) =>
        Expr(expr.bimap(accumulator.rewriteGroupRefs(_)(f), rewriteExprRefs(_)(f)))
      case Doc(doc)   => Doc(rewriteObjRefs(doc)(f))
    }

  private def rewriteDocPrefix(doc: ListMap[BsonField.Name, Expr], base: Base) =
    doc ∘ (rewriteExprPrefix(_, base))

  private def rewriteExprPrefix(expr: Expr, base: Base): Expr =
    expr.bimap(base.toDocVar.toJs >>> _, rewriteExprRefs(_)(prefixBase0(base)))

  private def prefixBase0(base: Base): PartialFunction[DocVar, DocVar] =
    prefixBase(base.toDocVar)

  type EitherE[X] = PlannerError \/ X
  type M[X] = StateT[EitherE, NameGen, X]

  // Wrappers for results that don't use state:
  def emit[A](a: A): M[A] = quasar.namegen.emit[EitherE, A](a)
  def fail[A](e: PlannerError): M[A] = lift(-\/(e))
  def lift[A](v: EitherE[A]): M[A] = quasar.namegen.lift[EitherE](v)
  def emitSt[A](v: State[NameGen, A]): M[A] = emitName[EitherE, A](v)
  def swapM[A](v: State[NameGen, PlannerError \/ A]): M[A] =
    StateT[EitherE, NameGen, A](s => { val (s1, x) = v.run(s); x.map(s1 -> _) })

  def commonMap[K, A, B](m: ListMap[K, A \/ B])(f: B => PlannerError \/ A):
      PlannerError \/ (ListMap[K, A] \/ ListMap[K, B]) = {
    m.sequenceU.fold(
      κ((m ∘ (_.fold(\/.right, f))).sequenceU.map(\/.left)),
      l => \/-(\/-(l)))
  }

  private def commonShape(shape: ListMap[BsonField.Name, Expr]) =
    commonMap(shape)(toJs)

  private val jsBase = jscore.Name("__val")

  private def toCollectionBuilder(wb: WorkflowBuilder): M[CollectionBuilderF] =
    wb.unFix match {
      case cb @ CollectionBuilderF(_, _, _) => emit(cb)
      case ValueBuilderF(value) =>
        emit(CollectionBuilderF($pure(value), Root(), None))
      case ShapePreservingBuilderF(src, inputs, op) =>
        // At least one argument has no deref (e.g. $$ROOT)
        def case1(src: WorkflowBuilder, input: WorkflowBuilder, op: PartialFunction[List[BsonField], Workflow => Workflow], fields: List[Base]): M[CollectionBuilderF] = {
          emitSt(freshName).flatMap(name =>
            fields.map(f => (DocField(name) \\ f.toDocVar).deref).sequence.fold(
              scala.sys.error("prefixed ${name}, but still no field"))(
              op.lift(_).fold(
                fail[CollectionBuilderF](UnsupportedFunction(set.Filter, "failed to build operation")))(
                op =>
                (toCollectionBuilder(src) |@| toCollectionBuilder(DocBuilder(input, ListMap(name -> \/-($$ROOT))))) {
                  case (
                    CollectionBuilderF(_, _, srcStruct),
                    CollectionBuilderF(graph, _, _)) =>
                    CollectionBuilderF(
                      chain(graph, op),
                      Field(name),
                      srcStruct)
                })))
        }
        // Every argument has a deref (so, a BsonField that can be given to the op)
        def case2(
          src: WorkflowBuilder,
          input: WorkflowBuilder,
          base: Base,
          op: PartialFunction[List[BsonField], Workflow => Workflow],
          fields: List[BsonField]):
            M[CollectionBuilderF] = {
          ((toCollectionBuilder(src) |@| toCollectionBuilder(input)) {
            case (
              CollectionBuilderF(_, _, srcStruct),
              CollectionBuilderF(graph, base0, bothStruct)) =>
              op.lift(fields.map(f => base0.toDocVar.deref.map(_ \ f).getOrElse(f))).fold[M[CollectionBuilderF]](
                fail[CollectionBuilderF](UnsupportedFunction(set.Filter, "failed to build operation")))(
                { op =>
                  val g = chain(graph, op)
                  if (srcStruct ≟ bothStruct)
                    emit(CollectionBuilderF(g, base0 \ base, srcStruct))
                  else {
                    val (g1, base1) = shift(base0 \ base, srcStruct, g)
                    emit(CollectionBuilderF(g1, base1, srcStruct))
                  }
                })
          }).join
        }
        inputs match {
          case Nil =>
            toCollectionBuilder(src).map {
              case CollectionBuilderF(g, b, s) =>
                CollectionBuilderF(op(Nil)(g), b, s)
            }
          case _ =>
            foldBuilders(src, inputs).flatMap { case (input1, base, fields) =>
              fields.map(_.toDocVar.deref).sequence.fold(
                case1(src, input1, op, fields))(
                case2(src, input1, base, op, _))
            }
        }
      case ExprBuilderF(src, \/-($var(d))) =>
        toCollectionBuilder(src).map {
          case CollectionBuilderF(graph, base, _) =>
            CollectionBuilderF(graph, base \ fromDocVar(d), None)
        }
      case ExprBuilderF(src, expr) => for {
        cb <- toCollectionBuilder(src)
        name <- emitSt(freshName)
      } yield cb match {
        case CollectionBuilderF(graph, base, _) =>
          CollectionBuilderF(
            chain(graph,
              rewriteExprPrefix(expr, base).fold(
                js => $simpleMap(NonEmptyList(MapExpr(JsFn(jsBase, jscore.Obj(ListMap(jscore.Name(name.asText) -> js(jscore.Ident(jsBase))))))), ListMap()),
                op => $project(Reshape(ListMap(name -> \/-(op)))))),
            Field(name),
            None)
      }
      case DocBuilderF(src, shape) =>
        workflow(src).flatMap { case (wf, base) =>
          commonShape(rewriteDocPrefix(shape, base)).fold(
            fail(_),
            s => shape.keys.toList.toNel.fold[M[CollectionBuilderF]](
              fail(InternalError("A shape with no fields does not make sense")))(
              fields => emit(CollectionBuilderF(
                chain(wf,
                  s.fold(
                    jsExprs => $simpleMap(NonEmptyList(
                      MapExpr(JsFn(jsBase,
                        jscore.Obj(jsExprs.map {
                          case (name, expr) => jscore.Name(name.asText) -> expr(jscore.Ident(jsBase))
                        })))),
                      ListMap()),
                    exprOps => $project(Reshape(exprOps ∘ \/.right)))),
                Root(),
                fields.some))))
        }
      case ArrayBuilderF(src, shape) =>
        workflow(src).flatMap { case (wf, base) =>
          lift(shape.map(exprToJs).sequenceU.map(jsExprs =>
            CollectionBuilderF(
              chain(wf,
                $simpleMap(NonEmptyList(
                  MapExpr(JsFn(jsBase,
                    jscore.Arr(jsExprs.map(_(base.toDocVar.toJs(jscore.Ident(jsBase))
                    )).toList)))),
                  ListMap())),
              Root(),
              None)))
        }
      case GroupBuilderF(src, keys, content, _) =>
        (foldBuilders(src, keys) |@| toCollectionBuilder(src)){ case ((wb, base, fields), CollectionBuilderF(_, _, struct)) =>
          def key(base: Base) = keys.zip(fields) match {
            case Nil        => \/-($literal(Bson.Null))
            case (key, field) :: Nil => key.unFix match {
              // NB: normalize to Null, to ease merging
              case ValueBuilderF(_) | ExprBuilderF(_, \/-($literal(_))) =>
                \/-($literal(Bson.Null))
              case _ =>
                field match {
                  case Root() | Field(_) => \/-(rewriteExprRefs($var(field.toDocVar))(prefixBase0(base)))
                  case Subset(fields) => -\/(Reshape(fields.toList.map(fld =>
                    fld -> \/-($var(DocField(fld)))).toListMap))
                }
            }
            case _ => -\/(Reshape(fields.map(_.toDocVar).zipWithIndex.map {
              case (field, index) =>
                BsonField.Index(index).toName -> \/-($var(field))
            }.toListMap).rewriteRefs(prefixBase0(base)))
          }

          content match {
            case Expr(-\/(grouped)) =>
              for {
                cb <- toCollectionBuilder(wb)
                rootName <- emitSt(freshName)
              } yield cb match {
                case CollectionBuilderF(wf, base0, _) =>
                  CollectionBuilderF(
                    chain(wf,
                      $group(Grouped(ListMap(rootName -> grouped)).rewriteRefs(prefixBase0(base0 \ base)),
                        key(base0))),
                    Field(rootName),
                    struct)
              }
            case Expr(\/-(expr)) =>
              // NB: This case just winds up a single value, then unwinds it.
              //     It’s effectively a no-op, so we just use the src and expr.
              toCollectionBuilder(ExprBuilder(src, \/-(expr)))
            case Doc(obj) =>
              val (grouped, ungrouped) =
                obj.foldLeft[(ListMap[BsonField.Leaf, Accumulator], ListMap[BsonField.Name, Expression])]((ListMap.empty[BsonField.Leaf, Accumulator], ListMap.empty[BsonField.Name, Expression]))((acc, item) =>
                  item match {
                    case (k, -\/(v)) =>
                      ((x: ListMap[BsonField.Leaf, Accumulator]) => x + (k -> v)).first(acc)
                    case (k, \/-(v)) =>
                      ((x: ListMap[BsonField.Name, Expression]) => x + (k -> v)).second(acc)
                  })
              if (grouped.isEmpty)
                toCollectionBuilder(DocBuilder(src, ungrouped ∘ (_.right)))
              else
                obj.keys.toList.toNel.fold[M[CollectionBuilderF]](
                  fail(InternalError("A shape with no fields does not make sense")))(
                  fields => workflow(wb).flatMap { case (wf, base0) =>
                    emitSt(ungrouped.size match {
                      case 0 =>
                        state[NameGen, Workflow](chain(wf,
                          $group(Grouped(grouped).rewriteRefs(prefixBase0(base0 \ base)), key(base0))))
                      case 1 =>
                        state[NameGen, Workflow](chain(wf,
                          $group(Grouped(
                            obj.transform {
                              case (_, -\/(v)) =>
                                accumulator.rewriteGroupRefs(v)(prefixBase0(base0 \ base))
                              case (_, \/-(v)) =>
                                $push(rewriteExprRefs(v)(prefixBase0(base0 \ base)))
                            }),
                            key(base0)),
                          $unwind(DocField(ungrouped.head._1))))
                      case _ => for {
                        ungroupedName <- freshName
                        groupedName <- freshName
                      } yield
                        chain(wf,
                          $project(Reshape(ListMap(
                            ungroupedName -> -\/(Reshape(ungrouped.map {
                              case (k, v) => k -> \/-(rewriteExprRefs(v)(prefixBase0(base0 \ base)))
                            })),
                            groupedName -> \/-($$ROOT)))),
                          $group(Grouped(
                            (grouped ∘ (accumulator.rewriteGroupRefs(_)(prefixBase0(Field(groupedName) \ base0)))) +
                              (ungroupedName -> $push($var(DocField(ungroupedName))))),
                            key(Field(groupedName) \ base0)),
                          $unwind(DocField(ungroupedName)),
                          $project(Reshape(obj.transform {
                            case (k, -\/ (_)) => \/-($var(DocField(k)))
                            case (k,  \/-(_)) => \/-($var(DocField(ungroupedName \ k)))
                          })))
                    }).map(CollectionBuilderF(
                      _,
                      Root(),
                      fields.some))
                  })
          }
        }.join
      case FlatteningBuilderF(src, fields) =>
        toCollectionBuilder(src).map {
          case CollectionBuilderF(graph, base, struct) =>
            CollectionBuilderF(fields.foldRight(graph) {
              case (StructureType.Array(field), acc) => $unwind(base.toDocVar \\ field)(acc)
              case (StructureType.Object(field), acc) =>
                $simpleMap(NonEmptyList(FlatExpr(JsFn(jsBase, (base.toDocVar \\ field).toJs(jscore.Ident(jsBase))))), ListMap())(acc)
            }, base, struct)
        }
      case sb @ SpliceBuilderF(_, _) =>
        workflow(sb.src).flatMap { case (wf, base) =>
          lift(
            sb.toJs.map { splice =>
              CollectionBuilderF(
                chain(wf,
                  $simpleMap(NonEmptyList(MapExpr(JsFn(jsBase, (base.toDocVar.toJs >>> splice)(jscore.Ident(jsBase))))), ListMap())),
                Root(),
                None)
            })
        }
      case sb @ ArraySpliceBuilderF(_, _) =>
        workflow(sb.src).flatMap { case (wf, base) =>
          lift(
            sb.toJs.map { splice =>
              CollectionBuilderF(
                chain(wf,
                  $simpleMap(NonEmptyList(MapExpr(JsFn(jsBase, (base.toDocVar.toJs >>> splice)(jscore.Ident(jsBase))))), ListMap())),
                Root(),
                None)
            })
        }
    }

  def workflow(wb: WorkflowBuilder): M[(Workflow, Base)] =
    toCollectionBuilder(wb).map(x => (x.src, x.base))

  def shift(base: Base, struct: Schema, graph: Workflow): (Workflow, Base) = {
    (base, struct) match {
      case (Field(ExprName), None) => (graph, Field(ExprName))
      case (_,       None)         =>
        (chain(graph,
          Workflow.$project(Reshape(ListMap(ExprName -> \/-($var(base.toDocVar)))),
            ExcludeId)),
          Field(ExprName))
      case (_,       Some(fields)) =>
        (chain(graph,
          Workflow.$project(
            Reshape(fields.map(name =>
              name -> \/-($var((base \ name).toDocVar))).toList.toListMap),
            if (fields.element(IdName)) IncludeId else ExcludeId)),
          Root())
    }
  }

  def build(wb: WorkflowBuilder): M[Workflow] =
    toCollectionBuilder(wb).map {
      case CollectionBuilderF(graph, base, struct) =>
        if (base == Root()) graph
        else shift(base, struct, graph)._1
    }

  private def $project(shape: Reshape): WorkflowOp =
    Workflow.$project(
      shape,
      shape.get(IdName).fold[IdHandling](IgnoreId)(κ(IncludeId)))

  def asLiteral(wb: WorkflowBuilder): Option[Bson] = wb.unFix match {
    case ValueBuilderF(value)                  => Some(value)
    case ExprBuilderF(_, \/-($literal(value))) => Some(value)
    case _                                     => None
  }

  private def fold1Builders(builders: List[WorkflowBuilder]):
      Option[M[(WorkflowBuilder, List[Expression])]] =
    builders match {
      case Nil             => None
      case builder :: Nil  => Some(emit((builder, List($$ROOT))))
      case Fix(ValueBuilderF(bson)) :: rest =>
        fold1Builders(rest).map(_.map { case (builder, fields) =>
          (builder, $literal(bson) +: fields)
        })
      case builder :: rest =>
        Some(rest.foldLeftM[M, (WorkflowBuilder, List[Expression])](
          (builder, List($$ROOT))) {
          case ((wf, fields), Fix(ValueBuilderF(bson))) =>
            emit((wf, fields :+ $literal(bson)))
          case ((wf, fields), x) =>
            merge(wf, x).map { case (lbase, rbase, src) =>
              (src, fields.map(rewriteExprRefs(_)(prefixBase0(lbase))) :+ $var(rbase.toDocVar))
            }
        })
    }

  private def foldBuilders(src: WorkflowBuilder, others: List[WorkflowBuilder]): M[(WorkflowBuilder, Base, List[Base])] =
    others.foldLeftM[M, (WorkflowBuilder, Base, List[Base])](
      (src, Root(), Nil)) {
      case ((wf, base, fields), x) =>
        merge(wf, x).map { case (lbase, rbase, src) =>
          (src, lbase \ base, fields.map(lbase \ _) :+ rbase)
        }
    }

  def filter(src: WorkflowBuilder, those: List[WorkflowBuilder], sel: PartialFunction[List[BsonField], Selector]):
      WorkflowBuilder =
    ShapePreservingBuilder(src, those, PartialFunction(fields => $match(sel(fields))))

  def expr1(wb: WorkflowBuilder)(f: Expression => Expression):
      M[WorkflowBuilder] =
    expr(List(wb)) { case List(e) => f(e) }

  def expr2(
    wb1: WorkflowBuilder, wb2: WorkflowBuilder)(
    f: (Expression, Expression) => Expression):
      M[WorkflowBuilder] =
    expr(List(wb1, wb2)) { case List(e1, e2) => f(e1, e2) }

  def expr(
    wbs: List[WorkflowBuilder])(
    f: List[Expression] => Expression):
      M[WorkflowBuilder] = {
    fold1Builders(wbs).fold[M[WorkflowBuilder]](
      fail(InternalError("impossible – no arguments")))(
      _.map { case (wb, exprs) => normalize(ExprBuilder(wb, \/-(f(exprs)))) })
  }

  // NB: Handled separately from `expr` because it is non-strict in some
  //     parameters. In future, generalizing non-strict parameters could be
  //     useful.
  def cond(test: WorkflowBuilder, con: WorkflowBuilder, alt: WorkflowBuilder) =
    (con.unFix, alt.unFix) match {
      case (ExprBuilderF(w1, e1), ExprBuilderF(w2, e2)) => for {
        t1 <- foldBuilders(test, List(w1, w2))
        (merged, value, fields) = t1
        exprs = Zip[List].zipWith(List(e1, e2), fields)(rewriteExprPrefix(_, _))
        rez <- lift(exprs.sequenceU.fold[PlannerError \/ Expr](
          κ {
            exprs.map(exprToJs).sequenceU.map {
              case List(e1, e2) =>
                val base = jscore.Ident(jsBase)
                -\/(JsFn(jsBase, jscore.If(value.toDocVar.toJs(base), e1(base), e2(base))))
            }
          },
          { case List(e1, e2) => \/-(\/-($cond($var(value.toDocVar), e1, e2))) }))
      } yield ExprBuilder(merged, rez)
      case (ExprBuilderF(w1, e1), ValueBuilderF(bson)) => for {
        t1 <- foldBuilders(test, List(w1))
        (merged, value, fields) = t1
        rez <- rewriteExprPrefix(e1, fields.head).fold[M[Expr]](
          e1 => {
            val base = jscore.Ident(jsBase)
            lift(toJs($literal(bson)).map(lit => -\/(JsFn(jsBase, jscore.If(value.toDocVar.toJs(base), e1(base), lit(base))))))
          },
          e1 => emit(\/-($cond($var(value.toDocVar), e1, $literal(bson)))))
      } yield ExprBuilder(merged, rez)
      case (ExprBuilderF(w1, e1), _) => for {
        t1 <- foldBuilders(test, List(w1, alt))
        (merged, value, fields) = t1
      } yield ExprBuilder(merged,
        rewriteExprPrefix(e1, fields.head).fold[Expr](
          e1 => {
            val base = jscore.Ident(jsBase)
            -\/(JsFn(jsBase, jscore.If(value.toDocVar.toJs(base), e1(base), fields(1).toDocVar.toJs(base))))
          },
          e1 => \/-($cond($var(value.toDocVar), e1, $var(fields(1).toDocVar)))))
      case (_, _) =>
        // TODO: Tried, but failed
        expr(List(test, con, alt)) { case List(e1, e2, e3) => $cond(e1, e2, e3) }
    }

  def jsExpr1(wb: WorkflowBuilder, js: JsFn): PlannerError \/ WorkflowBuilder =
    wb.unFix match {
      case ShapePreservingBuilderF(src, inputs, op) =>
        jsExpr1(src, js).map(ShapePreservingBuilder(_, inputs, op))
      case ExprBuilderF(wb1, \/-(expr1)) =>
        toJs(expr1).map(js1 => ExprBuilder(wb1, -\/(js1 >>> js)))
      case ExprBuilderF(wb1, -\/(js1)) =>
        \/-(ExprBuilder(wb1, -\/(js1 >>> js)))
      case GroupBuilderF(wb0, key, Expr(\/-(expr)), id) =>
        toJs(expr).flatMap(
          ex => jsExpr1(wb0, JsFn(jsBase, ex(js(jscore.Ident(jsBase))))).map(
            GroupBuilder(_, key, Expr(\/-($$ROOT)), id)))
      case _ => \/-(ExprBuilder(wb, -\/(js)))
    }

  object HasLiteral {
    def unapply(value: Bson): Option[JsCore] = value match {
      case Bson.Null         => Some(jscore.Literal(Js.Null))
      case Bson.Text(str)    => Some(jscore.Literal(Js.Str(str)))
      case Bson.Bool(value)  => Some(jscore.Literal(Js.Bool(value)))
      case Bson.Int32(value) => Some(jscore.Literal(Js.Num(value, false)))
      case Bson.Int64(value) => Some(jscore.Literal(Js.Num(value, false)))
      case Bson.Dec(value)   => Some(jscore.Literal(Js.Num(value, true)))
      case Bson.Doc(value)   =>
        value.map { case (name, bson) => HasLiteral.unapply(bson).map(jscore.Name(name) -> _) }.toList.sequenceU.map(pairs => jscore.Obj(pairs.toListMap))
      case _ => None
    }
  }

  def jsExpr2(wb1: WorkflowBuilder, wb2: WorkflowBuilder, js: (JsCore, JsCore) => JsCore): M[WorkflowBuilder] =
    (wb1.unFix, wb2.unFix) match {
      case (_, ValueBuilderF(HasLiteral(lit))) =>
        lift(jsExpr1(wb1, JsFn(jsBase, js(jscore.Ident(jsBase), lit))))
      case (ValueBuilderF(HasLiteral(lit)), _) =>
        lift(jsExpr1(wb2, JsFn(jsBase, js(lit, jscore.Ident(jsBase)))))
      case _ =>
        merge(wb1, wb2).map { case (lbase, rbase, src) =>
          ExprBuilder(src, -\/(JsFn(jsBase, js(lbase.toDocVar.toJs(jscore.Ident(jsBase)), rbase.toDocVar.toJs(jscore.Ident(jsBase))))))
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
        DocBuilder(wb, ListMap(BsonField.Name(name) -> \/-($$ROOT)))
    }

  def makeArray(wb: WorkflowBuilder): WorkflowBuilder = wb.unFix match {
    case ValueBuilderF(value) => ValueBuilder(Bson.Arr(List(value)))
    case _ => ArrayBuilder(wb, List(\/-($$ROOT)))
  }

  /** The location of the desired content relative to the current $$ROOT.
    *
    * Various transformations (merging, conversion to Workflow, etc.) combine
    * structures that we need to be able to extract later. This tells us how to
    * extract them.
    */
  sealed trait Base {
    def \ (that: Base): Base = (this, that) match {
      case (Root(),      _)            => that
      case (_,           Root())       => this
      case (Subset(_),   _)            => that // TODO: can we do better?
      case (_,           Subset(_))    => this // TODO: can we do better?
      case (Field(name), Field(name2)) => Field(name \ name2)
    }

    def \ (that: BsonField): Base = this \ Field(that)

    // NB: This is a lossy conversion.
    val toDocVar: DocVar = this match {
      case Root()      => DocVar.ROOT()
      case Field(name) => DocField(name)
      case Subset(_)   => DocVar.ROOT()
    }
  }
  /** The content is already at $$ROOT. */
  final case class Root()                              extends Base
  /** The content is nested in a field under $$ROOT. */
  final case class Field(name: BsonField)              extends Base
  /** The content is a subset of the document at $$ROOT. */
  final case class Subset(fields: Set[BsonField.Name]) extends Base

  val fromDocVar: DocVar => Base = {
    case DocVar.ROOT(None) => Root()
    case DocField(name)   => Field(name)
  }

  def mergeContents[A](c1: DocContents[A], c2: DocContents[A]):
      M[((Base, Base), DocContents[A])] = {
    def documentize(c: DocContents[A]):
        State[NameGen, (Base, ListMap[BsonField.Name, A])] =
      c match {
        case Doc(d) => state(Subset(d.keySet) -> d)
        case Expr(cont) =>
          freshName.map(name => (Field(name), ListMap(name -> cont)))
      }

    lazy val doc =
      swapM((documentize(c1) |@| documentize(c2)) {
        case ((lb, lshape), (rb, rshape)) =>
          Reshape.mergeMaps(lshape, rshape).fold[PlannerError \/ ((Base, Base), DocContents[A])](
            -\/(InternalError("conflicting fields when merging contents: " + lshape + ", " + rshape)))(
            map => {
              val llb = if (Subset(map.keySet) == lb) Root() else lb
              val rrb = if (Subset(map.keySet) == rb) Root() else rb
              \/-((llb, rrb) -> Doc(map))
            })
      })

    if (c1 == c2)
      emit((Root(), Root()) -> c1)
    else
      (c1, c2) match {
        case (Expr(v), Doc(o)) =>
          o.find { case (_, e) => v == e }.fold(doc) {
            case (lField, _) =>
              emit((Field(lField), Root()) -> Doc(o))
          }
        case (Doc(o), Expr(v)) =>
          o.find { case (_, e) => v == e }.fold(doc) {
            case (rField, _) =>
              emit((Root(), Field(rField)) -> Doc(o))
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
          M[((Base, Base), WorkflowBuilder)] =
        merge(s1, s2).flatMap { case (lbase, rbase, src) =>
          combine(
            rewriteGroupRefs(c1)(prefixBase0(lbase)),
            rewriteGroupRefs(c2)(prefixBase0(rbase)))(mergeContents).map {
            case ((lb, rb), contents) =>
              combine(lb, rb)((_, _) -> GroupBuilder(src, keys, contents, id1))
          }
        }

      (wb1.unFix, wb2.unFix) match {
        case (ShapePreservingBuilderF(s1, i1, o1), ShapePreservingBuilderF(s2, i2, o2))
            if i1 == i2 && o1 == o2 =>
          impl(s1, s2, combine).map(ShapePreservingBuilder(_, i1, o1))

        case (
          v1 @ ShapePreservingBuilderF(
            Fix(DocBuilderF(_, shape1)), inputs1, _),
          GroupBuilderF(
            Fix(v2 @ ShapePreservingBuilderF(_, inputs2, _)),
            Nil, _, id2))
          if inputs1 ≟ inputs2 && v1.dummyOp == v2.dummyOp =>
          impl(GroupBuilder(wb1, Nil, Doc(shape1.keys.toList.map(n => n -> \/-($var(DocField(n)))).toListMap), id2), wb2, combine)
        case (
          GroupBuilderF(
          Fix(v1 @ ShapePreservingBuilderF(_, inputs1, op1)),
            Nil, _, _),
          v2 @ ShapePreservingBuilderF(Fix(DocBuilderF(_, _)), inputs2, op2))
          if inputs1 ≟ inputs2 && v1.dummyOp == v2.dummyOp => delegate

        case (
          v1 @ ShapePreservingBuilderF(
            Fix(DocBuilderF(_, shape1)), inputs1, _),
          DocBuilderF(
            Fix(GroupBuilderF(
              Fix(v2 @ ShapePreservingBuilderF(_, inputs2, _)),
              Nil, _, id2)),
           shape2))
          if inputs1 ≟ inputs2 && v1.dummyOp == v2.dummyOp =>
          impl(GroupBuilder(wb1, Nil, Doc(shape1.keys.toList.map(n => n -> \/-($var(DocField(n)))).toListMap), id2), wb2, combine)
        case (
          DocBuilderF(
            Fix(GroupBuilderF(
              Fix(v1 @ ShapePreservingBuilderF(_, inputs1, _)),
              Nil, _, _)),
            shape2),
          v2 @ ShapePreservingBuilderF(Fix(DocBuilderF(_, _)), inputs2, _))
          if inputs1 ≟ inputs2 && v1.dummyOp == v2.dummyOp => delegate

        case (ShapePreservingBuilderF(s, i, o), _) =>
          impl(s, wb2, combine).map(ShapePreservingBuilder(_, i, o))
        case (_, ShapePreservingBuilderF(_, _, _)) => delegate

        case (ValueBuilderF(Bson.Doc(map1)), ValueBuilderF(Bson.Doc(map2))) =>
          emit(ValueBuilder(Bson.Doc(combine(map1, map2)(_ ++ _))))

        case (ValueBuilderF(Bson.Doc(map1)), DocBuilderF(s2, shape2)) =>
          emit(DocBuilder(s2,
            combine(
              map1.map { case (k, v) => BsonField.Name(k) -> \/-($literal(v)) },
              shape2)(_ ++ _)))
        case (DocBuilderF(_, _), ValueBuilderF(Bson.Doc(_))) => delegate

        case (ValueBuilderF(Bson.Doc(map1)), GroupBuilderF(s1, k1, Doc(c2), id2)) =>
          val content = combine(
            map1.map { case (k, v) => BsonField.Name(k) -> \/-($literal(v)) },
            c2)(_ ++ _)
          emit(GroupBuilder(s1, k1, Doc(content), id2))
        case (GroupBuilderF(_, _, Doc(_), _), ValueBuilderF(_)) => delegate

        case (
          GroupBuilderF(src1, keys, Expr(\/-($var(DocVar.ROOT(_)))), id1),
          GroupBuilderF(src2, _,    Expr(\/-($var(DocVar.ROOT(_)))), id2))
            if id1 == id2 =>
          impl(src1, src2, combine).map(GroupBuilder(_, keys, Expr(\/-($$ROOT)), id1))

        case (
          GroupBuilderF(s1, keys, c1 @ Doc(_), id1),
          GroupBuilderF(s2, _,    c2 @ Doc(_), id2))
            if id1 == id2 =>
          mergeGroups(s1, s2, c1, c2, keys, id1).map(_._2)

        case (
          GroupBuilderF(s1, keys, c1 @ Doc(d1), id1),
          DocBuilderF(Fix(GroupBuilderF(s2, _, c2, id2)), shape2))
            if id1 == id2 =>
          mergeGroups(s1, s2, c1, c2, keys, id1).map { case ((glbase, grbase), g) =>
            DocBuilder(g, combine(
              d1.transform { case (n, _) => \/-($var(DocField(n))) },
              shape2 ∘ (rewriteExprPrefix(_, grbase)))(_ ++ _))
          }
        case (
          DocBuilderF(Fix(GroupBuilderF(_, _, _, id1)), _),
          GroupBuilderF(_, _, Doc(_), id2))
            if id1 == id2 =>
          delegate

        case (
          GroupBuilderF(Fix(DocBuilderF(s1, shape1)), keys, c1 @ Expr(\/-($$ROOT)), id1),
          DocBuilderF(Fix(GroupBuilderF(s2, _, c2, id2)), shape2))
            if id1 == id2 =>
          mergeGroups(s1, s2, c1, c2, keys, id1).map { case ((glbase, grbase), g) =>
            DocBuilder(g, combine(
              shape1 ∘ (rewriteExprPrefix(_, glbase)),
              shape2 ∘ (rewriteExprPrefix(_, grbase)))(_ ++ _))
          }
        case (
          DocBuilderF(Fix(GroupBuilderF(_, _, _, id1)), _),
          GroupBuilderF(Fix(DocBuilderF(_, _)), _, Expr(\/-($$ROOT)), id2))
            if id1 == id2 =>
          delegate

        case (
          GroupBuilderF(Fix(DocBuilderF(s1, shape1)), keys, c1 @ Expr(\/-($$ROOT)), id1),
          GroupBuilderF(s2, _, c2 @ Doc(d2), id2))
            if id1 == id2 =>
          mergeGroups(s1, s2, c1, c2, keys, id1).map { case ((glbase, grbase), g) =>
            DocBuilder(g, combine(
              shape1 ∘ (rewriteExprPrefix(_, glbase)),
              d2.transform { case (n, _) => \/-($var(DocField(n))) })(_ ++ _))
          }
        case (
          GroupBuilderF(_, k1, _, id1),
          GroupBuilderF(Fix(DocBuilderF(_, _)), k2, Expr(\/-($$ROOT)), id2))
            if id1 == id2 =>
          delegate

        case (
          GroupBuilderF(Fix(ShapePreservingBuilderF(Fix(DocBuilderF(s1, shape1)), inputs, op)), keys, c1 @ Expr(\/-($$ROOT)), id1),
          GroupBuilderF(s2, _, c2 @ Doc(d2), id2))
            if id1 == id2 =>
          mergeGroups(ShapePreservingBuilder(s1, inputs, op), s2, c1, c2, keys, id1).map { case ((glbase, grbase), g) =>
            DocBuilder(g, combine(
              shape1 ∘ (rewriteExprPrefix(_, glbase)),
              d2.transform { case (n, _) => \/-($var(DocField(n))) })(_ ++ _))
          }
        case (
          GroupBuilderF(_, _, Doc(_), id1),
          GroupBuilderF(Fix(ShapePreservingBuilderF(Fix(DocBuilderF(_, _)), _, _)), _, Expr(\/-($$ROOT)), id2))
            if id1 == id2 =>
          delegate

        case (
          GroupBuilderF(Fix(ShapePreservingBuilderF(Fix(DocBuilderF(s1, shape1)), inputs, op)), keys, c1 @ Expr(\/-($$ROOT)), id1),
          DocBuilderF(Fix(GroupBuilderF(s2, _, c2, id2)), shape2))
            if id1 == id2 =>
          mergeGroups(ShapePreservingBuilder(s1, inputs, op), s2, c1, c2, keys, id1).map { case ((glbase, grbase), g) =>
            DocBuilder(g, combine(
              shape1 ∘ (rewriteExprPrefix(_, glbase)),
              shape2 ∘ (rewriteExprPrefix(_, grbase)))(_ ++ _))
          }
        case (
          DocBuilderF(Fix(GroupBuilderF(_, _, _, id1)), _),
          GroupBuilderF(Fix(ShapePreservingBuilderF(Fix(DocBuilderF(_, _)), _, _)), _, Expr(\/-($$ROOT)), id2))
            if id1 == id2 =>
          delegate

        case (
          GroupBuilderF(Fix(ShapePreservingBuilderF(Fix(DocBuilderF(s1, shape1)), inputs, op)), keys, c1 @ Expr(\/-($$ROOT)), id1),
          DocBuilderF(Fix(GroupBuilderF(s2, _, c2, id2)), shape2))
            if id1 == id2 =>
          mergeGroups(s1, s2, c1, c2, keys, id1).map { case ((glbase, grbase), g) =>
            ShapePreservingBuilder(
              DocBuilder(g, combine(
                shape1 ∘ (rewriteExprPrefix(_, glbase)),
                shape2 ∘ (rewriteExprPrefix(_, grbase)))(_ ++ _)),
              inputs,
              op)
          }
        case (
          GroupBuilderF(_, _, _, id1),
          GroupBuilderF(Fix(ShapePreservingBuilderF(Fix(DocBuilderF(_, _)), _, _)), _, Expr(\/-($$ROOT)), id2))
            if id1 == id2 =>
          delegate

        case (
          DocBuilderF(Fix(FlatteningBuilderF(Fix(GroupBuilderF(s1, keys, c1, id1)), struct1)), shape1),
          GroupBuilderF(s2, _, c2 @ Doc(d2), id2))
            if id1 == id1 =>
          mergeGroups(s1, s2, c1, c2, keys, id1).map { case ((glbase, grbase), g) =>
            DocBuilder(
              FlatteningBuilder(g, struct1.map(_.map(glbase.toDocVar \\ _))),
              combine(
                shape1 ∘ (rewriteExprPrefix(_, glbase)),
                d2.transform { case (n, _) => \/-($var(DocField(n))) })(_ ++ _))
          }
        case (
          GroupBuilderF(_, _, Doc(_), id1),
          DocBuilderF(Fix(FlatteningBuilderF(Fix(GroupBuilderF(_, _, _, id2)), _)), _))
            if id1 == id1 =>
          delegate

        case (
          DocBuilderF(Fix(GroupBuilderF(s1, keys, c1, id1)), shape1),
          DocBuilderF(Fix(GroupBuilderF(s2, _,    c2, id2)), shape2))
            if id1 == id2 =>
          mergeGroups(s1, s2, c1, c2, keys, id1).flatMap {
            case ((glbase, grbase), g) =>
              emit(DocBuilder(g, combine(
                shape1 ∘ (rewriteExprPrefix(_, glbase)),
                shape2 ∘ (rewriteExprPrefix(_, grbase)))(_ ++ _)))
          }

        case (
          DocBuilderF(_, shape),
          GroupBuilderF(_, Nil, _, id2)) =>
          impl(
            GroupBuilder(wb1, Nil, Doc(shape.map { case (n, _) => n -> \/-($var(DocField(n))) }), id2),
            wb2,
            combine)
        case (
          GroupBuilderF(_, Nil, _, _),
          DocBuilderF(_, _)) =>
          delegate

        case (
          GroupBuilderF(_, _, Doc(cont1), _),
          GroupBuilderF(_, Nil, _, id2)) =>
          impl(
            GroupBuilder(wb1, Nil, Doc(cont1.map { case (n, _) => n -> \/-($var(DocField(n))) }), id2),
            wb2,
            combine)
        case (
          GroupBuilderF(_, Nil, _, _),
          GroupBuilderF(_, _, _, _)) =>
          delegate

        case (
          DocBuilderF(_, shape),
          DocBuilderF(Fix(GroupBuilderF(_, Nil, _, id2)), _)) =>
          impl(
            GroupBuilder(wb1, Nil, Doc(shape.map { case (n, _) => n -> \/-($var(DocField(n))) }), id2),
            wb2,
            combine)
        case (
          DocBuilderF(Fix(GroupBuilderF(_, Nil, _, _)), _),
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

        case (SpliceBuilderF(src1, structure1), ExprBuilderF(src2, expr2)) =>
          merge(src1, src2).map { case (left, right, list) =>
            SpliceBuilder(list, combine(
              structure1,
              List(Expr(rewriteExprPrefix(expr2, right))))(_ ++ _))
          }
        case (ExprBuilderF(_, _), SpliceBuilderF(_, _)) => delegate

        case (SpliceBuilderF(src1, structure1), CollectionBuilderF(_, _, _)) =>
          merge(src1, wb2).map { case (_, right, list) =>
            SpliceBuilder(list, combine(
              structure1,
              List(Expr(\/-($var(right.toDocVar)))))(_ ++ _))
          }
        case (CollectionBuilderF(_, _, _), SpliceBuilderF(_, _)) => delegate

        case (DocBuilderF(src, shape), CollectionBuilderF(_, _, _)) =>
          merge(src, wb2).map { case (left, right, list) =>
            SpliceBuilder(list, combine(
              Doc(rewriteDocPrefix(shape, left)),
              Expr(\/-($var(right.toDocVar))))(List(_, _)))
          }
        case (CollectionBuilderF(_, _, _), DocBuilderF(_, _)) => delegate

        case (
          DocBuilderF(s1 @ Fix(
            ArraySpliceBuilderF(_, _)),
            shape1),
          GroupBuilderF(_, _, Doc(c2), _)) =>
          merge(s1, wb2).map { case (lbase, rbase, src) =>
            DocBuilder(src,
              combine(
                rewriteDocPrefix(shape1, lbase),
                c2.map { case (n, _) => (n, rewriteExprPrefix(\/-($var(DocField(n))), rbase)) })(_ ++ _))
          }
        case (GroupBuilderF(_, _, _, _), DocBuilderF(Fix(ArraySpliceBuilderF(_, _)), _)) => delegate

        case _ => fail(UnsupportedFunction(
          structural.ObjectConcat,
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
            combine(seq.map(x => \/-($literal(x))), shape)(_ ++ _)))
        case (ArrayBuilderF(_, _), ValueBuilderF(Bson.Arr(_))) => delegate

        case (
          ArrayBuilderF(Fix(
            v1 @ ShapePreservingBuilderF(src1, inputs1, op1)), shape1),
          v2 @ ShapePreservingBuilderF(src2, inputs2, op2))
          if inputs1 ≟ inputs2 && v1.dummyOp == v2.dummyOp =>
          merge(src1, src2).map { case (lbase, rbase, wb) =>
            ShapePreservingBuilder(
              ArraySpliceBuilder(wb, combine(
                Array(shape1.map(rewriteExprPrefix(_, lbase))),
                Expr(\/-($var(rbase.toDocVar))))(List(_, _))),
              inputs1, op1)
          }
        case (ShapePreservingBuilderF(_, in1, op1), ArrayBuilderF(Fix(ShapePreservingBuilderF(_, in2, op2)), _)) => delegate

        case (
          v1 @ ShapePreservingBuilderF(src1, inputs1, op1),
          ArrayBuilderF(Fix(
            GroupBuilderF(Fix(
              v2 @ ShapePreservingBuilderF(src2, inputs2, _)),
              Nil, cont2, id2)),
            shape2))
            if inputs1 ≟ inputs2 && v1.dummyOp == v2.dummyOp =>
          merge(src1, src2).flatMap { case (lbase, rbase, wb) =>
            combine(Expr(\/-($var(lbase.toDocVar))), rewriteGroupRefs(cont2)(prefixBase0(rbase)))(mergeContents).map { case ((lbase1, rbase1), cont) =>
              ShapePreservingBuilder(
                ArraySpliceBuilder(
                  GroupBuilder(wb, Nil, cont, id2),
                  combine(
                    Expr(\/-($var(lbase1.toDocVar))),
                    Array(shape2.map(rewriteExprPrefix(_, rbase1))))(List(_, _))),
                inputs1, op1)
            }
          }
        case (
          ArrayBuilderF(Fix(
            GroupBuilderF(Fix(
              v1 @ ShapePreservingBuilderF(_, inputs1, _)),
              Nil, _, _)), _),
          v2 @ ShapePreservingBuilderF(_, inputs2, _))
            if inputs1 ≟ inputs2 && v1.dummyOp == v2.dummyOp => delegate

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
              Array(shape1.map(rewriteExprPrefix(_, left))),
              Expr(rewriteExprPrefix(expr2, right)))(List(_, _)))
          }
        case (ExprBuilderF(_, _), ArrayBuilderF(_, _)) => delegate

        case (ArrayBuilderF(src1, shape1), GroupBuilderF(_, _, _, _)) =>
          merge(src1, wb2).map { case (left, right, wb) =>
            ArraySpliceBuilder(wb, combine(
              Array(shape1.map(rewriteExprPrefix(_, left))),
              Expr(\/-($var(right.toDocVar))))(List(_, _)))
          }
        case (GroupBuilderF(_, _, _, _), ArrayBuilderF(_, _)) => delegate

        case (ValueBuilderF(Bson.Arr(seq1)), ExprBuilderF(src2, expr2)) =>
          emit(ArraySpliceBuilder(src2, combine(
            Array(seq1.map(x => \/-($literal(x)))),
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
          merge(src1, src2).map { case (_, right, list) =>
            ArraySpliceBuilder(list, combine(
              structure1,
              List(Expr(rewriteExprPrefix(expr2, right))))(_ ++ _))
          }
        case (ExprBuilderF(_, _), ArraySpliceBuilderF(_, _)) => delegate

        case (ArraySpliceBuilderF(src1, structure1), ValueBuilderF(bson2)) =>
          emit(ArraySpliceBuilder(src1, combine(structure1, List(Expr(\/-($literal(bson2)))))(_ ++ _)))
        case (ValueBuilderF(_), ArraySpliceBuilderF(_, _)) => delegate

        case _ =>
          fail(UnsupportedFunction(
            structural.ArrayConcat,
            "values are not both arrays"))
      }
    }

    impl(left, right, unflipped)
  }

  def flattenObject(wb: WorkflowBuilder): WorkflowBuilder = wb.unFix match {
    case ShapePreservingBuilderF(src, inputs, op) =>
      ShapePreservingBuilder(flattenObject(src), inputs, op)
    case GroupBuilderF(src, keys, Expr(\/-($var(DocVar.ROOT(None)))), id) =>
      GroupBuilder(flattenObject(src), keys, Expr(\/-($$ROOT)), id)
    case _ => FlatteningBuilder(wb, Set(StructureType.Object(DocVar.ROOT())))
  }

  def flattenArray(wb: WorkflowBuilder): WorkflowBuilder = wb.unFix match {
    case ShapePreservingBuilderF(src, inputs, op) =>
      ShapePreservingBuilder(flattenArray(src), inputs, op)
    case GroupBuilderF(src, keys, Expr(\/-($var(DocVar.ROOT(None)))), id) =>
      GroupBuilder(flattenArray(src), keys, Expr(\/-($$ROOT)), id)
    case _ => FlatteningBuilder(wb, Set(StructureType.Array(DocVar.ROOT())))
  }

  def projectField(wb: WorkflowBuilder, name: String):
      PlannerError \/ WorkflowBuilder =
    wb.unFix match {
      case ShapePreservingBuilderF(src, inputs, op) =>
        projectField(src, name).map(ShapePreservingBuilder(_, inputs, op))
      case ValueBuilderF(Bson.Doc(fields)) =>
        fields.get(name).fold[PlannerError \/ WorkflowBuilder](
          -\/(UnsupportedFunction(structural.ObjectProject, "value does not contain a field ‘" + name + "’.")))(
          x => \/-(ValueBuilder(x)))
      case ValueBuilderF(_) =>
        -\/(UnsupportedFunction(structural.ObjectProject, "value is not a document."))
      case GroupBuilderF(wb0, key, Expr(\/-($var(dv))), id) =>
        projectField(wb0, name).map(GroupBuilder(_, key, Expr(\/-($var(dv))), id))
      case GroupBuilderF(wb0, key, Doc(doc), id) =>
        doc.get(BsonField.Name(name)).fold[PlannerError \/ WorkflowBuilder](
          -\/(UnsupportedFunction(structural.ObjectProject, "group does not contain a field ‘" + name + "’.")))(
          x => \/-(GroupBuilder(wb0, key, Expr(x), id)))
      case DocBuilderF(wb, doc) =>
        doc.get(BsonField.Name(name)).fold[PlannerError \/ WorkflowBuilder](
          -\/(UnsupportedFunction(structural.ObjectProject, "document does not contain a field ‘" + name + "’.")))(
          expr => \/-(ExprBuilder(wb, expr)))
      case ExprBuilderF(wb0,  -\/(js1)) =>
        \/-(ExprBuilder(wb0,
          -\/(JsFn(jsBase, DocField(BsonField.Name(name)).toJs(js1(jscore.Ident(jsBase)))))))
      case ExprBuilderF(wb, \/-($var(DocField(field)))) =>
        \/-(ExprBuilder(wb, \/-($var(DocField(field \ BsonField.Name(name))))))
      case _ => \/-(ExprBuilder(wb, \/-($var(DocField(BsonField.Name(name))))))
    }

  def projectIndex(wb: WorkflowBuilder, index: Int): PlannerError \/ WorkflowBuilder =
    wb.unFix match {
      case ValueBuilderF(Bson.Arr(elems)) =>
        if (index < elems.length) // UGH!
          \/-(ValueBuilder(elems(index)))
        else
          -\/(UnsupportedFunction(
            structural.ArrayProject,
            "value does not contain index ‘" + index + "’."))
      case ArrayBuilderF(wb0, elems) =>
        if (index < elems.length) // UGH!
          \/-(ExprBuilder(wb0, elems(index)))
        else
          -\/(UnsupportedFunction(
            structural.ArrayProject,
            "array does not contain index ‘" + index + "’."))
      case ValueBuilderF(_) =>
        -\/(UnsupportedFunction(
          structural.ArrayProject,
          "value is not an array."))
      case DocBuilderF(_, _) =>
        -\/(UnsupportedFunction(
          structural.ArrayProject,
          "value is not an array."))
      case _ =>
        jsExpr1(wb, JsFn(jsBase,
          jscore.Access(jscore.Ident(jsBase), jscore.Literal(Js.Num(index, false)))))
    }

  def deleteField(wb: WorkflowBuilder, name: String):
      PlannerError \/ WorkflowBuilder =
    wb.unFix match {
      case ShapePreservingBuilderF(src, inputs, op) =>
        deleteField(src, name).map(ShapePreservingBuilder(_, inputs, op))
      case ValueBuilderF(Bson.Doc(fields)) =>
        \/-(ValueBuilder(Bson.Doc(fields - name)))
      case ValueBuilderF(_) =>
        -\/(UnsupportedFunction(
          structural.DeleteField,
          "value is not a document."))
      case GroupBuilderF(wb0, key, Expr(\/-($var(DocVar.ROOT(None)))), id) =>
        deleteField(wb0, name).map(GroupBuilder(_, key, Expr(\/-($$ROOT)), id))
      case GroupBuilderF(wb0, key, Doc(doc), id) =>
        \/-(GroupBuilder(wb0, key, Doc(doc - BsonField.Name(name)), id))
      case DocBuilderF(wb0, doc) =>
        \/-(DocBuilder(wb0, doc - BsonField.Name(name)))
      case _ => jsExpr1(wb, JsFn(jsBase,
        // FIXME: Need to pull this back up from the top level (#663)
        jscore.Call(jscore.ident("remove"),
          List(jscore.Ident(jsBase), jscore.Literal(Js.Str(name))))))
    }

  def groupBy(src: WorkflowBuilder, keys: List[WorkflowBuilder]):
      WorkflowBuilder =
    GroupBuilder(src, keys, Expr(\/-($$ROOT)), GroupId(src :: keys))

  def reduce(wb: WorkflowBuilder)(f: Expression => Accumulator): WorkflowBuilder =
    wb.unFix match {
      case GroupBuilderF(wb0, keys, Expr(\/-(expr)), id) =>
        GroupBuilder(wb0, keys, Expr(-\/(f(expr))), id)
      case ShapePreservingBuilderF(src @ Fix(GroupBuilderF(_, _, Expr(\/-(_)), _)), inputs, op) =>
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
        GroupBuilder(wb, Nil, Expr(-\/(f($$ROOT))), id(wb))
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
      wf => checkTask(task(crystallize(wf._1))))
  }

  def join(left0: WorkflowBuilder, right0: WorkflowBuilder,
    tpe: Func,
    leftKey0: List[WorkflowBuilder], leftJs0: List[JsFn],
    rightKey0: List[WorkflowBuilder], rightJs0: List[JsFn]):
      M[WorkflowBuilder] = {

    import Js._

    // FIXME: these have to match the names used in the logical plan. Should
    //        change this to ensure left0/right0 are `Free` and pull the names
    //        from those.
    val leftField0: BsonField.Name = BsonField.Name("left")
    val rightField0: BsonField.Name = BsonField.Name("right")

    val (left, right, leftKey, rightKey, leftField, rightField) =
      if (requiresMapReduce(left0) && !requiresMapReduce(right0))
        (right0, left0, rightKey0, leftJs0, rightField0, leftField0)
      else
        (left0, right0, leftKey0, rightJs0, leftField0, rightField0)

    val nonEmpty: Selector.SelectorExpr = Selector.NotExpr(Selector.Size(0))

    def padEmpty(side: BsonField): Expression =
      $cond($eq($size($var(DocField(side))), $literal(Bson.Int32(0))),
        $literal(Bson.Arr(List(Bson.Doc(ListMap())))),
        $var(DocField(side)))

    def buildProjection(l: Expression, r: Expression): WorkflowOp =
      $project(Reshape(ListMap(leftField -> \/-(l), rightField -> \/-(r))))(_)

    def buildJoin(src: Workflow, tpe: Func): Workflow =
      tpe match {
        case set.FullOuterJoin =>
          chain(src,
            buildProjection(padEmpty(leftField), padEmpty(rightField)))
        case set.LeftOuterJoin =>
          chain(src,
            $match(Selector.Doc(ListMap(
              leftField.asInstanceOf[BsonField] -> nonEmpty))),
            buildProjection($var(DocField(leftField)), padEmpty(rightField)))
        case set.RightOuterJoin =>
          chain(src,
            $match(Selector.Doc(ListMap(
              rightField.asInstanceOf[BsonField] -> nonEmpty))),
            buildProjection(padEmpty(leftField), $var(DocField(rightField))))
        case set.InnerJoin =>
          chain(
            src,
            $match(
              Selector.Doc(ListMap(
                leftField.asInstanceOf[BsonField] -> nonEmpty,
                rightField -> nonEmpty))))
        case _ => scala.sys.error("How did this get here?")
      }

    def rightMap(keyExpr: List[JsFn]): AnonFunDecl =
      $Map.mapKeyVal(("key", "value"),
        keyExpr match {
          case Nil      => Js.Null
          case List(js) => js(jscore.ident("value")).toJs
          case _        =>
            jscore.Obj(keyExpr.map(_(jscore.ident("value"))).zipWithIndex.foldLeft[ListMap[jscore.Name, JsCore]](ListMap[jscore.Name, JsCore]()) {
              case (acc, (j, i)) => acc + (jscore.Name(i.toString) -> j)
            }).toJs
        },
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

    (workflow(DocBuilder(
      reduce(groupBy(left, leftKey))($push(_)),
      ListMap(
        leftField             -> \/-($$ROOT),
        rightField            -> \/-($literal(Bson.Arr(Nil))),
        BsonField.Name("_id") -> \/-($include())))) |@|
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
          Root(),
          None)
    }
  }

  def limit(wb: WorkflowBuilder, count: Long) =
    ShapePreservingBuilder(wb, Nil, { case Nil => $limit(count) })

  def skip(wb: WorkflowBuilder, count: Long) =
    ShapePreservingBuilder(wb, Nil, { case Nil => $skip(count) })

  def squash(wb: WorkflowBuilder): WorkflowBuilder = wb

  // TODO: Cases that match on `$$ROOT` should be generalized to look up the
  //       shape of any DocVar in the source.
  @tailrec def findKeys(wb: WorkflowBuilder): Option[Base] = {
    wb.unFix match {
      case CollectionBuilderF(_, _, s2) => s2.map(s => Subset(s.toSet))
      case DocBuilderF(_, shape) => Subset(shape.keySet).some
      case FlatteningBuilderF(src, _) => findKeys(src)
      case GroupBuilderF(src, _, Expr(\/-($$ROOT)), _) => findKeys(src)
      case GroupBuilderF(_, _, Doc(obj), _) => Subset(obj.keySet).some
      case ShapePreservingBuilderF(src, _, _) => findKeys(src)
      case ExprBuilderF(src, \/-($$ROOT)) => findKeys(src)
      case ExprBuilderF(_, _) => Root().some
      case ValueBuilderF(Bson.Doc(shape)) =>
        Subset(shape.keySet.map(BsonField.Name(_))).some
      case _ => None
    }
  }

  def distinct(src: WorkflowBuilder): M[WorkflowBuilder] =
    findKeys(src).fold(
      lift(deleteField(src, "_id")).flatMap(del => distinctBy(del, List(del))))(
      ks => distinctBy(src, ks match {
        case Root() => List(src)
        case Field(k) =>
          List(normalize(ExprBuilder(src, \/-($var(DocField(k))))))
        case Subset(ks) =>
          ks.toList.map(k =>
            normalize(ExprBuilder(src, \/-($var(DocField(k))))))
      }))

  def distinctBy(src: WorkflowBuilder, keys: List[WorkflowBuilder]):
      M[WorkflowBuilder] = {
    def distincting(s: WorkflowBuilder) = reduce(groupBy(s, keys))($first(_))

    @tailrec
    def findSort(wb: WorkflowBuilder): M[WorkflowBuilder] = wb.unFix match {
      case spb @ ShapePreservingBuilderF(spbSrc, sortKeys, f) =>
        spb.dummyOp.unFix match {
          case $Sort(_, _) =>
            foldBuilders(src, sortKeys).map { case (newSrc, dv, ks) =>

              val dist = distincting(normalize(ExprBuilder(newSrc, \/-($var(dv.toDocVar)))))
              ShapePreservingBuilder(
                normalize(ExprBuilder(dist, \/-($var(dv.toDocVar)))),
                ks.map(k => normalize(ExprBuilder(dist, \/-($var(k.toDocVar))))), f)
            }
          case _ => findSort(spbSrc)
        }
      case _ => distincting(src).point[M]
    }

    findSort(src)
  }

  private def merge(left: WorkflowBuilder, right: WorkflowBuilder):
      M[(Base, Base, WorkflowBuilder)] = {
    def delegate =
      merge(right, left).map { case (r, l, merged) => (l, r, merged) }

    (left.unFix, right.unFix) match {
      case (
        ExprBuilderF(src1, \/-($var(DocField(base1)))),
        ExprBuilderF(src2, \/-($var(DocField(base2)))))
          if src1 ≟ src2 =>
        emit((Field(base1), Field(base2), src1))

      case _ if left ≟ right => emit((Root(), Root(), left))

      case (ValueBuilderF(bson), ExprBuilderF(src, expr)) =>
        mergeContents(Expr(\/-($literal(bson))), Expr(expr)).map {
          case ((lbase, rbase), cont) =>
            (lbase, rbase, contentsToBuilder(cont)(src))
        }
      case (ExprBuilderF(_, _), ValueBuilderF(_)) => delegate

      case (ValueBuilderF(bson), DocBuilderF(src, shape)) =>
        mergeContents(Expr(\/-($literal(bson))), Doc(shape)).map {
          case ((lbase, rbase), cont) =>
            (lbase, rbase, contentsToBuilder(cont)(src))
        }
      case (DocBuilderF(_, _), ValueBuilderF(_)) => delegate

      case (ValueBuilderF(bson), _) =>
        mergeContents(Expr(\/-($literal(bson))), Expr(\/-($$ROOT))).map {
          case ((lbase, rbase), cont) =>
            (lbase, rbase, contentsToBuilder(cont)(right))
        }
      case (_, ValueBuilderF(_)) => delegate

      case (ExprBuilderF(src1, expr1), ExprBuilderF(src2, expr2)) if src1 ≟ src2 =>
        mergeContents(Expr(expr1), Expr(expr2)).map {
          case ((lbase, rbase), cont) =>
            (lbase, rbase, contentsToBuilder(cont)(src1))
        }
      case (ExprBuilderF(src, \/-($var(DocField(base)))), _) if src ≟ right =>
        emit((Field(base), Root(), right))
      case (_, ExprBuilderF(src, \/-($var(DocField(_))))) if left ≟ src =>
        delegate

      case (DocBuilderF(src1, shape1), ExprBuilderF(src2, expr2)) =>
        merge(src1, src2).flatMap { case (lb, rb, wb) =>
          mergeContents(Doc(rewriteDocPrefix(shape1, lb)), Expr(rewriteExprPrefix(expr2, rb))).map {
            case ((lbase, rbase), cont) =>
              (lbase, rbase, contentsToBuilder(cont)(wb))
          }
        }
      case (ExprBuilderF(_, _), DocBuilderF(_, _)) =>
        delegate

      case (DocBuilderF(src1, shape1), DocBuilderF(src2, shape2)) =>
        merge(src1, src2).flatMap { case (lbase, rbase, wb) =>
          mergeContents(
            Doc(rewriteDocPrefix(shape1, lbase)),
            Doc(rewriteDocPrefix(shape2, rbase))).map {
            case ((lbase, rbase), cont) =>
              (lbase, rbase, contentsToBuilder(cont)(wb))
          }
        }

      // NB: The SPB cases need to be handled fairly early, because it allows
      //     them to stay closer to the root of the Builder.
      case (
        spb1 @ ShapePreservingBuilderF(src1, inputs1, op1),
        spb2 @ ShapePreservingBuilderF(src2, inputs2, _))
          if inputs1 ≟ inputs2 && spb1.dummyOp == spb2.dummyOp =>
        merge(src1, src2).map { case (lbase, rbase, wb) =>
          (lbase, rbase, ShapePreservingBuilder(wb, inputs1, op1))
        }
      case (ShapePreservingBuilderF(src, inputs, op), _) =>
        merge(src, right).map { case (lbase, rbase, wb) =>
          (lbase, rbase, ShapePreservingBuilder(wb, inputs, op))
        }
      case (_, ShapePreservingBuilderF(src, inputs, op)) => delegate

      case (ExprBuilderF(src, expr), _) =>
        merge(src, right).flatMap { case (lbase, rbase, wb) =>
          mergeContents(Expr(rewriteExprPrefix(expr, lbase)), Expr(\/-($var(rbase.toDocVar)))).map {
            case ((lbase, rbase), cont) =>
              (lbase, rbase, contentsToBuilder(cont)(wb))
          }
        }
      case (_, ExprBuilderF(src, _)) => delegate

      case (DocBuilderF(src1, shape1), _) =>
        merge(src1, right).flatMap { case (lbase, rbase, wb) =>
          mergeContents(Doc(rewriteDocPrefix(shape1, lbase)), Expr(\/-($var(rbase.toDocVar)))).map {
            case ((lbase, rbase), cont) =>
              (lbase, rbase, contentsToBuilder(cont)(wb))
          }
        }
      case (_, DocBuilderF(_, _)) => delegate

      case (sb @ SpliceBuilderF(_, _), _) =>
        merge(sb.src, right).flatMap { case (lbase, rbase, wb) =>
          for {
            lName  <- emitSt(freshName)
            rName  <- emitSt(freshName)
            splice <- lift(sb.toJs)
          } yield (Field(lName), Field(rName),
            DocBuilder(wb, ListMap(
              lName -> -\/ (lbase.toDocVar.toJs >>> splice),
              rName ->  \/-($var(rbase.toDocVar)))))
        }
      case (_, SpliceBuilderF(_, _)) => delegate

      case (sb @ ArraySpliceBuilderF(_, _), _) =>
        merge(sb.src, right).flatMap { case (lbase, rbase, wb) =>
          for {
            lName  <- emitSt(freshName)
            rName  <- emitSt(freshName)
            splice <- lift(sb.toJs)
          } yield (Field(lName), Field(rName),
            DocBuilder(wb, ListMap(
              lName -> -\/ (lbase.toDocVar.toJs >>> splice),
              rName ->  \/-($var(rbase.toDocVar)))))
        }
      case (_, ArraySpliceBuilderF(_, _)) => delegate

      case (GroupBuilderF(src1, Nil, cont1, id1), CollectionBuilderF(_, _, _)) =>
        merge(src1, right).flatMap { case (lbase, rbase, wb) =>
          val cont1p = rewriteGroupRefs(cont1)(prefixBase0(lbase))
          mergeContents(cont1p, Expr(\/-($var(rbase.toDocVar)))).map {
            case ((lbase, rbase), cont) =>
              (lbase, rbase,
                GroupBuilder(wb, Nil, cont, id1))
          }
        }
      case (CollectionBuilderF(_, _, _), GroupBuilderF(_, Nil, _, _)) => delegate

      case (
        FlatteningBuilderF(src0, fields0),
        FlatteningBuilderF(src1, fields1)) =>
        left.cata(branchLengthƒ) cmp right.cata(branchLengthƒ) match {
          case Ordering.LT =>
            merge(left, src1).map { case (lbase, rbase, wb) =>
              (lbase, rbase, FlatteningBuilder(wb, fields1.map(_.map(rbase.toDocVar \\ _))))
            }
          case Ordering.EQ =>
            merge(src0, src1).map { case (lbase, rbase, wb) =>
              val lfields = fields0.map(_.map(lbase.toDocVar \\ _))
              val rfields = fields1.map(_.map(rbase.toDocVar \\ _))
              (lbase, rbase, FlatteningBuilder(wb, lfields union rfields))
            }
          case Ordering.GT =>
            merge(src0, right).map { case (lbase, rbase, wb) =>
              (lbase, rbase, FlatteningBuilder(wb, fields0.map(_.map(lbase.toDocVar \\ _))))
            }
        }
      case (FlatteningBuilderF(src, fields), _) =>
        merge(src, right).flatMap { case (lbase, rbase, wb) =>
          val lfields = fields.map(_.map(lbase.toDocVar \\ _))
          if (lfields.exists(x => x.field.startsWith(rbase.toDocVar) || rbase.toDocVar.startsWith(x.field)))
            for {
              lName <- emitSt(freshName)
              rName <- emitSt(freshName)
            } yield
              (Field(lName), Field(rName),
                FlatteningBuilder(
                  DocBuilder(wb, ListMap(
                    lName -> \/-($var(lbase.toDocVar)),
                    rName -> \/-($var(rbase.toDocVar)))),
                  fields.map(_.map(DocField(lName) \\ _))))
          else emit((lbase, rbase, FlatteningBuilder(wb, lfields)))
        }
      case (_, FlatteningBuilderF(_, _)) => delegate

      case (GroupBuilderF(src1, key1, cont1, id1), GroupBuilderF(src2, key2, cont2, id2))
          if id1 == id2 =>
        merge(src1, src2).flatMap { case (lbase, rbase, wb) =>
          mergeContents(rewriteGroupRefs(cont1)(prefixBase0(lbase)), rewriteGroupRefs(cont2)(prefixBase0(rbase))).map {
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
                  (lbase, Field(rName),
                    CollectionBuilder(
                      chain(src,
                        $project(Reshape(shape + (rName -> \/-($var(rbase.toDocVar)))))),
                      Root(),
                      None))))
              case _ => fail(InternalError("couldn’t merge array"))
            }
          }
        }
      case (_, ArrayBuilderF(_, _)) => delegate

      case _ =>
        fail(InternalError("failed to merge:\n" + left.show + "\n" + right.show))
    }
  }

  def read(coll: Collection) = CollectionBuilder($read(coll), Root(), None)
  def pure(bson: Bson) = ValueBuilder(bson)

  implicit def WorkflowBuilderRenderTree(implicit RG: RenderTree[Contents[GroupValue[Expression]]], RC: RenderTree[Contents[Expr]]): RenderTree[WorkflowBuilder] =
    new RenderTree[WorkflowBuilder] {
      val nodeType = "WorkflowBuilder" :: Nil

      def render(v: WorkflowBuilder) = v.unFix match {
        case CollectionBuilderF(graph, base, struct) =>
          NonTerminal("CollectionBuilder" :: nodeType, Some(base.toString),
            graph.render ::
              Terminal("Schema" :: "CollectionBuilder" :: nodeType, Some(struct.toString)) ::
              Nil)
        case spb @ ShapePreservingBuilderF(src, inputs, op) =>
          val nt = "ShapePreservingBuilder" :: nodeType
          NonTerminal(nt, None,
            render(src) ::
              (inputs.map(render) :+
                Terminal("Op" :: nt, Some(spb.dummyOp.toString))))
        case ValueBuilderF(value) =>
          Terminal("ValueBuilder" :: nodeType, Some(value.toString))
        case ExprBuilderF(src, expr) =>
          NonTerminal("ExprBuilder" :: nodeType, None,
            render(src) :: expr.render :: Nil)
        case DocBuilderF(src, shape) =>
          val nt = "DocBuilder" :: nodeType
          NonTerminal(nt, None,
            render(src) ::
              NonTerminal("Shape" :: nt, None,
                shape.toList.map {
                  case (name, expr) =>
                    NonTerminal("Name" :: nodeType, Some(name.value), List(expr.render))
                }) ::
              Nil)
        case ArrayBuilderF(src, shape) =>
          val nt = "ArrayBuilder" :: nodeType
          NonTerminal(nt, None,
            render(src) ::
              NonTerminal("Shape" :: nt, None, shape.map(_.render)) ::
              Nil)
        case GroupBuilderF(src, keys, content, id) =>
          val nt = "GroupBuilder" :: nodeType
          NonTerminal(nt, Some(id.toString),
            render(src) ::
              NonTerminal("By" :: nt, None, keys.map(render)) ::
              RG.render(content).copy(nodeType = "Content" :: nt) ::
              Nil)
        case FlatteningBuilderF(src, fields) =>
          val nt = "FlatteningBuilder" :: nodeType
          NonTerminal(nt, None,
            render(src) ::
              fields.toList.map(x => $var(x.field).render.copy(nodeType = (x match {
                case StructureType.Array(_) => "Array"
                case StructureType.Object(_) => "Object"
              }) :: nt)))
        case SpliceBuilderF(src, structure) =>
          NonTerminal("SpliceBuilder" :: nodeType, None,
            render(src) :: structure.map(RC.render))
        case ArraySpliceBuilderF(src, structure) =>
          NonTerminal("ArraySpliceBuilder" :: nodeType, None,
            render(src) :: structure.map(RC.render))
      }
    }
}
