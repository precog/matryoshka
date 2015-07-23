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

import slamdata.Predef._

import scalaz._
import Scalaz._

import slamdata.engine.{Error, RenderTree, Terminal, NonTerminal}
import slamdata.engine.analysis.fixplate.{Term}
import slamdata.engine.fp._
import slamdata.engine.javascript._

sealed trait ExprOp {
  def bson: Bson

  import ExprOp._

  def mapUp(f0: PartialFunction[ExprOp, ExprOp]): ExprOp = {
    (mapUpM[Free.Trampoline](new PartialFunction[ExprOp, Free.Trampoline[ExprOp]] {
      def isDefinedAt(v: ExprOp) = f0.isDefinedAt(v)
      def apply(v: ExprOp) = f0(v).point[Free.Trampoline]
    })).run
  }

  def rewriteRefs(applyVar: PartialFunction[DocVar, DocVar]) = this.mapUp {
    case f @ DocVar(_, _) => applyVar.lift(f).getOrElse(f)
  }

  // TODO: Port physical plan to fixplate to eliminate this madness! (#35)
  def mapUpM[F[_]](f0: PartialFunction[ExprOp, F[ExprOp]])(implicit F: Monad[F]): F[ExprOp] = {
    val f0l = f0.lift
    val f = (e: ExprOp) => f0l(e).getOrElse(e.point[F])

    def mapUp0(v: ExprOp): F[ExprOp] = {
      val rec = (v match {
        case Include            => v.point[F]
        case DocVar(_, _)       => v.point[F]
        case Add(l, r)          => (mapUp0(l) |@| mapUp0(r))(Add(_, _))
        case And(v)             => v.traverse(mapUp0).map(And(_))
        case SetEquals(l, r)       => (mapUp0(l) |@| mapUp0(r))(SetEquals(_, _))
        case SetIntersection(l, r) => (mapUp0(l) |@| mapUp0(r))(SetIntersection(_, _))
        case SetDifference(l, r)   => (mapUp0(l) |@| mapUp0(r))(SetDifference(_, _))
        case SetUnion(l, r)        => (mapUp0(l) |@| mapUp0(r))(SetUnion(_, _))
        case SetIsSubset(l, r)     => (mapUp0(l) |@| mapUp0(r))(SetIsSubset(_, _))
        case AnyElementTrue(v)     => mapUp0(v).map(AnyElementTrue(_))
        case AllElementsTrue(v)    => mapUp0(v).map(AllElementsTrue(_))
        case ArrayMap(a, b, c)  => (mapUp0(a) |@| mapUp0(c))(ArrayMap(_, b, _))
        case Cmp(l, r)          => (mapUp0(l) |@| mapUp0(r))(Cmp(_, _))
        case Concat(a, b, cs)   => (mapUp0(a) |@| mapUp0(b) |@| cs.traverse(mapUp0))(Concat(_, _, _))
        case Cond(a, b, c)      => (mapUp0(a) |@| mapUp0(b) |@| mapUp0(c))(Cond(_, _, _))
        case DayOfMonth(a)      => mapUp0(a).map(DayOfMonth(_))
        case DayOfWeek(a)       => mapUp0(a).map(DayOfWeek(_))
        case DayOfYear(a)       => mapUp0(a).map(DayOfYear(_))
        case Divide(a, b)       => (mapUp0(a) |@| mapUp0(b))(Divide(_, _))
        case Eq(a, b)           => (mapUp0(a) |@| mapUp0(b))(Eq(_, _))
        case Gt(a, b)           => (mapUp0(a) |@| mapUp0(b))(Gt(_, _))
        case Gte(a, b)          => (mapUp0(a) |@| mapUp0(b))(Gte(_, _))
        case Hour(a)            => mapUp0(a).map(Hour(_))
        case Meta                  => v.point[F]
        case Size(a)               => mapUp0(a).map(Size(_))
        case IfNull(a, b)       => (mapUp0(a) |@| mapUp0(b))(IfNull(_, _))
        case Let(a, b)          =>
          (Traverse[ListMap[ExprOp.DocVar.Name, ?]].sequence[F, ExprOp](a.map(t => t._1 -> mapUp0(t._2))) |@| mapUp0(b))(Let(_, _))
        case Literal(_)         => v.point[F]
        case Lt(a, b)           => (mapUp0(a) |@| mapUp0(b))(Lt(_, _))
        case Lte(a, b)          => (mapUp0(a) |@| mapUp0(b))(Lte(_, _))
        case Millisecond(a)     => mapUp0(a).map(Millisecond(_))
        case Minute(a)          => mapUp0(a).map(Minute(_))
        case Mod(a, b)          => (mapUp0(a) |@| mapUp0(b))(Mod(_, _))
        case Month(a)           => mapUp0(a).map(Month(_))
        case Multiply(a, b)     => (mapUp0(a) |@| mapUp0(b))(Multiply(_, _))
        case Neq(a, b)          => (mapUp0(a) |@| mapUp0(b))(Neq(_, _))
        case Not(a)             => mapUp0(a).map(Not(_))
        case Or(a)              => a.traverse(mapUp0).map(Or(_))
        case Second(a)          => mapUp0(a).map(Second(_))
        case Strcasecmp(a, b)   => (mapUp0(a) |@| mapUp0(b))(Strcasecmp(_, _))
        case Substr(a, b, c)    => (mapUp0(a) |@| mapUp0(b) |@| mapUp0(c))(Substr(_, _, _))
        case Subtract(a, b)     => (mapUp0(a) |@| mapUp0(b))(Subtract(_, _))
        case ToLower(a)         => mapUp0(a).map(ToLower(_))
        case ToUpper(a)         => mapUp0(a).map(ToUpper(_))
        case Week(a)            => mapUp0(a).map(Week(_))
        case Year(a)            => mapUp0(a).map(Year(_))
      })

      rec >>= f
    }

    mapUp0(this)
  }
}

object ExprOp {
  implicit val ExprOpRenderTree = RenderTree.fromToString[ExprOp]("ExprOp")

  def toJs(expr: ExprOp): Error \/ JsFn = {
    import slamdata.engine.PlannerError._

    def expr1(x1: ExprOp)(f: Term[JsCore] => Term[JsCore]): Error \/ JsFn =
      toJs(x1).map(x1 => JsFn(JsFn.base, f(x1(JsFn.base.fix))))
    def expr2(x1: ExprOp, x2: ExprOp)(f: (Term[JsCore], Term[JsCore]) => Term[JsCore]): Error \/ JsFn =
      (toJs(x1) |@| toJs(x2))((x1, x2) => JsFn(JsFn.base, f(x1(JsFn.base.fix), x2(JsFn.base.fix))))

    def unop(op: JsCore.UnaryOperator, x: ExprOp) =
      expr1(x)(x => JsCore.UnOp(op, x).fix)
    def binop(op: JsCore.BinaryOperator, l: ExprOp, r: ExprOp) =
      expr2(l, r)((l, r) => JsCore.BinOp(op, l, r).fix)
    def invoke(x: ExprOp, name: String) =
      expr1(x)(x => JsCore.Call(JsCore.Select(x, name).fix, Nil).fix)

    def const(bson: Bson): Error \/ Term[JsCore] = {
      def js(l: Js.Lit) = \/-(JsCore.Literal(l).fix)
      bson match {
        case Bson.Int64(n)        => js(Js.Num(n, false))
        case Bson.Int32(n)        => js(Js.Num(n, false))
        case Bson.Dec(x)          => js(Js.Num(x, true))
        case Bson.Bool(v)         => js(Js.Bool(v))
        case Bson.Text(v)         => js(Js.Str(v))
        case Bson.Null            => js(Js.Null)
        case Bson.Doc(values)     => values.map { case (k, v) => k -> const(v) }.sequenceU.map(JsCore.Obj(_).fix)
        case Bson.Arr(values)     => values.toList.map(const(_)).sequenceU.map(JsCore.Arr(_).fix)
        case o @ Bson.ObjectId(_) => \/-(Conversions.jsObjectId(o))
        case d @ Bson.Date(_)     => \/-(Conversions.jsDate(d))
        // TODO: implement the rest of these (see #449)
        case Bson.Regex(pattern)  => -\/(UnsupportedJS(bson.toString))
        case Bson.Symbol(value)   => -\/(UnsupportedJS(bson.toString))

        case _ => -\/(NonRepresentableInJS(bson.toString))
      }
    }

    expr match {
      // TODO: The following few cases are places where the ExprOp created from
      //       the LogicalPlan needs special handling to behave the same when
      //       converted to JS. See #734 for the way forward.

      // matches the pattern the planner generates for converting epoch time
      // values to timestamps. Adding numbers to dates works in ExprOp, but not
      // in Javacript.
      case Add(Literal(Bson.Date(inst)), r) if inst.toEpochMilli == 0 =>
        expr1(r)(x => JsCore.New("Date", List(x)).fix)
      // typechecking in ExprOp involves abusing total ordering. This ordering
      // doesn’t hold in JS, so we need to convert back to a typecheck. This
      // checks for a (non-array) object.
      case And(NonEmptyList(
             Lte(Literal(Bson.Doc(m1)), f1),
             Lt(f2, Literal(Bson.Arr(List())))))
          if f1 == f2 && m1 == ListMap() =>
        toJs(f1).map(f =>
          JsFn(JsFn.base,
            JsCore.BinOp(JsCore.And,
              JsCore.BinOp(JsCore.Instance, f(JsFn.base.fix), JsCore.Ident("Object").fix).fix,
              JsCore.UnOp(JsCore.Not, JsCore.BinOp(JsCore.Instance, f(JsFn.base.fix), JsCore.Ident("Array").fix).fix).fix).fix))
      // same as above, but for arrays
      case And(NonEmptyList(
             Lte(Literal(Bson.Arr(List())), f1),
             Lt(f2, Literal(b1))))
          if f1 == f2 && b1 == Bson.Binary(scala.Array[Byte]())=>
        toJs(f1).map(f =>
          JsFn(JsFn.base,
            JsCore.BinOp(JsCore.Instance, f(JsFn.base.fix), JsCore.Ident("Array").fix).fix))

      case Include               => -\/(NonRepresentableInJS(expr.toString))
      case dv @ DocVar(_, _)     => \/-(dv.toJs)
      case Add(l, r)             => binop(JsCore.Add, l, r)
      case And(v)                =>
        v.traverse[Error \/ ?, JsFn](toJs).map(v =>
          v.foldLeft1((l, r) => JsFn(JsFn.base, JsCore.BinOp(JsCore.And, l(JsFn.base.fix), r(JsFn.base.fix)).fix)))
      case Cond(t, c, a)         =>
        (toJs(t) |@| toJs(c) |@| toJs(a))((t, c, a) =>
          JsFn(JsFn.base,
            JsCore.If(t(JsFn.base.fix), c(JsFn.base.fix), a(JsFn.base.fix)).fix))
      case Divide(l, r)          => binop(JsCore.Div, l, r)
      case Eq(l, r)              => binop(JsCore.Eq, l, r)
      case Gt(l, r)              => binop(JsCore.Gt, l, r)
      case Gte(l, r)             => binop(JsCore.Gte, l, r)
      case ExprOp.Literal(bson)  => const(bson).map(l => JsFn.const(l))
      case Lt(l, r)              => binop(JsCore.Lt, l, r)
      case Lte(l, r)             => binop(JsCore.Lte, l, r)
      case Meta                  => -\/(NonRepresentableInJS(expr.toString))
      case Multiply(l, r)        => binop(JsCore.Mult, l, r)
      case Neq(l, r)             => binop(JsCore.Neq, l, r)
      case Not(a)                => unop(JsCore.Not, a)

      case Concat(l, r, Nil)     => binop(JsCore.Add, l, r)
      case Substr(f, start, len) =>
        (toJs(f) |@| toJs(start) |@| toJs(len))((f, s, l) =>
          JsFn(JsFn.base,
            JsCore.Call(
              JsCore.Select(f(JsFn.base.fix), "substr").fix,
              List(s(JsFn.base.fix), l(JsFn.base.fix))).fix))
      case Subtract(l, r)        => binop(JsCore.Sub, l, r)
      case ToLower(a)            => invoke(a, "toLowerCase")
      case ToUpper(a)            => invoke(a, "toUpperCase")

      case Hour(a)               => invoke(a, "getUTCHours")
      case Minute(a)             => invoke(a, "getUTCMinutes")
      case Second(a)             => invoke(a, "getUTCSeconds")
      case Millisecond(a)        => invoke(a, "getUTCMilliseconds")

      // TODO: implement the rest of these and remove the catch-all (see #449)
      case _                     => -\/(UnsupportedJS(expr.toString))
    }
  }

  private[ExprOp] sealed trait SimpleOp extends ExprOp {
    val op: String
    def rhs: Bson

    def bson = Bson.Doc(ListMap(op -> rhs))
  }

  final case object Include extends ExprOp { def bson = Bson.Bool(true) }

  sealed trait FieldLike extends ExprOp
  object DocField {
    def apply(field: BsonField): DocVar = DocVar.ROOT(field)

    def unapply(docVar: DocVar): Option[BsonField] = docVar match {
      case DocVar.ROOT(tail) => tail
      case _ => None
    }
  }
  final case class DocVar(name: DocVar.Name, deref: Option[BsonField]) extends FieldLike {
    def path: List[BsonField.Leaf] = deref.toList.flatMap(_.flatten.toList)

    def startsWith(that: DocVar) = (this.name == that.name) && {
      (this.deref |@| that.deref)(_ startsWith (_)) getOrElse (that.deref.isEmpty)
    }

    def bson: Bson.Text = this match {
      case DocVar(DocVar.ROOT, Some(deref)) => Bson.Text(deref.asField)

      case _ =>
        val root = BsonField.Name(name.name)

        Bson.Text(deref.map(root \ _).getOrElse(root).asVar)
    }

    def \ (that: DocVar): Option[DocVar] = (this, that) match {
      case (DocVar(n1, f1), DocVar(n2, f2)) if (n1 == n2) =>
        val f3 = (f1 |@| f2)(_ \ _) orElse (f1) orElse (f2)

        Some(DocVar(n1, f3))

      case _ => None
    }

    def \\ (that: DocVar): DocVar = (this, that) match {
      case (DocVar(n1, f1), DocVar(_, f2)) =>
        val f3 = (f1 |@| f2)(_ \ _) orElse (f1) orElse (f2)

        DocVar(n1, f3)
    }

    def \ (field: BsonField): DocVar = copy(deref = Some(deref.map(_ \ field).getOrElse(field)))

    def toJs: JsFn = JsFn(JsFn.base, this match {
      case DocVar(_, None)        => JsFn.base.fix
      case DocVar(_, Some(deref)) => deref.toJs(JsFn.base.fix)
    })

    override def toString = this match {
      case DocVar(DocVar.ROOT, None) => "DocVar.ROOT()"
      case DocVar(DocVar.ROOT, Some(deref)) => s"DocField($deref)"
      case _ => s"DocVar($name, $deref)"
    }
  }
  object DocVar {
    final case class Name(name: String) {
      def apply() = DocVar(this, None)

      def apply(field: BsonField) = DocVar(this, Some(field))

      def apply(deref: Option[BsonField]) = DocVar(this, deref)

      def apply(leaves: List[BsonField.Leaf]) = DocVar(this, BsonField(leaves))

      def unapply(v: DocVar): Option[Option[BsonField]] = Some(v.deref)
    }
    val ROOT    = Name("ROOT")
    val CURRENT = Name("CURRENT")
  }

  sealed trait GroupOp {
    val value: ExprOp
    val op: String

    def bson = Bson.Doc(ListMap(op -> value.bson))

    def rewriteRefs(applyVar: PartialFunction[DocVar, DocVar]) = this.mapUp {
      case f @ DocVar(_, _) => applyVar.lift(f).getOrElse(f)
    }

    def mapUp(f0: PartialFunction[ExprOp, ExprOp]): GroupOp = {
      (mapUpM[Free.Trampoline](new PartialFunction[ExprOp, Free.Trampoline[ExprOp]] {
        def isDefinedAt(v: ExprOp) = f0.isDefinedAt(v)
        def apply(v: ExprOp) = f0(v).point[Free.Trampoline]
      })).run
    }

    // TODO: Port physical plan to fixplate to eliminate this madness! (#35)
    def mapUpM[F[_]](f0: PartialFunction[ExprOp, F[ExprOp]])(implicit F: Monad[F]): F[GroupOp] = {
      this match {
        case AddToSet(d) => d.mapUpM(f0).map(AddToSet(_))
        case Avg(v)      => v.mapUpM(f0).map(Avg(_))
        case First(a)    => a.mapUpM(f0).map(First(_))
        case Last(a)     => a.mapUpM(f0).map(Last(_))
        case Max(a)      => a.mapUpM(f0).map(Max(_))
        case Min(a)      => a.mapUpM(f0).map(Min(_))
        case Push(d)     => d.mapUpM(f0).map(Push(_))
        case Sum(a)      => a.mapUpM(f0).map(Sum(_))
      }
    }
  }
  final case class AddToSet(value: ExprOp) extends GroupOp { val op = "$addToSet" }
  final case class Push(value: ExprOp)     extends GroupOp { val op = "$push" }
  final case class First(value: ExprOp)    extends GroupOp { val op = "$first" }
  final case class Last(value: ExprOp)     extends GroupOp { val op = "$last" }
  final case class Max(value: ExprOp)      extends GroupOp { val op = "$max" }
  final case class Min(value: ExprOp)      extends GroupOp { val op = "$min" }
  final case class Avg(value: ExprOp)      extends GroupOp { val op = "$avg" }
  final case class Sum(value: ExprOp)      extends GroupOp { val op = "$sum" }

  implicit val GroupOpRenderTree = RenderTree.fromToString[GroupOp]("GroupOp")

  sealed trait BoolOp extends SimpleOp
  final case class And(values: NonEmptyList[ExprOp]) extends BoolOp {
    val op = "$and"
    def rhs = Bson.Arr(values.list.map(_.bson))
  }
  final case class Or(values: NonEmptyList[ExprOp]) extends BoolOp {
    val op = "$or"
    def rhs = Bson.Arr(values.list.map(_.bson))
  }
  final case class Not(value: ExprOp) extends BoolOp {
    val op = "$not"
    def rhs = value.bson
  }

  sealed trait BinarySetOp extends SimpleOp {
    def left: ExprOp
    def right: ExprOp

    def rhs = Bson.Arr(left.bson :: right.bson :: Nil)
  }
  final case class SetEquals(left: ExprOp, right: ExprOp) extends BinarySetOp {
    val op = "$setEquals"
  }
  final case class SetIntersection(left: ExprOp, right: ExprOp) extends BinarySetOp {
    val op = "$setIntersection"
  }
  final case class SetDifference(left: ExprOp, right: ExprOp) extends BinarySetOp {
    val op = "$setDifference"
  }
  final case class SetUnion(left: ExprOp, right: ExprOp) extends BinarySetOp {
    val op = "$setUnion"
  }
  final case class SetIsSubset(left: ExprOp, right: ExprOp) extends BinarySetOp {
    val op = "$setIsSubset"
  }

  sealed trait UnarySetOp extends SimpleOp {
    def value: ExprOp

    def rhs = value.bson
  }
  final case class AnyElementTrue(value: ExprOp) extends UnarySetOp {
    val op = "$anyElementTrue"
  }
  final case class AllElementsTrue(value: ExprOp) extends UnarySetOp {
    val op = "$allElementsTrue"
  }

  sealed trait CompOp extends SimpleOp {
    def left: ExprOp
    def right: ExprOp

    def rhs = Bson.Arr(left.bson :: right.bson :: Nil)
  }
  final case class Cmp(left: ExprOp, right: ExprOp) extends CompOp { val op = "$cmp" }
  final case class Eq(left: ExprOp, right: ExprOp)  extends CompOp { val op = "$eq" }
  final case class Gt(left: ExprOp, right: ExprOp)  extends CompOp { val op = "$gt" }
  final case class Gte(left: ExprOp, right: ExprOp) extends CompOp { val op = "$gte" }
  final case class Lt(left: ExprOp, right: ExprOp)  extends CompOp { val op = "$lt" }
  final case class Lte(left: ExprOp, right: ExprOp) extends CompOp { val op = "$lte" }
  final case class Neq(left: ExprOp, right: ExprOp) extends CompOp { val op = "$ne" }

  sealed trait MathOp extends SimpleOp {
    def left: ExprOp
    def right: ExprOp

    def rhs = Bson.Arr(left.bson :: right.bson :: Nil)
  }
  final case class Add(left: ExprOp, right: ExprOp) extends MathOp {
    val op = "$add"
  }
  final case class Divide(left: ExprOp, right: ExprOp) extends MathOp {
    val op = "$divide"
  }
  final case class Mod(left: ExprOp, right: ExprOp) extends MathOp {
    val op = "$mod"
  }
  final case class Multiply(left: ExprOp, right: ExprOp) extends MathOp {
    val op = "$multiply"
  }
  final case class Subtract(left: ExprOp, right: ExprOp) extends MathOp {
    val op = "$subtract"
  }

  sealed trait StringOp extends SimpleOp
  final case class Concat(first: ExprOp, second: ExprOp, others: List[ExprOp]) extends StringOp {
    val op = "$concat"
    def rhs = Bson.Arr(first.bson :: second.bson :: others.map(_.bson))
  }
  final case class Strcasecmp(left: ExprOp, right: ExprOp) extends StringOp {
    val op = "$strcasecmp"
    def rhs = Bson.Arr(left.bson :: right.bson :: Nil)
  }
  final case class Substr(value: ExprOp, start: ExprOp, count: ExprOp)
      extends StringOp {
    val op = "$substr"
    def rhs = Bson.Arr(value.bson :: start.bson :: count.bson :: Nil)
  }
  final case class ToLower(value: ExprOp) extends StringOp {
    val op = "$toLower"
    def rhs = value.bson
  }
  final case class ToUpper(value: ExprOp) extends StringOp {
    val op = "$toUpper"
    def rhs = value.bson
  }

  sealed trait TextSearchOp extends SimpleOp
  final case object Meta extends TextSearchOp {
    val op = "$meta"
    def rhs = Bson.Text("textScore")
  }

  sealed trait ArrayOp extends SimpleOp
  final case class Size(array: ExprOp) extends ArrayOp {
    val op = "$size"
    def rhs = array.bson
  }

  sealed trait ProjOp extends ExprOp
  final case class ArrayMap(input: ExprOp, as: DocVar.Name, in: ExprOp)
      extends SimpleOp {
    val op = "$map"
    def rhs = Bson.Doc(ListMap(
      "input" -> input.bson,
      "as"    -> Bson.Text(as.name),
      "in"    -> in.bson
    ))
  }
  final case class Let(vars: ListMap[DocVar.Name, ExprOp], in: ExprOp) extends SimpleOp {
    val op = "$let"
    def rhs = Bson.Doc(ListMap(
      "vars" -> Bson.Doc(vars.map(t => (t._1.name, t._2.bson))),
      "in"   -> in.bson
    ))
  }
  final case class Literal(value: Bson) extends ProjOp {
    def bson = Bson.Doc(ListMap("$literal" -> value))
  }

  sealed trait DateOp extends SimpleOp {
    def date: ExprOp

    def rhs = date.bson
  }
  final case class DayOfYear(date: ExprOp)   extends DateOp { val op = "$dayOfYear" }
  final case class DayOfMonth(date: ExprOp)  extends DateOp { val op = "$dayOfMonth" }
  final case class DayOfWeek(date: ExprOp)   extends DateOp { val op = "$dayOfWeek" }
  final case class Year(date: ExprOp)        extends DateOp { val op = "$year" }
  final case class Month(date: ExprOp)       extends DateOp { val op = "$month" }
  final case class Week(date: ExprOp)        extends DateOp { val op = "$week" }
  final case class Hour(date: ExprOp)        extends DateOp { val op = "$hour" }
  final case class Minute(date: ExprOp)      extends DateOp { val op = "$minute" }
  final case class Second(date: ExprOp)      extends DateOp { val op = "$second" }
  final case class Millisecond(date: ExprOp) extends DateOp { val op = "$millisecond" }

  sealed trait CondOp extends SimpleOp
  final case class Cond(predicate: ExprOp, ifTrue: ExprOp, ifFalse: ExprOp)
      extends CondOp {
    val op = "$cond"
    def rhs = Bson.Arr(predicate.bson :: ifTrue.bson :: ifFalse.bson :: Nil)
  }
  final case class IfNull(expr: ExprOp, replacement: ExprOp) extends CondOp {
    val op = "$ifNull"
    def rhs = Bson.Arr(expr.bson :: replacement.bson :: Nil)
  }
}
