package slamdata.engine.physical.mongodb

import collection.immutable.ListMap

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
        case And(v)             => v.map(mapUp0 _).sequenceU.map(And(_))
        case SetEquals(l, r)       => (mapUp0(l) |@| mapUp0(r))(SetEquals(_, _))
        case SetIntersection(l, r) => (mapUp0(l) |@| mapUp0(r))(SetIntersection(_, _))
        case SetDifference(l, r)   => (mapUp0(l) |@| mapUp0(r))(SetDifference(_, _))
        case SetUnion(l, r)        => (mapUp0(l) |@| mapUp0(r))(SetUnion(_, _))
        case SetIsSubset(l, r)     => (mapUp0(l) |@| mapUp0(r))(SetIsSubset(_, _))
        case AnyElementTrue(v)     => mapUp0(v).map(AnyElementTrue(_))
        case AllElementsTrue(v)    => mapUp0(v).map(AllElementsTrue(_))
        case ArrayMap(a, b, c)  => (mapUp0(a) |@| mapUp0(c))(ArrayMap(_, b, _))
        case Cmp(l, r)          => (mapUp0(l) |@| mapUp0(r))(Cmp(_, _))
        case Concat(a, b, cs)   => (mapUp0(a) |@| mapUp0(b) |@| cs.map(mapUp0 _).sequenceU)(Concat(_, _, _))
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
          type MapDocVarName[X] = ListMap[ExprOp.DocVar.Name, X]

          (Traverse[MapDocVarName].sequence[F, ExprOp](a.map(t => t._1 -> mapUp0(t._2))) |@| mapUp0(b))(Let(_, _))

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
        case Or(a)              => a.map(mapUp0 _).sequenceU.map(Or(_))
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
  implicit object ExprOpRenderTree extends RenderTree[ExprOp] {
    override def render(v: ExprOp) = Terminal(v.toString, List("ExprOp"))  // TODO
  }

  def toJs(expr: ExprOp): Error \/ JsMacro = {
    import slamdata.engine.PlannerError._

    def expr1(x1: ExprOp)(f: Term[JsCore] => Term[JsCore]): Error \/ JsMacro =
      toJs(x1).map(x1 => JsMacro(x => f(x1(x))))
    def expr2(x1: ExprOp, x2: ExprOp)(f: (Term[JsCore], Term[JsCore]) => Term[JsCore]): Error \/ JsMacro =
      (toJs(x1) |@| toJs(x2))((x1, x2) => JsMacro(x => f(x1(x), x2(x))))

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
      case Include               => \/-(JsMacro(identity))
      case dv @ DocVar(_, _)     => \/-(dv.toJs)
      case Add(l, r)             => binop(JsCore.Add, l, r)
      case Divide(l, r)          => binop(JsCore.Div, l, r)
      case Eq(l, r)              => binop(JsCore.Eq, l, r)
      case Gt(l, r)              => binop(JsCore.Gt, l, r)
      case Gte(l, r)             => binop(JsCore.Gte, l, r)
      case ExprOp.Literal(bson)  => const(bson).map(l => JsMacro(κ(l)))
      case Lt(l, r)              => binop(JsCore.Lt, l, r)
      case Lte(l, r)             => binop(JsCore.Lte, l, r)
      case Meta                  => -\/(NonRepresentableInJS(expr.toString))
      case Multiply(l, r)        => binop(JsCore.Mult, l, r)
      case Neq(l, r)             => binop(JsCore.Neq, l, r)
      case Not(a)                => unop(JsCore.Not, a)

      case Concat(l, r, Nil)     => binop(JsCore.Add, l, r)
      case Substr(f, start, len) =>
        (toJs(f) |@| toJs(start) |@| toJs(len))((f, s, l) =>
          JsMacro(x =>
            JsCore.Call(JsCore.Select(f(x), "substr").fix, List(s(x), l(x))).fix))
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

  case object Include extends ExprOp { def bson = Bson.Bool(true) }

  sealed trait FieldLike extends ExprOp
  object DocField {
    def apply(field: BsonField): DocVar = DocVar.ROOT(field)

    def unapply(docVar: DocVar): Option[BsonField] = docVar match {
      case DocVar.ROOT(tail) => tail
      case _ => None
    }
  }
  case class DocVar(name: DocVar.Name, deref: Option[BsonField]) extends FieldLike {
    def path: List[BsonField.Leaf] = deref.toList.flatMap(_.flatten)

    def startsWith(that: DocVar) = (this.name == that.name) && {
      (this.deref |@| that.deref)(_ startsWith (_)) getOrElse (that.deref.isEmpty)
    }

    def bson = this match {
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

    def toJs: JsMacro = JsMacro(base => this match {
      case DocVar(_, None)        => base
      case DocVar(_, Some(deref)) => deref.toJs(base)
    })

    override def toString = this match {
      case DocVar(DocVar.ROOT, None) => "DocVar.ROOT()"
      case DocVar(DocVar.ROOT, Some(deref)) => s"DocField($deref)"
      case _ => s"DocVar($name, $deref)"
    }
  }
  object DocVar {
    case class Name(name: String) {
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
  case class AddToSet(value: ExprOp) extends GroupOp { val op = "$addToSet" }
  case class Push(value: ExprOp)     extends GroupOp { val op = "$push" }
  case class First(value: ExprOp)    extends GroupOp { val op = "$first" }
  case class Last(value: ExprOp)     extends GroupOp { val op = "$last" }
  case class Max(value: ExprOp)      extends GroupOp { val op = "$max" }
  case class Min(value: ExprOp)      extends GroupOp { val op = "$min" }
  case class Avg(value: ExprOp)      extends GroupOp { val op = "$avg" }
  case class Sum(value: ExprOp)      extends GroupOp { val op = "$sum" }

  implicit object GroupOpRenderTree extends RenderTree[GroupOp] {
    override def render(v: GroupOp) = Terminal(v.toString, List("GroupOp"))
  }

  sealed trait BoolOp extends SimpleOp
  case class And(values: NonEmptyList[ExprOp]) extends BoolOp {
    val op = "$and"
    def rhs = Bson.Arr(values.list.map(_.bson))
  }
  case class Or(values: NonEmptyList[ExprOp]) extends BoolOp {
    val op = "$or"
    def rhs = Bson.Arr(values.list.map(_.bson))
  }
  case class Not(value: ExprOp) extends BoolOp {
    val op = "$not"
    def rhs = value.bson
  }

  sealed trait BinarySetOp extends SimpleOp {
    def left: ExprOp
    def right: ExprOp

    def rhs = Bson.Arr(left.bson :: right.bson :: Nil)
  }
  case class SetEquals(left: ExprOp, right: ExprOp) extends BinarySetOp {
    val op = "$setEquals"
  }
  case class SetIntersection(left: ExprOp, right: ExprOp) extends BinarySetOp {
    val op = "$setIntersection"
  }
  case class SetDifference(left: ExprOp, right: ExprOp) extends BinarySetOp {
    val op = "$setDifference"
  }
  case class SetUnion(left: ExprOp, right: ExprOp) extends BinarySetOp {
    val op = "$setUnion"
  }
  case class SetIsSubset(left: ExprOp, right: ExprOp) extends BinarySetOp {
    val op = "$setIsSubset"
  }

  sealed trait UnarySetOp extends SimpleOp {
    def value: ExprOp

    def rhs = value.bson
  }
  case class AnyElementTrue(value: ExprOp) extends UnarySetOp {
    val op = "$anyElementTrue"
  }
  case class AllElementsTrue(value: ExprOp) extends UnarySetOp {
    val op = "$allElementsTrue"
  }

  sealed trait CompOp extends SimpleOp {
    def left: ExprOp
    def right: ExprOp

    def rhs = Bson.Arr(left.bson :: right.bson :: Nil)
  }
  case class Cmp(left: ExprOp, right: ExprOp) extends CompOp { val op = "$cmp" }
  case class Eq(left: ExprOp, right: ExprOp)  extends CompOp { val op = "$eq" }
  case class Gt(left: ExprOp, right: ExprOp)  extends CompOp { val op = "$gt" }
  case class Gte(left: ExprOp, right: ExprOp) extends CompOp { val op = "$gte" }
  case class Lt(left: ExprOp, right: ExprOp)  extends CompOp { val op = "$lt" }
  case class Lte(left: ExprOp, right: ExprOp) extends CompOp { val op = "$lte" }
  case class Neq(left: ExprOp, right: ExprOp) extends CompOp { val op = "$ne" }

  sealed trait MathOp extends SimpleOp {
    def left: ExprOp
    def right: ExprOp

    def rhs = Bson.Arr(left.bson :: right.bson :: Nil)
  }
  case class Add(left: ExprOp, right: ExprOp) extends MathOp {
    val op = "$add"
  }
  case class Divide(left: ExprOp, right: ExprOp) extends MathOp {
    val op = "$divide"
  }
  case class Mod(left: ExprOp, right: ExprOp) extends MathOp {
    val op = "$mod"
  }
  case class Multiply(left: ExprOp, right: ExprOp) extends MathOp {
    val op = "$multiply"
  }
  case class Subtract(left: ExprOp, right: ExprOp) extends MathOp {
    val op = "$subtract"
  }

  sealed trait StringOp extends SimpleOp
  case class Concat(first: ExprOp, second: ExprOp, others: List[ExprOp]) extends StringOp {
    val op = "$concat"
    def rhs = Bson.Arr(first.bson :: second.bson :: others.map(_.bson))
  }
  case class Strcasecmp(left: ExprOp, right: ExprOp) extends StringOp {
    val op = "$strcasecmp"
    def rhs = Bson.Arr(left.bson :: right.bson :: Nil)
  }
  case class Substr(value: ExprOp, start: ExprOp, count: ExprOp)
      extends StringOp {
    val op = "$substr"
    def rhs = Bson.Arr(value.bson :: start.bson :: count.bson :: Nil)
  }
  case class ToLower(value: ExprOp) extends StringOp {
    val op = "$toLower"
    def rhs = value.bson
  }
  case class ToUpper(value: ExprOp) extends StringOp {
    val op = "$toUpper"
    def rhs = value.bson
  }

  sealed trait TextSearchOp extends SimpleOp
  case object Meta extends TextSearchOp {
    val op = "$meta"
    def rhs = Bson.Text("textScore")
  }

  sealed trait ArrayOp extends SimpleOp
  case class Size(array: ExprOp) extends ArrayOp {
    val op = "$size"
    def rhs = array.bson
  }

  sealed trait ProjOp extends ExprOp
  case class ArrayMap(input: ExprOp, as: DocVar.Name, in: ExprOp)
      extends SimpleOp {
    val op = "$map"
    def rhs = Bson.Doc(ListMap(
      "input" -> input.bson,
      "as"    -> Bson.Text(as.name),
      "in"    -> in.bson
    ))
  }
  case class Let(vars: ListMap[DocVar.Name, ExprOp], in: ExprOp) extends SimpleOp {
    val op = "$let"
    def rhs = Bson.Doc(ListMap(
      "vars" -> Bson.Doc(vars.map(t => (t._1.name, t._2.bson))),
      "in"   -> in.bson
    ))
  }
  case class Literal(value: Bson) extends ProjOp {
    def bson = Bson.Doc(ListMap("$literal" -> value))
  }

  sealed trait DateOp extends SimpleOp {
    def date: ExprOp

    def rhs = date.bson
  }
  case class DayOfYear(date: ExprOp)   extends DateOp { val op = "$dayOfYear" }
  case class DayOfMonth(date: ExprOp)  extends DateOp { val op = "$dayOfMonth" }
  case class DayOfWeek(date: ExprOp)   extends DateOp { val op = "$dayOfWeek" }
  case class Year(date: ExprOp)        extends DateOp { val op = "$year" }
  case class Month(date: ExprOp)       extends DateOp { val op = "$month" }
  case class Week(date: ExprOp)        extends DateOp { val op = "$week" }
  case class Hour(date: ExprOp)        extends DateOp { val op = "$hour" }
  case class Minute(date: ExprOp)      extends DateOp { val op = "$minute" }
  case class Second(date: ExprOp)      extends DateOp { val op = "$second" }
  case class Millisecond(date: ExprOp) extends DateOp { val op = "$millisecond" }

  sealed trait CondOp extends SimpleOp
  case class Cond(predicate: ExprOp, ifTrue: ExprOp, ifFalse: ExprOp)
      extends CondOp {
    val op = "$cond"
    def rhs = Bson.Arr(predicate.bson :: ifTrue.bson :: ifFalse.bson :: Nil)
  }
  case class IfNull(expr: ExprOp, replacement: ExprOp) extends CondOp {
    val op = "$ifNull"
    def rhs = Bson.Arr(expr.bson :: replacement.bson :: Nil)
  }
}
