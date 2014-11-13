package slamdata.engine.physical.mongodb

import collection.immutable.ListMap

import scalaz._
import Scalaz._

import slamdata.engine.{RenderTree, Terminal, NonTerminal}
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

    def docVar(d: DocVar): F[DocVar] = f(d).map {
      case d @ DocVar(_, _) => d
      case _ => d
    }

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

  def children(expr: ExprOp): List[ExprOp] = expr match {
    case Include               => Nil
    case DocVar(_, _)          => Nil
    case Add(l, r)             => l :: r :: Nil
    case And(v)                => v.toList
    case SetEquals(l, r)       => l :: r :: Nil
    case SetIntersection(l, r) => l :: r :: Nil
    case SetDifference(l, r)   => l :: r :: Nil
    case SetUnion(l, r)        => l :: r :: Nil
    case SetIsSubset(l, r)     => l :: r :: Nil
    case AnyElementTrue(v)     => v :: Nil
    case AllElementsTrue(v)    => v :: Nil
    case ArrayMap(a, _, c)     => a :: c :: Nil
    case Cmp(l, r)             => l :: r :: Nil
    case Concat(a, b, cs)      => a :: b :: cs
    case Cond(a, b, c)         => a :: b :: c :: Nil
    case DayOfMonth(a)         => a :: Nil
    case DayOfWeek(a)          => a :: Nil
    case DayOfYear(a)          => a :: Nil
    case Divide(a, b)          => a :: b :: Nil
    case Eq(a, b)              => a :: b :: Nil
    case Gt(a, b)              => a :: b :: Nil
    case Gte(a, b)             => a :: b :: Nil
    case Hour(a)               => a :: Nil
    case Meta                  => Nil
    case Size(a)               => a :: Nil
    case IfNull(a, b)          => a :: b :: Nil
    case Let(_, b)             => b :: Nil
    case Literal(_)            => Nil
    case Lt(a, b)              => a :: b :: Nil
    case Lte(a, b)             => a :: b :: Nil
    case Millisecond(a)        => a :: Nil
    case Minute(a)             => a :: Nil
    case Mod(a, b)             => a :: b :: Nil
    case Month(a)              => a :: Nil
    case Multiply(a, b)        => a :: b :: Nil
    case Neq(a, b)             => a :: b :: Nil
    case Not(a)                => a :: Nil
    case Or(a)                 => a.toList
    case Second(a)             => a :: Nil
    case Strcasecmp(a, b)      => a :: b :: Nil
    case Substr(a, b, c)       => a :: b :: c :: Nil
    case Subtract(a, b)        => a :: b :: Nil
    case ToLower(a)            => a :: Nil
    case ToUpper(a)            => a :: Nil
    case Week(a)               => a :: Nil
    case Year(a)               => a :: Nil
  }

  // TODO: This only has `Option` because we haven’t completed all cases
  def toJs(expr: ExprOp): Js.Expr => Option[Js.Expr] = {
    def binop(op: String, l: ExprOp, r: ExprOp) =
      (x: Js.Expr) => (toJs(l)(x) |@| toJs(r)(x))(Js.BinOp(op, _, _))
    expr match {
      case dv @ DocVar(_, _) => x => Some(dv.toJs(x))
      case Add(l, r)         => binop("+", l, r)
      case Eq(l, r)          => binop("==", l, r)
      case Neq(l, r)         => binop("!=", l, r)
      case Lt(l, r)          => binop("<", l, r)
      case Lte(l, r)         => binop("<=", l, r)
      case Gt(l, r)          => binop(">", l, r)
      case Gte(l, r)         => binop(">=", l, r)
      case Divide(l, r)      => binop("/", l, r)
      case Multiply(l, r)    => binop("*", l, r)
      case Subtract(l, r)    => binop("-", l, r)
      case _                 => Function.const(None)
    }
  }

  def foldMap[Z: Monoid](f0: PartialFunction[ExprOp, Z])(v: ExprOp): Z = {
    val f = (e: ExprOp) => f0.lift(e).getOrElse(Monoid[Z].zero)
    Monoid[Z].append(f(v), Foldable[List].foldMap(children(v))(foldMap(f0)))
  }

  private[ExprOp] sealed trait SimpleOp extends ExprOp {
    val op: String
    def rhs: Bson

    def bson = Bson.Doc(ListMap(op -> rhs))
  }

  case object Include extends ExprOp {
    def bson = Bson.Bool(true)

    override def toString = s"ExprOp.Include"
  }

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

    def nestsWith(that: DocVar): Boolean = this.name == that.name

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

    def toJs: Js.Expr => Js.Expr = base => this match {
      case DocVar(_, None)        => base
      case DocVar(_, Some(deref)) => deref.toJs(base)
    }

    def toJsCore: Term[JsCore] => Term[JsCore] = base => this match {
      case DocVar(_, None)        => base
      case DocVar(_, Some(deref)) => deref.toJsCore(base)
    }

    override def toString = this match {
      case DocVar(DocVar.ROOT, None) => "ExprOp.DocVar.ROOT()"
      case DocVar(DocVar.ROOT, Some(deref)) => s"ExprOp.DocField($deref)"
      case _ => s"ExprOp.DocVar($name, $deref)"
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
  object GroupOp {
    def decon(g: GroupOp): (DocVar => GroupOp, ExprOp) = g match {
      case AddToSet(e)  => ((AddToSet.apply _) -> e)
      case Push(e)      => ((Push.apply _) -> e)
      case First(e)     => ((First.apply _) -> e)
      case Last(e)      => ((Last.apply _) -> e)
      case Max(e)       => ((Max.apply _) -> e)
      case Min(e)       => ((Min.apply _) -> e)
      case Avg(e)       => ((Avg.apply _) -> e)
      case Sum(e)       => ((Sum.apply _) -> e)
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

  sealed trait BoolOp extends SimpleOp
  case class And(values: NonEmptyList[ExprOp]) extends BoolOp {
    val op = "$and"
    def rhs = Bson.Arr(values.list.map(_.bson))

    override def toString = s"ExprOp.And($values)"
  }
  case class Or(values: NonEmptyList[ExprOp]) extends BoolOp {
    val op = "$or"
    def rhs = Bson.Arr(values.list.map(_.bson))

    override def toString = s"ExprOp.Or($values)"
  }
  case class Not(value: ExprOp) extends BoolOp {
    val op = "$not"
    def rhs = value.bson

    override def toString = s"ExprOp.Not($value)"
  }

  sealed trait BinarySetOp extends SimpleOp {
    def left: ExprOp
    def right: ExprOp
    
    def rhs = Bson.Arr(left.bson :: right.bson :: Nil)
  }
  case class SetEquals(left: ExprOp, right: ExprOp) extends BinarySetOp {
    val op = "$setEquals"
    override def toString = s"ExprOp.SetEquals($left, $right)"
  }
  case class SetIntersection(left: ExprOp, right: ExprOp) extends BinarySetOp {
    val op = "$setIntersection"
    override def toString = s"ExprOp.SetIntersection($left, $right)"
  }
  case class SetDifference(left: ExprOp, right: ExprOp) extends BinarySetOp {
    val op = "$setDifference"
    override def toString = s"ExprOp.SetDifference($left, $right)"
  }
  case class SetUnion(left: ExprOp, right: ExprOp) extends BinarySetOp {
    val op = "$setUnion"
    override def toString = s"ExprOp.SetUnion($left, $right)"
  }
  case class SetIsSubset(left: ExprOp, right: ExprOp) extends BinarySetOp {
    val op = "$setIsSubset"
    override def toString = s"ExprOp.SetIsSubset($left, $right)"
  }

  sealed trait UnarySetOp extends SimpleOp {
    def value: ExprOp
    
    def rhs = value.bson
  }
  case class AnyElementTrue(value: ExprOp) extends UnarySetOp {
    val op = "$anyElementTrue"
    override def toString = s"ExprOp.AnyElementTrue($value)"
  }
  case class AllElementsTrue(value: ExprOp) extends UnarySetOp {
    val op = "$allElementsTrue"
    override def toString = s"ExprOp.AllElementsTrue($value)"
  }

  sealed trait CompOp extends SimpleOp {
    def left: ExprOp    
    def right: ExprOp

    def rhs = Bson.Arr(left.bson :: right.bson :: Nil)
  }
  case class Cmp(left: ExprOp, right: ExprOp) extends CompOp {
    val op = "$cmp"
    override def toString = s"ExprOp.Cmp($left, $right)"
  }
  case class Eq(left: ExprOp, right: ExprOp) extends CompOp {
    val op = "$eq"
    override def toString = s"ExprOp.Eq($left, $right)"
  }
  case class Gt(left: ExprOp, right: ExprOp) extends CompOp {
    val op = "$gt"
    override def toString = s"ExprOp.Gt($left, $right)"
  }
  case class Gte(left: ExprOp, right: ExprOp) extends CompOp {
    val op = "$gte"
    override def toString = s"ExprOp.Gte($left, $right)"
  }
  case class Lt(left: ExprOp, right: ExprOp) extends CompOp {
    val op = "$lt"
    override def toString = s"ExprOp.Lt($left, $right)"
  }
  case class Lte(left: ExprOp, right: ExprOp) extends CompOp {
    val op = "$lte"
    override def toString = s"ExprOp.Lte($left, $right)"
  }
  case class Neq(left: ExprOp, right: ExprOp) extends CompOp {
    val op = "$ne"
    override def toString = s"ExprOp.Neq($left, $right)"
  }

  sealed trait MathOp extends SimpleOp {
    def left: ExprOp
    def right: ExprOp

    def rhs = Bson.Arr(left.bson :: right.bson :: Nil)
  }
  case class Add(left: ExprOp, right: ExprOp) extends MathOp {
    val op = "$add"
    override def toString = s"ExprOp.Add($left, $right)"
  }
  case class Divide(left: ExprOp, right: ExprOp) extends MathOp {
    val op = "$divide"
    override def toString = s"ExprOp.Divide($left, $right)"
  }
  case class Mod(left: ExprOp, right: ExprOp) extends MathOp {
    val op = "$mod"
    override def toString = s"ExprOp.Mod($left, $right)"
  }
  case class Multiply(left: ExprOp, right: ExprOp) extends MathOp {
    val op = "$multiply"
    override def toString = s"ExprOp.Multiply($left, $right)"
  }
  case class Subtract(left: ExprOp, right: ExprOp) extends MathOp {
    val op = "$subtract"
    override def toString = s"ExprOp.Subtract($left, $right)"
  }

  sealed trait StringOp extends SimpleOp
  case class Concat(first: ExprOp, second: ExprOp, others: List[ExprOp]) extends StringOp {
    val op = "$concat"
    def rhs = Bson.Arr(first.bson :: second.bson :: others.map(_.bson))

    override def toString = s"ExprOp.Concat($first, $second, $others)"
  }
  case class Strcasecmp(left: ExprOp, right: ExprOp) extends StringOp {
    val op = "$strcasecmp"
    def rhs = Bson.Arr(left.bson :: right.bson :: Nil)

    override def toString = s"ExprOp.Strcasecmp($left, $right)"
  }
  case class Substr(value: ExprOp, start: ExprOp, count: ExprOp) extends StringOp {
    val op = "$substr"
    def rhs = Bson.Arr(value.bson :: start.bson :: count.bson :: Nil)

    override def toString = s"ExprOp.Substr($value, $start, $count)"
  }
  case class ToLower(value: ExprOp) extends StringOp {
    val op = "$toLower"
    def rhs = value.bson

    override def toString = s"ExprOp.ToLower($value)"
  }
  case class ToUpper(value: ExprOp) extends StringOp {
    val op = "$toUpper"
    def rhs = value.bson

    override def toString = s"ExprOp.ToUpper($value)"
  }

  sealed trait TextSearchOp extends SimpleOp
  case object Meta extends TextSearchOp {
    val op = "$meta"
    def rhs = Bson.Text("textScore")

    override def toString = "ExprOp.Meta"
  }

  sealed trait ArrayOp extends SimpleOp
  case class Size(array: ExprOp) extends ArrayOp {
    val op = "$size"
    def rhs = array.bson

    override def toString = s"ExprOp.Size($array)"
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

    override def toString = s"ExprOp.ArrayMap($input, $as, $in)"
  }
  case class Let(vars: ListMap[DocVar.Name, ExprOp], in: ExprOp) extends SimpleOp {
    val op = "$let"
    def rhs = Bson.Doc(ListMap(
      "vars" -> Bson.Doc(vars.map(t => (t._1.name, t._2.bson))),
      "in"   -> in.bson
    ))

    override def toString = s"ExprOp.Let($vars, $in)"
  }
  case class Literal(value: Bson) extends ProjOp {
    def bson = Bson.Doc(ListMap("$literal" -> value))

    override def toString = s"ExprOp.Literal($value)"
  }

  sealed trait DateOp extends SimpleOp {
    def date: ExprOp

    def rhs = date.bson
  }
  case class DayOfYear(date: ExprOp) extends DateOp {
    val op = "$dayOfYear"
    override def toString = s"ExprOp.DayOfYear($date)"
  }
  case class DayOfMonth(date: ExprOp) extends DateOp {
    val op = "$dayOfMonth"
    override def toString = s"ExprOp.DayOfMonth($date)"
  }
  case class DayOfWeek(date: ExprOp) extends DateOp {
    val op = "$dayOfWeek"
    override def toString = s"ExprOp.DayOfWeek($date)"
  }
  case class Year(date: ExprOp) extends DateOp {
    val op = "$year"
    override def toString = s"ExprOp.Year($date)"
  }
  case class Month(date: ExprOp) extends DateOp {
    val op = "$month"
    override def toString = s"ExprOp.Month($date)"
  }
  case class Week(date: ExprOp) extends DateOp {
    val op = "$week"
    override def toString = s"ExprOp.Week($date)"
  }
  case class Hour(date: ExprOp) extends DateOp {
    val op = "$hour"
    override def toString = s"ExprOp.Hour($date)"
  }
  case class Minute(date: ExprOp) extends DateOp {
    val op = "$minute"
    override def toString = s"ExprOp.Minute($date)"
  }
  case class Second(date: ExprOp) extends DateOp {
    val op = "$second"
    override def toString = s"ExprOp.Second($date)"
  }
  case class Millisecond(date: ExprOp) extends DateOp {
    val op = "$millisecond"
    override def toString = s"ExprOp.Millisecond($date)"
  }

  sealed trait CondOp extends SimpleOp
  case class Cond(predicate: ExprOp, ifTrue: ExprOp, ifFalse: ExprOp) extends CondOp {
    val op = "$cond"
    def rhs = Bson.Arr(predicate.bson :: ifTrue.bson :: ifFalse.bson :: Nil)

    override def toString = s"ExprOp.Cond($predicate, $ifTrue, $ifFalse)"
  }
  case class IfNull(expr: ExprOp, replacement: ExprOp) extends CondOp {
    val op = "$ifNull"
    def rhs = Bson.Arr(expr.bson :: replacement.bson :: Nil)

    override def toString = s"ExprOp.IfNull($expr, $replacement)"
  }
}
