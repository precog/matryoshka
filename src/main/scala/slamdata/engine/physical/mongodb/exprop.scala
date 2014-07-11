package slamdata.engine.physical.mongodb

import scalaz._
import Scalaz._

import slamdata.engine.{RenderTree, Terminal, NonTerminal}
import slamdata.engine.fp._

sealed trait ExprOp {
  def bson: Bson

  import ExprOp._

  def mapUp(f0: PartialFunction[ExprOp, ExprOp]): ExprOp = {
    (mapUpM[Free.Trampoline](new PartialFunction[ExprOp, Free.Trampoline[ExprOp]] {
      def isDefinedAt(v: ExprOp) = f0.isDefinedAt(v)
      def apply(v: ExprOp) = f0(v).point[Free.Trampoline]
    })).run
  }

  // TODO: Port physical plan to fixplate to eliminate this madness!!!!!!!!!!!!!!!!!!!!!
  def mapUpM[F[_]](f0: PartialFunction[ExprOp, F[ExprOp]])(implicit F: Monad[F]): F[ExprOp] = {
    val f0l = f0.lift
    val f = (e: ExprOp) => f0l(e).getOrElse(e.point[F])

    def mapUp0(v: ExprOp): F[ExprOp] = {
      val rec = (v match {
        case Include            => v.point[F]
        case Exclude            => v.point[F]
        case DocVar(_, _)       => v.point[F]
        case Add(l, r)          => (mapUp0(l) |@| mapUp0(r))(Add(_, _))
        case AddToSet(_)        => v.point[F]
        case And(v)             => v.map(mapUp0 _).sequenceU.map(And(_))
        case SetEquals(l, r)       => (mapUp0(l) |@| mapUp0(r))(SetEquals(_, _))
        case SetIntersection(l, r) => (mapUp0(l) |@| mapUp0(r))(SetIntersection(_, _))
        case SetDifference(l, r)   => (mapUp0(l) |@| mapUp0(r))(SetDifference(_, _))
        case SetUnion(l, r)        => (mapUp0(l) |@| mapUp0(r))(SetUnion(_, _))
        case SetIsSubset(l, r)     => (mapUp0(l) |@| mapUp0(r))(SetIsSubset(_, _))
        case AnyElementTrue(v)     => mapUp0(v).map(AnyElementTrue(_))
        case AllElementsTrue(v)    => mapUp0(v).map(AllElementsTrue(_))
        case ArrayMap(a, b, c)  => (mapUp0(a) |@| mapUp0(c))(ArrayMap(_, b, _))
        case Avg(v)             => mapUp0(v).map(Avg(_))
        case Cmp(l, r)          => (mapUp0(l) |@| mapUp0(r))(Cmp(_, _))
        case Concat(a, b, cs)   => (mapUp0(a) |@| mapUp0(b) |@| cs.map(mapUp0 _).sequenceU)(Concat(_, _, _))
        case Cond(a, b, c)      => (mapUp0(a) |@| mapUp0(b) |@| mapUp0(c))(Cond(_, _, _))
        case DayOfMonth(a)      => mapUp0(a).map(DayOfMonth(_))
        case DayOfWeek(a)       => mapUp0(a).map(DayOfWeek(_))
        case DayOfYear(a)       => mapUp0(a).map(DayOfYear(_))
        case Divide(a, b)       => (mapUp0(a) |@| mapUp0(b))(Divide(_, _))
        case Eq(a, b)           => (mapUp0(a) |@| mapUp0(b))(Eq(_, _))
        case First(a)           => mapUp0(a).map(First(_))
        case Gt(a, b)           => (mapUp0(a) |@| mapUp0(b))(Gt(_, _))
        case Gte(a, b)          => (mapUp0(a) |@| mapUp0(b))(Gte(_, _))
        case Hour(a)            => mapUp0(a).map(Hour(_))
        case Meta                  => v.point[F]
        case Size(a)               => mapUp0(a).map(Size(_))
        case IfNull(a, b)       => (mapUp0(a) |@| mapUp0(b))(IfNull(_, _))
        case Last(a)            => mapUp0(a).map(Last(_))
        case Let(a, b)          => 
          type MapDocVarName[X] = Map[ExprOp.DocVar.Name, X]

          (Traverse[MapDocVarName].sequence[F, ExprOp](a.map(t => t._1 -> mapUp0(t._2))) |@| mapUp0(b))(Let(_, _))

        case Literal(_)         => v.point[F]
        case Lt(a, b)           => (mapUp0(a) |@| mapUp0(b))(Lt(_, _))
        case Lte(a, b)          => (mapUp0(a) |@| mapUp0(b))(Lte(_, _))
        case Max(a)             => mapUp0(a).map(Max(_))
        case Millisecond(a)     => mapUp0(a).map(Millisecond(_))
        case Min(a)             => mapUp0(a).map(Min(_))
        case Minute(a)          => mapUp0(a).map(Minute(_))
        case Mod(a, b)          => (mapUp0(a) |@| mapUp0(b))(Mod(_, _))
        case Month(a)           => mapUp0(a).map(Month(_))
        case Multiply(a, b)     => (mapUp0(a) |@| mapUp0(b))(Multiply(_, _))
        case Neq(a, b)          => (mapUp0(a) |@| mapUp0(b))(Neq(_, _))
        case Not(a)             => mapUp0(a).map(Not(_))
        case Or(a)              => a.map(mapUp0 _).sequenceU.map(Or(_))
        case Push(a)            => v.point[F]
        case Second(a)          => mapUp0(a).map(Second(_))
        case Strcasecmp(a, b)   => (mapUp0(a) |@| mapUp0(b))(Strcasecmp(_, _))
        case Substr(a, b, c)    => (mapUp0(a) |@| mapUp0(b) |@| mapUp0(c))(Substr(_, _, _))
        case Subtract(a, b)     => (mapUp0(a) |@| mapUp0(b))(Subtract(_, _))
        case Sum(a)             => mapUp0(a).map(Sum(_))
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
    override def render(v: ExprOp) = Terminal(v.toString)  // TODO
  }

  def children(expr: ExprOp): List[ExprOp] = expr match {
    case Include               => Nil
    case Exclude               => Nil
    case DocVar(_, _)          => Nil
    case Add(l, r)             => l :: r :: Nil
    case AddToSet(_)           => Nil
    case And(v)                => v.toList
    case SetEquals(l, r)       => l :: r :: Nil
    case SetIntersection(l, r) => l :: r :: Nil
    case SetDifference(l, r)   => l :: r :: Nil
    case SetUnion(l, r)        => l :: r :: Nil
    case SetIsSubset(l, r)     => l :: r :: Nil
    case AnyElementTrue(v)     => v :: Nil
    case AllElementsTrue(v)    => v :: Nil
    case ArrayMap(a, _, c)     => a :: c :: Nil
    case Avg(v)                => v :: Nil
    case Cmp(l, r)             => l :: r :: Nil
    case Concat(a, b, cs)      => a :: b :: cs
    case Cond(a, b, c)         => a :: b :: c :: Nil
    case DayOfMonth(a)         => a :: Nil
    case DayOfWeek(a)          => a :: Nil
    case DayOfYear(a)          => a :: Nil
    case Divide(a, b)          => a :: b :: Nil
    case Eq(a, b)              => a :: b :: Nil
    case First(a)              => a :: Nil
    case Gt(a, b)              => a :: b :: Nil
    case Gte(a, b)             => a :: b :: Nil
    case Hour(a)               => a :: Nil
    case Meta                  => Nil
    case Size(a)               => a :: Nil
    case IfNull(a, b)          => a :: b :: Nil
    case Last(a)               => a :: Nil
    case Let(_, b)             => b :: Nil
    case Literal(_)            => Nil
    case Lt(a, b)              => a :: b :: Nil
    case Lte(a, b)             => a :: b :: Nil
    case Max(a)                => a :: Nil
    case Millisecond(a)        => a :: Nil
    case Min(a)                => a :: Nil
    case Minute(a)             => a :: Nil
    case Mod(a, b)             => a :: b :: Nil
    case Month(a)              => a :: Nil
    case Multiply(a, b)        => a :: b :: Nil
    case Neq(a, b)             => a :: b :: Nil
    case Not(a)                => a :: Nil
    case Or(a)                 => a.toList
    case Push(a)               => Nil
    case Second(a)             => a :: Nil
    case Strcasecmp(a, b)      => a :: b :: Nil
    case Substr(a, b, c)       => a :: b :: c :: Nil
    case Subtract(a, b)        => a :: b :: Nil
    case Sum(a)                => a :: Nil
    case ToLower(a)            => a :: Nil
    case ToUpper(a)            => a :: Nil
    case Week(a)               => a :: Nil
    case Year(a)               => a :: Nil
  }

  def foldMap[Z: Monoid](f0: PartialFunction[ExprOp, Z])(v: ExprOp): Z = {
    val f = (e: ExprOp) => f0.lift(e).getOrElse(Monoid[Z].zero)
    Monoid[Z].append(f(v), Foldable[List].foldMap(children(v))(foldMap(f0)))
  }

  private[ExprOp] abstract sealed class SimpleOp(op: String) extends ExprOp {
    def rhs: Bson

    def bson = Bson.Doc(Map(op -> rhs))
  }

  sealed trait IncludeExclude extends ExprOp
  case object Include extends IncludeExclude {
    def bson = Bson.Int32(1)
  }
  case object Exclude extends IncludeExclude {
    def bson = Bson.Int32(0)
  }

  sealed trait FieldLike extends ExprOp {
    def field: BsonField
  }
  object DocField {
    def apply(field: BsonField): DocVar = DocVar.ROOT(field)

    def unapply(docVar: DocVar): Option[BsonField] = docVar match {
      case DocVar.ROOT(tail) => tail
      case _ => None
    }
  }
  case class DocVar(name: DocVar.Name, deref: Option[BsonField]) extends FieldLike {
    def field: BsonField = BsonField.Name(name.name) \\ deref.toList.flatMap(_.flatten)

    def bson = this match {
      case DocVar(DocVar.ROOT, Some(deref)) => Bson.Text(deref.asField)
      case _                                => Bson.Text(field.asVar)
    }

    def nestsWith(that: DocVar): Boolean = this.name == that.name

    def \ (that: DocVar): Option[DocVar] = (this, that) match {
      case (DocVar(n1, f1), DocVar(n2, f2)) if (n1 == n2) => 
        val f3 = (f1 |@| f2)(_ \ _) orElse (f1) orElse (f2)

        Some(DocVar(n1, f3))

      case _ => None
    }

    def \ (field: BsonField): DocVar = copy(deref = Some(deref.map(_ \ field).getOrElse(field)))

    override def toString = this match {
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

  sealed trait GroupOp extends ExprOp
  case class AddToSet(field: DocVar) extends SimpleOp("$addToSet") with GroupOp {
    def rhs = field.bson
  }
  case class Push(field: DocVar) extends SimpleOp("$push") with GroupOp {
    def rhs = field.bson
  }
  case class First(value: ExprOp) extends SimpleOp("$first") with GroupOp {
    def rhs = value.bson
  }
  case class Last(value: ExprOp) extends SimpleOp("$last") with GroupOp {
    def rhs = value.bson
  }
  case class Max(value: ExprOp) extends SimpleOp("$max") with GroupOp {
    def rhs = value.bson
  }
  case class Min(value: ExprOp) extends SimpleOp("$min") with GroupOp {
    def rhs = value.bson
  }
  case class Avg(value: ExprOp) extends SimpleOp("$avg") with GroupOp {
    def rhs = value.bson
  }
  case class Sum(value: ExprOp) extends SimpleOp("$sum") with GroupOp {
    def rhs = value.bson
  }
  object Count extends Sum(Literal(Bson.Int32(1)))

  sealed trait BoolOp extends ExprOp
  case class And(values: NonEmptyList[ExprOp]) extends SimpleOp("$and") with BoolOp {
    def rhs = Bson.Arr(values.list.map(_.bson))
  }
  case class Or(values: NonEmptyList[ExprOp]) extends SimpleOp("$or") with BoolOp {
    def rhs = Bson.Arr(values.list.map(_.bson))
  }
  case class Not(value: ExprOp) extends SimpleOp("$not") with BoolOp {
    def rhs = value.bson
  }

  sealed trait BinarySetOp extends ExprOp {
    def left: ExprOp
    def right: ExprOp
    
    def rhs = Bson.Arr(left.bson :: right.bson :: Nil)
  }
  case class SetEquals(left: ExprOp, right: ExprOp) extends SimpleOp("$setEquals") with BinarySetOp
  case class SetIntersection(left: ExprOp, right: ExprOp) extends SimpleOp("$setIntersection") with BinarySetOp
  case class SetDifference(left: ExprOp, right: ExprOp) extends SimpleOp("$setDifference") with BinarySetOp
  case class SetUnion(left: ExprOp, right: ExprOp) extends SimpleOp("$setUnion") with BinarySetOp
  case class SetIsSubset(left: ExprOp, right: ExprOp) extends SimpleOp("$setIsSubset") with BinarySetOp

  sealed trait UnarySetOp extends ExprOp {
    def value: ExprOp
    
    def rhs = value.bson
  }
  case class AnyElementTrue(value: ExprOp) extends SimpleOp("$anyElementTrue") with UnarySetOp
  case class AllElementsTrue(value: ExprOp) extends SimpleOp("$allElementsTrue") with UnarySetOp

  sealed trait CompOp extends ExprOp {
    def left: ExprOp    
    def right: ExprOp

    def rhs = Bson.Arr(left.bson :: right.bson :: Nil)
  }
  case class Cmp(left: ExprOp, right: ExprOp) extends SimpleOp("$cmp") with CompOp
  case class Eq(left: ExprOp, right: ExprOp) extends SimpleOp("$eq") with CompOp
  case class Gt(left: ExprOp, right: ExprOp) extends SimpleOp("$gt") with CompOp
  case class Gte(left: ExprOp, right: ExprOp) extends SimpleOp("$gte") with CompOp
  case class Lt(left: ExprOp, right: ExprOp) extends SimpleOp("$lt") with CompOp
  case class Lte(left: ExprOp, right: ExprOp) extends SimpleOp("$lte") with CompOp
  case class Neq(left: ExprOp, right: ExprOp) extends SimpleOp("$ne") with CompOp

  sealed trait MathOp extends ExprOp {
    def left: ExprOp
    def right: ExprOp

    def rhs = Bson.Arr(left.bson :: right.bson :: Nil)
  }
  case class Add(left: ExprOp, right: ExprOp) extends SimpleOp("$add") with MathOp
  case class Divide(left: ExprOp, right: ExprOp) extends SimpleOp("$divide") with MathOp
  case class Mod(left: ExprOp, right: ExprOp) extends SimpleOp("$mod") with MathOp
  case class Multiply(left: ExprOp, right: ExprOp) extends SimpleOp("$multiply") with MathOp
  case class Subtract(left: ExprOp, right: ExprOp) extends SimpleOp("$subtract") with MathOp

  sealed trait StringOp extends ExprOp
  case class Concat(first: ExprOp, second: ExprOp, others: List[ExprOp]) extends SimpleOp("$concat") with StringOp {
    def rhs = Bson.Arr(first.bson :: second.bson :: others.map(_.bson))
  }
  case class Strcasecmp(left: ExprOp, right: ExprOp) extends SimpleOp("$strcasecmp") with StringOp {
    def rhs = Bson.Arr(left.bson :: right.bson :: Nil)
  }
  case class Substr(value: ExprOp, start: ExprOp, count: ExprOp) extends SimpleOp("$substr") with StringOp {
    def rhs = Bson.Arr(value.bson :: start.bson :: count.bson :: Nil)
  }
  case class ToLower(value: ExprOp) extends SimpleOp("$toLower") with StringOp {
    def rhs = value.bson
  }
  case class ToUpper(value: ExprOp) extends SimpleOp("$toUpper") with StringOp {
    def rhs = value.bson
  }

  sealed trait TextSearchOp extends ExprOp
  case object Meta extends SimpleOp("$meta") with TextSearchOp {
    def rhs = Bson.Text("textScore")
  }

  sealed trait ArrayOp extends ExprOp
  case class Size(array: ExprOp) extends SimpleOp("$size") with ArrayOp {
    def rhs = array.bson
  }

  sealed trait ProjOp extends ExprOp
  case class ArrayMap(input: ExprOp, as: String, in: ExprOp) extends SimpleOp("$map") with ProjOp {
    def rhs = Bson.Doc(Map(
      "input" -> input.bson,
      "as"    -> Bson.Text(as),
      "in"    -> in.bson
    ))
  }
  case class Let(vars: Map[DocVar.Name, ExprOp], in: ExprOp) extends SimpleOp("$let") with ProjOp {
    def rhs = Bson.Doc(Map(
      "vars" -> Bson.Doc(vars.map(t => (t._1.name, t._2.bson))),
      "in"   -> in.bson
    ))
  }
  case class Literal(value: Bson) extends ProjOp {
    def bson = Bson.Doc(Map("$literal" -> value))
  }

  sealed trait DateOp extends ExprOp {
    def date: ExprOp

    def rhs = date.bson
  }
  case class DayOfYear(date: ExprOp) extends SimpleOp("$dayOfYear") with DateOp
  case class DayOfMonth(date: ExprOp) extends SimpleOp("$dayOfMonth") with DateOp
  case class DayOfWeek(date: ExprOp) extends SimpleOp("$dayOfWeek") with DateOp
  case class Year(date: ExprOp) extends SimpleOp("$year") with DateOp
  case class Month(date: ExprOp) extends SimpleOp("$month") with DateOp
  case class Week(date: ExprOp) extends SimpleOp("$week") with DateOp
  case class Hour(date: ExprOp) extends SimpleOp("$hour") with DateOp
  case class Minute(date: ExprOp) extends SimpleOp("$minute") with DateOp
  case class Second(date: ExprOp) extends SimpleOp("$second") with DateOp
  case class Millisecond(date: ExprOp) extends SimpleOp("$millisecond") with DateOp

  sealed trait CondOp extends ExprOp
  case class Cond(predicate: ExprOp, ifTrue: ExprOp, ifFalse: ExprOp) extends SimpleOp("$cond") with CondOp {
    def rhs = Bson.Arr(predicate.bson :: ifTrue.bson :: ifFalse.bson :: Nil)
  }
  case class IfNull(expr: ExprOp, replacement: ExprOp) extends CondOp {
    def bson = Bson.Arr(expr.bson :: replacement.bson :: Nil)
  }
}
