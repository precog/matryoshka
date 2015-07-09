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

import scalaz._
import Scalaz._

import slamdata.engine.{Error, RenderTree, Terminal, NonTerminal}
import slamdata.engine.analysis.fixplate.{Term}
import slamdata.engine.fp._
import slamdata.engine.javascript._

sealed trait ExprOp[A]

object ExprOp {
  object Types {
    final case class Include[A]() extends ExprOp[A]
    final case class DV[A](docVar: DocVar) extends ExprOp[A]

    final case class And[A](values: NonEmptyList[A]) extends ExprOp[A]
    final case class Or[A](values: NonEmptyList[A])  extends ExprOp[A]
    final case class Not[A](value: A)                extends ExprOp[A]

    final case class SetEquals[A](left: A, right: A) extends ExprOp[A]
    final case class SetIntersection[A](left: A, right: A) extends ExprOp[A]
    final case class SetDifference[A](left: A, right: A) extends ExprOp[A]
    final case class SetUnion[A](left: A, right: A) extends ExprOp[A]
    final case class SetIsSubset[A](left: A, right: A) extends ExprOp[A]

    final case class AnyElementTrue[A](value: A) extends ExprOp[A]
    final case class AllElementsTrue[A](value: A) extends ExprOp[A]

    final case class Cmp[A](left: A, right: A) extends ExprOp[A]
    final case class Eq[A](left: A, right: A)  extends ExprOp[A]
    final case class Gt[A](left: A, right: A)  extends ExprOp[A]
    final case class Gte[A](left: A, right: A) extends ExprOp[A]
    final case class Lt[A](left: A, right: A)  extends ExprOp[A]
    final case class Lte[A](left: A, right: A) extends ExprOp[A]
    final case class Neq[A](left: A, right: A) extends ExprOp[A]

    final case class Add[A](left: A, right: A) extends ExprOp[A]
    final case class Divide[A](left: A, right: A) extends ExprOp[A]
    final case class Mod[A](left: A, right: A) extends ExprOp[A]
    final case class Multiply[A](left: A, right: A) extends ExprOp[A]
    final case class Subtract[A](left: A, right: A) extends ExprOp[A]

    final case class Concat[A](first: A, second: A, others: List[A])
        extends ExprOp[A]
    final case class Strcasecmp[A](left: A, right: A) extends ExprOp[A]
    final case class Substr[A](value: A, start: A, count: A) extends ExprOp[A]
    final case class ToLower[A](value: A) extends ExprOp[A]
    final case class ToUpper[A](value: A) extends ExprOp[A]

    final case class Meta[A]() extends ExprOp[A]

    final case class Size[A](array: A) extends ExprOp[A]

    final case class ArrayMap[A](input: A, as: DocVar.Name, in: A)
        extends ExprOp[A]
    final case class Let[A](vars: ListMap[DocVar.Name, A], in: A)
        extends ExprOp[A]
    final case class Literal[A](value: Bson) extends ExprOp[A]

    final case class DayOfYear[A](date: A)   extends ExprOp[A]
    final case class DayOfMonth[A](date: A)  extends ExprOp[A]
    final case class DayOfWeek[A](date: A)   extends ExprOp[A]
    final case class Year[A](date: A)        extends ExprOp[A]
    final case class Month[A](date: A)       extends ExprOp[A]
    final case class Week[A](date: A)        extends ExprOp[A]
    final case class Hour[A](date: A)        extends ExprOp[A]
    final case class Minute[A](date: A)      extends ExprOp[A]
    final case class Second[A](date: A)      extends ExprOp[A]
    final case class Millisecond[A](date: A) extends ExprOp[A]

    final case class Cond[A](predicate: A, ifTrue: A, ifFalse: A)
        extends ExprOp[A]
    final case class IfNull[A](expr: A, replacement: A) extends ExprOp[A]
  }

  object DSL {
    object $include {
      def apply(): Expression = Term($includeF())
      def unapply(obj: Expression): Boolean = $includeF.unapply(obj.unFix)
    }
    object $ {
      def apply(docVar: DocVar): Expression = Term($F(docVar))
      def apply(field: String, others: String*): Expression =
        $(DocField(others.map(BsonField.Name).foldLeft[BsonField](BsonField.Name(field))(_ \ _)))
      def unapply(obj: Expression): Option[DocVar] = $F.unapply(obj.unFix)
    }
    object $and { def apply(v: Expression, vs: Expression*): Expression = Term($andF(v, vs: _*)) }
    object $or { def apply(v: Expression, vs: Expression*): Expression = Term($orF(v, vs: _*)) }
    object $not {
      def apply(value: Expression): Expression = Term($notF(value))
      def unapply(obj: Expression): Option[Expression] = $notF.unapply(obj.unFix)
    }

    object $setEquals { def apply(left: Expression, right: Expression): Expression = Term($setEqualsF(left, right)) }
    object $setIntersection { def apply(left: Expression, right: Expression): Expression = Term($setIntersectionF(left, right)) }
    object $setDifference { def apply(left: Expression, right: Expression): Expression = Term($setDifferenceF(left, right)) }
    object $setUnion { def apply(left: Expression, right: Expression): Expression = Term($setUnionF(left, right)) }
    object $setIsSubset { def apply(left: Expression, right: Expression): Expression = Term($setIsSubsetF(left, right)) }
    object $anyElementTrue { def apply(value: Expression): Expression = Term($anyElementTrueF(value)) }
    object $allElementsTrue { def apply(value: Expression): Expression = Term($allElementsTrueF(value)) }

    object $cmp { def apply(left: Expression, right: Expression): Expression = Term($cmpF(left, right)) }
    object $eq { def apply(left: Expression, right: Expression): Expression = Term($eqF(left, right)) }
    object $gt { def apply(left: Expression, right: Expression): Expression = Term($gtF(left, right)) }
    object $gte { def apply(left: Expression, right: Expression): Expression = Term($gteF(left, right)) }
    object $lt { def apply(left: Expression, right: Expression): Expression = Term($ltF(left, right)) }
    object $lte { def apply(left: Expression, right: Expression): Expression = Term($lteF(left, right)) }
    object $neq { def apply(left: Expression, right: Expression): Expression = Term($neqF(left, right)) }

    object $add { def apply(left: Expression, right: Expression): Expression = Term($addF(left, right)) }
    object $divide { def apply(left: Expression, right: Expression): Expression = Term($divideF(left, right)) }
    object $mod { def apply(left: Expression, right: Expression): Expression = Term($modF(left, right)) }
    object $multiply { def apply(left: Expression, right: Expression): Expression = Term($multiplyF(left, right)) }
    object $subtract { def apply(left: Expression, right: Expression): Expression = Term($subtractF(left, right)) }

    object $concat { def apply(first: Expression, second: Expression, others: Expression*): Expression = Term($concatF(first, second, others: _*)) }
    object $strcasecmp { def apply(left: Expression, right: Expression): Expression = Term($strcasecmpF(left, right)) }
    object $substr { def apply(value: Expression, start: Expression, count: Expression): Expression = Term($substrF(value, start, count)) }
    object $toLower {
      def apply(value: Expression): Expression = Term($toLowerF(value))
      def unapply(obj: Expression): Option[Expression] = $toLowerF.unapply(obj.unFix)
    }
    object $toUpper {
      def apply(value: Expression): Expression = Term($toUpperF(value))
      def unapply(obj: Expression): Option[Expression] = $toUpperF.unapply(obj.unFix)
    }

    object $meta {
      def apply(): Expression = Term($metaF())
      def unapply(obj: Expression): Boolean = $metaF.unapply(obj.unFix)
    }

    object $size {
      def apply(array: Expression): Expression = Term($sizeF(array))
      def unapply(obj: Expression): Option[Expression] = $sizeF.unapply(obj.unFix)
    }

    object $arrayMap {
      def apply(input: Expression, as: DocVar.Name, in: Expression): Expression =
        Term($arrayMapF(input, as, in))
      def unapply(obj: Expression):
          Option[(Expression, DocVar.Name, Expression)] =
        $arrayMapF.unapply(obj.unFix)
    }
    object $let {
      def apply(vars: ListMap[DocVar.Name, Expression], in: Expression):
          Expression =
        Term($letF(vars, in))
      def unapply(obj: Expression):
          Option[(ListMap[DocVar.Name, Expression], Expression)] =
        $letF.unapply(obj.unFix)
    }
    object $literal {
      def apply(value: Bson): Expression = Term($literalF(value))
      def unapply(obj: Expression): Option[Bson] = $literalF.unapply(obj.unFix)
    }

    object $dayOfYear {
      def apply(date: Expression): Expression = Term($dayOfYearF(date))
      def unapply(obj: Expression): Option[Expression] = $dayOfYearF.unapply(obj.unFix)
    }
    object $dayOfMonth {
      def apply(date: Expression): Expression = Term($dayOfMonthF(date))
      def unapply(obj: Expression): Option[Expression] = $dayOfMonthF.unapply(obj.unFix)
    }
    object $dayOfWeek {
      def apply(date: Expression): Expression = Term($dayOfWeekF(date))
      def unapply(obj: Expression): Option[Expression] = $dayOfWeekF.unapply(obj.unFix)
    }
    object $year {
      def apply(date: Expression): Expression = Term($yearF(date))
      def unapply(obj: Expression): Option[Expression] = $yearF.unapply(obj.unFix)
    }
    object $month {
      def apply(date: Expression): Expression = Term($monthF(date))
      def unapply(obj: Expression): Option[Expression] = $monthF.unapply(obj.unFix)
    }
    object $week {
      def apply(date: Expression): Expression = Term($weekF(date))
      def unapply(obj: Expression): Option[Expression] = $weekF.unapply(obj.unFix)
    }
    object $hour {
      def apply(date: Expression): Expression = Term($hourF(date))
      def unapply(obj: Expression): Option[Expression] = $hourF.unapply(obj.unFix)
    }
    object $minute {
      def apply(date: Expression): Expression = Term($minuteF(date))
      def unapply(obj: Expression): Option[Expression] = $minuteF.unapply(obj.unFix)
    }
    object $second {
      def apply(date: Expression): Expression = Term($secondF(date))
      def unapply(obj: Expression): Option[Expression] = $secondF.unapply(obj.unFix)
    }
    object $millisecond {
      def apply(date: Expression): Expression = Term($millisecondF(date))
      def unapply(obj: Expression): Option[Expression] = $millisecondF.unapply(obj.unFix)
    }

    object $cond { def apply(predicate: Expression, ifTrue: Expression, ifFalse: Expression): Expression = Term($condF(predicate, ifTrue, ifFalse)) }
    object $ifNull { def apply(expr: Expression, replacement: Expression): Expression = Term($ifNullF(expr, replacement)) }

    val $$ROOT = $(DocVar.ROOT())
    val $$CURRENT = $(DocVar.CURRENT())
  }

  import Types._

  object $includeF {
    def apply[A](): ExprOp[A] = Include[A]()
    def unapply[A](obj: ExprOp[A]): Boolean = obj match {
      case Include() => true
      case _         => false
    }
  }
  object $F {
    def apply[A](docVar: DocVar): ExprOp[A] = DV[A](docVar)
    def unapply[A](obj: ExprOp[A]): Option[DocVar] = obj match {
      case DV(docVar) => Some(docVar)
      case _          => None
    }
  }

  object $andF { def apply[A](v: A, vs: A*): ExprOp[A] = And[A](NonEmptyList(v, vs: _*)) }
  object $orF { def apply[A](v: A, vs: A*): ExprOp[A] = Or[A](NonEmptyList(v, vs: _*)) }

  object $notF {
    def apply[A](value: A): ExprOp[A] = Not[A](value)
    def unapply[A](obj: ExprOp[A]): Option[A] = obj match {
      case Not(value) => Some(value)
      case _          => None
    }
  }

  object $setEqualsF { def apply[A](left: A, right: A): ExprOp[A] = SetEquals[A](left, right) }
  object $setIntersectionF { def apply[A](left: A, right: A): ExprOp[A] = SetIntersection[A](left, right) }
  object $setDifferenceF { def apply[A](left: A, right: A): ExprOp[A] = SetDifference[A](left, right) }
  object $setUnionF { def apply[A](left: A, right: A): ExprOp[A] = SetUnion[A](left, right) }
  object $setIsSubsetF { def apply[A](left: A, right: A): ExprOp[A] = SetIsSubset[A](left, right) }

  object $anyElementTrueF { def apply[A](value: A): ExprOp[A] = AnyElementTrue[A](value) }
  object $allElementsTrueF { def apply[A](value: A): ExprOp[A] = AllElementsTrue[A](value) }

  object $cmpF { def apply[A](left: A, right: A): ExprOp[A] = Cmp[A](left, right) }
  object $eqF { def apply[A](left: A, right: A): ExprOp[A] = Eq[A](left, right) }
  object $gtF { def apply[A](left: A, right: A): ExprOp[A] = Gt[A](left, right) }
  object $gteF { def apply[A](left: A, right: A): ExprOp[A] = Gte[A](left, right) }
  object $ltF { def apply[A](left: A, right: A): ExprOp[A] = Lt[A](left, right) }
  object $lteF { def apply[A](left: A, right: A): ExprOp[A] = Lte[A](left, right) }
  object $neqF { def apply[A](left: A, right: A): ExprOp[A] = Neq[A](left, right) }

  object $addF { def apply[A](left: A, right: A): ExprOp[A] = Add[A](left, right) }
  object $divideF { def apply[A](left: A, right: A): ExprOp[A] = Divide[A](left, right) }
  object $modF { def apply[A](left: A, right: A): ExprOp[A] = Mod[A](left, right) }
  object $multiplyF { def apply[A](left: A, right: A): ExprOp[A] = Multiply[A](left, right) }
  object $subtractF { def apply[A](left: A, right: A): ExprOp[A] = Subtract[A](left, right) }

  object $concatF { def apply[A](first: A, second: A, others: A*): ExprOp[A] = Concat[A](first, second, List(others: _*)) }
  object $strcasecmpF { def apply[A](left: A, right: A): ExprOp[A] = Strcasecmp[A](left, right) }
  object $substrF { def apply[A](value: A, start: A, count: A): ExprOp[A] = Substr[A](value, start, count) }
  object $toLowerF {
    def apply[A](value: A): ExprOp[A] = ToLower[A](value)
    def unapply[A](obj: ExprOp[A]): Option[A] = obj match {
      case ToLower(value) => Some(value)
      case _          => None
    }
  }
  object $toUpperF {
    def apply[A](value: A): ExprOp[A] = ToUpper[A](value)
    def unapply[A](obj: ExprOp[A]): Option[A] = obj match {
      case ToUpper(value) => Some(value)
      case _          => None
    }
  }

  object $metaF {
    def apply[A](): ExprOp[A] = Meta[A]()
    def unapply[A](obj: ExprOp[A]): Boolean = obj match {
      case Meta() => true
      case _      => false
    }
  }

  object $sizeF {
    def apply[A](array: A): ExprOp[A] = Size[A](array)
    def unapply[A](obj: ExprOp[A]): Option[A] = obj match {
      case Size(array) => Some(array)
      case _          => None
    }
  }

  object $arrayMapF {
    def apply[A](input: A, as: DocVar.Name, in: A): ExprOp[A] =
      ArrayMap[A](input, as, in)
    def unapply[A](obj: ExprOp[A]): Option[(A, DocVar.Name, A)] = obj match {
      case ArrayMap(input, as, in) => Some((input, as, in))
      case _                       => None
    }
  }
  object $letF {
    def apply[A](vars: ListMap[DocVar.Name, A], in: A): ExprOp[A] =
      Let[A](vars, in)
    def unapply[A](obj: ExprOp[A]): Option[(ListMap[DocVar.Name, A], A)] =
      obj match {
        case Let(vars, in) => Some((vars, in))
        case _             => None
      }
  }
  object $literalF {
    def apply[A](value: Bson): ExprOp[A] = Literal[A](value)
    def unapply[A](obj: ExprOp[A]): Option[Bson] = obj match {
      case Literal(value) => Some(value)
      case _              => None
    }
  }

  object $dayOfYearF {
    def apply[A](date: A): ExprOp[A] = DayOfYear[A](date)
    def unapply[A](obj: ExprOp[A]): Option[A] = obj match {
      case DayOfYear(date) => Some(date)
      case _          => None
    }
  }
  object $dayOfMonthF {
    def apply[A](date: A): ExprOp[A] = DayOfMonth[A](date)
    def unapply[A](obj: ExprOp[A]): Option[A] = obj match {
      case DayOfMonth(date) => Some(date)
      case _          => None
    }
  }
  object $dayOfWeekF {
    def apply[A](date: A): ExprOp[A] = DayOfWeek[A](date)
    def unapply[A](obj: ExprOp[A]): Option[A] = obj match {
      case DayOfWeek(date) => Some(date)
      case _          => None
    }
  }
  object $yearF {
    def apply[A](date: A): ExprOp[A] = Year[A](date)
    def unapply[A](obj: ExprOp[A]): Option[A] = obj match {
      case Year(date) => Some(date)
      case _          => None
    }
  }
  object $monthF {
    def apply[A](date: A): ExprOp[A] = Month[A](date)
    def unapply[A](obj: ExprOp[A]): Option[A] = obj match {
      case Month(date) => Some(date)
      case _          => None
    }
  }
  object $weekF {
    def apply[A](date: A): ExprOp[A] = Week[A](date)
    def unapply[A](obj: ExprOp[A]): Option[A] = obj match {
      case Week(date) => Some(date)
      case _          => None
    }
  }
  object $hourF {
    def apply[A](date: A): ExprOp[A] = Hour[A](date)
    def unapply[A](obj: ExprOp[A]): Option[A] = obj match {
      case Hour(date) => Some(date)
      case _          => None
    }
  }
  object $minuteF {
    def apply[A](date: A): ExprOp[A] = Minute[A](date)
    def unapply[A](obj: ExprOp[A]): Option[A] = obj match {
      case Minute(date) => Some(date)
      case _          => None
    }
  }
  object $secondF {
    def apply[A](date: A): ExprOp[A] = Second[A](date)
    def unapply[A](obj: ExprOp[A]): Option[A] = obj match {
      case Second(date) => Some(date)
      case _          => None
    }
  }
  object $millisecondF {
    def apply[A](date: A): ExprOp[A] = Millisecond[A](date)
    def unapply[A](obj: ExprOp[A]): Option[A] = obj match {
      case Millisecond(date) => Some(date)
      case _          => None
    }
  }

  object $condF { def apply[A](predicate: A, ifTrue: A, ifFalse: A): ExprOp[A] = Cond[A](predicate, ifTrue, ifFalse) }
  object $ifNullF { def apply[A](expr: A, replacement: A): ExprOp[A] = IfNull[A](expr, replacement) }

  type Expression = Term[ExprOp]

  object DocField {
    def apply(field: BsonField): DocVar = DocVar.ROOT(field)

    def unapply(docVar: DocVar): Option[BsonField] = docVar match {
      case DocVar.ROOT(tail) => tail
      case _ => None
    }
  }
  final case class DocVar(name: DocVar.Name, deref: Option[BsonField]) {
    def path: List[BsonField.Leaf] = deref.toList.flatMap(_.flatten.toList)

    def startsWith(that: DocVar) = (this.name == that.name) && {
      (this.deref |@| that.deref)(_ startsWith (_)) getOrElse (that.deref.isEmpty)
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

    def bson: Bson = this match {
      case DocVar(DocVar.ROOT, Some(deref)) => Bson.Text(deref.asField)
      case DocVar(name,        deref) =>
        val root = BsonField.Name(name.name)
        Bson.Text(deref.map(root \ _).getOrElse(root).asVar)
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

  def bsonDoc(op: String, rhs: Bson) = Bson.Doc(ListMap(op -> rhs))
  private def bsonArr(op: String, elems: Bson*) =
    bsonDoc(op, Bson.Arr(elems.toList))

  val bsonƒ: ExprOp[Bson] => Bson = {
    case Include() => Bson.Bool(true)
    case DV(dv) => dv.bson
    case And(values) => bsonArr("$and", values.list: _*)
    case Or(values) => bsonArr("$or", values.list: _*)
    case Not(value) => bsonDoc("$not", value)
    case SetEquals(left, right) => bsonArr("$setEquals", left, right)
    case SetIntersection(left, right) =>
      bsonArr("$setIntersection", left, right)
    case SetDifference(left, right) => bsonArr("$setDifference", left, right)
    case SetUnion(left, right) => bsonArr("$setUnion", left, right)
    case SetIsSubset(left, right) => bsonArr("$setIsSubset", left, right)
    case AnyElementTrue(value) => bsonDoc("$anyElementTrue", value)
    case AllElementsTrue(value) => bsonDoc("$allElementsTrue", value)
    case Cmp(left, right) => bsonArr("$cmp", left, right)
    case Eq(left, right) => bsonArr("$eq", left, right)
    case Gt(left, right) => bsonArr("$gt", left, right)
    case Gte(left, right) => bsonArr("$gte", left, right)
    case Lt(left, right) => bsonArr("$lt", left, right)
    case Lte(left, right) => bsonArr("$lte", left, right)
    case Neq(left, right) => bsonArr("$ne", left, right)
    case Add(left, right) => bsonArr("$add", left, right)
    case Divide(left, right) => bsonArr("$divide", left, right)
    case Mod(left, right) => bsonArr("$mod", left, right)
    case Multiply(left, right) => bsonArr("$multiply", left, right)
    case Subtract(left, right) => bsonArr("$subtract", left, right)
    case Concat(first, second, others) =>
      bsonArr("$concat", first :: second :: others: _*)
    case Strcasecmp(left, right) => bsonArr("$strcasecmp", left, right)
    case Substr(value, start, count) =>
      bsonArr("$substr", value, start, count)
    case ToLower(value) => bsonDoc("$toLower", value)
    case ToUpper(value) => bsonDoc("$toUpper", value)
    case Meta() => bsonDoc("$meta", Bson.Text("textScore"))
    case Size(array) => bsonDoc("$size", array)
    case ArrayMap(input, as, in) =>
      bsonDoc(
        "$map",
        Bson.Doc(ListMap(
          "input" -> input,
          "as"    -> Bson.Text(as.name),
          "in"    -> in)))
    case Let(vars, in) =>
      bsonDoc(
        "$let",
        Bson.Doc(ListMap(
          "vars" -> Bson.Doc(vars.map(t => (t._1.name, t._2))),
          "in"   -> in)))
    case Literal(value) => bsonDoc("$literal", value)
    case DayOfYear(date) => bsonDoc("$dayOfYear", date)
    case DayOfMonth(date) => bsonDoc("$dayOfMonth", date)
    case DayOfWeek(date) => bsonDoc("$dayOfWeek", date)
    case Year(date) => bsonDoc("$year", date)
    case Month(date) => bsonDoc("$month", date)
    case Week(date) => bsonDoc("$week", date)
    case Hour(date) => bsonDoc("$hour", date)
    case Minute(date) => bsonDoc("$minute", date)
    case Second(date) => bsonDoc("$second", date)
    case Millisecond(date) => bsonDoc("$millisecond", date)
    case Cond(predicate, ifTrue, ifFalse) =>
      bsonArr("$cond", predicate, ifTrue, ifFalse)
    case IfNull(expr, replacement) => bsonArr("$ifNull", expr, replacement)
  }

  def rewriteExprRefs(t: Expression)(applyVar: PartialFunction[DocVar, DocVar]) =
    t.cata[Expression] {
      case DV(f) => Term[ExprOp](DV(applyVar.lift(f).getOrElse(f)))
      case x     => Term(x)
    }

  implicit val ExprOpTraverse = new Traverse[ExprOp] {
    def traverseImpl[G[_], A, B](fa: ExprOp[A])(f: A => G[B])(implicit G: Applicative[G]):
        G[ExprOp[B]] =
      fa match {
        case Include()          => G.point(Include())
        case DV(dv)             => G.point(DV(dv))
        case Add(l, r)          => (f(l) |@| f(r))(Add(_, _))
        case And(v)             => G.map(v.traverse(f))(And(_))
        case SetEquals(l, r)       => (f(l) |@| f(r))(SetEquals(_, _))
        case SetIntersection(l, r) => (f(l) |@| f(r))(SetIntersection(_, _))
        case SetDifference(l, r)   => (f(l) |@| f(r))(SetDifference(_, _))
        case SetUnion(l, r)        => (f(l) |@| f(r))(SetUnion(_, _))
        case SetIsSubset(l, r)     => (f(l) |@| f(r))(SetIsSubset(_, _))
        case AnyElementTrue(v)     => G.map(f(v))(AnyElementTrue(_))
        case AllElementsTrue(v)    => G.map(f(v))(AllElementsTrue(_))
        case ArrayMap(a, b, c)  => (f(a) |@| f(c))(ArrayMap(_, b, _))
        case Cmp(l, r)          => (f(l) |@| f(r))(Cmp(_, _))
        case Concat(a, b, cs)   => (f(a) |@| f(b) |@| cs.traverse(f))(Concat(_, _, _))
        case Cond(a, b, c)      => (f(a) |@| f(b) |@| f(c))(Cond(_, _, _))
        case DayOfMonth(a)      => G.map(f(a))(DayOfMonth(_))
        case DayOfWeek(a)       => G.map(f(a))(DayOfWeek(_))
        case DayOfYear(a)       => G.map(f(a))(DayOfYear(_))
        case Divide(a, b)       => (f(a) |@| f(b))(Divide(_, _))
        case Eq(a, b)           => (f(a) |@| f(b))(Eq(_, _))
        case Gt(a, b)           => (f(a) |@| f(b))(Gt(_, _))
        case Gte(a, b)          => (f(a) |@| f(b))(Gte(_, _))
        case Hour(a)            => G.map(f(a))(Hour(_))
        case Meta()             => G.point(Meta())
        case Size(a)            => G.map(f(a))(Size(_))
        case IfNull(a, b)       => (f(a) |@| f(b))(IfNull(_, _))
        case Let(a, b)          =>
          (Traverse[ListMap[ExprOp.DocVar.Name, ?]].sequence[G, B](a.map(t => t._1 -> f(t._2))) |@| f(b))(Let(_, _))
        case Literal(lit)       => G.point(Literal(lit))
        case Lt(a, b)           => (f(a) |@| f(b))(Lt(_, _))
        case Lte(a, b)          => (f(a) |@| f(b))(Lte(_, _))
        case Millisecond(a)     => G.map(f(a))(Millisecond(_))
        case Minute(a)          => G.map(f(a))(Minute(_))
        case Mod(a, b)          => (f(a) |@| f(b))(Mod(_, _))
        case Month(a)           => G.map(f(a))(Month(_))
        case Multiply(a, b)     => (f(a) |@| f(b))(Multiply(_, _))
        case Neq(a, b)          => (f(a) |@| f(b))(Neq(_, _))
        case Not(a)             => G.map(f(a))(Not(_))
        case Or(a)              => G.map(a.traverse(f))(Or(_))
        case Second(a)          => G.map(f(a))(Second(_))
        case Strcasecmp(a, b)   => (f(a) |@| f(b))(Strcasecmp(_, _))
        case Substr(a, b, c)    => (f(a) |@| f(b) |@| f(c))(Substr(_, _, _))
        case Subtract(a, b)     => (f(a) |@| f(b))(Subtract(_, _))
        case ToLower(a)         => G.map(f(a))(ToLower(_))
        case ToUpper(a)         => G.map(f(a))(ToUpper(_))
        case Week(a)            => G.map(f(a))(Week(_))
        case Year(a)            => G.map(f(a))(Year(_))
      }
  }

  implicit val ExprOpRenderTree = RenderTree.fromToString[Expression]("ExprOp")

  // TODO: rewrite as a fold (probably a histo)
  def toJs(expr: Expression): Error \/ JsFn = {
    import slamdata.engine.PlannerError._

    def expr1(x1: Expression)(f: Term[JsCore] => Term[JsCore]): Error \/ JsFn =
      toJs(x1).map(x1 => JsFn(JsFn.base, f(x1(JsFn.base.fix))))
    def expr2(x1: Expression, x2: Expression)(f: (Term[JsCore], Term[JsCore]) => Term[JsCore]): Error \/ JsFn =
      (toJs(x1) |@| toJs(x2))((x1, x2) => JsFn(JsFn.base, f(x1(JsFn.base.fix), x2(JsFn.base.fix))))

    def unop(op: JsCore.UnaryOperator, x: Expression) =
      expr1(x)(x => JsCore.UnOp(op, x).fix)
    def binop(op: JsCore.BinaryOperator, l: Expression, r: Expression) =
      expr2(l, r)((l, r) => JsCore.BinOp(op, l, r).fix)
    def invoke(x: Expression, name: String) =
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

    expr.unFix match {
      // TODO: The following few cases are places where the ExprOp created from
      //       the LogicalPlan needs special handling to behave the same when
      //       converted to JS. See #734 for the way forward.

      // matches the pattern the planner generates for converting epoch time
      // values to timestamps. Adding numbers to dates works in ExprOp, but not
      // in Javacript.
      case Add(Term(Literal(Bson.Date(inst))), r) if inst.toEpochMilli == 0 =>
        expr1(r)(x => JsCore.New("Date", List(x)).fix)
      // typechecking in ExprOp involves abusing total ordering. This ordering
      // doesn’t hold in JS, so we need to convert back to a typecheck. This
      // checks for a (non-array) object.
      case And(NonEmptyList(
        Term(Lte(Term(Literal(Bson.Doc(m1))), f1)),
        Term(Lt(f2, Term(Literal(Bson.Arr(List())))))))
          if f1 == f2 && m1 == ListMap() =>
        toJs(f1).map(f =>
          JsFn(JsFn.base,
            JsCore.BinOp(JsCore.And,
              JsCore.BinOp(JsCore.Instance, f(JsFn.base.fix), JsCore.Ident("Object").fix).fix,
              JsCore.UnOp(JsCore.Not, JsCore.BinOp(JsCore.Instance, f(JsFn.base.fix), JsCore.Ident("Array").fix).fix).fix).fix))
      // same as above, but for arrays
      case And(NonEmptyList(
        Term(Lte(Term(Literal(Bson.Arr(List()))), f1)),
        Term(Lt(f2, Term(Literal(b1))))))
          if f1 == f2 && b1 == Bson.Binary(scala.Array[Byte]())=>
        toJs(f1).map(f =>
          JsFn(JsFn.base,
            JsCore.BinOp(JsCore.Instance, f(JsFn.base.fix), JsCore.Ident("Array").fix).fix))

      case Include()             => -\/(NonRepresentableInJS(expr.toString))
      case DV(dv)                => \/-(dv.toJs)
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
      case Literal(bson)         => const(bson).map(l => JsFn.const(l))
      case Lt(l, r)              => binop(JsCore.Lt, l, r)
      case Lte(l, r)             => binop(JsCore.Lte, l, r)
      case Meta()                => -\/(NonRepresentableInJS(expr.toString))
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
}

sealed trait GroupOp[A]
object GroupOpTypes {
  final case class AddToSet[A](value: A) extends GroupOp[A]
  final case class Push[A](value: A)     extends GroupOp[A]
  final case class First[A](value: A)    extends GroupOp[A]
  final case class Last[A](value: A)     extends GroupOp[A]
  final case class Max[A](value: A)      extends GroupOp[A]
  final case class Min[A](value: A)      extends GroupOp[A]
  final case class Avg[A](value: A)      extends GroupOp[A]
  final case class Sum[A](value: A)      extends GroupOp[A]
}
object GroupOp {
  import ExprOp._
  import GroupOpTypes._

  object $addToSet {
    def apply[A](value: A): GroupOp[A] = AddToSet[A](value)
    def unapply[A](obj: GroupOp[A]): Option[A] = obj match {
      case AddToSet(value) => Some(value)
      case _          => None
    }
  }
  object $push {
    def apply[A](value: A): GroupOp[A] = Push[A](value)
    def unapply[A](obj: GroupOp[A]): Option[A] = obj match {
      case Push(value) => Some(value)
      case _          => None
    }
  }
  object $first {
    def apply[A](value: A): GroupOp[A] = First[A](value)
    def unapply[A](obj: GroupOp[A]): Option[A] = obj match {
      case First(value) => Some(value)
      case _          => None
    }
  }
  object $last {
    def apply[A](value: A): GroupOp[A] = Last[A](value)
    def unapply[A](obj: GroupOp[A]): Option[A] = obj match {
      case Last(value) => Some(value)
      case _          => None
    }
  }
  object $max {
    def apply[A](value: A): GroupOp[A] = Max[A](value)
    def unapply[A](obj: GroupOp[A]): Option[A] = obj match {
      case Max(value) => Some(value)
      case _          => None
    }
  }
  object $min {
    def apply[A](value: A): GroupOp[A] = Min[A](value)
    def unapply[A](obj: GroupOp[A]): Option[A] = obj match {
      case Min(value) => Some(value)
      case _          => None
    }
  }
  object $avg {
    def apply[A](value: A): GroupOp[A] = Avg[A](value)
    def unapply[A](obj: GroupOp[A]): Option[A] = obj match {
      case Avg(value) => Some(value)
      case _          => None
    }
  }
  object $sum {
    def apply[A](value: A): GroupOp[A] = Sum[A](value)
    def unapply[A](obj: GroupOp[A]): Option[A] = obj match {
      case Sum(value) => Some(value)
      case _          => None
    }
  }

  type Accumulator = GroupOp[Expression]

  def rewriteGroupRefs(t: Accumulator)(applyVar: PartialFunction[DocVar, DocVar]) =
    t.map(rewriteExprRefs(_)(applyVar))

  val groupBsonƒ: GroupOp[Bson] => Bson = {
    case AddToSet(value) => bsonDoc("$addToSet", value)
    case Push(value)     => bsonDoc("$push", value)
    case First(value)    => bsonDoc("$first", value)
    case Last(value)     => bsonDoc("$last", value)
    case Max(value)      => bsonDoc("$max", value)
    case Min(value)      => bsonDoc("$min", value)
    case Avg(value)      => bsonDoc("$max", value)
    case Sum(value)      => bsonDoc("$sum", value)
  }

  def groupBson(g: Accumulator) = groupBsonƒ(g.map(_.cata(bsonƒ)))

  implicit val GroupOpTraverse = new Traverse[GroupOp] {
    def traverseImpl[G[_], A, B](fa: GroupOp[A])(f: A => G[B])(implicit G: Applicative[G]):
        G[GroupOp[B]] =
      fa match {
        case AddToSet(value) => G.map(f(value))(AddToSet(_))
        case Avg(value)      => G.map(f(value))(Avg(_))
        case First(value)    => G.map(f(value))(First(_))
        case Last(value)     => G.map(f(value))(Last(_))
        case Max(value)      => G.map(f(value))(Max(_))
        case Min(value)      => G.map(f(value))(Min(_))
        case Push(value)     => G.map(f(value))(Push(_))
        case Sum(value)      => G.map(f(value))(Sum(_))
      }
  }

  implicit val GroupOpRenderTree =
    RenderTree.fromToString[Accumulator]("GroupOp")
}
