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
    final case class $includeF[A]() extends ExprOp[A]
    final case class $varF[A](docVar: DocVar) extends ExprOp[A]

    final case class $andF[A](first: A, second: A, others: A*) extends ExprOp[A]
    final case class $orF[A](first: A, second: A, others: A*)  extends ExprOp[A]
    final case class $notF[A](value: A)                        extends ExprOp[A]

    final case class $setEqualsF[A](left: A, right: A) extends ExprOp[A]
    final case class $setIntersectionF[A](left: A, right: A) extends ExprOp[A]
    final case class $setDifferenceF[A](left: A, right: A) extends ExprOp[A]
    final case class $setUnionF[A](left: A, right: A) extends ExprOp[A]
    final case class $setIsSubsetF[A](left: A, right: A) extends ExprOp[A]

    final case class $anyElementTrueF[A](value: A) extends ExprOp[A]
    final case class $allElementsTrueF[A](value: A) extends ExprOp[A]

    final case class $cmpF[A](left: A, right: A) extends ExprOp[A]
    final case class $eqF[A](left: A, right: A)  extends ExprOp[A]
    final case class $gtF[A](left: A, right: A)  extends ExprOp[A]
    final case class $gteF[A](left: A, right: A) extends ExprOp[A]
    final case class $ltF[A](left: A, right: A)  extends ExprOp[A]
    final case class $lteF[A](left: A, right: A) extends ExprOp[A]
    final case class $neqF[A](left: A, right: A) extends ExprOp[A]

    final case class $addF[A](left: A, right: A) extends ExprOp[A]
    final case class $divideF[A](left: A, right: A) extends ExprOp[A]
    final case class $modF[A](left: A, right: A) extends ExprOp[A]
    final case class $multiplyF[A](left: A, right: A) extends ExprOp[A]
    final case class $subtractF[A](left: A, right: A) extends ExprOp[A]

    final case class $concatF[A](first: A, second: A, others: A*)
        extends ExprOp[A]
    final case class $strcasecmpF[A](left: A, right: A) extends ExprOp[A]
    final case class $substrF[A](value: A, start: A, count: A) extends ExprOp[A]
    final case class $toLowerF[A](value: A) extends ExprOp[A]
    final case class $toUpperF[A](value: A) extends ExprOp[A]

    final case class $metaF[A]() extends ExprOp[A]

    final case class $sizeF[A](array: A) extends ExprOp[A]

    final case class $arrayMapF[A](input: A, as: DocVar.Name, in: A)
        extends ExprOp[A]
    final case class $letF[A](vars: ListMap[DocVar.Name, A], in: A)
        extends ExprOp[A]
    final case class $literalF[A](value: Bson) extends ExprOp[A]

    final case class $dayOfYearF[A](date: A)   extends ExprOp[A]
    final case class $dayOfMonthF[A](date: A)  extends ExprOp[A]
    final case class $dayOfWeekF[A](date: A)   extends ExprOp[A]
    final case class $yearF[A](date: A)        extends ExprOp[A]
    final case class $monthF[A](date: A)       extends ExprOp[A]
    final case class $weekF[A](date: A)        extends ExprOp[A]
    final case class $hourF[A](date: A)        extends ExprOp[A]
    final case class $minuteF[A](date: A)      extends ExprOp[A]
    final case class $secondF[A](date: A)      extends ExprOp[A]
    final case class $millisecondF[A](date: A) extends ExprOp[A]

    final case class $condF[A](predicate: A, ifTrue: A, ifFalse: A)
        extends ExprOp[A]
    final case class $ifNullF[A](expr: A, replacement: A) extends ExprOp[A]
  }

  object DSL {
    object $include {
      def apply(): Expression = Term($includeF())
      def unapply(obj: Expression): Boolean = $includeF.unapply(obj.unFix)
    }
    object $var {
      def apply(docVar: DocVar): Expression = Term($varF(docVar))
      def apply(field: String, others: String*): Expression =
        $var(DocField(others.map(BsonField.Name).foldLeft[BsonField](BsonField.Name(field))(_ \ _)))
      def unapply(obj: Expression): Option[DocVar] = $varF.unapply(obj.unFix)
    }
    object $and {
      def apply(first: Expression, second: Expression, others: Expression*):
          Expression =
        Term($andF(first, second, others: _*))
    }
    object $or {
      def apply(first: Expression, second: Expression, others: Expression*):
          Expression =
        Term($orF(first, second, others: _*))
    }
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
    object $lt {
      def apply(left: Expression, right: Expression): Expression =
        Term($ltF(left, right))
      def unapply(obj: Expression): Option[(Expression, Expression)] =
        $ltF.unapply(obj.unFix)
    }
    object $lte {
      def apply(left: Expression, right: Expression): Expression =
        Term($lteF(left, right))
      def unapply(obj: Expression): Option[(Expression, Expression)] =
        $lteF.unapply(obj.unFix)
    }
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

    val $$ROOT = $var(DocVar.ROOT())
    val $$CURRENT = $var(DocVar.CURRENT())
  }

  object $includeF {
    def apply[A](): ExprOp[A] = Types.$includeF[A]()
    def unapply[A](obj: ExprOp[A]): Boolean = obj match {
      case Types.$includeF() => true
      case _                 => false
    }
  }
  object $varF {
    def apply[A](docVar: DocVar): ExprOp[A] = Types.$varF[A](docVar)
    def unapply[A](obj: ExprOp[A]): Option[DocVar] = obj match {
      case Types.$varF(docVar) => Some(docVar)
      case _                   => None
    }
  }

  object $andF {
    def apply[A](first: A, second: A, others: A*): ExprOp[A] =
      Types.$andF[A](first, second, others: _*)
    def unapplySeq[A](obj: ExprOp[A]): Option[Seq[A]] = obj match {
      case Types.$andF(first, second, others @ _*) => Some(first +: second +: others)
      case _                                       => None
    }
  }
  object $orF {
    def apply[A](first: A, second: A, others: A*): ExprOp[A] =
      Types.$orF[A](first, second, others: _*)
    def unapplySeq[A](obj: ExprOp[A]): Option[Seq[A]] = obj match {
      case Types.$orF(first, second, others @ _*) => Some(first +: second +: others)
      case _                                      => None
    }
  }

  object $notF {
    def apply[A](value: A): ExprOp[A] = Types.$notF[A](value)
    def unapply[A](obj: ExprOp[A]): Option[A] = obj match {
      case Types.$notF(value) => Some(value)
      case _                  => None
    }
  }

  object $setEqualsF {
    def apply[A](left: A, right: A): ExprOp[A] =
      Types.$setEqualsF[A](left, right)
    def unapply[A](obj: ExprOp[A]): Option[(A, A)] = obj match {
      case Types.$setEqualsF(left, right) => Some((left, right))
      case _                              => None
    }
  }
  object $setIntersectionF {
    def apply[A](left: A, right: A): ExprOp[A] =
      Types.$setIntersectionF[A](left, right)
    def unapply[A](obj: ExprOp[A]): Option[(A, A)] = obj match {
      case Types.$setIntersectionF(left, right) => Some((left, right))
      case _                              => None
    }
  }
  object $setDifferenceF {
    def apply[A](left: A, right: A): ExprOp[A] =
      Types.$setDifferenceF[A](left, right)
    def unapply[A](obj: ExprOp[A]): Option[(A, A)] = obj match {
      case Types.$setDifferenceF(left, right) => Some((left, right))
      case _                              => None
    }
  }
  object $setUnionF {
    def apply[A](left: A, right: A): ExprOp[A] =
      Types.$setUnionF[A](left, right)
    def unapply[A](obj: ExprOp[A]): Option[(A, A)] = obj match {
      case Types.$setUnionF(left, right) => Some((left, right))
      case _                              => None
    }
  }
  object $setIsSubsetF {
    def apply[A](left: A, right: A): ExprOp[A] =
      Types.$setIsSubsetF[A](left, right)
    def unapply[A](obj: ExprOp[A]): Option[(A, A)] = obj match {
      case Types.$setIsSubsetF(left, right) => Some((left, right))
      case _                              => None
    }
  }

  object $anyElementTrueF {
    def apply[A](value: A): ExprOp[A] = Types.$anyElementTrueF[A](value)
    def unapply[A](obj: ExprOp[A]): Option[A] = obj match {
      case Types.$anyElementTrueF(value) => Some(value)
      case _                             => None
    }
  }
  object $allElementsTrueF {
    def apply[A](value: A): ExprOp[A] = Types.$allElementsTrueF[A](value)
    def unapply[A](obj: ExprOp[A]): Option[A] = obj match {
      case Types.$anyElementTrueF(value) => Some(value)
      case _                             => None
    }
  }

  object $cmpF {
    def apply[A](left: A, right: A): ExprOp[A] = Types.$cmpF[A](left, right)
    def unapply[A](obj: ExprOp[A]): Option[(A, A)] = obj match {
      case Types.$cmpF(left, right) => Some((left, right))
      case _                              => None
    }
  }
  object $eqF {
    def apply[A](left: A, right: A): ExprOp[A] = Types.$eqF[A](left, right)
    def unapply[A](obj: ExprOp[A]): Option[(A, A)] = obj match {
      case Types.$eqF(left, right) => Some((left, right))
      case _                              => None
    }
  }
  object $gtF {
    def apply[A](left: A, right: A): ExprOp[A] = Types.$gtF[A](left, right)
    def unapply[A](obj: ExprOp[A]): Option[(A, A)] = obj match {
      case Types.$gtF(left, right) => Some((left, right))
      case _                              => None
    }
  }
  object $gteF {
    def apply[A](left: A, right: A): ExprOp[A] = Types.$gteF[A](left, right)
    def unapply[A](obj: ExprOp[A]): Option[(A, A)] = obj match {
      case Types.$gteF(left, right) => Some((left, right))
      case _                              => None
    }
  }
  object $ltF {
    def apply[A](left: A, right: A): ExprOp[A] = Types.$ltF[A](left, right)
    def unapply[A](obj: ExprOp[A]): Option[(A, A)] = obj match {
      case Types.$ltF(left, right) => Some((left, right))
      case _                              => None
    }
  }
  object $lteF {
    def apply[A](left: A, right: A): ExprOp[A] = Types.$lteF[A](left, right)
    def unapply[A](obj: ExprOp[A]): Option[(A, A)] = obj match {
      case Types.$lteF(left, right) => Some((left, right))
      case _                              => None
    }
  }
  object $neqF {
    def apply[A](left: A, right: A): ExprOp[A] = Types.$neqF[A](left, right)
    def unapply[A](obj: ExprOp[A]): Option[(A, A)] = obj match {
      case Types.$neqF(left, right) => Some((left, right))
      case _                              => None
    }
  }

  object $addF {
    def apply[A](left: A, right: A): ExprOp[A] = Types.$addF[A](left, right)
    def unapply[A](obj: ExprOp[A]): Option[(A, A)] = obj match {
      case Types.$addF(left, right) => Some((left, right))
      case _                              => None
    }
  }
  object $divideF {
    def apply[A](left: A, right: A): ExprOp[A] = Types.$divideF[A](left, right)
    def unapply[A](obj: ExprOp[A]): Option[(A, A)] = obj match {
      case Types.$divideF(left, right) => Some((left, right))
      case _                              => None
    }
  }
  object $modF {
    def apply[A](left: A, right: A): ExprOp[A] = Types.$modF[A](left, right)
    def unapply[A](obj: ExprOp[A]): Option[(A, A)] = obj match {
      case Types.$modF(left, right) => Some((left, right))
      case _                              => None
    }
  }
  object $multiplyF {
    def apply[A](left: A, right: A): ExprOp[A] = Types.$multiplyF[A](left, right)
    def unapply[A](obj: ExprOp[A]): Option[(A, A)] = obj match {
      case Types.$multiplyF(left, right) => Some((left, right))
      case _                              => None
    }
  }
  object $subtractF {
    def apply[A](left: A, right: A): ExprOp[A] = Types.$subtractF[A](left, right)
    def unapply[A](obj: ExprOp[A]): Option[(A, A)] = obj match {
      case Types.$subtractF(left, right) => Some((left, right))
      case _                              => None
    }
  }

  object $concatF {
    def apply[A](first: A, second: A, others: A*): ExprOp[A] = Types.$concatF[A](first, second, others: _*)
    def unapplySeq[A](obj: ExprOp[A]): Option[Seq[A]] = obj match {
      case Types.$concatF(first, second, others @ _*) => Some(first +: second +: others)
      case _                              => None
    }
  }
  object $strcasecmpF {
    def apply[A](left: A, right: A): ExprOp[A] = Types.$strcasecmpF[A](left, right)
    def unapply[A](obj: ExprOp[A]): Option[(A, A)] = obj match {
      case Types.$strcasecmpF(left, right) => Some((left, right))
      case _                              => None
    }
  }
  object $substrF {
    def apply[A](value: A, start: A, count: A): ExprOp[A] = Types.$substrF[A](value, start, count)
    def unapply[A](obj: ExprOp[A]): Option[(A, A, A)] = obj match {
      case Types.$substrF(value, start, count) => Some((value, start, count))
      case _                                   => None
    }
  }
  object $toLowerF {
    def apply[A](value: A): ExprOp[A] = Types.$toLowerF[A](value)
    def unapply[A](obj: ExprOp[A]): Option[A] = obj match {
      case Types.$toLowerF(value) => Some(value)
      case _          => None
    }
  }
  object $toUpperF {
    def apply[A](value: A): ExprOp[A] = Types.$toUpperF[A](value)
    def unapply[A](obj: ExprOp[A]): Option[A] = obj match {
      case Types.$toUpperF(value) => Some(value)
      case _          => None
    }
  }

  object $metaF {
    def apply[A](): ExprOp[A] = Types.$metaF[A]()
    def unapply[A](obj: ExprOp[A]): Boolean = obj match {
      case Types.$metaF() => true
      case _      => false
    }
  }

  object $sizeF {
    def apply[A](array: A): ExprOp[A] = Types.$sizeF[A](array)
    def unapply[A](obj: ExprOp[A]): Option[A] = obj match {
      case Types.$sizeF(array) => Some(array)
      case _          => None
    }
  }

  object $arrayMapF {
    def apply[A](input: A, as: DocVar.Name, in: A): ExprOp[A] =
      Types.$arrayMapF[A](input, as, in)
    def unapply[A](obj: ExprOp[A]): Option[(A, DocVar.Name, A)] = obj match {
      case Types.$arrayMapF(input, as, in) => Some((input, as, in))
      case _                       => None
    }
  }
  object $letF {
    def apply[A](vars: ListMap[DocVar.Name, A], in: A): ExprOp[A] =
      Types.$letF[A](vars, in)
    def unapply[A](obj: ExprOp[A]): Option[(ListMap[DocVar.Name, A], A)] =
      obj match {
        case Types.$letF(vars, in) => Some((vars, in))
        case _             => None
      }
  }
  object $literalF {
    def apply[A](value: Bson): ExprOp[A] = Types.$literalF[A](value)
    def unapply[A](obj: ExprOp[A]): Option[Bson] = obj match {
      case Types.$literalF(value) => Some(value)
      case _              => None
    }
  }

  object $dayOfYearF {
    def apply[A](date: A): ExprOp[A] = Types.$dayOfYearF[A](date)
    def unapply[A](obj: ExprOp[A]): Option[A] = obj match {
      case Types.$dayOfYearF(date) => Some(date)
      case _          => None
    }
  }
  object $dayOfMonthF {
    def apply[A](date: A): ExprOp[A] = Types.$dayOfMonthF[A](date)
    def unapply[A](obj: ExprOp[A]): Option[A] = obj match {
      case Types.$dayOfMonthF(date) => Some(date)
      case _          => None
    }
  }
  object $dayOfWeekF {
    def apply[A](date: A): ExprOp[A] = Types.$dayOfWeekF[A](date)
    def unapply[A](obj: ExprOp[A]): Option[A] = obj match {
      case Types.$dayOfWeekF(date) => Some(date)
      case _          => None
    }
  }
  object $yearF {
    def apply[A](date: A): ExprOp[A] = Types.$yearF[A](date)
    def unapply[A](obj: ExprOp[A]): Option[A] = obj match {
      case Types.$yearF(date) => Some(date)
      case _          => None
    }
  }
  object $monthF {
    def apply[A](date: A): ExprOp[A] = Types.$monthF[A](date)
    def unapply[A](obj: ExprOp[A]): Option[A] = obj match {
      case Types.$monthF(date) => Some(date)
      case _          => None
    }
  }
  object $weekF {
    def apply[A](date: A): ExprOp[A] = Types.$weekF[A](date)
    def unapply[A](obj: ExprOp[A]): Option[A] = obj match {
      case Types.$weekF(date) => Some(date)
      case _          => None
    }
  }
  object $hourF {
    def apply[A](date: A): ExprOp[A] = Types.$hourF[A](date)
    def unapply[A](obj: ExprOp[A]): Option[A] = obj match {
      case Types.$hourF(date) => Some(date)
      case _          => None
    }
  }
  object $minuteF {
    def apply[A](date: A): ExprOp[A] = Types.$minuteF[A](date)
    def unapply[A](obj: ExprOp[A]): Option[A] = obj match {
      case Types.$minuteF(date) => Some(date)
      case _          => None
    }
  }
  object $secondF {
    def apply[A](date: A): ExprOp[A] = Types.$secondF[A](date)
    def unapply[A](obj: ExprOp[A]): Option[A] = obj match {
      case Types.$secondF(date) => Some(date)
      case _          => None
    }
  }
  object $millisecondF {
    def apply[A](date: A): ExprOp[A] = Types.$millisecondF[A](date)
    def unapply[A](obj: ExprOp[A]): Option[A] = obj match {
      case Types.$millisecondF(date) => Some(date)
      case _          => None
    }
  }

  object $condF {
    def apply[A](predicate: A, ifTrue: A, ifFalse: A): ExprOp[A] = Types.$condF[A](predicate, ifTrue, ifFalse)
    def unapply[A](obj: ExprOp[A]): Option[(A, A, A)] = obj match {
      case Types.$condF(pred, t, f) => Some((pred, t, f))
      case _                        => None
    }
  }
  object $ifNullF {
    def apply[A](expr: A, replacement: A): ExprOp[A] = Types.$ifNullF[A](expr, replacement)
    def unapply[A](obj: ExprOp[A]): Option[(A, A)] = obj match {
      case Types.$ifNullF(expr, replacement) => Some((expr, replacement))
      case _                                 => None
    }
  }

  import DSL._

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
    case $includeF() => Bson.Bool(true)
    case $varF(dv) => dv.bson
    case $andF(first, second, others @ _*) =>
      bsonArr("$and", first +: second +: others: _*)
    case $orF(first, second, others @ _*) =>
      bsonArr("$or", first +: second +: others: _*)
    case $notF(value) => bsonDoc("$not", value)
    case $setEqualsF(left, right) => bsonArr("$setEquals", left, right)
    case $setIntersectionF(left, right) =>
      bsonArr("$setIntersection", left, right)
    case $setDifferenceF(left, right) => bsonArr("$setDifference", left, right)
    case $setUnionF(left, right) => bsonArr("$setUnion", left, right)
    case $setIsSubsetF(left, right) => bsonArr("$setIsSubset", left, right)
    case $anyElementTrueF(value) => bsonDoc("$anyElementTrue", value)
    case $allElementsTrueF(value) => bsonDoc("$allElementsTrue", value)
    case $cmpF(left, right) => bsonArr("$cmp", left, right)
    case $eqF(left, right) => bsonArr("$eq", left, right)
    case $gtF(left, right) => bsonArr("$gt", left, right)
    case $gteF(left, right) => bsonArr("$gte", left, right)
    case $ltF(left, right) => bsonArr("$lt", left, right)
    case $lteF(left, right) => bsonArr("$lte", left, right)
    case $neqF(left, right) => bsonArr("$ne", left, right)
    case $addF(left, right) => bsonArr("$add", left, right)
    case $divideF(left, right) => bsonArr("$divide", left, right)
    case $modF(left, right) => bsonArr("$mod", left, right)
    case $multiplyF(left, right) => bsonArr("$multiply", left, right)
    case $subtractF(left, right) => bsonArr("$subtract", left, right)
    case $concatF(first, second, others @ _*) =>
      bsonArr("$concat", first +: second +: others: _*)
    case $strcasecmpF(left, right) => bsonArr("$strcasecmp", left, right)
    case $substrF(value, start, count) =>
      bsonArr("$substr", value, start, count)
    case $toLowerF(value) => bsonDoc("$toLower", value)
    case $toUpperF(value) => bsonDoc("$toUpper", value)
    case $metaF() => bsonDoc("$meta", Bson.Text("textScore"))
    case $sizeF(array) => bsonDoc("$size", array)
    case $arrayMapF(input, as, in) =>
      bsonDoc(
        "$map",
        Bson.Doc(ListMap(
          "input" -> input,
          "as"    -> Bson.Text(as.name),
          "in"    -> in)))
    case $letF(vars, in) =>
      bsonDoc(
        "$let",
        Bson.Doc(ListMap(
          "vars" -> Bson.Doc(vars.map(t => (t._1.name, t._2))),
          "in"   -> in)))
    case $literalF(value) => bsonDoc("$literal", value)
    case $dayOfYearF(date) => bsonDoc("$dayOfYear", date)
    case $dayOfMonthF(date) => bsonDoc("$dayOfMonth", date)
    case $dayOfWeekF(date) => bsonDoc("$dayOfWeek", date)
    case $yearF(date) => bsonDoc("$year", date)
    case $monthF(date) => bsonDoc("$month", date)
    case $weekF(date) => bsonDoc("$week", date)
    case $hourF(date) => bsonDoc("$hour", date)
    case $minuteF(date) => bsonDoc("$minute", date)
    case $secondF(date) => bsonDoc("$second", date)
    case $millisecondF(date) => bsonDoc("$millisecond", date)
    case $condF(predicate, ifTrue, ifFalse) =>
      bsonArr("$cond", predicate, ifTrue, ifFalse)
    case $ifNullF(expr, replacement) => bsonArr("$ifNull", expr, replacement)
  }

  def rewriteExprRefs(t: Expression)(applyVar: PartialFunction[DocVar, DocVar]) =
    t.cata[Expression] {
      case $varF(f) => Term[ExprOp]($varF(applyVar.lift(f).getOrElse(f)))
      case x        => Term(x)
    }

  implicit val ExprOpTraverse = new Traverse[ExprOp] {
    def traverseImpl[G[_], A, B](fa: ExprOp[A])(f: A => G[B])(implicit G: Applicative[G]):
        G[ExprOp[B]] =
      fa match {
        case $includeF()          => G.point($includeF())
        case $varF(dv)            => G.point($varF(dv))
        case $addF(l, r)          => (f(l) |@| f(r))($addF(_, _))
        case $andF(a, b, cs @ _*) => (f(a) |@| f(b) |@| cs.toList.traverse(f))($andF(_, _, _: _*))
        case $setEqualsF(l, r)       => (f(l) |@| f(r))($setEqualsF(_, _))
        case $setIntersectionF(l, r) => (f(l) |@| f(r))($setIntersectionF(_, _))
        case $setDifferenceF(l, r)   => (f(l) |@| f(r))($setDifferenceF(_, _))
        case $setUnionF(l, r)        => (f(l) |@| f(r))($setUnionF(_, _))
        case $setIsSubsetF(l, r)     => (f(l) |@| f(r))($setIsSubsetF(_, _))
        case $anyElementTrueF(v)     => G.map(f(v))($anyElementTrueF(_))
        case $allElementsTrueF(v)    => G.map(f(v))($allElementsTrueF(_))
        case $arrayMapF(a, b, c)  => (f(a) |@| f(c))($arrayMapF(_, b, _))
        case $cmpF(l, r)          => (f(l) |@| f(r))($cmpF(_, _))
        case $concatF(a, b, cs @ _*) => (f(a) |@| f(b) |@| cs.toList.traverse(f))($concatF(_, _, _: _*))
        case $condF(a, b, c)      => (f(a) |@| f(b) |@| f(c))($condF(_, _, _))
        case $dayOfMonthF(a)      => G.map(f(a))($dayOfMonthF(_))
        case $dayOfWeekF(a)       => G.map(f(a))($dayOfWeekF(_))
        case $dayOfYearF(a)       => G.map(f(a))($dayOfYearF(_))
        case $divideF(a, b)       => (f(a) |@| f(b))($divideF(_, _))
        case $eqF(a, b)           => (f(a) |@| f(b))($eqF(_, _))
        case $gtF(a, b)           => (f(a) |@| f(b))($gtF(_, _))
        case $gteF(a, b)          => (f(a) |@| f(b))($gteF(_, _))
        case $hourF(a)            => G.map(f(a))($hourF(_))
        case $metaF()             => G.point($metaF())
        case $sizeF(a)            => G.map(f(a))($sizeF(_))
        case $ifNullF(a, b)       => (f(a) |@| f(b))($ifNullF(_, _))
        case $letF(a, b)          =>
          (Traverse[ListMap[ExprOp.DocVar.Name, ?]].sequence[G, B](a.map(t => t._1 -> f(t._2))) |@| f(b))($letF(_, _))
        case $literalF(lit)       => G.point($literalF(lit))
        case $ltF(a, b)           => (f(a) |@| f(b))($ltF(_, _))
        case $lteF(a, b)          => (f(a) |@| f(b))($lteF(_, _))
        case $millisecondF(a)     => G.map(f(a))($millisecondF(_))
        case $minuteF(a)          => G.map(f(a))($minuteF(_))
        case $modF(a, b)          => (f(a) |@| f(b))($modF(_, _))
        case $monthF(a)           => G.map(f(a))($monthF(_))
        case $multiplyF(a, b)     => (f(a) |@| f(b))($multiplyF(_, _))
        case $neqF(a, b)          => (f(a) |@| f(b))($neqF(_, _))
        case $notF(a)             => G.map(f(a))($notF(_))
        case $orF(a, b, cs @ _*)  => (f(a) |@| f(b) |@| cs.toList.traverse(f))($orF(_, _, _: _*))
        case $secondF(a)          => G.map(f(a))($secondF(_))
        case $strcasecmpF(a, b)   => (f(a) |@| f(b))($strcasecmpF(_, _))
        case $substrF(a, b, c)    => (f(a) |@| f(b) |@| f(c))($substrF(_, _, _))
        case $subtractF(a, b)     => (f(a) |@| f(b))($subtractF(_, _))
        case $toLowerF(a)         => G.map(f(a))($toLowerF(_))
        case $toUpperF(a)         => G.map(f(a))($toUpperF(_))
        case $weekF(a)            => G.map(f(a))($weekF(_))
        case $yearF(a)            => G.map(f(a))($yearF(_))
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
      case $addF($literal(Bson.Date(inst)), r) if inst.toEpochMilli == 0 =>
        expr1(r)(x => JsCore.New("Date", List(x)).fix)
      // typechecking in ExprOp involves abusing total ordering. This ordering
      // doesn’t hold in JS, so we need to convert back to a typecheck. This
      // checks for a (non-array) object.
      case $andF(
        $lte($literal(Bson.Doc(m1)), f1),
        $lt(f2, $literal(Bson.Arr(List()))))
          if f1 == f2 && m1 == ListMap() =>
        toJs(f1).map(f =>
          JsFn(JsFn.base,
            JsCore.BinOp(JsCore.And,
              JsCore.BinOp(JsCore.Instance, f(JsFn.base.fix), JsCore.Ident("Object").fix).fix,
              JsCore.UnOp(JsCore.Not, JsCore.BinOp(JsCore.Instance, f(JsFn.base.fix), JsCore.Ident("Array").fix).fix).fix).fix))
      // same as above, but for arrays
      case $andF(
        $lte($literal(Bson.Arr(List())), f1),
        $lt(f2, $literal(b1)))
          if f1 == f2 && b1 == Bson.Binary(scala.Array[Byte]())=>
        toJs(f1).map(f =>
          JsFn(JsFn.base,
            JsCore.BinOp(JsCore.Instance, f(JsFn.base.fix), JsCore.Ident("Array").fix).fix))

      case $includeF()             => -\/(NonRepresentableInJS(expr.toString))
      case $varF(dv)               => \/-(dv.toJs)
      case $addF(l, r)             => binop(JsCore.Add, l, r)
      case $andF(f, s, o @ _*)     =>
        NonEmptyList(f, s +: o: _*).traverse[Error \/ ?, JsFn](toJs).map(v =>
          v.foldLeft1((l, r) => JsFn(JsFn.base, JsCore.BinOp(JsCore.And, l(JsFn.base.fix), r(JsFn.base.fix)).fix)))
      case $condF(t, c, a)         =>
        (toJs(t) |@| toJs(c) |@| toJs(a))((t, c, a) =>
          JsFn(JsFn.base,
            JsCore.If(t(JsFn.base.fix), c(JsFn.base.fix), a(JsFn.base.fix)).fix))
      case $divideF(l, r)          => binop(JsCore.Div, l, r)
      case $eqF(l, r)              => binop(JsCore.Eq, l, r)
      case $gtF(l, r)              => binop(JsCore.Gt, l, r)
      case $gteF(l, r)             => binop(JsCore.Gte, l, r)
      case $literalF(bson)         => const(bson).map(l => JsFn.const(l))
      case $ltF(l, r)              => binop(JsCore.Lt, l, r)
      case $lteF(l, r)             => binop(JsCore.Lte, l, r)
      case $metaF()                => -\/(NonRepresentableInJS(expr.toString))
      case $multiplyF(l, r)        => binop(JsCore.Mult, l, r)
      case $neqF(l, r)             => binop(JsCore.Neq, l, r)
      case $notF(a)                => unop(JsCore.Not, a)

      case $concatF(f, s, o @ _*)  =>
        NonEmptyList(f, s +: o: _*).traverse[Error \/ ?, JsFn](toJs).map(v =>
          v.foldLeft1((l, r) => JsFn(JsFn.base, JsCore.BinOp(JsCore.Add, l(JsFn.base.fix), r(JsFn.base.fix)).fix)))
      case $substrF(f, start, len) =>
        (toJs(f) |@| toJs(start) |@| toJs(len))((f, s, l) =>
          JsFn(JsFn.base,
            JsCore.Call(
              JsCore.Select(f(JsFn.base.fix), "substr").fix,
              List(s(JsFn.base.fix), l(JsFn.base.fix))).fix))
      case $subtractF(l, r)        => binop(JsCore.Sub, l, r)
      case $toLowerF(a)            => invoke(a, "toLowerCase")
      case $toUpperF(a)            => invoke(a, "toUpperCase")

      case $hourF(a)               => invoke(a, "getUTCHours")
      case $minuteF(a)             => invoke(a, "getUTCMinutes")
      case $secondF(a)             => invoke(a, "getUTCSeconds")
      case $millisecondF(a)        => invoke(a, "getUTCMilliseconds")

      // TODO: implement the rest of these and remove the catch-all (see #449)
      case _                       => -\/(UnsupportedJS(expr.toString))
    }
  }
}

sealed trait AccumOp[A]
object AccumOp {
  import ExprOp._

  object Types {
    final case class $addToSet[A](value: A) extends AccumOp[A]
    final case class $push[A](value: A)     extends AccumOp[A]
    final case class $first[A](value: A)    extends AccumOp[A]
    final case class $last[A](value: A)     extends AccumOp[A]
    final case class $max[A](value: A)      extends AccumOp[A]
    final case class $min[A](value: A)      extends AccumOp[A]
    final case class $avg[A](value: A)      extends AccumOp[A]
    final case class $sum[A](value: A)      extends AccumOp[A]
  }

  object $addToSet {
    def apply[A](value: A): AccumOp[A] = Types.$addToSet[A](value)
    def unapply[A](obj: AccumOp[A]): Option[A] = obj match {
      case Types.$addToSet(value) => Some(value)
      case _                      => None
    }
  }
  object $push {
    def apply[A](value: A): AccumOp[A] = Types.$push[A](value)
    def unapply[A](obj: AccumOp[A]): Option[A] = obj match {
      case Types.$push(value) => Some(value)
      case _                  => None
    }
  }
  object $first {
    def apply[A](value: A): AccumOp[A] = Types.$first[A](value)
    def unapply[A](obj: AccumOp[A]): Option[A] = obj match {
      case Types.$first(value) => Some(value)
      case _                   => None
    }
  }
  object $last {
    def apply[A](value: A): AccumOp[A] = Types.$last[A](value)
    def unapply[A](obj: AccumOp[A]): Option[A] = obj match {
      case Types.$last(value) => Some(value)
      case _                  => None
    }
  }
  object $max {
    def apply[A](value: A): AccumOp[A] = Types.$max[A](value)
    def unapply[A](obj: AccumOp[A]): Option[A] = obj match {
      case Types.$max(value) => Some(value)
      case _                 => None
    }
  }
  object $min {
    def apply[A](value: A): AccumOp[A] = Types.$min[A](value)
    def unapply[A](obj: AccumOp[A]): Option[A] = obj match {
      case Types.$min(value) => Some(value)
      case _                 => None
    }
  }
  object $avg {
    def apply[A](value: A): AccumOp[A] = Types.$avg[A](value)
    def unapply[A](obj: AccumOp[A]): Option[A] = obj match {
      case Types.$avg(value) => Some(value)
      case _                 => None
    }
  }
  object $sum {
    def apply[A](value: A): AccumOp[A] = Types.$sum[A](value)
    def unapply[A](obj: AccumOp[A]): Option[A] = obj match {
      case Types.$sum(value) => Some(value)
      case _          => None
    }
  }

  type Accumulator = AccumOp[Expression]

  def rewriteGroupRefs(t: Accumulator)(applyVar: PartialFunction[DocVar, DocVar]) =
    t.map(rewriteExprRefs(_)(applyVar))

  val groupBsonƒ: AccumOp[Bson] => Bson = {
    case $addToSet(value) => bsonDoc("$addToSet", value)
    case $push(value)     => bsonDoc("$push", value)
    case $first(value)    => bsonDoc("$first", value)
    case $last(value)     => bsonDoc("$last", value)
    case $max(value)      => bsonDoc("$max", value)
    case $min(value)      => bsonDoc("$min", value)
    case $avg(value)      => bsonDoc("$avg", value)
    case $sum(value)      => bsonDoc("$sum", value)
  }

  def groupBson(g: Accumulator) = groupBsonƒ(g.map(_.cata(bsonƒ)))

  implicit val AccumOpTraverse = new Traverse[AccumOp] {
    def traverseImpl[G[_], A, B](fa: AccumOp[A])(f: A => G[B])(implicit G: Applicative[G]):
        G[AccumOp[B]] =
      fa match {
        case $addToSet(value) => G.map(f(value))($addToSet(_))
        case $avg(value)      => G.map(f(value))($avg(_))
        case $first(value)    => G.map(f(value))($first(_))
        case $last(value)     => G.map(f(value))($last(_))
        case $max(value)      => G.map(f(value))($max(_))
        case $min(value)      => G.map(f(value))($min(_))
        case $push(value)     => G.map(f(value))($push(_))
        case $sum(value)      => G.map(f(value))($sum(_))
      }
  }

  implicit val AccumOpRenderTree =
    RenderTree.fromToString[Accumulator]("AccumOp")
}
