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

package slamdata.engine.physical.mongodb.expression

import collection.immutable.ListMap

import slamdata.engine.analysis.fixplate.Term
import slamdata.engine.physical.mongodb.Bson

object $include {
  def apply(): Expression = Term($includeF())
  def unapply(obj: Expression): Boolean = $includeF.unapply(obj.unFix)
}
object $var {
  def apply(docVar: DocVar): Expression = Term($varF(docVar))
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

object $cond {
  def apply(predicate: Expression, ifTrue: Expression, ifFalse: Expression):
      Expression =
    Term($condF(predicate, ifTrue, ifFalse))
  def unapply(obj: Expression): Option[(Expression, Expression, Expression)] =
    $condF.unapply(obj.unFix)
}
object $ifNull {
  def apply(expr: Expression, replacement: Expression): Expression =
    Term($ifNullF(expr, replacement))
  def unapply(obj: Expression): Option[(Expression, Expression)] =
    $ifNullF.unapply(obj.unFix)
}
