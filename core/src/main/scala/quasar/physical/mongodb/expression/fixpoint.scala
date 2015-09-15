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

package quasar.physical.mongodb.expression

import quasar.Predef._
import quasar.recursionschemes.Fix
import quasar.physical.mongodb.Bson

object $include {
  def apply(): Expression = Fix($includeF())
  def unapply(obj: Expression): Boolean = $includeF.unapply(obj.unFix)
}
object $var {
  def apply(docVar: DocVar): Expression = Fix($varF(docVar))
  def unapply(obj: Expression): Option[DocVar] = $varF.unapply(obj.unFix)
}
object $and {
  def apply(first: Expression, second: Expression, others: Expression*):
      Expression =
    Fix($andF(first, second, others: _*))
  def unapplySeq(obj: Expression): Option[(Expression, Expression, Seq[Expression])] = $andF.unapplySeq(obj.unFix)
}
object $or {
  def apply(first: Expression, second: Expression, others: Expression*):
      Expression =
    Fix($orF(first, second, others: _*))
  def unapplySeq(obj: Expression): Option[(Expression, Expression, Seq[Expression])] = $orF.unapplySeq(obj.unFix)
}
object $not {
  def apply(value: Expression): Expression = Fix($notF(value))
  def unapply(obj: Expression): Option[Expression] = $notF.unapply(obj.unFix)
}

object $setEquals {
  def apply(left: Expression, right: Expression): Expression = Fix($setEqualsF(left, right))
  def unapply(obj: Expression): Option[(Expression, Expression)] = $setEqualsF.unapply(obj.unFix)
}
object $setIntersection {
  def apply(left: Expression, right: Expression): Expression = Fix($setIntersectionF(left, right))
  def unapply(obj: Expression): Option[(Expression, Expression)] = $setIntersectionF.unapply(obj.unFix)
}
object $setDifference {
  def apply(left: Expression, right: Expression): Expression = Fix($setDifferenceF(left, right))
  def unapply(obj: Expression): Option[(Expression, Expression)] = $setDifferenceF.unapply(obj.unFix)
}
object $setUnion {
  def apply(left: Expression, right: Expression): Expression = Fix($setUnionF(left, right))
  def unapply(obj: Expression): Option[(Expression, Expression)] = $setUnionF.unapply(obj.unFix)
}
object $setIsSubset {
  def apply(left: Expression, right: Expression): Expression = Fix($setIsSubsetF(left, right))
  def unapply(obj: Expression): Option[(Expression, Expression)] = $setIsSubsetF.unapply(obj.unFix)
}
object $anyElementTrue {
  def apply(value: Expression): Expression = Fix($anyElementTrueF(value))
  def unapply(obj: Expression): Option[Expression] = $anyElementTrueF.unapply(obj.unFix)
}
object $allElementsTrue {
  def apply(value: Expression): Expression = Fix($allElementsTrueF(value))
  def unapply(obj: Expression): Option[Expression] = $allElementsTrueF.unapply(obj.unFix)
}

object $cmp {
  def apply(left: Expression, right: Expression): Expression = Fix($cmpF(left, right))
  def unapply(obj: Expression): Option[(Expression, Expression)] = $cmpF.unapply(obj.unFix)
}
object $eq {
  def apply(left: Expression, right: Expression): Expression = Fix($eqF(left, right))
  def unapply(obj: Expression): Option[(Expression, Expression)] = $eqF.unapply(obj.unFix)
}
object $gt {
  def apply(left: Expression, right: Expression): Expression = Fix($gtF(left, right))
  def unapply(obj: Expression): Option[(Expression, Expression)] = $gtF.unapply(obj.unFix)
}
object $gte {
  def apply(left: Expression, right: Expression): Expression = Fix($gteF(left, right))
  def unapply(obj: Expression): Option[(Expression, Expression)] = $gteF.unapply(obj.unFix)
}
object $lt {
  def apply(left: Expression, right: Expression): Expression = Fix($ltF(left, right))
  def unapply(obj: Expression): Option[(Expression, Expression)] = $ltF.unapply(obj.unFix)
}
object $lte {
  def apply(left: Expression, right: Expression): Expression = Fix($lteF(left, right))
  def unapply(obj: Expression): Option[(Expression, Expression)] = $lteF.unapply(obj.unFix)
}
object $neq {
  def apply(left: Expression, right: Expression): Expression = Fix($neqF(left, right))
  def unapply(obj: Expression): Option[(Expression, Expression)] = $neqF.unapply(obj.unFix)
}

object $add {
  def apply(left: Expression, right: Expression): Expression = Fix($addF(left, right))
  def unapply(obj: Expression): Option[(Expression, Expression)] = $addF.unapply(obj.unFix)
}
object $divide {
  def apply(left: Expression, right: Expression): Expression = Fix($divideF(left, right))
  def unapply(obj: Expression): Option[(Expression, Expression)] = $divideF.unapply(obj.unFix)
}
object $mod {
  def apply(left: Expression, right: Expression): Expression = Fix($modF(left, right))
  def unapply(obj: Expression): Option[(Expression, Expression)] = $modF.unapply(obj.unFix)
}
object $multiply {
  def apply(left: Expression, right: Expression): Expression = Fix($multiplyF(left, right))
  def unapply(obj: Expression): Option[(Expression, Expression)] = $multiplyF.unapply(obj.unFix)
}
object $subtract {
  def apply(left: Expression, right: Expression): Expression = Fix($subtractF(left, right))
  def unapply(obj: Expression): Option[(Expression, Expression)] = $subtractF.unapply(obj.unFix)
}

object $concat {
  def apply(first: Expression, second: Expression, others: Expression*): Expression = Fix($concatF(first, second, others: _*))
  def unapplySeq(obj: Expression): Option[(Expression, Expression, Seq[Expression])] = $concatF.unapplySeq(obj.unFix)
}
object $strcasecmp {
  def apply(left: Expression, right: Expression): Expression = Fix($strcasecmpF(left, right))
  def unapply(obj: Expression): Option[(Expression, Expression)] = $strcasecmpF.unapply(obj.unFix)
}
object $substr {
  def apply(value: Expression, start: Expression, count: Expression): Expression = Fix($substrF(value, start, count))
  def unapply(obj: Expression): Option[(Expression, Expression, Expression)] = $substrF.unapply(obj.unFix)
}
object $toLower {
  def apply(value: Expression): Expression = Fix($toLowerF(value))
  def unapply(obj: Expression): Option[Expression] = $toLowerF.unapply(obj.unFix)
}
object $toUpper {
  def apply(value: Expression): Expression = Fix($toUpperF(value))
  def unapply(obj: Expression): Option[Expression] = $toUpperF.unapply(obj.unFix)
}

object $meta {
  def apply(): Expression = Fix($metaF())
  def unapply(obj: Expression): Boolean = $metaF.unapply(obj.unFix)
}

object $size {
  def apply(array: Expression): Expression = Fix($sizeF(array))
  def unapply(obj: Expression): Option[Expression] = $sizeF.unapply(obj.unFix)
}

object $arrayMap {
  def apply(input: Expression, as: DocVar.Name, in: Expression): Expression =
    Fix($arrayMapF(input, as, in))
  def unapply(obj: Expression):
      Option[(Expression, DocVar.Name, Expression)] =
    $arrayMapF.unapply(obj.unFix)
}
object $let {
  def apply(vars: ListMap[DocVar.Name, Expression], in: Expression):
      Expression =
    Fix($letF(vars, in))
  def unapply(obj: Expression):
      Option[(ListMap[DocVar.Name, Expression], Expression)] =
    $letF.unapply(obj.unFix)
}
object $literal {
  def apply(value: Bson): Expression = Fix($literalF(value))
  def unapply(obj: Expression): Option[Bson] = $literalF.unapply(obj.unFix)
}

object $dayOfYear {
  def apply(date: Expression): Expression = Fix($dayOfYearF(date))
  def unapply(obj: Expression): Option[Expression] = $dayOfYearF.unapply(obj.unFix)
}
object $dayOfMonth {
  def apply(date: Expression): Expression = Fix($dayOfMonthF(date))
  def unapply(obj: Expression): Option[Expression] = $dayOfMonthF.unapply(obj.unFix)
}
object $dayOfWeek {
  def apply(date: Expression): Expression = Fix($dayOfWeekF(date))
  def unapply(obj: Expression): Option[Expression] = $dayOfWeekF.unapply(obj.unFix)
}
object $year {
  def apply(date: Expression): Expression = Fix($yearF(date))
  def unapply(obj: Expression): Option[Expression] = $yearF.unapply(obj.unFix)
}
object $month {
  def apply(date: Expression): Expression = Fix($monthF(date))
  def unapply(obj: Expression): Option[Expression] = $monthF.unapply(obj.unFix)
}
object $week {
  def apply(date: Expression): Expression = Fix($weekF(date))
  def unapply(obj: Expression): Option[Expression] = $weekF.unapply(obj.unFix)
}
object $hour {
  def apply(date: Expression): Expression = Fix($hourF(date))
  def unapply(obj: Expression): Option[Expression] = $hourF.unapply(obj.unFix)
}
object $minute {
  def apply(date: Expression): Expression = Fix($minuteF(date))
  def unapply(obj: Expression): Option[Expression] = $minuteF.unapply(obj.unFix)
}
object $second {
  def apply(date: Expression): Expression = Fix($secondF(date))
  def unapply(obj: Expression): Option[Expression] = $secondF.unapply(obj.unFix)
}
object $millisecond {
  def apply(date: Expression): Expression = Fix($millisecondF(date))
  def unapply(obj: Expression): Option[Expression] = $millisecondF.unapply(obj.unFix)
}

object $cond {
  def apply(predicate: Expression, ifTrue: Expression, ifFalse: Expression):
      Expression =
    Fix($condF(predicate, ifTrue, ifFalse))
  def unapply(obj: Expression): Option[(Expression, Expression, Expression)] =
    $condF.unapply(obj.unFix)
}
object $ifNull {
  def apply(expr: Expression, replacement: Expression): Expression =
    Fix($ifNullF(expr, replacement))
  def unapply(obj: Expression): Option[(Expression, Expression)] =
    $ifNullF.unapply(obj.unFix)
}
