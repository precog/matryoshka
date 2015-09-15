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
  def apply(): PipelineExpression = Fix($includeF())
  def unapply(obj: PipelineExpression): Boolean = $includeF.unapply(obj.unFix)
}
object $var {
  def apply(docVar: DocVar): PipelineExpression = Fix($varF(docVar))
  def unapply(obj: PipelineExpression): Option[DocVar] = $varF.unapply(obj.unFix)
}
object $and {
  def apply(first: PipelineExpression, second: PipelineExpression, others: PipelineExpression*):
      PipelineExpression =
    Fix($andF(first, second, others: _*))
  def unapplySeq(obj: PipelineExpression): Option[(PipelineExpression, PipelineExpression, Seq[PipelineExpression])] = $andF.unapplySeq(obj.unFix)
}
object $or {
  def apply(first: PipelineExpression, second: PipelineExpression, others: PipelineExpression*):
      PipelineExpression =
    Fix($orF(first, second, others: _*))
  def unapplySeq(obj: PipelineExpression): Option[(PipelineExpression, PipelineExpression, Seq[PipelineExpression])] = $orF.unapplySeq(obj.unFix)
}
object $not {
  def apply(value: PipelineExpression): PipelineExpression = Fix($notF(value))
  def unapply(obj: PipelineExpression): Option[PipelineExpression] = $notF.unapply(obj.unFix)
}

object $setEquals {
  def apply(left: PipelineExpression, right: PipelineExpression): PipelineExpression = Fix($setEqualsF(left, right))
  def unapply(obj: PipelineExpression): Option[(PipelineExpression, PipelineExpression)] = $setEqualsF.unapply(obj.unFix)
}
object $setIntersection {
  def apply(left: PipelineExpression, right: PipelineExpression): PipelineExpression = Fix($setIntersectionF(left, right))
  def unapply(obj: PipelineExpression): Option[(PipelineExpression, PipelineExpression)] = $setIntersectionF.unapply(obj.unFix)
}
object $setDifference {
  def apply(left: PipelineExpression, right: PipelineExpression): PipelineExpression = Fix($setDifferenceF(left, right))
  def unapply(obj: PipelineExpression): Option[(PipelineExpression, PipelineExpression)] = $setDifferenceF.unapply(obj.unFix)
}
object $setUnion {
  def apply(left: PipelineExpression, right: PipelineExpression): PipelineExpression = Fix($setUnionF(left, right))
  def unapply(obj: PipelineExpression): Option[(PipelineExpression, PipelineExpression)] = $setUnionF.unapply(obj.unFix)
}
object $setIsSubset {
  def apply(left: PipelineExpression, right: PipelineExpression): PipelineExpression = Fix($setIsSubsetF(left, right))
  def unapply(obj: PipelineExpression): Option[(PipelineExpression, PipelineExpression)] = $setIsSubsetF.unapply(obj.unFix)
}
object $anyElementTrue {
  def apply(value: PipelineExpression): PipelineExpression = Fix($anyElementTrueF(value))
  def unapply(obj: PipelineExpression): Option[PipelineExpression] = $anyElementTrueF.unapply(obj.unFix)
}
object $allElementsTrue {
  def apply(value: PipelineExpression): PipelineExpression = Fix($allElementsTrueF(value))
  def unapply(obj: PipelineExpression): Option[PipelineExpression] = $allElementsTrueF.unapply(obj.unFix)
}

object $cmp {
  def apply(left: PipelineExpression, right: PipelineExpression): PipelineExpression = Fix($cmpF(left, right))
  def unapply(obj: PipelineExpression): Option[(PipelineExpression, PipelineExpression)] = $cmpF.unapply(obj.unFix)
}
object $eq {
  def apply(left: PipelineExpression, right: PipelineExpression): PipelineExpression = Fix($eqF(left, right))
  def unapply(obj: PipelineExpression): Option[(PipelineExpression, PipelineExpression)] = $eqF.unapply(obj.unFix)
}
object $gt {
  def apply(left: PipelineExpression, right: PipelineExpression): PipelineExpression = Fix($gtF(left, right))
  def unapply(obj: PipelineExpression): Option[(PipelineExpression, PipelineExpression)] = $gtF.unapply(obj.unFix)
}
object $gte {
  def apply(left: PipelineExpression, right: PipelineExpression): PipelineExpression = Fix($gteF(left, right))
  def unapply(obj: PipelineExpression): Option[(PipelineExpression, PipelineExpression)] = $gteF.unapply(obj.unFix)
}
object $lt {
  def apply(left: PipelineExpression, right: PipelineExpression): PipelineExpression = Fix($ltF(left, right))
  def unapply(obj: PipelineExpression): Option[(PipelineExpression, PipelineExpression)] = $ltF.unapply(obj.unFix)
}
object $lte {
  def apply(left: PipelineExpression, right: PipelineExpression): PipelineExpression = Fix($lteF(left, right))
  def unapply(obj: PipelineExpression): Option[(PipelineExpression, PipelineExpression)] = $lteF.unapply(obj.unFix)
}
object $neq {
  def apply(left: PipelineExpression, right: PipelineExpression): PipelineExpression = Fix($neqF(left, right))
  def unapply(obj: PipelineExpression): Option[(PipelineExpression, PipelineExpression)] = $neqF.unapply(obj.unFix)
}

object $add {
  def apply(left: PipelineExpression, right: PipelineExpression): PipelineExpression = Fix($addF(left, right))
  def unapply(obj: PipelineExpression): Option[(PipelineExpression, PipelineExpression)] = $addF.unapply(obj.unFix)
}
object $divide {
  def apply(left: PipelineExpression, right: PipelineExpression): PipelineExpression = Fix($divideF(left, right))
  def unapply(obj: PipelineExpression): Option[(PipelineExpression, PipelineExpression)] = $divideF.unapply(obj.unFix)
}
object $mod {
  def apply(left: PipelineExpression, right: PipelineExpression): PipelineExpression = Fix($modF(left, right))
  def unapply(obj: PipelineExpression): Option[(PipelineExpression, PipelineExpression)] = $modF.unapply(obj.unFix)
}
object $multiply {
  def apply(left: PipelineExpression, right: PipelineExpression): PipelineExpression = Fix($multiplyF(left, right))
  def unapply(obj: PipelineExpression): Option[(PipelineExpression, PipelineExpression)] = $multiplyF.unapply(obj.unFix)
}
object $subtract {
  def apply(left: PipelineExpression, right: PipelineExpression): PipelineExpression = Fix($subtractF(left, right))
  def unapply(obj: PipelineExpression): Option[(PipelineExpression, PipelineExpression)] = $subtractF.unapply(obj.unFix)
}

object $concat {
  def apply(first: PipelineExpression, second: PipelineExpression, others: PipelineExpression*): PipelineExpression = Fix($concatF(first, second, others: _*))
  def unapplySeq(obj: PipelineExpression): Option[(PipelineExpression, PipelineExpression, Seq[PipelineExpression])] = $concatF.unapplySeq(obj.unFix)
}
object $strcasecmp {
  def apply(left: PipelineExpression, right: PipelineExpression): PipelineExpression = Fix($strcasecmpF(left, right))
  def unapply(obj: PipelineExpression): Option[(PipelineExpression, PipelineExpression)] = $strcasecmpF.unapply(obj.unFix)
}
object $substr {
  def apply(value: PipelineExpression, start: PipelineExpression, count: PipelineExpression): PipelineExpression = Fix($substrF(value, start, count))
  def unapply(obj: PipelineExpression): Option[(PipelineExpression, PipelineExpression, PipelineExpression)] = $substrF.unapply(obj.unFix)
}
object $toLower {
  def apply(value: PipelineExpression): PipelineExpression = Fix($toLowerF(value))
  def unapply(obj: PipelineExpression): Option[PipelineExpression] = $toLowerF.unapply(obj.unFix)
}
object $toUpper {
  def apply(value: PipelineExpression): PipelineExpression = Fix($toUpperF(value))
  def unapply(obj: PipelineExpression): Option[PipelineExpression] = $toUpperF.unapply(obj.unFix)
}

object $meta {
  def apply(): PipelineExpression = Fix($metaF())
  def unapply(obj: PipelineExpression): Boolean = $metaF.unapply(obj.unFix)
}

object $size {
  def apply(array: PipelineExpression): PipelineExpression = Fix($sizeF(array))
  def unapply(obj: PipelineExpression): Option[PipelineExpression] = $sizeF.unapply(obj.unFix)
}

object $arrayMap {
  def apply(input: PipelineExpression, as: DocVar.Name, in: PipelineExpression): PipelineExpression =
    Fix($arrayMapF(input, as, in))
  def unapply(obj: PipelineExpression):
      Option[(PipelineExpression, DocVar.Name, PipelineExpression)] =
    $arrayMapF.unapply(obj.unFix)
}
object $let {
  def apply(vars: ListMap[DocVar.Name, PipelineExpression], in: PipelineExpression):
      PipelineExpression =
    Fix($letF(vars, in))
  def unapply(obj: PipelineExpression):
      Option[(ListMap[DocVar.Name, PipelineExpression], PipelineExpression)] =
    $letF.unapply(obj.unFix)
}
object $literal {
  def apply(value: Bson): PipelineExpression = Fix($literalF(value))
  def unapply(obj: PipelineExpression): Option[Bson] = $literalF.unapply(obj.unFix)
}

object $dayOfYear {
  def apply(date: PipelineExpression): PipelineExpression = Fix($dayOfYearF(date))
  def unapply(obj: PipelineExpression): Option[PipelineExpression] = $dayOfYearF.unapply(obj.unFix)
}
object $dayOfMonth {
  def apply(date: PipelineExpression): PipelineExpression = Fix($dayOfMonthF(date))
  def unapply(obj: PipelineExpression): Option[PipelineExpression] = $dayOfMonthF.unapply(obj.unFix)
}
object $dayOfWeek {
  def apply(date: PipelineExpression): PipelineExpression = Fix($dayOfWeekF(date))
  def unapply(obj: PipelineExpression): Option[PipelineExpression] = $dayOfWeekF.unapply(obj.unFix)
}
object $year {
  def apply(date: PipelineExpression): PipelineExpression = Fix($yearF(date))
  def unapply(obj: PipelineExpression): Option[PipelineExpression] = $yearF.unapply(obj.unFix)
}
object $month {
  def apply(date: PipelineExpression): PipelineExpression = Fix($monthF(date))
  def unapply(obj: PipelineExpression): Option[PipelineExpression] = $monthF.unapply(obj.unFix)
}
object $week {
  def apply(date: PipelineExpression): PipelineExpression = Fix($weekF(date))
  def unapply(obj: PipelineExpression): Option[PipelineExpression] = $weekF.unapply(obj.unFix)
}
object $hour {
  def apply(date: PipelineExpression): PipelineExpression = Fix($hourF(date))
  def unapply(obj: PipelineExpression): Option[PipelineExpression] = $hourF.unapply(obj.unFix)
}
object $minute {
  def apply(date: PipelineExpression): PipelineExpression = Fix($minuteF(date))
  def unapply(obj: PipelineExpression): Option[PipelineExpression] = $minuteF.unapply(obj.unFix)
}
object $second {
  def apply(date: PipelineExpression): PipelineExpression = Fix($secondF(date))
  def unapply(obj: PipelineExpression): Option[PipelineExpression] = $secondF.unapply(obj.unFix)
}
object $millisecond {
  def apply(date: PipelineExpression): PipelineExpression = Fix($millisecondF(date))
  def unapply(obj: PipelineExpression): Option[PipelineExpression] = $millisecondF.unapply(obj.unFix)
}

object $cond {
  def apply(predicate: PipelineExpression, ifTrue: PipelineExpression, ifFalse: PipelineExpression):
      PipelineExpression =
    Fix($condF(predicate, ifTrue, ifFalse))
  def unapply(obj: PipelineExpression): Option[(PipelineExpression, PipelineExpression, PipelineExpression)] =
    $condF.unapply(obj.unFix)
}
object $ifNull {
  def apply(expr: PipelineExpression, replacement: PipelineExpression): PipelineExpression =
    Fix($ifNullF(expr, replacement))
  def unapply(obj: PipelineExpression): Option[(PipelineExpression, PipelineExpression)] =
    $ifNullF.unapply(obj.unFix)
}
