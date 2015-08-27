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

package quasar.std

import quasar.Predef._
import quasar._

import scalaz._, NonEmptyList.nel, Validation.{success, failure}

trait AggLib extends Library {
  private val reflexiveUntyper: Func.Untyper =
    untyper(t => success(List(t)))

  val Count = Reduction("COUNT", "Counts the values in a set",
    Type.Int, Type.Top :: Nil,
    noSimplification,
    partialTyper {
      case List(Type.Const(Data.Set(Nil))) => Type.Const(Data.Int(0))
      case List(_)                         => Type.Int
    },
    basicUntyper)

  val Sum = Reduction("SUM", "Sums the values in a set",
    Type.Numeric | Type.Interval, (Type.Numeric | Type.Interval) :: Nil,
    noSimplification,
    partialTyper {
      case List(Type.Const(Data.Set(Nil)))  => Type.Const(Data.Int(0))
      case List(Type.Set(t))                => t
      case List(t)                          => t
    },
    reflexiveUntyper)

  val Min = Reduction("MIN", "Finds the minimum in a set of values",
    Type.Comparable, Type.Comparable :: Nil,
    noSimplification,
    reflexiveTyper,
    reflexiveUntyper)

  val Max = Reduction("MAX", "Finds the maximum in a set of values",
    Type.Comparable, Type.Comparable :: Nil,
    noSimplification,
    reflexiveTyper,
    reflexiveUntyper)

  val Avg = Reduction("AVG", "Finds the average in a set of numeric values",
    Type.Numeric | Type.Interval, (Type.Numeric | Type.Interval) :: Nil,
    noSimplification,
    constTyper(Type.Dec),
    reflexiveUntyper)

  val Arbitrary = Reduction("ARBITRARY", "Returns an arbitrary value from a set",
    Type.Top, Type.Top :: Nil,
    noSimplification,
    reflexiveTyper,
    reflexiveUntyper)

  def functions = Count :: Sum :: Min :: Max :: Avg :: Arbitrary :: Nil
}
object AggLib extends AggLib
