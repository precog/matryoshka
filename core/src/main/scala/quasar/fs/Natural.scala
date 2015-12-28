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

package quasar
package fs

import quasar.Predef._

import scalaz._
import scalaz.Tags.{Multiplication => Mult}

final class Natural private[fs] (val value: Long) extends scala.AnyVal {
  def plus(other: Natural): Natural =
    new Natural(value + other.value)

  def + (other: Natural): Natural =
    plus(other)

  def times(other: Natural): Natural =
    new Natural(value * other.value)

  def * (other: Natural): Natural =
    times(other)

  def toInt: Int =
    value.toInt
}

object Natural {
  def apply(n: Long): Option[Natural] =
    Some(n).filter(_ >= 0).map(new Natural(_))

  def unapply(n: Natural): Option[Long] = Some(n.value)

  val _0: Natural = new Natural(0)
  val _1: Natural = new Natural(1)
  val _2: Natural = new Natural(2)
  val _3: Natural = new Natural(3)
  val _4: Natural = new Natural(4)
  val _5: Natural = new Natural(5)
  val _6: Natural = new Natural(6)
  val _7: Natural = new Natural(7)
  val _8: Natural = new Natural(8)
  val _9: Natural = new Natural(9)

  def fromPositive(n: Positive): Natural =
    new Natural(n.value)

  implicit val naturalAddition: Monoid[Natural] =
    Monoid.instance(_ + _, _0)

  implicit val naturalMultiplication: Monoid[Natural @@ Mult] =
    Monoid.instance(
      (x, y) => Mult(Mult.unwrap(x) * Mult.unwrap(y)),
      Mult(_1))

  implicit val equal: Equal[Natural] = Equal.equalA

  implicit val show: Show[Natural] = Show.shows(_.value.toString)
}
