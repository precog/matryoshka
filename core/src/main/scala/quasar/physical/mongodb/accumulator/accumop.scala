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

package quasar.physical.mongodb.accumulator

import quasar.Predef._

sealed trait AccumOp[A]
object AccumOp {
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
  def apply[A](value: A): AccumOp[A] = AccumOp.$addToSet[A](value)
  def unapply[A](obj: AccumOp[A]): Option[A] = obj match {
    case AccumOp.$addToSet(value) => Some(value)
    case _                      => None
  }
}
object $push {
  def apply[A](value: A): AccumOp[A] = AccumOp.$push[A](value)
  def unapply[A](obj: AccumOp[A]): Option[A] = obj match {
    case AccumOp.$push(value) => Some(value)
    case _                  => None
  }
}
object $first {
  def apply[A](value: A): AccumOp[A] = AccumOp.$first[A](value)
  def unapply[A](obj: AccumOp[A]): Option[A] = obj match {
    case AccumOp.$first(value) => Some(value)
    case _                   => None
  }
}
object $last {
  def apply[A](value: A): AccumOp[A] = AccumOp.$last[A](value)
  def unapply[A](obj: AccumOp[A]): Option[A] = obj match {
    case AccumOp.$last(value) => Some(value)
    case _                  => None
  }
}
object $max {
  def apply[A](value: A): AccumOp[A] = AccumOp.$max[A](value)
  def unapply[A](obj: AccumOp[A]): Option[A] = obj match {
    case AccumOp.$max(value) => Some(value)
    case _                 => None
  }
}
object $min {
  def apply[A](value: A): AccumOp[A] = AccumOp.$min[A](value)
  def unapply[A](obj: AccumOp[A]): Option[A] = obj match {
    case AccumOp.$min(value) => Some(value)
    case _                 => None
  }
}
object $avg {
  def apply[A](value: A): AccumOp[A] = AccumOp.$avg[A](value)
  def unapply[A](obj: AccumOp[A]): Option[A] = obj match {
    case AccumOp.$avg(value) => Some(value)
    case _                 => None
  }
}
object $sum {
  def apply[A](value: A): AccumOp[A] = AccumOp.$sum[A](value)
  def unapply[A](obj: AccumOp[A]): Option[A] = obj match {
    case AccumOp.$sum(value) => Some(value)
    case _          => None
  }
}
