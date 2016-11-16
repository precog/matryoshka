/*
 * Copyright 2014–2016 SlamData Inc.
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

package matryoshka

import scala.{None, Option, Some}

import org.specs2.mutable._
import org.typelevel.discipline.specs2.mutable._
import scalaz._

package object helpers extends SpecificationLike with Discipline {
  implicit val optionEqualNT: Delay[Equal, Option] = new Delay[Equal, Option] {
    def apply[A](eq: Equal[A]) =
      Equal.equal {
        case (None,    None)    => true
        case (Some(a), Some(b)) => eq.equal(a, b)
        case (_,       _)       => false
      }
  }

  implicit val optionShowNT: Delay[Show, Option] = new Delay[Show, Option] {
    def apply[A](s: Show[A]) =
      Show.show(_.fold(Cord("None"))(Cord("Some(") ++ s.show(_) ++ Cord(")")))
  }

  implicit def nonEmptyListEqual: Delay[Equal, NonEmptyList] =
    new Delay[Equal, NonEmptyList] {
      def apply[A](eq: Equal[A]) = NonEmptyList.nonEmptyListEqual(eq)
    }
}
