/*
 * Copyright 2014–2017 SlamData Inc.
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

package matryoshka.data

import slamdata.Predef._
import matryoshka._

import scalaz._

trait OptionInstances {
  implicit def optionBirecursive[A]
      : Birecursive.Aux[Option[A], Const[Option[A], ?]] =
    id.idBirecursive[Option[A]]

  implicit val optionDelayEqual: Delay[Equal, Option] =
    new Delay[Equal, Option] {
      def apply[A](a: Equal[A]) = {
        implicit val aʹ: Equal[A] = a
        Equal[Option[A]]
      }
    }

  implicit val optionDelayOrder: Delay[Order, Option] =
    Delay.fromNT(λ[Order ~> (Order ∘ Option)#λ](ord =>
      Order.order {
        case (None,    None)    => Ordering.EQ
        case (None,    Some(_)) => Ordering.LT
        case (Some(_), None)    => Ordering.GT
        case (Some(a), Some(b)) => ord.order(a, b)
      }))

  implicit val optionDelayShow: Delay[Show, Option] =
    Delay.fromNT(λ[Show ~> (Show ∘ Option)#λ](s =>
      Show.show(_.fold(Cord("None"))(Cord("Some(") ++ s.show(_) ++ Cord(")")))))
}

object option extends OptionInstances
