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

package matryoshka.exp2

import matryoshka._

import scala.Int

import org.scalacheck._
import scalaz.{Apply => _, _}, Scalaz._

sealed trait Exp2[A]
case class Const[A]() extends Exp2[A]
case class Num2[A](value: Int) extends Exp2[A]
case class Single[A](a: A) extends Exp2[A]

object Exp2 {
  implicit val arbitrary: Delay[Arbitrary, Exp2] = new Delay[Arbitrary, Exp2] {
    def apply[α](arb: Arbitrary[α]): Arbitrary[Exp2[α]] =
      Arbitrary(Gen.oneOf(
        Gen.const(Const[α]),
        Arbitrary.arbitrary[Int].map(Num2[α](_)),
        arb.arbitrary.map(Single(_))))
  }

  // NB: This isn’t implicit in order to allow us to test our low-priority
  //     instances for CoEnv.
  val traverse: Traverse[Exp2] = new Traverse[Exp2] {
    def traverseImpl[G[_], A, B](
      fa: Exp2[A])(
      f: (A) ⇒ G[B])(
      implicit G: Applicative[G]) =
      fa match {
        case Const()   => G.point(Const[B]())
        case Num2(v)   => G.point(Num2[B](v))
        case Single(a) => f(a) ∘ (Single(_))
      }
  }

  implicit val functor: Functor[Exp2] = traverse
  implicit val foldable: Foldable[Exp2] = traverse

  implicit val show: Delay[Show, Exp2] = new Delay[Show, Exp2] {
    def apply[α](show: Show[α]) =
      Show.show {
        case Const()   => "Const()"
        case Num2(v)   => "Num2(" + v.shows + ")"
        case Single(a) => "Single(" + show.shows(a) + ")"
      }
  }

  implicit val equal: Delay[Equal, Exp2] = new Delay[Equal, Exp2] {
    def apply[α](eq: Equal[α]) =
      Equal.equal[Exp2[α]] {
        case (Const(), Const())       => true
        case (Num2(v1), Num2(v2))     => v1 ≟ v2
        case (Single(a1), Single(a2)) => eq.equal(a1, a2)
        case _                        => false
      }
  }
}
