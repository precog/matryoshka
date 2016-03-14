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

package matryoshka.exp2

import matryoshka._

import scala.Int

import org.scalacheck._
import org.specs2.scalaz._
import scalaz._, Scalaz._
import scalaz.scalacheck.ScalazProperties._

sealed trait Exp2[A]
case class Const[A]()          extends Exp2[A]
case class Num2[A](value: Int) extends Exp2[A]
case class Single[A](a: A)     extends Exp2[A]

object Exp2 {
  implicit val arbitrary: Arbitrary ~> λ[α => Arbitrary[Exp2[α]]] =
    new (Arbitrary ~> λ[α => Arbitrary[Exp2[α]]]) {
      def apply[α](arb: Arbitrary[α]): Arbitrary[Exp2[α]] =
        Arbitrary(Gen.oneOf(
          Gen.const(Const[α]),
          Arbitrary.arbitrary[Int].map(Num2[α](_)),
          arb.arbitrary.map(Single(_))))
    }

  def const = Fix[Exp2](Const())
  def num2(v: Int) = Fix[Exp2](Num2(v))
  def single(a: Fix[Exp2]) = Fix[Exp2](Single(a))

  implicit val functor: Functor[Exp2] = new Functor[Exp2] {
    def map[A, B](fa: Exp2[A])(f: A => B): Exp2[B] = fa match {
      case Const()   => Const[B]()
      case Num2(v)   => Num2[B](v)
      case Single(a) => Single(f(a))
    }
  }

  implicit val show: Show ~> λ[α => Show[Exp2[α]]] =
    new (Show ~> λ[α => Show[Exp2[α]]]) {
      def apply[α](show: Show[α]) =
        Show.show {
          case Const()   => "Const()"
          case Num2(v)   => "Num2(" + v.shows + ")"
          case Single(a) => "Single(" + show.shows(a) + ")"
        }
    }

  implicit val equal: Equal ~> λ[α => Equal[Exp2[α]]] =
    new (Equal ~> λ[α => Equal[Exp2[α]]]) {
      def apply[α](eq: Equal[α]) =
        Equal.equal[Exp2[α]] {
          case (Const(), Const())       => true
          case (Num2(v1), Num2(v2))     => v1 ≟ v2
          case (Single(a1), Single(a2)) => eq.equal(a1, a2)
          case _                        => false
        }
    }
  implicit def equal2[A](implicit A: Equal[A]): Equal[Exp2[A]] = equal(A)
}

class Exp2Spec extends Spec {
  implicit val arbExp2Int: Arbitrary[Exp2[Int]] = Exp2.arbitrary(Arbitrary.arbInt)
  // NB: These are just a sanity check that the data structure created for the
  //     tests is lawful.
  checkAll(equal.laws[Exp2[Int]])
  checkAll(functor.laws[Exp2])
}
