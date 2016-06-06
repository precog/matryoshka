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

import scala.Predef.implicitly

import org.scalacheck._
import scalaz._, Scalaz._
import scalaz.scalacheck.ScalaCheckBinding._

package object helpers {
  implicit def NTArbitrary[F[_], A](
    implicit A: Arbitrary[A], F: Arbitrary ~> λ[α => Arbitrary[F[α]]]):
      Arbitrary[F[A]] =
    F(A)

  implicit def corecArbitrary[T[_[_]]: Corecursive, F[_]: Functor](
    implicit F: Arbitrary ~> λ[α => Arbitrary[F[α]]]):
      Arbitrary[T[F]] =
    Arbitrary(Gen.sized(size =>
      F(
        if (size <= 0)
          Arbitrary(Gen.fail[T[F]])
        else
          Arbitrary(Gen.resize(size - 1, corecArbitrary[T, F].arbitrary))).arbitrary.map(_.embed)))

  implicit def freeArbitrary[F[_]](
    implicit F: Arbitrary ~> λ[α => Arbitrary[F[α]]]):
      Arbitrary ~> λ[α => Arbitrary[Free[F, α]]] =
    new (Arbitrary ~> λ[α => Arbitrary[Free[F, α]]]) {
      def apply[α](arb: Arbitrary[α]) =
        // FIXME: This is only generating leaf nodes
        Arbitrary(Gen.sized(size =>
          if (size <= 1)
            arb.map(_.point[Free[F, ?]]).arbitrary
          else
            Gen.oneOf(
              arb.arbitrary.map(_.point[Free[F, ?]]),
              F(freeArbitrary[F](F)(arb)).arbitrary.map(Free.roll))))
    }

  implicit def freeEqual[F[_]: Functor](
    implicit F: Equal ~> λ[α => Equal[F[α]]]):
      Equal ~> λ[α => Equal[Free[F, α]]] =
    new (Equal ~> λ[α => Equal[Free[F, α]]]) {
      def apply[α](eq: Equal[α]) =
        Equal.equal((a, b) => (a.resume, b.resume) match {
          case (-\/(f1), -\/(f2)) =>
            F(freeEqual[F](implicitly, F)(eq)).equal(f1, f2)
          case (\/-(a1), \/-(a2)) => eq.equal(a1, a2)
          case (_,       _)       => false
        })
    }
}
