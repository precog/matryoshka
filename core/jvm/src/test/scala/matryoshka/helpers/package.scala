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

import monocle.law.discipline._
import org.scalacheck._
import org.specs2.mutable._
import org.typelevel.discipline.specs2.mutable._
import scalaz._, Scalaz._
import scalaz.scalacheck.ScalaCheckBinding.{GenMonad => _, _}

package object helpers extends Specification with Discipline {
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
    implicit F: Arbitrary ~> (Arbitrary ∘ F)#λ):
      Arbitrary ~> (Arbitrary ∘ Free[F, ?])#λ =
    new (Arbitrary ~> (Arbitrary ∘ Free[F, ?])#λ) {
      def apply[α](arb: Arbitrary[α]) =
        Arbitrary(Gen.sized(size =>
          if (size <= 1)
            arb.map(_.point[Free[F, ?]]).arbitrary
          else
            Gen.oneOf(
              arb.arbitrary.map(_.point[Free[F, ?]]),
              F(freeArbitrary[F](F)(arb)).arbitrary.map(Free.roll))))
    }

  implicit def cofreeArbitrary[F[_]](
    implicit F: Arbitrary ~> (Arbitrary ∘ F)#λ):
      Arbitrary ~> (Arbitrary ∘ Cofree[F, ?])#λ =
    new (Arbitrary ~> (Arbitrary ∘ Cofree[F, ?])#λ) {
      def apply[A](arb: Arbitrary[A]) =
        Arbitrary(Gen.sized(size =>
          if (size <= 0)
            Gen.fail[Cofree[F, A]]
          else (arb.arbitrary ⊛ F(cofreeArbitrary(F)(arb)).arbitrary)(Cofree(_, _))))
    }

  implicit def optionArbitrary: Arbitrary ~> (Arbitrary ∘ Option)#λ =
    new (Arbitrary ~> (Arbitrary ∘ Option)#λ) {
      def apply[A](arb: Arbitrary[A]) =
        Arbitrary(Gen.oneOf(
          None.point[Gen],
          arb.arbitrary.map(_.some)))
    }

  implicit def optionEqualNT: Equal ~> (Equal ∘ Option)#λ =
    new (Equal ~> (Equal ∘ Option)#λ) {
      def apply[A](eq: Equal[A]) =
        Equal.equal {
          case (None,    None)    => true
          case (Some(a), Some(b)) => eq.equal(a, b)
          case (_,       _)       => false
        }
    }

  implicit def nonEmptyListArbitrary: Arbitrary ~> (Arbitrary ∘ NonEmptyList)#λ =
    new (Arbitrary ~> (Arbitrary ∘ NonEmptyList)#λ) {
      def apply[A](arb: Arbitrary[A]) =
        Arbitrary((arb.arbitrary ⊛ Gen.listOf[A](arb.arbitrary))((h, t) =>
          NonEmptyList.nel(h, t.toIList)))
    }

  implicit def nonEmptyListEqual: Equal ~> (Equal ∘ NonEmptyList)#λ =
    new (Equal ~> (Equal ∘ NonEmptyList)#λ) {
      def apply[A](eq: Equal[A]) = NonEmptyList.nonEmptyListEqual(eq)
    }


  def checkAlgebraIsoLaws[F[_], A](iso: AlgebraIso[F, A])(
    implicit FA: Arbitrary ~> (Arbitrary ∘ F)#λ, AA: Arbitrary[A], FE: Equal ~> (Equal ∘ F)#λ, AE: Equal[A]) =
    checkAll("algebra Iso", IsoTests(iso))

  def checkAlgebraPrismLaws[F[_], A](prism: AlgebraPrism[F, A])(
    implicit FA: Arbitrary ~> (Arbitrary ∘ F)#λ, AA: Arbitrary[A], FE: Equal ~> (Equal ∘ F)#λ, AE: Equal[A]) =
    checkAll("algebra Prism", PrismTests(prism))

  def checkCoalgebraPrismLaws[F[_], A](prism: CoalgebraPrism[F, A])(
    implicit FA: Arbitrary ~> (Arbitrary ∘ F)#λ, AA: Arbitrary[A], FE: Equal ~> (Equal ∘ F)#λ, AE: Equal[A]) =
    checkAll("coalgebra Prism", PrismTests(prism))
}
