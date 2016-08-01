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

import matryoshka.patterns.CoEnv

import java.lang.String
import scala.{None, Option, Some}

import monocle.law.discipline._
import org.scalacheck._
import org.specs2.mutable._
import org.typelevel.discipline.specs2.mutable._
import scalaz._, Scalaz._
import scalaz.scalacheck.ScalaCheckBinding.{GenMonad => _, _}

package object helpers extends Specification with Discipline {
  implicit def delayArbitrary[F[_], A](
    implicit A: Arbitrary[A], F: Delay[Arbitrary, F]):
      Arbitrary[F[A]] =
    F(A)

  implicit def corecArbitrary[T[_[_]]: Corecursive, F[_]: Functor](
    implicit F: Delay[Arbitrary, F]):
      Arbitrary[T[F]] =
    Arbitrary(Gen.sized(size =>
      F(
        if (size <= 0)
          Arbitrary(Gen.fail[T[F]])
        else
          Arbitrary(Gen.resize(size - 1, corecArbitrary[T, F].arbitrary))).arbitrary.map(_.embed)))

  implicit def coEnvArbitrary[E: Arbitrary, F[_]](
    implicit F: Delay[Arbitrary, F]):
      Delay[Arbitrary, CoEnv[E, F, ?]] =
    new Delay[Arbitrary, CoEnv[E, F, ?]] {
      def apply[α](arb: Arbitrary[α]) =
        // NB: Not sure why this version doesn’t work.
        // Arbitrary.arbitrary[E \/ F[α]] ∘ (CoEnv(_))
        Arbitrary(Gen.oneOf(
          Arbitrary.arbitrary[E].map(_.left),
          F(arb).arbitrary.map(_.right))) ∘ (CoEnv(_))
    }

  implicit def freeArbitrary[F[_]](implicit F: Delay[Arbitrary, F]):
      Delay[Arbitrary, Free[F, ?]] =
    new Delay[Arbitrary, Free[F, ?]] {
      def apply[α](arb: Arbitrary[α]) =
        Arbitrary(Gen.sized(size =>
          if (size <= 1)
            arb.map(_.point[Free[F, ?]]).arbitrary
          else
            Gen.oneOf(
              arb.arbitrary.map(_.point[Free[F, ?]]),
              F(freeArbitrary[F](F)(arb)).arbitrary.map(Free.roll))))
    }

  implicit def cofreeArbitrary[F[_]](implicit F: Delay[Arbitrary, F]):
      Delay[Arbitrary, Cofree[F, ?]] =
    new Delay[Arbitrary, Cofree[F, ?]] {
      def apply[A](arb: Arbitrary[A]) =
        Arbitrary(Gen.sized(size =>
          if (size <= 0)
            Gen.fail[Cofree[F, A]]
          else (arb.arbitrary ⊛ F(cofreeArbitrary(F)(arb)).arbitrary)(Cofree(_, _))))
    }

  implicit val optionArbitrary: Delay[Arbitrary, Option] =
    new Delay[Arbitrary, Option] {
      def apply[A](arb: Arbitrary[A]) =
        Arbitrary(Gen.frequency(
          ( 1, None.point[Gen]),
          (75, arb.arbitrary.map(_.some))))
    }

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

  implicit def eitherArbitrary[A: Arbitrary]: Delay[Arbitrary, A \/ ?] =
    new Delay[Arbitrary, A \/ ?] {
      def apply[B](arb: Arbitrary[B]) =
        Arbitrary(Gen.oneOf(
          Arbitrary.arbitrary[A].map(-\/(_)),
          arb.arbitrary.map(\/-(_))))
    }

  implicit def nonEmptyListArbitrary: Delay[Arbitrary, NonEmptyList] =
    new Delay[Arbitrary, NonEmptyList] {
      def apply[A](arb: Arbitrary[A]) =
        Arbitrary((arb.arbitrary ⊛ Gen.listOf[A](arb.arbitrary))((h, t) =>
          NonEmptyList.nel(h, t.toIList)))
    }

  implicit def nonEmptyListEqual: Delay[Equal, NonEmptyList] =
    new Delay[Equal, NonEmptyList] {
      def apply[A](eq: Equal[A]) = NonEmptyList.nonEmptyListEqual(eq)
    }


  def checkFoldIsoLaws[T[_[_]]: Recursive: Corecursive: EqualT, F[_]: Functor, A](
    name: String, iso: AlgebraIso[F, A])(
    implicit FA: Delay[Arbitrary, F], AA: Arbitrary[A], FE: Delay[Equal, F], AE: Equal[A]) =
    checkAll(name + " Iso", IsoTests(foldIso[T, F, A](iso)))

  def checkFoldPrismLaws[T[_[_]]: Recursive: Corecursive: EqualT, F[_]: Traverse, A](
    name: String, prism: AlgebraPrism[F, A])(
    implicit FA: Delay[Arbitrary, F], AA: Arbitrary[A], FE: Delay[Equal, F], AE: Equal[A]) =
    checkAll(name + " Prism", PrismTests(foldPrism(prism)))

  def checkUnfoldPrismLaws[T[_[_]]: Recursive: Corecursive: EqualT, F[_]: Traverse, A](
    name: String, prism: CoalgebraPrism[F, A])(
    implicit FA: Delay[Arbitrary, F], AA: Arbitrary[A], FE: Delay[Equal, F], AE: Equal[A]) =
    checkAll(name + " Prism", PrismTests(unfoldPrism(prism)))

  def checkAlgebraIsoLaws[F[_], A](
    name: String, iso: AlgebraIso[F, A])(
    implicit FA: Delay[Arbitrary, F], AA: Arbitrary[A], FE: Delay[Equal, F], AE: Equal[A]) =
    checkAll(name + " Iso", IsoTests(iso))

  def checkAlgebraPrismLaws[F[_], A](
    name: String, prism: AlgebraPrism[F, A])(
    implicit FA: Delay[Arbitrary, F], AA: Arbitrary[A], FE: Delay[Equal, F], AE: Equal[A]) =
    checkAll(name + " Prism", PrismTests(prism))

  def checkCoalgebraPrismLaws[F[_], A](
    name: String, prism: CoalgebraPrism[F, A])(
    implicit FA: Delay[Arbitrary, F], AA: Arbitrary[A], FE: Delay[Equal, F], AE: Equal[A]) =
    checkAll(name + " Prism", PrismTests(prism))
}
