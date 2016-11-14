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

package matryoshka.scalacheck

import matryoshka._
import matryoshka.data._
import matryoshka.implicits._
import matryoshka.patterns._

import scala.{Option, None}

import org.scalacheck._
import scalaz._, Scalaz._
import scalaz.scalacheck.ScalaCheckBinding._

sealed trait ArbitraryInstances0 {
  implicit def delayArbitrary[F[_], A](
    implicit A: Arbitrary[A], F: Delay[Arbitrary, F]):
      Arbitrary[F[A]] =
    F(A)
}

trait ArbitraryInstances extends ArbitraryInstances0 {
  def corecursiveArbitrary[T, F[_]: Functor]
    (implicit T: Corecursive.Aux[T, F], fArb: Delay[Arbitrary, F])
      : Arbitrary[T] =
    Arbitrary(Gen.sized(size =>
      fArb(Arbitrary(
        if (size <= 0)
          Gen.fail[T]
        else
          Gen.resize(size - 1, corecursiveArbitrary[T, F].arbitrary))).arbitrary ∘ (_.embed)))

  implicit def fixArbitrary[F[_]: Functor](implicit fArb: Delay[Arbitrary, F]): Arbitrary[Fix[F]] =
    corecursiveArbitrary[Fix[F], F]

  implicit def muArbitrary[F[_]: Functor](implicit fArb: Delay[Arbitrary, F]): Arbitrary[Mu[F]] =
    corecursiveArbitrary[Mu[F], F]

  implicit def nuArbitrary[F[_]: Functor](implicit fArb: Delay[Arbitrary, F]): Arbitrary[Nu[F]] =
    corecursiveArbitrary[Nu[F], F]

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

  implicit def envTArbitrary[E: Arbitrary, F[_]](implicit F: Delay[Arbitrary, F]): Delay[Arbitrary, EnvT[E, F, ?]] =
    new Delay[Arbitrary, EnvT[E, F, ?]] {
      def apply[A](arb: Arbitrary[A]) =
        Arbitrary(
          (Arbitrary.arbitrary[E] ⊛ F(arb).arbitrary)((e, f) => EnvT((e, f))))
    }

  implicit def listFArbitrary[A: Arbitrary]: Delay[Arbitrary, ListF[A, ?]] =
    new Delay[Arbitrary, ListF[A, ?]] {
      def apply[B](arb: Arbitrary[B]) =
        Arbitrary(Gen.oneOf[ListF[A, B]](
          NilF[A, B]().point[Gen],
          (Arbitrary.arbitrary[A] ⊛ arb.arbitrary)(ConsF[A, B])))
    }

  // TODO: Submit these to Scalaz
  implicit def cofreeArbitrary[F[_], A](implicit F: Delay[Arbitrary, F], A: Arbitrary[A]): Arbitrary[Cofree[F, A]] =
    Arbitrary(
      Gen.sized(size =>
        (Gen.resize(size / 2, A.arbitrary) ⊛
          F(Arbitrary(
              if (size <= 0)
                Gen.fail[Cofree[F, A]]
              else
                Gen.resize(size / 2, cofreeArbitrary(F, A).arbitrary))).arbitrary)(
          Cofree(_, _))))

  implicit def freeArbitrary[F[_], A](implicit F: Delay[Arbitrary, F], A: Arbitrary[A]): Arbitrary[Free[F, A]] =
    Arbitrary(
      Gen.sized(size =>
        Gen.resize(size - 1,
          Gen.oneOf(
            A.arbitrary ∘ (_.point[Free[F, ?]]),
            F(Arbitrary(
              if (size <= 0)
                Gen.fail[Free[F, A]]
              else
                freeArbitrary(F, A).arbitrary)).arbitrary ∘ (Free.liftF(_).join)))))

  implicit val optionArbitrary: Delay[Arbitrary, Option] =
    new Delay[Arbitrary, Option] {
      def apply[A](arb: Arbitrary[A]) =
        Arbitrary(Gen.frequency(
          ( 1, None.point[Gen]),
          (75, arb.arbitrary.map(_.some))))
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
}

package object arbitrary extends ArbitraryInstances
