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

package matryoshka.scalacheck

import matryoshka._

import org.scalacheck._
import scalaz._
import scalaz.scalacheck.ScalaCheckBinding._
import scalaz.syntax.monad._

trait ArbitraryInstances0 {
  implicit def corecursiveArbitrary[T[_[_]]: Corecursive, F[_]: Functor](
    fArb: Arbitrary ~> λ[α => Arbitrary[F[α]]]):
      Arbitrary[T[F]] =
    Arbitrary(Gen.sized(size =>
      fArb(Arbitrary(
        if (size <= 0)
          Gen.fail[T[F]]
        else
          Gen.resize(size - 1, corecursiveArbitrary(fArb).arbitrary))).arbitrary ∘ (_.embed)))
}

trait ArbitraryInstances extends ArbitraryInstances0 {
  // TODO: Submit these to Scalaz
  implicit def cofreeArbitrary[F[_], A](
    fArb: Arbitrary ~> λ[α => Arbitrary[F[α]]])(
    implicit A: Arbitrary[A]):
      Arbitrary[Cofree[F, A]] =
    Arbitrary(
      Gen.sized(size =>
        (Gen.resize(size / 2, A.arbitrary) ⊛
          fArb(Arbitrary(
              if (size <= 0)
                Gen.fail[Cofree[F, A]]
              else
                Gen.resize(size / 2, cofreeArbitrary(fArb).arbitrary))).arbitrary)(
          Cofree(_, _))))

  implicit def freeArbitrary[F[_]: Functor, A](
    fArb: Arbitrary ~> λ[α => Arbitrary[F[α]]])(
    implicit A: Arbitrary[A]):
      Arbitrary[Free[F, A]] =
    Arbitrary(
      Gen.sized(size =>
        Gen.resize(size - 1,
          Gen.oneOf(
            A.arbitrary ∘ (_.point[Free[F, ?]]),
            fArb(Arbitrary(
              if (size <= 0)
                Gen.fail[Free[F, A]]
              else
                freeArbitrary(fArb).arbitrary)).arbitrary ∘ (Free.liftF(_).join)))))
}

package object arbitrary extends ArbitraryInstances
