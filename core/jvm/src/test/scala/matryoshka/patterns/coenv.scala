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

package matryoshka.patterns

import matryoshka.exp._
import matryoshka.helpers._

import scala.Int

import org.scalacheck._
import org.specs2.mutable._
import org.typelevel.discipline.specs2.mutable._
import scalaz._, Scalaz._
import scalaz.scalacheck.ScalaCheckBinding._

class CoEnvSpec extends Specification with Discipline {
  implicit def NTArbitrary[F[_], A](implicit A: Arbitrary[A], F: Arbitrary ~> λ[α => Arbitrary[F[α]]]):
      Arbitrary[F[A]] =
    F(A)

  implicit def coEnvArbitrary[E: Arbitrary, F[_]](
    implicit F: Arbitrary ~> λ[α => Arbitrary[F[α]]]):
      Arbitrary ~> λ[α => Arbitrary[CoEnv[E, F, α]]] =
    new (Arbitrary ~> λ[α => Arbitrary[CoEnv[E, F, α]]]) {
      def apply[α](arb: Arbitrary[α]) =
        // NB: Not sure why this version doesn’t work.
        // Arbitrary.arbitrary[E \/ F[α]] ∘ (CoEnv(_))
        Arbitrary(Gen.oneOf(
          Arbitrary.arbitrary[E].map(_.left),
          F(arb).arbitrary.map(_.right))) ∘ (CoEnv(_))
    }

  checkAlgebraIsoLaws(CoEnv.freeIso[Int, Exp])
}
