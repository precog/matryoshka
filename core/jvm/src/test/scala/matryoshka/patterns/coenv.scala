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

import matryoshka._
import matryoshka.exp._
import matryoshka.helpers._
import matryoshka.specs2.scalacheck._

import java.lang.{String}
import scala.{Int}

import org.scalacheck._
import org.specs2.mutable._
import scalaz._, Scalaz._
import scalaz.scalacheck.ScalaCheckBinding.{GenMonad => _, _}
import scalaz.scalacheck.ScalazProperties._

class CoEnvSpec extends Specification with CheckAll {
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

  "CoEnv should satisfy relevant laws" in {
    checkAll(equal.laws[CoEnv[String, Exp, Int]])
    checkAll(traverse.laws[CoEnv[String, Exp, ?]])
    // FIXME: These instances don’t fulfill the laws
    // checkAll(monad.laws[CoEnv[String, Option, ?]])
    // checkAll(monad.laws[CoEnv[String, NonEmptyList, ?]])
  }

  checkAlgebraIsoLaws(CoEnv.freeIso[Int, Exp])
}
