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
import matryoshka.specs2.scalacheck.CheckAll

import java.lang.String
import scala.Int

import org.scalacheck._
import org.specs2.ScalaCheck
import org.specs2.mutable._
import scalaz._, Scalaz._
import scalaz.scalacheck.ScalazProperties._

class PotentialFailureSpec extends Specification with ScalaCheck with CheckAll {
  implicit def potentialFailureArbitrary[T[_[_]], F[_], E: Arbitrary](
    implicit F: Arbitrary ~> λ[α => Arbitrary[F[α]]], T: Arbitrary[T[F]]):
      Arbitrary ~> λ[α => Arbitrary[PotentialFailure[T, F, E, α]]] =
    new (Arbitrary ~> λ[α => Arbitrary[PotentialFailure[T, F, E, α]]]) {
      def apply[α](arb: Arbitrary[α]) =
        Arbitrary(Gen.oneOf(
          T.arbitrary.map(Success[T, F, E, α](_)),
          Arbitrary.arbitrary[E].map(Failure[T, F, E, α](_)),
          F(arb).arbitrary.map(PartialFailure[T, F, E, α](_))))
    }

  "PotentialFailure should satisfy relevant laws" >> {
    checkAll(equal.laws[PotentialFailure[Fix, Exp, String, Int]])
    checkAll(bitraverse.laws[PotentialFailure[Fix, Exp, ?, ?]])
  }
}
