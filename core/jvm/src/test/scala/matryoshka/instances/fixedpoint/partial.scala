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

package matryoshka.instances.fixedpoint

import matryoshka._
import matryoshka.helpers._
import matryoshka.specs2.scalacheck.CheckAll

import scala.Predef.implicitly
import scala.Int

import org.scalacheck._
import org.specs2.ScalaCheck
import org.specs2.mutable._
import scalaz._, Scalaz._
import scalaz.scalacheck.ScalazProperties._

class PartialSpec extends Specification with ScalaCheck with CheckAll {
  implicit def eitherArbitrary[A: Arbitrary]:
      Arbitrary ~> λ[β => Arbitrary[A \/ β]] =
    new (Arbitrary ~> λ[β => Arbitrary[A \/ β]]) {
      def apply[β](arb: Arbitrary [β]) =
        Arbitrary(Gen.oneOf(
          Arbitrary.arbitrary[A].map(-\/(_)),
          arb.arbitrary.map(\/-(_))))
    }

  /** For testing cases that should work with truly diverging functions. */
  def sometimesNeverGen[A: Arbitrary]: Gen[Partial[A]] =
    Gen.oneOf(Arbitrary.arbitrary[Partial[A]], Gen.const(Partial.never[A]))

  "Partial" should {
    "satisfy relevant laws" in {
      checkAll(equal.laws[Partial[Int]](Partial.equal, implicitly))
      checkAll(monad.laws[Partial](Partial.monad, implicitly, implicitly, implicitly, Partial.equal))
    }
  }
}
