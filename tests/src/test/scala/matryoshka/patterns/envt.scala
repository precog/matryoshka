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
import scalaz.scalacheck.ScalazProperties._
import scalaz.scalacheck.ScalazArbitrary._

class EnvTSpec extends Specification with CheckAll with AlgebraChecks {
  implicit def envTCogen[E, F[_], A](implicit CA: Cogen[E], CF: Cogen[F[A]]): Cogen[EnvT[E, F, A]] =
    Cogen((seed, env) => Cogen.perturbPair(seed, env.run))

  implicit def envTArbitrary[E: Arbitrary, F[_]](
    implicit F: Delay[Arbitrary, F]):
      Delay[Arbitrary, EnvT[E, F, ?]] =
    new Delay[Arbitrary, EnvT[E, F, ?]] {
      def apply[A](arb: Arbitrary[A]) =
        Arbitrary(
          (Arbitrary.arbitrary[E] ⊛ F(arb).arbitrary)((e, f) => EnvT((e, f))))
    }

  "EnvT" should {
    "satisfy relevant laws" in {
      checkAll(equal.laws[EnvT[String, Exp, Int]])
      /* TODO */
      checkAll(comonad.laws[EnvT[String, NonEmptyList, ?]])
    }
  }

  // FIXME: enable this
  // checkAlgebraIsoLaws("EnvT ⇔ Cofree", EnvT.cofreeIso[Int, Exp])
}
