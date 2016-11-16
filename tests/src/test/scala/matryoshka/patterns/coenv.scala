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
import matryoshka.data._
import matryoshka.exp.Exp
import matryoshka.exp2.Exp2
import matryoshka.helpers._
import matryoshka.scalacheck.arbitrary._
import matryoshka.specs2.scalacheck._

import java.lang.{String}
import scala.{Int}

import org.specs2.mutable._
import scalaz._, Scalaz._
import scalaz.scalacheck.ScalazProperties._

class CoEnvSpec extends Specification with CheckAll with AlgebraChecks {
  "CoEnv should satisfy relevant laws" in {
    checkAll(equal.laws[CoEnv[String, Exp, Int]])
    checkAll(bitraverse.laws[CoEnv[?, Exp, ?]])
    checkAll(traverse.laws[CoEnv[Int, Exp, ?]])
    // NB: This is to test the low-prio Bi-functor/-foldable instances, so if
    //     Exp2 gets a Traverse instance, this needs to change.
    checkAll(bifunctor.laws[CoEnv[?, Exp2, ?]])
    checkAll(functor.laws[CoEnv[Int, Exp2, ?]])
    checkAll(bifoldable.laws[CoEnv[?, Exp2, ?]])
    checkAll(foldable.laws[CoEnv[Int, Exp2, ?]])
    // FIXME: These instances don’t fulfill the laws
    // checkAll(monad.laws[CoEnv[String, Option, ?]])
    // checkAll(monad.laws[CoEnv[String, NonEmptyList, ?]])
  }

  checkAlgebraIsoLaws("CoEnv ⇔ Free", CoEnv.freeIso[Int, Exp])
}
