/*
 * Copyright 2014–2017 SlamData Inc.
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
import matryoshka.scalacheck.cogen._

import java.lang.{String}
import scala.{Int}

import org.specs2.mutable._
import scalaz._, Scalaz._
import scalaz.scalacheck.ScalazProperties._

class CoEnvSpec extends Specification with AlgebraChecks {
  "CoEnv" >> {
    addFragments(properties(equal.laws[CoEnv[String, Exp, Int]]))
    addFragments(properties(bitraverse.laws[CoEnv[?, Exp, ?]]))
    addFragments(properties(traverse.laws[CoEnv[Int, Exp, ?]]))
    // NB: This is to test the low-prio Bi-functor/-foldable instances, so if
    //     Exp2 gets a Traverse instance, this needs to change.
    addFragments(properties(bifunctor.laws[CoEnv[?, Exp2, ?]]))
    addFragments(properties(functor.laws[CoEnv[Int, Exp2, ?]]))
    addFragments(properties(bifoldable.laws[CoEnv[?, Exp2, ?]]))
    addFragments(properties(foldable.laws[CoEnv[Int, Exp2, ?]]))
    // FIXME: These instances don’t fulfill the laws
    // monad.laws[CoEnv[String, Option, ?]].check(Test.Parameters.default)
    // monad.laws[CoEnv[String, NonEmptyList, ?]].check(Test.Parameters.default)

    checkAlgebraIsoLaws("CoEnv ⇔ Free", CoEnv.freeIso[Int, Exp])
  }
}
