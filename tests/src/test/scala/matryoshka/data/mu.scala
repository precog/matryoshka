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

package matryoshka.data

import matryoshka.exp.Exp
import matryoshka.helpers._
import matryoshka.patterns.CoEnv
import matryoshka.scalacheck.arbitrary._
import matryoshka.specs2.scalacheck._

import scala.Int

import org.specs2.mutable._
import scalaz._, Scalaz._
import scalaz.scalacheck.ScalazProperties._

class MuSpec extends Specification with CheckAll with AlgebraChecks {
  "Mu" should {
    "satisfy relevant laws" in {
      checkAll(equal.laws[Mu[Exp]])
    }
  }

  checkFoldIsoLaws[Mu[CoEnv[Int, Exp, ?]], CoEnv[Int, Exp, ?], Free[Exp, Int]]("Mu", CoEnv.freeIso)
}
