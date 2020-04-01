/*
 * Copyright 2020 Precog Data
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

import slamdata.Predef._
import matryoshka.exp.Exp
import matryoshka.helpers._
import matryoshka.patterns.CoEnv
import matryoshka.scalacheck.arbitrary._
import matryoshka.scalacheck.cogen._

import org.specs2.mutable._
import scalaz._, Scalaz._
import scalaz.scalacheck.ScalazProperties._

class FixSpec extends Specification with AlgebraChecks {

  "Fix" >> {

    addFragments(properties(equal.laws[Fix[Exp]]))

    checkFoldIsoLaws[Fix[CoEnv[Int, Exp, ?]], CoEnv[Int, Exp, ?], Free[Exp, Int]]("Fix", CoEnv.freeIso)
  }
}
