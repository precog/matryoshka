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
import matryoshka.helpers._
import matryoshka.specs2.scalacheck.CheckAll

import java.lang.String
import scala.{Int, Seq}
import scala.Predef.{implicitly => imp}

import org.scalacheck._
import org.specs2.ScalaCheck
import org.specs2.mutable._
import scalaz._, Scalaz._
import scalaz.scalacheck.ScalazProperties._

class ListFSpec extends Specification with ScalaCheck with CheckAll with AlgebraChecks {
  implicit def listFArbitrary[A: Arbitrary]: Delay[Arbitrary, ListF[A, ?]] =
    new Delay[Arbitrary, ListF[A, ?]] {
      def apply[B](arb: Arbitrary[B]) =
        Arbitrary(Gen.oneOf(
          NilF[A, B]().point[Gen],
          (Arbitrary.arbitrary[A] ⊛ arb.arbitrary)(ConsF[A, B])))
    }

  "ListF should satisfy relevant laws" >> {
    checkAll(equal.laws[ListF[String, Int]])
    checkAll(bitraverse.laws[ListF])
  }

  checkAlgebraIsoLaws("ListF ⇔ List", ListF.listIso[Int])(imp, Arbitrary(Gen.listOf(Arbitrary.arbInt.arbitrary)), imp, imp)
  checkAlgebraIsoLaws("ListF ⇔ Seq", ListF.seqIso[Int])(imp, Arbitrary(Gen.listOf(Arbitrary.arbInt.arbitrary).map(_.toSeq)), imp, Equal.equalA[Seq[Int]])
}
