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

package matryoshka.instances.fixedpoint

import slamdata.Predef._
import matryoshka._
import matryoshka.implicits._
import matryoshka.scalacheck.arbitrary._

import scala.Predef.implicitly

import org.scalacheck._
import org.specs2.ScalaCheck
import org.specs2.mutable._
import org.specs2.scalaz.{ScalazMatchers}
import scalaz._, Scalaz._
import scalaz.scalacheck.{ScalazProperties => Props}

class PartialSpec extends Specification with ScalazMatchers with ScalaCheck {
  /** For testing cases that should work with truly diverging functions. */
  def sometimesNeverGen[A: Arbitrary]: Gen[Partial[A]] =
    Gen.oneOf(Arbitrary.arbitrary[Partial[A]], Gen.const(Partial.never[A]))

  "Partial laws" >> {

    addFragments(properties(Props.equal.laws[Partial[Int]](Partial.equal, implicitly)))
    addFragments(properties(Props.monad.laws[Partial](implicitly, implicitly, implicitly, implicitly, Partial.equal)))
    addFragments(properties(Props.foldable.laws[Partial]))
  }

  // https://en.wikipedia.org/wiki/McCarthy_91_function
  def mc91(n: Int): Partial[Int] =
    if (n > 100) Partial.now(n - 10)
    else mc91(n + 11) >>= mc91

  "never" should {
    "always have more steps" >> prop { (i: Conat) =>
      Partial.never[Int].runFor(i) must beRightDisjunction
    }
  }

  "runFor" should {
    "return now immediately" in {
      Partial.now(13).runFor(Nat.zero[Nat]) must beLeftDisjunction(13)
    }

    "return a value when it runs past the end" >> prop { (i: Conat) =>
      i.transAna[Partial[Int]](Partial.delay(7)).runFor(i) must
        beLeftDisjunction(7)
    }

    "return after multiple runs" >> prop { (a: Conat, b: Conat) =>
      b > Nat.zero[Conat] ==> {
        val first = (a + b).transAna[Partial[Int]](Partial.delay(27)).runFor(a)
        first must beRightDisjunction
        first.flatMap(_.runFor(b)) must beLeftDisjunction(27)
      }
    }

    "still pending one short" >> prop { (a: Conat) =>
      val first = (a + Nat.one[Conat]).transAna[Partial[Int]](Partial.delay(27)).runFor(a)
      first must beRightDisjunction
      first.flatMap(_.runFor(a + Nat.one[Conat])) must beLeftDisjunction(27)
    }

    "return exactly at the end" >> prop { (n: Conat, i: Int) =>
      n.transAna[Partial[Int]](Partial.delay(i)).runFor(n) must
        beLeftDisjunction(i)
    }
  }

  "unsafePerformSync" should {
    "return novw immediately" in {
      Partial.now(12).unsafePerformSync must equal(12)
    }

    "return a value when it gets to the end" in {
      Partial.later(Partial.later(Partial.now(3))).unsafePerformSync must
        equal(3)
    }

    // TODO: Should work with any Int, but stack overflows on big negatives.
    "always terminate with mc91" >> prop { (n: Int) =>
      n > -90000 ==>
        (mc91(n).unsafePerformSync must equal(if (n <= 100) 91 else n - 10))
    }
  }
}
