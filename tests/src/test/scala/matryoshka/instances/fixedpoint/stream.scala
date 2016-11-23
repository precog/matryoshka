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
import matryoshka.implicits._

import scala.Int

import org.specs2.ScalaCheck
import org.specs2.mutable._
import org.specs2.scalaz.{ScalazMatchers}
import scalaz._, Scalaz._

class StreamSpec extends Specification with ScalaCheck with ScalazMatchers {
  /** Infinite sequence of Fibonacci numbers (at least until they overflow
    * int32)
    */
  val fib = (1, 0).ana[Stream[Int]](binarySequence(_ + _))

  /** Generates an infinite stream of the carrier value.
    */
  def constantly[A]: Coalgebra[(A, ?), A] = i => (i, i)

  "fib" should {
    "begin with 1" in {
      fib.head must equal(1)
    }

    "lazily generate the correct sequence" in {
      5.anaM[Nat](Nat.fromInt) ∘ (fib.drop(_).head) must equal(8.some)
    }

    "have a proper prefix" in {
      5.anaM[Nat](Nat.fromInt) ∘ fib.take[Nat, List[Int]] must
        equal(List(1, 1, 2, 3, 5).some)
    }

    "get a subsequence" in {
      (10.anaM[Nat](Nat.fromInt) ⊛ 5.anaM[Nat](Nat.fromInt))((d, t) =>
        fib.drop(d).take[Nat, List[Int]](t)) must
        equal(List(89, 144, 233, 377, 610).some)
    }
  }

  "constantly" should {
    "begin with the given value" >> prop { (i: Int) =>
      i.ana[Stream[Int]](constantly).head must_== i
    }

    // FIXME: These two blow up the stack with much larger inputs

    "have the given value at an arbitrary point" >> prop { (i: Int, d: Int) =>
      450.anaM[Nat](Nat.fromInt) ∘
        (i.ana[Stream[Int]](constantly).drop(_).head) must
        equal(i.some)
    }

    "have subsequence of the given value" >> prop { (i: Int, t: Int) =>
      450.anaM[Nat](Nat.fromInt) ∘
        (i.ana[Stream[Int]](constantly).take[Nat, List[Int]](_)) must
        equal(450.anaM[Nat](Nat.fromInt) ∘ (List.fill(_)(i)))
    }
  }
}
