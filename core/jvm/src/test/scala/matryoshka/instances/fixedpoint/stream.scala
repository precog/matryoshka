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

import scala.Int

import org.specs2.ScalaCheck
import org.specs2.mutable._
import org.specs2.scalaz.{ScalazMatchers}
import scalaz._, Scalaz._

class StreamSpec extends Specification with ScalaCheck with ScalazMatchers {
  /** Infinite sequence of Fibonacci numbers (at least until they overflow
    * int32)
    */
  val fib = (1, 0).ana[Nu, (Int, ?)](binarySequence(_ + _))

  /** Generates an infinite stream of the carrier value.
    */
  def constantly[A]: Coalgebra[(A, ?), A] = i => (i, i)

  "fib" should {
    "begin with 1" in {
      fib.head must equal(1)
    }

    "lazily generate the correct sequence" in {
      fib.drop(5).head must equal(8)
    }

    "have a proper prefix" in {
      fib.take(5) must equal(List(1, 1, 2, 3, 5))
    }

    "get a subsequence" in {
      fib.drop(10).take(5) must equal(List(89, 144, 233, 377, 610))
    }
  }

  "constantly" should {
    "begin with the given value" >> prop { (i: Int) =>
      i.ana[Nu, (Int, ?)](constantly).head must_== i
    }

    // FIXME: These two blow up the stack with much larger inputs

    "have the given value at an arbitrary point" >> prop { (i: Int, d: Int) =>
      i.ana[Nu, (Int, ?)](constantly).drop(30000).head must equal(i)
    }

    "have subsequence of the given value" >> prop { (i: Int, t: Int) =>
      i.ana[Nu, (Int, ?)](constantly).take(450) must
        equal(List.fill(450)(i))
    }
  }
}
