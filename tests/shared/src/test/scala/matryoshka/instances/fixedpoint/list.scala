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
import matryoshka.patterns._

import scala.Int

import org.specs2.ScalaCheck
import org.specs2.mutable._
import org.specs2.scalaz.ScalazMatchers
import scalaz._, Scalaz._

class ListSpec extends Specification with ScalaCheck with ScalazMatchers {
  "apply" should {
    "be equivalent to scala.List.apply" in {
      List(1, 2, 3, 4).cata(ListF.listIso.get) must
        equal(scala.List(1, 2, 3, 4))
    }
  }

  "fill" should {
    "be equivalent to scala.List.fill" >> prop { (v: Int) =>
      100.anaM[Nat](Nat.fromInt) ∘ (List.fill(_)(v).cata(ListF.listIso.get)) must
        equal(scala.List.fill(100)(v).some)
    }
  }

  "length" should {
    "count the number of elements" >> prop { (v: Int) =>
      30.anaM[Nat](Nat.fromInt) ∘ (List.fill(_)(v).length) must
        equal(30.some)
    }
  }

  "headOption" should {
    "return the first element" in {
      List(1, 2, 3, 4).headOption must beSome(1)
    }

    "if there is one" in {
      List().headOption must beNone
    }
  }

  "tailOption" should {
    "return the remainder of the list" in {
      List(1, 2, 3, 4).tailOption must equal(List(2, 3, 4).some)
      List(1).tailOption must equal(List[Int]().some)
    }

    "if there is one" in {
      List().tailOption must beNone
    }
  }
}
