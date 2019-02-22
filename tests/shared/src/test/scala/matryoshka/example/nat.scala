/*
 * Copyright 2014–2018 SlamData Inc.
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

package matryoshka.example

import org.specs2.mutable._

import slamdata.Predef._
import matryoshka._
import matryoshka.data._
import matryoshka.implicits._
import scalaz.Functor

sealed trait Nat[A]

object Nat {

  case class Zero[A]() extends Nat[A]
  case class Succ[A](n: A) extends Nat[A]

  implicit val natFunctor: Functor[Nat] = new Functor[Nat] {
    def map[A, B](nat: Nat[A])(f: A => B): Nat[B] = nat match {
      case Zero() => Zero()
      case Succ(a) => Succ(f(a))
    }
  }

  val toNat: Coalgebra[Nat, Int] = {
    case 0 => Zero()
    case n => Succ(n - 1)  
  }

  val toInt: Algebra[Nat, Int] = {
    case Zero() => 0
    case Succ(n) => n + 1
  }

  val factorial: GAlgebra[(Int, ?), Nat, Int] = {
    case Zero() => 1
    case Succ((i, n)) => (i + 1) * n
  }
}

class NatSpec extends Specification {
  import Nat._

  val nat2 = Fix(Succ(Fix(Succ(Fix(Zero[Fix[Nat]]())))))

  "create a Nat from an Int" >> {
    2.ana[Fix[Nat]](toNat) should ===(nat2)
  }
  "create an Int from a Nat" >> {
    nat2.cata(toInt) should ===(2)
  }
  "convert an Int to a Nat and back" >> {
    2.hylo(toInt, toNat) should ===(2)
  }
  "calculate the factorial of a Nat" >> {
    val nat4 = 4.ana[Fix[Nat]](toNat)
    nat4.zygo(toInt, factorial) should ===((24))
  }
}
