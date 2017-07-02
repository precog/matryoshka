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

package matryoshka.example

import org.specs2.mutable._

import slamdata.Predef._
import matryoshka._
import matryoshka.data._
import matryoshka.implicits._
import scalaz._

sealed trait IntList[A]

object IntList {

  case class IntCons[A](h: Int, tail: A) extends IntList[A]
  case class IntNil[A]() extends IntList[A]

  implicit val intListFunctor: Functor[IntList] = new Functor[IntList] {
    def map[A, B](list: IntList[A])(f: A => B): IntList[B] = list match {
      case IntNil() => IntNil()
      case IntCons(h, t) => IntCons(h, f(t))
    }
  }

  val to: Coalgebra[IntList, List[Int]] = {
    case Nil => IntNil()
    case h :: t => IntCons(h, t)
  }

  val from: Algebra[IntList, List[Int]] = {
    case IntNil() => Nil
    case IntCons(h, t) => h :: t
  }

  val sum: Algebra[IntList, Int] = {
    case IntNil() => 0
    case IntCons(h, t) => h + t
  }

  val len: Algebra[IntList, Int] = {
    case IntNil() => 0
    case IntCons(_, t) => t + 1
  }

  def filter(f: Int => Boolean): Algebra[IntList, List[Int]] = {
    case IntNil() => Nil
    case IntCons(h, t) => if(f(h)) h :: t else t
  }

  def lessThan(i: Int): IntList ~> IntList = new (IntList ~> IntList) {
    def apply[A](l: IntList[A]): IntList[A] = l match {
      case IntNil() =>
        println(s"intnil")
        IntNil()
      case l @ IntCons(h, t) =>
        println(s"head is $h")
        if(h < i) l else IntNil()
    }
  }

  def mapHead(f: Int => Int): GCoalgebra[Fix[IntList] \/ ?, IntList, Fix[IntList]] = {
    case Fix(IntNil()) => IntNil()
    case Fix(IntCons(h, t)) => IntCons(f(h), \/.left(t))
  }

  val infinite: Coalgebra[IntList, Int] = n => IntCons(n, n + 1)
}

class IntListSpec extends Specification {
  import IntList._

  val intList = Fix(IntCons(1, Fix(IntCons(2, Fix(IntNil[Fix[IntList]]())))))

  "construct an IntList" >> {
    List(1, 2).ana[Fix[IntList]](to) should ===(intList)
  }
  "convert an IntList to a List" >> {
    intList.cata(from) should ===(List(1, 2))
  }

  "filter an IntList" >> {
    (0 until 10).toList.hylo(filter(_ < 5), to) should ===((0 until 5).toList)
  }

  "map the head of an IntList" >> {
    intList.apo.apply(mapHead(_ * 3)).cata(from) should ===(List(3, 2))
  }

  // "short circuit the creation of infinite IntList" >> {
  //   1.postpro[Fix[IntList]](IntList.lessThan(10), infinite).cata(len) should ===(9)
  // }

  "short circuit the fold of an IntList" >> {
    intList.prepro(IntList.lessThan(2), sum) should ===(1)
  }
}
