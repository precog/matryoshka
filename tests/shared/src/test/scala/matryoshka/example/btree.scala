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
import matryoshka.implicits._
import scalaz._

sealed trait BTree[A]

object BTree {

  case class BTNil[A]() extends BTree[A]
  case class Leaf[A](value: Int) extends BTree[A]
  case class Node[A](l: A , r: A) extends BTree[A]

  implicit val treeFunctor: Functor[BTree] = new Functor[BTree] {
    def map[A, B](tree: BTree[A])(f: A => B): BTree[B] = tree match {
      case BTNil() => BTNil()
      case Leaf(v) => Leaf(v)
      case Node(l, r) => Node(f(l), f(r))
    }
  }

  def merge(l: List[Int], r: List[Int]): List[Int] =
    (l, r) match {
	  case(l, Nil) => l
	  case(Nil, r) => r
	  case(lh :: ls, rh :: rs) =>
	    if (lh < rh) lh::merge(ls, r)
            else rh :: merge(l, rs)
    }

  //builds a balanced binary tree
  val to: Coalgebra[BTree, List[Int]] = {
    case Nil => BTNil()
    case x :: Nil => Leaf(x)
    case xs => val (l, r) = xs.splitAt(xs.length / 2)
      Node(l, r)
  }

  //sorts a balanced binary tree
  val mergeSort: Algebra[BTree, List[Int]] = {
    case BTNil() => Nil
    case Leaf(v) => v :: Nil
    case Node(l, r) => merge(l, r)
  }
}

class BTreeSpec extends Specification {
  import BTree._

  "should sort a list" >> {
    val list = List(23, 565, 6, 23, 45, 25, 678, 5)
    val sorted = list.hylo(mergeSort, to)
    sorted should ===(list.sorted)
  }
}
