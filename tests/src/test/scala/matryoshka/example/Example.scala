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

package matryoshka.example

import matryoshka._
import matryoshka.helpers._
import matryoshka.implicits._
import matryoshka.patterns._
import matryoshka.scalacheck.arbitrary._
import matryoshka.specs2.scalacheck.CheckAll

import java.lang.String
import scala.{Int, List, None, Option}

import org.scalacheck._
import org.specs2.ScalaCheck
import org.specs2.mutable._
import scalaz._, Scalaz._
import scalaz.scalacheck.ScalazProperties._

sealed abstract class Example[A]
final case class Empty[A]()                                   extends Example[A]
final case class NonRec[A](a: String, b: Int)                 extends Example[A]
final case class SemiRec[A](a: Int, b: A)                     extends Example[A]
final case class MultiRec[A](a: A, b: A)                      extends Example[A]
final case class OneList[A](l: List[A])                       extends Example[A]
final case class TwoLists[A](first: List[A], second: List[A]) extends Example[A]

object Example {
  implicit val traverse: Traverse[Example] = new Traverse[Example] {
    def traverseImpl[G[_], A, B](fa: Example[A])(f: A => G[B])(
      implicit G: Applicative[G]):
        G[Example[B]] = fa match {
      case Empty()        => G.point(Empty())
      case NonRec(a, b)   => G.point(NonRec(a, b))
      case SemiRec(a, b)  => f(b).map(SemiRec(a, _))
      case MultiRec(a, b) => (f(a) ⊛ f(b))(MultiRec(_, _))
      case OneList(a)     => a.traverse(f) ∘ (OneList(_))
      case TwoLists(a, b) => (a.traverse(f) ⊛ b.traverse(f))(TwoLists(_, _))
    }
  }

  implicit val equal: Delay[Equal, Example] = new Delay[Equal, Example] {
    def apply[α](eq: Equal[α]) = Equal.equal((a, b) => {
      implicit val ieq = eq
        (a, b) match {
        case (Empty(),          Empty())          => true
        case (NonRec(s1, i1),   NonRec(s2, i2))   => s1 ≟ s2 && i1 ≟ i2
        case (SemiRec(i1, a1),  SemiRec(i2, a2))  =>
          i1 ≟ i2 && eq.equal(a1, a2)
        case (MultiRec(a1, b1), MultiRec(a2, b2)) =>
          eq.equal(a1, a2) && eq.equal(b1, b2)
        case (OneList(l),       OneList(r))       => l ≟ r
        case (TwoLists(l1, l2), TwoLists(r1, r2)) => l1 ≟ r1 && l2 ≟ r2
        case (_,                _)                => false
      }
    })
  }

  implicit val show: Delay[Show, Example] = new Delay[Show, Example] {
    def apply[α](s: Show[α]) = {
      implicit val is = s
      Show.show {
        case Empty()          => Cord("Empty()")
        case NonRec(s2, i2)   =>
          Cord("NonRec(" + s2.shows + ", " + i2.shows + ")")
        case SemiRec(i2, a2)   =>
          Cord("SemiRec(") ++ i2.show ++ Cord(", ") ++ s.show(a2) ++ Cord(")")
        case MultiRec(a2, b2) =>
          Cord("MultiRec(") ++ s.show(a2) ++ Cord(", ") ++ s.show(b2) ++ Cord(")")
        case OneList(r)       => Cord("OneList(") ++ r.show ++ Cord(")")
        case TwoLists(r1, r2) =>
          Cord("TwoLists(") ++ r1.show ++ Cord(", ") ++ r2.show ++ Cord(")")
      }
    }
  }

  implicit val diffable: Diffable[Example] = new Diffable[Example] {
    def diffImpl[T[_[_]]: RecursiveT: CorecursiveT](l: T[Example], r: T[Example]):
        Option[DiffT[T, Example]] =
      (l.projectT, r.projectT) match {
        case (l @ Empty(),        r @ Empty())        => localDiff(l, r).some
        case (l @ NonRec(_, _),   r @ NonRec(_, _))   => localDiff(l, r).some
        case (l @ SemiRec(_, _),  r @ SemiRec(_, _))  => localDiff(l, r).some
        case (l @ MultiRec(_, _), r @ MultiRec(_, _)) => localDiff(l, r).some
        case (OneList(l),         OneList(r))         =>
          Similar[T, Example, T[Diff[T, Example, ?]]](OneList[DiffT[T, Example]](diffTraverse(l, r))).embedT.some
        case (TwoLists(l1, l2),   TwoLists(r1, r2))   =>
          Similar[T, Example, T[Diff[T, Example, ?]]](TwoLists[DiffT[T, Example]](diffTraverse(l1, r1), diffTraverse(l2, r2))).embedT.some
        case (_,                  _)                  => None
      }
  }

  implicit val arbitrary: Delay[Arbitrary, Example] =
    new Delay[Arbitrary, Example] {
      def apply[α](arb: Arbitrary[α]) =
        Arbitrary(Gen.sized(size =>
          Gen.oneOf(
            Empty[α]().point[Gen],
            (Arbitrary.arbitrary[String] ⊛ Arbitrary.arbitrary[Int])(
              NonRec[α](_, _)),
            (Arbitrary.arbitrary[Int] ⊛ arb.arbitrary)(SemiRec(_, _)),
            (arb.arbitrary ⊛ arb.arbitrary)(MultiRec(_, _)),
            Gen.listOfN(size, arb.arbitrary).map(OneList(_)),
            (Gen.listOfN(size / 2, arb.arbitrary) ⊛ Gen.listOfN(size / 2, arb.arbitrary))(
              TwoLists(_, _)))
        ))
    }
}

class ExampleSpec extends Specification with ScalaCheck with CheckAll {
  "Example" should {
    "satisfy relevant laws" in {
      checkAll(equal.laws[Example[Int]])
      checkAll(traverse.laws[Example])
    }
  }
}
