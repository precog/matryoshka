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

package matryoshka.exp

import matryoshka._
import matryoshka.implicits._

import java.lang.String
import scala.{Int, Symbol}

import org.scalacheck._
import scalaz._, Scalaz._
import scalaz.scalacheck.ScalaCheckBinding._

sealed trait Exp[A]
case class Num[A](value: Int) extends Exp[A]
case class Mul[A](left: A, right: A) extends Exp[A]
case class Var[A](value: Symbol) extends Exp[A]
case class Lambda[A](param: Symbol, body: A) extends Exp[A]
case class Apply[A](func: A, arg: A) extends Exp[A]
case class Let[A](name: Symbol, value: A, inBody: A) extends Exp[A]

object Exp {
  implicit val arbSymbol = Arbitrary(Arbitrary.arbitrary[String].map(Symbol(_)))

  implicit val arbitrary: Delay[Arbitrary, Exp] = new Delay[Arbitrary, Exp] {
    def apply[α](arb: Arbitrary[α]): Arbitrary[Exp[α]] =
      Arbitrary(Gen.oneOf(
        Arbitrary.arbitrary[Int].map(Num[α](_)),
        (arb.arbitrary ⊛ arb.arbitrary)(Mul(_, _)),
        Arbitrary.arbitrary[Symbol].map(Var[α](_)),
        (Arbitrary.arbitrary[Symbol] ⊛ arb.arbitrary)(Lambda(_, _)),
        (arb.arbitrary ⊛ arb.arbitrary)(Apply(_, _)),
        (Arbitrary.arbitrary[Symbol] ⊛ arb.arbitrary ⊛ arb.arbitrary)(
          Let(_, _, _))))
  }

  implicit val traverse: Traverse[Exp] = new Traverse[Exp] {
    def traverseImpl[G[_], A, B](fa: Exp[A])(f: A => G[B])(implicit G: Applicative[G]): G[Exp[B]] = fa match {
      case Num(v)           => G.point(Num(v))
      case Mul(left, right) => G.apply2(f(left), f(right))(Mul(_, _))
      case Var(v)           => G.point(Var(v))
      case Lambda(p, b)     => G.map(f(b))(Lambda(p, _))
      case Apply(func, arg) => G.apply2(f(func), f(arg))(Apply(_, _))
      case Let(n, v, i)     => G.apply2(f(v), f(i))(Let(n, _, _))
    }
  }

  // NB: an unusual definition of equality, in that only the first 3 characters
  //     of variable names are significant. This is to distinguish it from `==`
  //     as well as from a derivable Equal.
  implicit val equal: Delay[Equal, Exp] = new Delay[Equal, Exp] {
    def apply[α](eq: Equal[α]) =
      Equal.equal[Exp[α]] {
        case (Num(v1), Num(v2))                 => v1 ≟ v2
        case (Mul(a1, b1), Mul(a2, b2))         =>
          eq.equal(a1, a2) && eq.equal(b1, b2)
        case (Var(s1), Var(s2))                 =>
          s1.name.substring(0, 3 min s1.name.length) ==
          s2.name.substring(0, 3 min s2.name.length)
        case (Lambda(p1, a1), Lambda(p2, a2))   =>
          p1 == p2 && eq.equal(a1, a2)
        case (Apply(f1, a1), Apply(f2, a2))     =>
          eq.equal(f1, f2) && eq.equal(a1, a2)
        case (Let(n1, v1, i1), Let(n2, v2, i2)) =>
          n1 == n2 && eq.equal(v1, v2) && eq.equal(i1, i2)
        case (_, _)                             => false
      }
  }

  // NB: Something like this currently needs to be defined for any Functor in
  //     order to get the generalize operations for the algebra.
  implicit def toExpAlgebraOps[A](a: Algebra[Exp, A]): AlgebraOps[Exp, A] =
    toAlgebraOps[Exp, A](a)

  implicit val show: Delay[Show, Exp] = new Delay[Show, Exp] {
    def apply[α](show: Show[α]) =
      Show.show {
        case Num(v)       => v.shows
        case Mul(a, b)    =>
          "Mul(" + show.shows(a) + ", " + show.shows(b) + ")"
        case Var(s)       => "$" + s.name
        case Lambda(p, a) => "Lambda(" + p.name + ", " + show.shows(a) + ")"
        case Apply(f, a)  =>
          "Apply(" + show.shows(f) + ", " + show.shows(a) + ")"
        case Let(n, v, i) =>
          "Let(" + n.name + ", " + show.shows(v) + ", " + show.shows(i) + ")"
      }
  }

  implicit val unzip = new Unzip[Exp] {
    def unzip[A, B](f: Exp[(A, B)]) = (f.map(_._1), f.map(_._2))
  }
}
