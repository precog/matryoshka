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

package matryoshka

import Recursive.ops._, FunctorT.ops._, Fix._
import matryoshka.algebra._
import matryoshka.exp._
import matryoshka.scalacheck.arbitrary._

import java.lang.String
import scala.{Boolean, Function, Int, None, Option, Predef, Symbol, Unit},
  Predef.{implicitly, wrapString}
import scala.collection.immutable.{List, Map, Nil, ::}

import org.scalacheck._
import org.specs2.ScalaCheck
import org.specs2.mutable._
import org.specs2.scalaz._
import scalaz._, Scalaz._

class MatryoshkaSpecs extends Specification with ScalaCheck with ScalazMatchers {
  "Fix" should {
    "isLeaf" should {
      "be true for simple literal" in {
        num(1).isLeaf must beTrue
        num(1).convertTo[Mu].isLeaf must beTrue
      }

      "be false for expression" in {
        mul(num(1), num(2)).isLeaf must beFalse
        mul(num(1), num(2)).convertTo[Mu].isLeaf must beFalse
      }
    }

    "children" should {
      "be empty for simple literal" in {
        num(1).children must equal(Nil)
        num(1).convertTo[Mu].children must equal(Nil)
      }

      "contain sub-expressions" in {
        mul(num(1), num(2)).children must equal(List(num(1), num(2)))
        mul(num(1), num(2)).convertTo[Mu].children must
          equal(List(num(1), num(2)).map(_.convertTo[Mu]))
      }
    }

    "universe" should {
      "be one for simple literal" in {
        num(1).universe must equal(List(num(1)))
        num(1).convertTo[Mu].universe must
          equal(List(num(1)).map(_.convertTo[Mu]))
      }

      "contain root and sub-expressions" in {
        mul(num(1), num(2)).universe must
          equal(List(mul(num(1), num(2)), num(1), num(2)))
        mul(num(1), num(2)).convertTo[Mu].universe must
          equal(List(mul(num(1), num(2)), num(1), num(2)).map(_.convertTo[Mu]))
      }
    }

    "transCata" should {
      "change simple literal" in {
        num(1).transCata(addOneƒ) must equal(num(2))
        num(1).convertTo[Mu].transCata(addOneƒ) must equal(num(2).convertTo[Mu])
      }

      "change sub-expressions" in {
        mul(num(1), num(2)).transCata(addOneƒ) must equal(mul(num(2), num(3)))
        mul(num(1), num(2)).convertTo[Mu].transCata(addOneƒ) must
          equal(mul(num(2), num(3)).convertTo[Mu])
      }

      "be bottom-up" in {
        mul(num(0), num(1)).transCata(addOneOrSimplifyƒ) must equal(num(2))
        mul(num(1), num(2)).transCata(addOneOrSimplifyƒ) must equal(mul(num(2), num(3)))
      }
    }

    "transAna" should {
      "change simple literal" in {
        num(1).transAna(addOneƒ) must equal(num(2))
      }

      "change sub-expressions" in {
        mul(num(1), num(2)).transAna(addOneƒ) must equal(mul(num(2), num(3)))
      }

      "be top-down" in {
        mul(num(0), num(1)).transAna(addOneOrSimplifyƒ) must equal(num(0))
        mul(num(1), num(2)).transAna(addOneOrSimplifyƒ) must equal(num(2))
      }
    }

    "foldMap" should {
      "fold stuff" in {
        mul(num(0), num(1)).foldMap(_ :: Nil) must equal(mul(num(0), num(1)) :: num(0) :: num(1) :: Nil)
      }
    }

    "cata" should {
      "evaluate simple expr" in {
        val v = mul(num(1), mul(num(2), num(3)))
        v.cata(eval) must equal(6)
        v.convertTo[Mu].cata(eval) must equal(6)
      }

      "find all constants" in {
        mul(num(0), num(1)).cata(findConstants) must equal(List(0, 1))
        mul(num(0), num(1)).convertTo[Mu].cata(findConstants) must equal(List(0, 1))
      }

      "produce correct annotations for 5 * 2" in {
        mul(num(5), num(2)).cata(example1ƒ) must beSome(10)
        mul(num(5), num(2)).convertTo[Mu].cata(example1ƒ) must beSome(10)
      }
    }

    "zipAlgebras" should {
      "both eval and find all constants" in {
        mul(num(5), num(2)).cata(AlgebraZip[Exp].zip(eval, findConstants)) must
          equal((10, List(5, 2)))
        mul(num(5), num(2)).convertTo[Mu].cata(AlgebraZip[Exp].zip(eval, findConstants)) must
          equal((10, List(5, 2)))
      }
    }

    "generalize" should {
      "behave like cata" in {
        val v = mul(num(1), mul(num(2), num(3)))
        v.para(eval.generalize[(Fix[Exp], ?)]) must equal(v.cata(eval))
        v.convertTo[Mu].para(eval.generalize[(Mu[Exp], ?)]) must equal(v.cata(eval))
      }
    }

    "generalizeElgot" should {
      "behave like cata on an algebra" ! prop { (i: Int) =>
        val x = i.ana(extractFactors).cata(eval)
        i.coelgot(eval.generalizeElgot[(Int, ?)], extractFactors) must equal(x)
      }

      "behave like ana on an coalgebra" ! prop { (i: Int) =>
        val x = i.ana(extractFactors).cata(eval)
        i.elgot(eval, extractFactors.generalizeElgot[Int \/ ?]) must equal(x)
      }
    }

    "generalizeCoalgebra" should {
      "behave like ana" ! prop { (i: Int) =>
        i.apo(extractFactors.generalize[Fix[Exp] \/ ?]) must
          equal(i.ana(extractFactors))
      }
    }

    "topDownCata" should {
      "bind vars" in {
        val v = let('x, num(1), mul(num(0), vari('x)))
        v.topDownCata(Map.empty[Symbol, Fix[Exp]])(subst) must
          equal(mul(num(0), num(1)))
        v.convertTo[Mu].topDownCata(Map.empty[Symbol, Mu[Exp]])(subst) must
          equal(mul(num(0), num(1)).convertTo[Mu])
      }
    }

    "trans" should {
      // TODO
    }

    "attributePara" should {
      "provide a catamorphism" in {
        val v = mul(num(4), mul(num(2), num(3)))
        v.cata(attributePara(peval[Fix])) must
          equal(
            Cofree[Exp, Int](24, Mul(
              Cofree(4, Num(4)),
              Cofree(6, Mul(
                Cofree(2, Num(2)),
                Cofree(3, Num(3)))))))
      }
    }

    "para" should {
      "evaluate simple expr" in {
        val v = mul(num(1), mul(num(2), num(3)))
        v.para(peval[Fix]) must equal(6)
        v.convertTo[Mu].para(peval[Mu]) must equal(6)
      }

      "evaluate special-case" in {
        val v = mul(num(0), num(0))
        v.para(peval[Fix]) must equal(-1)
        v.convertTo[Mu].para(peval[Mu]) must equal(-1)
      }

      "evaluate equiv" in {
        val v = mul(num(0), mul(num(0), num(1)))
        v.para(peval[Fix]) must equal(0)
        v.convertTo[Mu].para(peval[Mu]) must equal(0)
      }
    }

    "gpara" should {
      "behave like para" in {
        val v = mul(num(0), mul(num(0), num(1)))
        v.gpara[Id, Int](
          new (λ[α => Exp[Id[α]]] ~> λ[α => Id[Exp[α]]]) {
            def apply[A](ex: Exp[Id[A]]): Id[Exp[A]] =
              ex.map(_.copoint).point[Id]
          },
          expr => { peval(expr.map(_.runEnvT)) }) must equal(0)
      }
    }

    "apomorphism" should {
      "pull out factors of two" in {
        def fM(x: Int): Option[Exp[Fix[Exp] \/ Int]] =
          if (x == 5) None else f(x).some
        def f(x: Int): Exp[Fix[Exp] \/ Int] =
          if (x % 2 == 0) Mul(-\/(num(2)), \/-(x.toInt / 2))
          else Num(x)
        "apoM" in {
          "should be some" in {
            12.apoM(fM) must beSome(mul(num(2), mul(num(2), num(3))))
          }
          "should be none" in {
            10.apoM(fM) must beNone
          }
        }
        "apo should be an optimization over apoM and be semantically equivalent" ! prop { i: Int =>
          if (i == 0) ok
          else i.apoM[Fix, Exp, Id](f) must equal(i.apo(f))
        }
      }
      "construct factorial" in {
        def fact(x: Int): Exp[Fix[Exp] \/ Int] =
          if (x > 1) Mul(-\/(num(x)), \/-(x-1))
          else Num(x)

        4.apo(fact) must equal(mul(num(4), mul(num(3), mul(num(2), num(1)))))
      }
    }

    "anamorphism" should {
      "pull out factors of two" in {
        "anaM" should {
          def extractFactorsM(x: Int): Option[Exp[Int]] =
            if (x == 5) None else extractFactors(x).some
          "pull out factors of two" in {
            12.anaM(extractFactorsM) must beSome(
              mul(num(2), mul(num(2), num(3)))
            )
          }
          "fail if 5 is present" in {
            10.anaM(extractFactorsM) must beNone
          }
        }
        "ana should be an optimization over anaM and be semantically equivalent" ! prop { i: Int =>
          i.anaM[Fix, Exp,Id](extractFactors) must equal(i.ana(extractFactors))
        }
      }
    }

    "hylo" should {
      "factor and then evaluate" ! prop { (i: Int) =>
        i.hylo(eval, extractFactors) must equal(i)
      }
    }

    def strings(t: Exp[(Int, String)]): String = t match {
      case Num(x) => x.toString
      case Mul((x, xs), (y, ys)) =>
        xs + " (" + x + ")" + ", " + ys + " (" + y + ")"
      case _ => Predef.???
    }

    "zygo" should {
      "eval and strings" in {
        mul(mul(num(0), num(0)), mul(num(2), num(5))).zygo(eval, strings) must
          equal("0 (0), 0 (0) (0), 2 (2), 5 (5) (10)")
      }
    }

    "paraZygo" should {
      "peval and strings" in {
        mul(mul(num(0), num(0)), mul(num(2), num(5))).paraZygo(peval[Fix], strings) must
          equal("0 (0), 0 (0) (-1), 2 (2), 5 (5) (10)")
      }

    }

    sealed trait Nat[A]
    final case class Z[A]()        extends Nat[A]
    final case class S[A](prev: A) extends Nat[A]
    object Nat {
      implicit val NatTraverse: Traverse[Nat] = new Traverse[Nat] {
        def traverseImpl[G[_], A, B](fa: Nat[A])(f: A => G[B])(implicit G: Applicative[G]):
            G[Nat[B]] =
          fa match {
            case Z()  => G.point(Z())
            case S(a) => f(a).map(S(_))
          }
      }
    }

    "mutu" should {
      val toNat: Int => Fix[Nat] = _.ana({
        case 0 => Z()
        case n => S(n - 1)
      })

      case class Even(even: Boolean)
      case class Odd(odd: Boolean)

      val isOdd: Nat[(Even, Odd)] => Odd = {
        case Z()             => Odd(false)
        case S((Even(b), _)) => Odd(b)
      }
      val isEven: Nat[(Odd, Even)] => Even = {
        case Z()            => Even(true)
        case S((Odd(b), _)) => Even(b)
      }

      "determine even" in {
        toNat(8).mutu(isOdd, isEven) must_== Even(true)
      }

      "determine odd" in {
        toNat(5).mutu(isEven, isOdd) must_== Odd(true)
      }

      "determine not even" in {
        toNat(7).mutu(isOdd, isEven) must_== Even(false)
      }
    }

    "histo" should {
      "eval simple literal multiplication" in {
        mul(num(5), num(10)).histo(partialEval[Fix]) must equal(num(50))
        mul(num(5), num(10)).histo(partialEval[Mu]) must equal(num(50).convertTo[Mu])
      }

      "partially evaluate mul in lambda" in {
        lam('foo, mul(mul(num(4), num(7)), vari('foo))).histo(partialEval[Fix]) must
          equal(lam('foo, mul(num(28), vari('foo))))
        lam('foo, mul(mul(num(4), num(7)), vari('foo))).histo(partialEval[Mu]) must
          equal(lam('foo, mul(num(28), vari('foo))).convertTo[Mu])
      }
    }

    "futu" should {
      "factor multiples of two" in {
        8.futu(extract2and3) must equal(mul(num(2), mul(num(2), num(2))))
      }

      "factor multiples of three" in {
        81.futu(extract2and3) must equal(mul(num(3), num(27)))
      }

      "factor 3 within 2" in {
        324.futu(extract2and3) must
          equal(mul(num(2), mul(num(2), mul(num(3), num(27)))))
      }
    }

    "chrono" should {
      "factor and partially eval" ! prop { (i: Int) =>
        i.chrono(partialEval[Fix], extract2and3) must equal(num(i))
        i.chrono(partialEval[Mu], extract2and3) must equal(num(i).convertTo[Mu])
      }
    }
  }

  "Attr" should {
    "attrSelf" should {
      "annotate all" ! Prop.forAll(expGen) { exp =>
        Recursive[Cofree[?[_], Fix[Exp]]].universe(exp.cata(attrSelf)) must
          equal(exp.universe.map(_.cata(attrSelf)))
      }
    }

    "convert" should {
      "forget unit" ! Prop.forAll(expGen) { exp =>
        Recursive[Cofree[?[_], Unit]].convertTo(exp.cata(attrK(()))) must
          equal(exp)
      }
    }

    "foldMap" should {
      "zeros" ! Prop.forAll(expGen) { exp =>
        Foldable[Cofree[Exp, ?]].foldMap(exp.cata(attrK(0)))(_ :: Nil) must
          equal(exp.universe.map(Function.const(0)))
      }

      "selves" ! Prop.forAll(expGen) { exp =>
        Foldable[Cofree[Exp, ?]].foldMap(exp.cata(attrSelf))(_ :: Nil) must
          equal(exp.universe)
      }
    }
  }

  def expGen = Gen.resize(100, corecursiveArbitrary(Exp.arbExp).arbitrary)
}
