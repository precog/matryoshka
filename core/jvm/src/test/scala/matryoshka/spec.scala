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

import Recursive.ops._, FunctorT.ops._
import matryoshka.exp._
import matryoshka.exp2._
import matryoshka.helpers._
import matryoshka.runners._
import matryoshka.specs2.scalacheck.CheckAll

import java.lang.String
import scala.{Boolean, Function, Int, None, Option, Predef, Symbol, Unit}
import scala.collection.immutable.{List, Map, Nil, ::}

import monocle.law.discipline._
import org.scalacheck._
import org.specs2.ScalaCheck
import org.specs2.mutable._
import org.specs2.scalaz.{ScalazMatchers}
import org.typelevel.discipline.specs2.mutable._
import scalaz.{Apply => _, _}, Scalaz._
import scalaz.scalacheck.ScalazProperties._

class ExpSpec extends Specification with CheckAll {
  // NB: These are just a sanity check that the data structure created for the
  //     tests is lawful.
  "Exp should satisfy relevant laws" >> {
    checkAll(equal.laws[Exp[Int]])
    checkAll(traverse.laws[Exp])
  }
}

class Exp2Spec extends Specification with CheckAll {
  // NB: These are just a sanity check that the data structure created for the
  //     tests is lawful.
  "Exp2 should satisfy relevant laws" >> {
    checkAll(equal.laws[Exp2[Int]])
    checkAll(functor.laws[Exp2])
  }
}

class MatryoshkaSpecs extends Specification with ScalaCheck with ScalazMatchers with Discipline {
  val example1ƒ: Exp[Option[Int]] => Option[Int] = {
    case Num(v)           => v.some
    case Mul(left, right) => (left ⊛ right)(_ * _)
    case Var(v)           => None
    case Lambda(_, b)     => b
    case Apply(func, arg) => None
    case Let(_, _, i)     => i
  }

  def addOneOptƒ[T[_[_]]]: Exp[T[Exp]] => Option[Exp[T[Exp]]] = {
    case Num(n) => Num(n+1).some
    case _      => None
  }

  def addOneƒ[T[_[_]]]: Exp[T[Exp]] => Exp[T[Exp]] =
    orOriginal(addOneOptƒ)

  def addOneOptExpExp2ƒ[T[_[_]]]: Exp[T[Exp2]] => Option[Exp2[T[Exp2]]] = {
    case Num(n) => Num2(n+1).some
    case _      => None
  }

  def addOneExpExp2ƒ[T[_[_]]]: Exp[T[Exp2]] => Exp2[T[Exp2]] =
    orDefault[Exp[T[Exp2]], Exp2[T[Exp2]]](exp2.Const())(addOneOptExpExp2ƒ)

  def addOneOptExp2Expƒ[T[_[_]]]: Exp2[T[Exp2]] => Option[Exp[T[Exp2]]] = {
    case Num2(n) => Num(n+1).some
    case _       => None
  }

  def addOneExp2Expƒ[T[_[_]]]: Exp2[T[Exp2]] => Exp[T[Exp2]] =
    orDefault[Exp2[T[Exp2]], Exp[T[Exp2]]](Num(0))(addOneOptExp2Expƒ)

  def simplifyƒ[T[_[_]]: Recursive]: Exp[T[Exp]] => Option[Exp[T[Exp]]] = {
    case Mul(a, b) => (a.project, b.project) match {
      case (Num(0), Num(_)) => Num(0).some
      case (Num(1), Num(n)) => Num(n).some
      case (Num(_), Num(0)) => Num(0).some
      case (Num(n), Num(1)) => Num(n).some
      case (_,      _)      => None
    }
    case _         => None
  }

  def addOneOrSimplifyƒ[T[_[_]]: Recursive]: Exp[T[Exp]] => Exp[T[Exp]] = {
    case t @ Num(_)    => addOneƒ(t)
    case t @ Mul(_, _) => repeatedly(simplifyƒ[T]).apply(t)
    case t             => t
  }

  def extractLambdaƒ[T[_[_]]: Recursive]: Exp[(T[Exp], T[Exp2])] => Exp2[T[Exp2]] = {
    case Lambda(_, (exp, exp2)) => exp.project match {
      case Num(a) => Num2(a)
      case _      => Single(exp2)
    }
    case _                      => exp2.Const[T[Exp2]]
  }

  val MinusThree: Exp ~> Exp =
    new (Exp ~> Exp) {
      def apply[A](exp: Exp[A]): Exp[A] = exp match {
        case Num(x) => Num(x-3)
        case t      => t
      }
    }

  // NB: This is better done with cata, but we fake it here
  def partialEval[T[_[_]]: Corecursive: Recursive](t: Exp[Cofree[Exp, T[Exp]]]):
      T[Exp] =
    t match {
      case Mul(x, y) => (x.head.project, y.head.project) match {
        case (Num(a), Num(b)) => Num[T[Exp]](a * b).embed
        case _                => t.map(_.head).embed
      }
      case _ => t.map(_.head).embed
    }

  checkAlgebraIsoLaws(recCorecIso[Mu, Exp])
  checkAlgebraIsoLaws(lambekIso[Mu, Exp])

  // TODO: add checks for the prism versions
  checkAll("fold Iso", IsoTests(foldIso[Mu, Exp, Mu[Exp]](lambekIso)))

  "Recursive" >> {
    "isLeaf" >> {
      "be true for simple literal" in {
        num(1).isLeaf must beTrue
        num(1).convertTo[Mu].isLeaf must beTrue
        num(1).convertTo[Nu].isLeaf must beTrue
      }

      "be false for expression" in {
        mul(num(1), num(2)).isLeaf must beFalse
        mul(num(1), num(2)).convertTo[Mu].isLeaf must beFalse
        mul(num(1), num(2)).convertTo[Nu].isLeaf must beFalse
      }
    }

    "children" >> {
      "be empty for simple literal" in {
        num(1).children must be empty;
        num(1).convertTo[Mu].children must be empty;
        num(1).convertTo[Nu].children must be empty
      }

      "contain sub-expressions" in {
        mul(num(1), num(2)).children must equal(List(num(1), num(2)))
        mul(num(1), num(2)).convertTo[Mu].children must
          equal(List(num(1), num(2)).map(_.convertTo[Mu]))
        mul(num(1), num(2)).convertTo[Nu].children must
          equal(List(num(1), num(2)).map(_.convertTo[Nu]))
      }
    }

    "universe" >> {
      "be one for simple literal" in {
        num(1).universe must equal(List(num(1)))
        num(1).convertTo[Mu].universe must
          equal(List(num(1)).map(_.convertTo[Mu]))
        num(1).convertTo[Nu].universe must
          equal(List(num(1)).map(_.convertTo[Nu]))
      }

      "contain root and sub-expressions" in {
        mul(num(1), num(2)).universe must
          equal(List(mul(num(1), num(2)), num(1), num(2)))
        mul(num(1), num(2)).convertTo[Mu].universe must
          equal(List(mul(num(1), num(2)), num(1), num(2)).map(_.convertTo[Mu]))
        mul(num(1), num(2)).convertTo[Nu].universe must
          equal(List(mul(num(1), num(2)), num(1), num(2)).map(_.convertTo[Nu]))
      }
    }

    "transCata" >> {
      "change simple literal" in {
        testFunc(
          num(1),
          new FuncRunner[Exp, Exp] {
            def run[T[_[_]]: FunctorT: Corecursive](implicit Eq: Equal[T[Exp]], S: Show[T[Exp]]) =
              _.transCata(addOneƒ) must equal(num(2).convertTo[T])
          })
      }

      "change sub-expressions" in {
        testFunc(
          mul(num(1), num(2)),
          new FuncRunner[Exp, Exp] {
            def run[T[_[_]]: FunctorT: Corecursive](implicit Eq: Equal[T[Exp]], S: Show[T[Exp]]) =
              _.transCata(addOneƒ) must equal(mul(num(2), num(3)).convertTo[T])
          })
      }

      "be bottom-up" in {
        (mul(num(0), num(1)).transCata(addOneOrSimplifyƒ) must equal(num(2))) and
        (mul(num(1), num(2)).transCata(addOneOrSimplifyƒ) must equal(mul(num(2), num(3))))
      }
    }

    "transAna" >> {
      "change simple literal" in {
        testFunc(
          num(1),
          new FuncRunner[Exp, Exp] {
            def run[T[_[_]]: FunctorT: Corecursive](implicit Eq: Equal[T[Exp]], S: Show[T[Exp]]) =
              _.transAna(addOneƒ) must equal(num(2).convertTo[T])
          })
      }

      "change sub-expressions" in {
        testFunc(
          mul(num(1), num(2)),
          new FuncRunner[Exp, Exp] {
            def run[T[_[_]]: FunctorT: Corecursive](implicit Eq: Equal[T[Exp]], S: Show[T[Exp]]) =
              _.transAna(addOneƒ) must equal(mul(num(2), num(3)).convertTo[T])
          })
      }

      "be top-down" in {
        mul(num(0), num(1)).transAna(addOneOrSimplifyƒ) must equal(num(0))
        mul(num(1), num(2)).transAna(addOneOrSimplifyƒ) must equal(num(2))
      }
    }

    "prepro" >> {
      "multiply original with identity ~>" in {
        mul(num(1), mul(num(12), num(8)))
          .prepro(NaturalTransformation.refl[Exp], example1ƒ) must
          equal(96.some)
      }

      "apply ~> repeatedly" in {
        mul(num(1), mul(num(12), num(8))).prepro(MinusThree, example1ƒ) must
          equal(-24.some)
      }
    }

    "gprepro" >> {
      "multiply original with identity ~>" in {
        lam('meh, mul(vari('meh), mul(num(10), num(8))))
          .gprepro[Cofree[Exp, ?], Fix[Exp]](
            distHisto, NaturalTransformation.refl[Exp], partialEval[Fix]) must
          equal(lam('meh, mul(vari('meh), num(80))))
      }

      "apply ~> repeatedly" in {
        lam('meh, mul(vari('meh), mul(num(13), num(8))))
          .gprepro[Cofree[Exp, ?], Fix[Exp]](
            distHisto, MinusThree, partialEval[Fix]) must
          equal(lam('meh, mul(vari('meh), num(-4))))
      }
    }

    "transPrepro" >> {
      "change literal with identity ~>" in {
        testFunc(
          num(1),
          new FuncRunner[Exp, Exp] {
            def run[T[_[_]]: FunctorT: Corecursive](implicit Eq: Equal[T[Exp]], S: Show[T[Exp]]) =
              _.transPrepro(NaturalTransformation.refl[Exp], addOneƒ) must
                equal(num(2).convertTo[T])
          })
      }

      "apply ~> in original space" in {
        testFunc(
          mul(num(1), mul(num(12), num(8))),
          new FuncRunner[Exp, Exp] {
            def run[T[_[_]]: FunctorT: Corecursive](implicit Eq: Equal[T[Exp]], S: Show[T[Exp]]) =
              _.transPrepro(MinusThree, addOneƒ) must
                equal(mul(num(-1), mul(num(7), num(3))).convertTo[T])
          })
      }

      "apply ~> with change of space" in {
        testFunc(
          num(1),
          new FuncRunner[Exp, Exp2] {
            def run[T[_[_]]: FunctorT: Corecursive](implicit Eq: Equal[T[Exp2]], S: Show[T[Exp2]]) =
              _.transPrepro(MinusThree, addOneExpExp2ƒ) must
                equal(num2(2).convertTo[T])
          })
      }
    }

    "transPostpro" >> {
      "change literal with identity ~>" in {
        testFunc(
          num(1),
          new FuncRunner[Exp, Exp] {
            def run[T[_[_]]: FunctorT: Corecursive](implicit Eq: Equal[T[Exp]], S: Show[T[Exp]]) =
              _.transPostpro(NaturalTransformation.refl[Exp], addOneƒ) must
                equal(num(2).convertTo[T])
          })
      }

      "apply ~> in original space" in {
        testFunc(
          mul(num(1), mul(num(12), num(8))),
          new FuncRunner[Exp, Exp] {
            def run[T[_[_]]: FunctorT: Corecursive](implicit Eq: Equal[T[Exp]], S: Show[T[Exp]]) =
              _.transPostpro(MinusThree, addOneƒ) must
                equal(mul(num(-1), mul(num(7), num(3))).convertTo[T])
          })
      }

      "apply ~> with change of space" in {
        testFunc(
          num2(1),
          new FuncRunner[Exp2, Exp] {
            def run[T[_[_]]: FunctorT: Corecursive](implicit Eq: Equal[T[Exp]], S: Show[T[Exp]]) =
              _.transPostpro(MinusThree, addOneExp2Expƒ) must
                equal(num(2).convertTo[T])
          })
      }
    }

    "transPara" >> {
      "project basic exp" in {
        lam('sym, num(3)).transPara(extractLambdaƒ) must equal(num2(3))
      }

      "project basic exp recursively" in {
        lam('sym, mul(num(5), num(7))).transPara(extractLambdaƒ) must
          equal(single(const))
      }
    }

    "foldMap" >> {
      "fold stuff" in {
        mul(num(0), num(1)).foldMap(_ :: Nil) must equal(mul(num(0), num(1)) :: num(0) :: num(1) :: Nil)
      }
    }

    val eval: Algebra[Exp, Int] = {
      case Num(x) => x
      case Mul(x, y) => x*y
      case _ => Predef.???
    }

    val findConstants: Exp[List[Int]] => List[Int] = {
      case Num(x) => x :: Nil
      case t      => t.fold
    }

    "cata" >> {
      "evaluate simple expr" in {
        testRec(
          mul(num(1), mul(num(2), num(3))),
          new RecRunner[Exp, Int] {
            def run[T[_[_]]: Recursive] = _.cata(eval) must equal(6)
          })
      }

      "find all constants" in {
        testRec(
          mul(num(0), num(1)),
          new RecRunner[Exp, List[Int]] {
            def run[T[_[_]]: Recursive] =
              _.cata(findConstants) must equal(List(0, 1))
          })
      }

      "produce correct annotations for 5 * 2" in {
        testRec(
          mul(num(5), num(2)),
          new RecRunner[Exp, Option[Int]] {
            def run[T[_[_]]: Recursive] = _.cata(example1ƒ) must beSome(10)
          })
      }
    }

    "zipAlgebras" >> {
      "both eval and find all constants" in {
        testRec(
          mul(num(5), num(2)),
          new RecRunner[Exp, (Int, List[Int])] {
            def run[T[_[_]]: Recursive] =
              _.cata(AlgebraZip[Exp].zip(eval, findConstants)) must
                equal((10, List(5, 2)))
          })
      }
    }

    "generalize" >> {
      "behave like cata" in {
        testRec(
          mul(num(1), mul(num(2), num(3))),
          new RecRunner[Exp, Int] {
            def run[T[_[_]]: Recursive] = t =>
              t.para(eval.generalize[(T[Exp], ?)]) must equal(t.cata(eval))
          })
      }
    }

    "coelgot" >> {
      "behave like cofCata ⋘ attributeAna" ! prop { (i: Int) =>
        i.coelgot(eval.generalizeElgot[(Int, ?)], extractFactors) must equal(
          i.attributeAna(extractFactors).cofCata(eval.generalizeElgot[(Int, ?)]))
      }
    }

    "elgot" >> {
      "behave like interpCata ⋘ freeAna" ! prop { (i: Int) =>
        i.elgot(eval, extractFactors.generalizeElgot[Int \/ ?]) must equal(
          i.freeAna(extractFactors.generalizeElgot[Int \/ ?]).interpretCata(eval))
      }
    }

    "generalizeElgot" >> {
      "behave like cata on an algebra" ! prop { (i: Int) =>
        val x = i.ana[Fix, Exp](extractFactors).cata(eval)
        i.coelgot(eval.generalizeElgot[(Int, ?)], extractFactors) must equal(x)
      }

      "behave like ana on an coalgebra" ! prop { (i: Int) =>
        val x = i.ana[Fix, Exp](extractFactors).cata(eval)
        i.elgot(eval, extractFactors.generalizeElgot[Int \/ ?]) must equal(x)
      }
    }

    def extractFactors: Coalgebra[Exp, Int] = x =>
      if (x > 2 && x % 2 == 0) Mul(2, x/2)
      else Num(x)

    "generalizeCoalgebra" >> {
      "behave like ana" ! prop { (i: Int) =>
        i.apo(extractFactors.generalize[Fix[Exp] \/ ?]) must
          equal(i.ana[Fix, Exp](extractFactors))
      }
    }

    "topDownCata" >> {
      def subst[T[_[_]]: Recursive](vars: Map[Symbol, T[Exp]], t: T[Exp]):
          (Map[Symbol, T[Exp]], T[Exp]) = t.project match {
        case Let(sym, value, body) => (vars + ((sym, value)), body)
        case Var(sym)              => (vars, vars.get(sym).getOrElse(t))
        case _                     => (vars, t)
      }

      "bind vars" in {
        val v = let('x, num(1), mul(num(0), vari('x)))
        v.topDownCata(Map.empty[Symbol, Fix[Exp]])(subst) must
          equal(mul(num(0), num(1)))
        v.convertTo[Mu].topDownCata(Map.empty[Symbol, Mu[Exp]])(subst) must
          equal(mul(num(0), num(1)).convertTo[Mu])
        v.convertTo[Nu].topDownCata(Map.empty[Symbol, Nu[Exp]])(subst) must
          equal(mul(num(0), num(1)).convertTo[Nu])
      }
    }

    // Evaluate as usual, but trap 0*0 as a special case
    def peval[T[_[_]]: Recursive](t: Exp[(T[Exp], Int)]): Int = t match {
      case Mul((Embed(Num(0)), _), (Embed(Num(0)), _)) => -1
      case Mul((_,             x), (_,             y)) => x * y
      case Num(x)                                      => x
      case _                                           => Predef.???
    }

    "attributePara" >> {
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

    val weightedEval: ElgotAlgebraM[(Int, ?), Option, Exp, Int] = {
      case (weight, Num(x))    => (weight * x).some
      case (weight, Mul(x, y)) => (weight * x * y).some
      case (_,      _)         => None
    }

    "attributeElgotM" >> {
      "fold to Cofree" in {
        Cofree[Exp, Int](1, Mul(
          Cofree(2, Num(1)),
          Cofree(2, Mul(
            Cofree(3, Num(2)),
            Cofree(3, Num(3))))))
          .cofCataM(attributeElgotM[(Int, ?), Option](weightedEval)) must
          equal(
            Cofree[Exp, Int](216, Mul(
              Cofree(2, Num(1)),
              Cofree(108, Mul(
                Cofree(6, Num(2)),
                Cofree(9, Num(3)))))).some)
      }
    }

    "para" >> {
      "evaluate simple expr" in {
        testRec(
          mul(num(1), mul(num(2), num(3))),
          new RecRunner[Exp, Int] {
            def run[T[_[_]]: Recursive] = _.para(peval[T]) must equal(6)
          })
      }

      "evaluate special-case" in {
        testRec(
          mul(num(0), num(0)),
          new RecRunner[Exp, Int] {
            def run[T[_[_]]: Recursive] = _.para(peval[T]) must equal(-1)
          })
      }

      "evaluate equiv" in {
        testRec(
          mul(num(0), mul(num(0), num(1))),
          new RecRunner[Exp, Int] {
            def run[T[_[_]]: Recursive] = _.para(peval[T]) must equal(0)
          })
      }
    }

    "gpara" >> {
      "behave like para" in {
        mul(num(0), mul(num(0), num(1)))
          .gpara[Id, Int](distCata, exp => peval(exp.map(_.runEnvT))) must
            equal(0)
      }
    }

    def depth[T[_[_]], F[_]]: (Int, F[T[F]]) => Int = (i, _) => i + 1

    def sequential[T[_[_]], F[_]]: (Int, F[T[F]]) => State[Int, Int] =
      (_, _) => State.get[Int] <* State.modify[Int](_ + 1)

    "attributeTopDown" >> {
      "increase toward leaves" in {
        val v = mul(num(0), mul(num(0), num(1)))
        v.attributeTopDown(0)(depth) must equal(
          Cofree[Exp, Int](1, Mul(
            Cofree(2, Num(0)),
            Cofree(2, Mul(
              Cofree(3, Num(0)),
              Cofree(3, Num(1)))))))
      }

      "increase toward leaves, ltr" in {
        val v = mul(num(0), mul(num(0), num(1)))
        v.attributeTopDownM[State[Int, ?], Int](0)(sequential).eval(0) must
          equal(
            Cofree[Exp, Int](0, Mul(
              Cofree(1, Num(0)),
              Cofree(2, Mul(
                Cofree(3, Num(0)),
                Cofree(4, Num(1)))))))
      }
    }

    "distCata" >> {
      "behave like cata" in {
        val v = mul(num(0), mul(num(0), num(1)))
        v.gcata[Id, Int](distCata, eval) must equal(v.cata(eval))
        v.convertTo[Mu].gcata[Id, Int](distCata, eval) must equal(v.cata(eval))
        v.convertTo[Nu].gcata[Id, Int](distCata, eval) must equal(v.cata(eval))
      }
    }

    "distPara" >> {
      "behave like para" in {
        val v = mul(num(0), mul(num(0), num(1)))
        v.gcata[(Fix[Exp], ?), Int](distPara, peval[Fix]) must equal(v.para(peval[Fix]))
        v.convertTo[Mu].gcata[(Mu[Exp], ?), Int](distPara, peval[Mu]) must equal(v.convertTo[Mu].para(peval[Mu]))
        v.convertTo[Nu].gcata[(Nu[Exp], ?), Int](distPara, peval[Nu]) must equal(v.convertTo[Nu].para(peval[Nu]))
      }
    }

    def extract2s[T[_[_]]: Corecursive]: Int => Exp[T[Exp] \/ Int] = x =>
      if (x == 0) Num(x)
      else if (x % 2 == 0) Mul(-\/(Num[T[Exp]](2).embed), \/-(x.toInt / 2))
      else Num(x)

    def extract2sAnd5[T[_[_]]: Corecursive]:
        Int => T[Exp] \/ Exp[Int] = x =>
      if (x <= 2) Num(x).right
      else if (x % 2 == 0) \/-(Mul(2, x / 2))
      else if (x % 5 == 0)
        Mul(Num[T[Exp]](5).embed, Num[T[Exp]](x / 5).embed).embed.left
      else Num(x).right

    def extract2sNot5[T[_[_]]: Corecursive](x: Int):
        Option[Exp[T[Exp] \/ Int]] =
      if (x == 5) None else extract2s[T].apply(x).some

    def fact[T[_[_]]: Corecursive](x: Int): Exp[T[Exp] \/ Int] =
      if (x > 1) Mul(-\/(Num[T[Exp]](x).embed), \/-(x - 1))
      else Num(x)


    "apomorphism" >> {
      "pull out factors of two" in {
        "apoM" in {
          "should be some" in {
            12.apoM(extract2sNot5[Fix]) must
              beSome(mul(num(2), mul(num(2), num(3))))
          }
          "should be none" in {
            10.apoM(extract2sNot5[Fix]) must beNone
          }
        }
        "apo should be an optimization over apoM and be semantically equivalent" ! prop { i: Int =>
          if (i == 0) ok
          else i.apoM[Fix, Id, Exp](extract2s) must equal(i.apo[Fix, Exp](extract2s))
        }
      }
      "construct factorial" in {
        4.apo(fact[Fix]) must
          equal(mul(num(4), mul(num(3), mul(num(2), num(1)))))
      }
    }

    "elgotApo" >> {
      "pull out factors of two and stop on 5" in {
        420.elgotApo(extract2sAnd5[Fix]) must
          equal(mul(num(2), mul(num(2), mul(num(5), num(21)))))
      }
    }

    "anamorphism" >> {
      "pull out factors of two" in {
        "anaM" >> {
          def extractFactorsM(x: Int): Option[Exp[Int]] =
            if (x == 5) None else extractFactors(x).some
          "pull out factors of two" in {
            testCorec(
              12,
              new CorecRunner[Option, Exp, Int] {
                def run[T[_[_]]: Corecursive](implicit Eq: Equal[T[Exp]], S: Show[T[Exp]]) =
                  _.anaM[T, Option, Exp](extractFactorsM) must
                    equal(mul(num(2), mul(num(2), num(3))).convertTo[T].some)
              })
          }
          "fail if 5 is present" in {
            testCorec(
              10,
              new CorecRunner[Option, Exp, Int] {
                def run[T[_[_]]: Corecursive](implicit Eq: Equal[T[Exp]], S: Show[T[Exp]]) =
                  _.anaM[T, Option, Exp](extractFactorsM) must beNone
              })
          }
        }
        "ana should be an optimization over anaM and be semantically equivalent" ! prop { i: Int =>
          testCorec(
            i,
            new CorecRunner[Id, Exp, Int] {
              def run[T[_[_]]: Corecursive](implicit Eq: Equal[T[Exp]], S: Show[T[Exp]]) =
                _.anaM[T, Id, Exp](extractFactors) must
                  equal(i.ana[T, Exp](extractFactors))
            })
        }
      }
    }

    "distAna" >> {
      "behave like ana in gana" ! prop { (i: Int) =>
        testCorec(
          i,
          new CorecRunner[Id, Exp, Int] {
            def run[T[_[_]]: Corecursive](implicit Eq: Equal[T[Exp]], S: Show[T[Exp]]) =
              _.gana[T, Id, Exp](distAna, extractFactors) must
                equal(i.ana[T, Exp](extractFactors))
          })
      }

      "behave like ana in elgotAna" ! prop { (i: Int) =>
        testCorec(
          i,
          new CorecRunner[Id, Exp, Int] {
            def run[T[_[_]]: Corecursive](implicit Eq: Equal[T[Exp]], S: Show[T[Exp]]) =
              _.elgotAna[T, Id, Exp](distAna, extractFactors) must
                equal(i.ana[T, Exp](extractFactors))
          })
      }
    }

    "distApo" >> {
      "behave like apo in gana" ! prop { (i: Int) =>
        (i.gana[Fix, Fix[Exp] \/ ?, Exp](distApo, extract2s) must
          equal(i.apo[Fix, Exp](extract2s))).toResult and
        (i.gana[Mu, Mu[Exp] \/ ?, Exp](distApo, extract2s) must
          equal(i.apo[Mu, Exp](extract2s))).toResult and
        (i.gana[Nu, Nu[Exp] \/ ?, Exp](distApo, extract2s) must
          equal(i.apo[Nu, Exp](extract2s))).toResult
      }

      "behave like elgotApo in elgotAna" ! prop { (i: Int) =>
        (i.elgotAna[Fix, Fix[Exp] \/ ?, Exp](distApo, extract2sAnd5[Fix]) must
          equal(i.elgotApo[Fix, Exp](extract2sAnd5[Fix]))).toResult and
        (i.elgotAna[Mu, Mu[Exp] \/ ?, Exp](distApo, extract2sAnd5[Mu]) must
          equal(i.elgotApo[Mu, Exp](extract2sAnd5[Mu]))).toResult and
        (i.elgotAna[Nu, Nu[Exp] \/ ?, Exp](distApo, extract2sAnd5[Nu]) must
          equal(i.elgotApo[Nu, Exp](extract2sAnd5[Nu]))).toResult
      }
    }

    "hylo" >> {
      "factor and then evaluate" ! prop { (i: Int) =>
        i.hylo(eval, extractFactors) must equal(i)
      }
    }

    "ghylo" >> {
      "behave like hylo with distCata/distAna" ! prop { (i: Int) =>
        i.ghylo[Id, Id, Exp, Int](distCata, distAna, eval, extractFactors) must
          equal(i.hylo(eval, extractFactors))
      }

      "behave like chrono with distHisto/distFutu" ! prop { i: Int =>
        i.ghylo[Cofree[Exp, ?], Free[Exp, ?], Exp, Fix[Exp]](
          distHisto, distFutu, partialEval[Fix], extract2and3) must
          equal(i.chrono(partialEval[Fix], extract2and3))
      }
    }

    def strings(t: Exp[(Int, String)]): String = t match {
      case Num(x) => x.toString
      case Mul((x, xs), (y, ys)) =>
        xs + " (" + x + ")" + ", " + ys + " (" + y + ")"
      case _ => Predef.???
    }

    "zygo" >> {
      "eval and strings" in {
        testRec(
          mul(mul(num(0), num(0)), mul(num(2), num(5))),
          new RecRunner[Exp, String] {
            def run[T[_[_]]: Recursive] =
              _.zygo(eval, strings) must
                equal("0 (0), 0 (0) (0), 2 (2), 5 (5) (10)")
          })
      }
    }

    "paraZygo" >> {
      "peval and strings" in {
        testRec(
          mul(mul(num(0), num(0)), mul(num(2), num(5))),
          new RecRunner[Exp, String] {
            def run[T[_[_]]: Recursive] =
              _.paraZygo(peval[T], strings) must
                equal("0 (0), 0 (0) (-1), 2 (2), 5 (5) (10)")
          })
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

    "mutu" >> {
      val toNat: Int => Fix[Nat] = _.ana[Fix, Nat]({
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

    "histo" >> {
      "eval simple literal multiplication" in {
        mul(num(5), num(10)).histo(partialEval[Fix]) must equal(num(50))
        mul(num(5), num(10)).histo(partialEval[Mu]) must equal(num(50).convertTo[Mu])
        mul(num(5), num(10)).histo(partialEval[Nu]) must equal(num(50).convertTo[Nu])
      }

      "partially evaluate mul in lambda" in {
        lam('foo, mul(mul(num(4), num(7)), vari('foo))).histo(partialEval[Fix]) must
          equal(lam('foo, mul(num(28), vari('foo))))
        lam('foo, mul(mul(num(4), num(7)), vari('foo))).histo(partialEval[Mu]) must
          equal(lam('foo, mul(num(28), vari('foo))).convertTo[Mu])
        lam('foo, mul(mul(num(4), num(7)), vari('foo))).histo(partialEval[Nu]) must
          equal(lam('foo, mul(num(28), vari('foo))).convertTo[Nu])
      }
    }

    def extract2and3(x: Int): Exp[Free[Exp, Int]] =
      // factors all the way down
      if (x > 2 && x % 2 == 0) Mul(Free.point(2), Free.point(x/2))
      // factors once and then stops
      else if (x > 3 && x % 3 == 0)
        Mul(Free.liftF(Num(3)), Free.liftF(Num(x/3)))
      else Num(x)

    "postpro" >> {
      "extract original with identity ~>" in {
        (72.postpro[Fix, Exp](NaturalTransformation.refl[Exp], extractFactors) must
          equal(mul(num(2), mul(num(2), mul(num(2), num(9)))))).toResult and
        (72.postpro[Mu, Exp](NaturalTransformation.refl[Exp], extractFactors) must
          equal(mul(num(2), mul(num(2), mul(num(2), num(9)))).convertTo[Mu])).toResult and
        (72.postpro[Nu, Exp](NaturalTransformation.refl[Exp], extractFactors) must
          equal(mul(num(2), mul(num(2), mul(num(2), num(9)))).convertTo[Nu])).toResult
      }

      "apply ~> repeatedly" in {
        (72.postpro[Fix, Exp](MinusThree, extractFactors) must
          equal(mul(num(-1), mul(num(-4), mul(num(-7), num(0)))))).toResult and
        (72.postpro[Mu, Exp](MinusThree, extractFactors) must
          equal(mul(num(-1), mul(num(-4), mul(num(-7), num(0)))).convertTo[Mu])).toResult and
        (72.postpro[Nu, Exp](MinusThree, extractFactors) must
          equal(mul(num(-1), mul(num(-4), mul(num(-7), num(0)))).convertTo[Nu])).toResult
      }
    }

    "gpostpro" >> {
      "extract original with identity ~>" in {
        72.gpostpro[Fix, Free[Exp, ?], Exp](distFutu, NaturalTransformation.refl[Exp], extract2and3) must
          equal(mul(num(2), mul(num(2), mul(num(2), mul(num(3), num(3))))))
      }

      "apply ~> repeatedly" in {
        72.gpostpro[Fix, Free[Exp, ?], Exp](distFutu, MinusThree, extract2and3) must
          equal(mul(num(-1), mul(num(-4), mul(num(-7), mul(num(-9), num(-9))))))
      }
    }

    "futu" >> {
      "factor multiples of two" in {
        testCorec(
          8,
          new CorecRunner[Id, Exp, Int] {
            def run[T[_[_]]: Corecursive](implicit Eq: Equal[T[Exp]], S: Show[T[Exp]]) =
              _.futu[T, Exp](extract2and3) must equal(mul(num(2), mul(num(2), num(2))).convertTo[T])
          })
      }

      "factor multiples of three" in {
        testCorec(
          81,
          new CorecRunner[Id, Exp, Int] {
            def run[T[_[_]]: Corecursive](implicit Eq: Equal[T[Exp]], S: Show[T[Exp]]) =
              _.futu[T, Exp](extract2and3) must
                equal(mul(num(3), num(27)).convertTo[T])
          })
      }

      "factor 3 within 2" in {
        testCorec(
          324,
          new CorecRunner[Id, Exp, Int] {
            def run[T[_[_]]: Corecursive](implicit Eq: Equal[T[Exp]], S: Show[T[Exp]]) =
              _.futu[T, Exp](extract2and3) must
                equal(mul(num(2), mul(num(2), mul(num(3), num(27)))).convertTo[T])
          })
      }
    }

    "chrono" >> {
      "factor and partially eval" ! prop { (i: Int) =>
        i.chrono(partialEval[Fix], extract2and3) must equal(num(i))
        i.chrono(partialEval[Mu], extract2and3) must equal(num(i).convertTo[Mu])
        i.chrono(partialEval[Nu], extract2and3) must equal(num(i).convertTo[Nu])
      }
    }
  }

  "Holes" >> {
    "holes" >> {
      "find none" in {
        holes[Exp, Unit](Num(0)) must_=== Num(0)
      }

      "find and replace two children" in {
        (holes(mul(num(0), num(1)).unFix) match {
          case Mul((Fix(Num(0)), f1), (Fix(Num(1)), f2)) =>
            f1(num(2)) must equal(Mul(num(2), num(1)))
            f2(num(2)) must equal(Mul(num(0), num(2)))
          case r => failure
        }): org.specs2.execute.Result
      }
    }

    "holesList" >> {
      "find none" in {
        holesList[Exp, Unit](Num(0)) must be empty
      }

      "find and replace two children" in {
        (holesList(mul(num(0), num(1)).unFix) match {
          case (t1, f1) :: (t2, f2) :: Nil =>
            t1         must equal(num(0))
            f1(num(2)) must equal(Mul(num(2), num(1)))
            t2         must equal(num(1))
            f2(num(2)) must equal(Mul(num(0), num(2)))
          case _ => failure
        }): org.specs2.execute.Result
      }
    }

    "project" >> {
      "not find child of leaf" in {
        project(0, num(0).unFix) must beNone
      }

      "find first child of simple expr" in {
        project(0, mul(num(0), num(1)).unFix) must beSome(num(0))
      }

      "not find child with bad index" in {
        project(-1, mul(num(0), num(1)).unFix) must beNone
        project(2, mul(num(0), num(1)).unFix) must beNone
      }
    }
  }

  "Attr" >> {
    "attrSelf" >> {
      "annotate all" ! Prop.forAll(expGen) { exp =>
        // NB: This would look like
        //     >   exp.cata(attrSelf).universe must
        //     >     equal(exp.universe.map(_.cata(attrSelf)))
        //     if scalac could find the implicit
        Recursive[Cofree[?[_], Mu[Exp]]].universe(exp.cata[Cofree[Exp, Mu[Exp]]](attrSelf)) must
          equal(exp.universe.map(_.cata[Cofree[Exp, Mu[Exp]]](attrSelf)))
      }
    }

    "convert" >> {
      "forget unit" ! Prop.forAll(expGen) { exp =>
        // NB: This would look like
        //     >   exp.cata(attrK(())).convertTo[Fix] must equal(exp)
        //     if scalac could find the implicit
        Recursive[Cofree[?[_], Unit]].convertTo[Exp, Mu](exp.cata(attrK(()))) must
          equal(exp)
      }
    }

    "foldMap" >> {
      "zeros" ! Prop.forAll(expGen) { exp =>
        Foldable[Cofree[Exp, ?]].foldMap(exp.cata(attrK(0)))(_ :: Nil) must
          equal(exp.universe.map(Function.const(0)))
      }

      "selves" ! Prop.forAll(expGen) { exp =>
        Foldable[Cofree[Exp, ?]].foldMap(exp.cata[Cofree[Exp, Mu[Exp]]](attrSelf))(_ :: Nil) must
          equal(exp.universe)
      }
    }
  }

  def expGen = Gen.resize(100, corecArbitrary[Mu, Exp].arbitrary)
}
