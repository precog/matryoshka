/*
 * Copyright 2014 - 2015 SlamData Inc.
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

import java.lang.String
import scala.{Boolean, Function, Int, None, Option, Predef, Symbol, Unit},
  Predef.{implicitly, wrapString}
import scala.collection.immutable.{List, Map, Nil, ::}

import org.scalacheck._
import org.specs2.ScalaCheck
import org.specs2.mutable._
import org.specs2.scalaz._
import scalaz._, Scalaz._
import scalaz.scalacheck.ScalaCheckBinding._
import scalaz.scalacheck.ScalazProperties._

sealed trait Exp[A]
object Exp {
  case class Num[A](value: Int) extends Exp[A]
  case class Mul[A](left: A, right: A) extends Exp[A]
  case class Var[A](value: Symbol) extends Exp[A]
  case class Lambda[A](param: Symbol, body: A) extends Exp[A]
  case class Apply[A](func: A, arg: A) extends Exp[A]
  case class Let[A](name: Symbol, value: A, inBody: A) extends Exp[A]

  implicit val arbSymbol = Arbitrary(Arbitrary.arbitrary[String].map(Symbol(_)))

  implicit val arbExp: Arbitrary ~> λ[α => Arbitrary[Exp[α]]] =
    new (Arbitrary ~> λ[α => Arbitrary[Exp[α]]]) {
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

  def num(v: Int) = Fix[Exp](Num(v))
  def mul(left: Fix[Exp], right: Fix[Exp]) = Fix[Exp](Mul(left, right))
  def vari(v: Symbol) = Fix[Exp](Var(v))
  def lam(param: Symbol, body: Fix[Exp]) = Fix[Exp](Lambda(param, body))
  def ap(func: Fix[Exp], arg: Fix[Exp]) = Fix[Exp](Apply(func, arg))
  def let(name: Symbol, v: Fix[Exp], inBody: Fix[Exp]) = Fix[Exp](Let(name, v, inBody))

  implicit val ExpTraverse: Traverse[Exp] = new Traverse[Exp] {
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
  implicit val ExpEqual: Equal ~> λ[α => Equal[Exp[α]]] =
    new (Equal ~> λ[α => Equal[Exp[α]]]) {
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
          case _                                  => false
        }
    }
  implicit def ExpEqual2[A](implicit A: Equal[A]): Equal[Exp[A]] = ExpEqual(A)

  implicit val ExpShow: Show ~> λ[α => Show[Exp[α]]] =
    new (Show ~> λ[α => Show[Exp[α]]]) {
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

  implicit val ExpUnzip = new Unzip[Exp] {
    def unzip[A, B](f: Exp[(A, B)]) = (f.map(_._1), f.map(_._2))
  }
}

sealed trait Exp2[A]
object Exp2 {
  case class Const[A]() extends Exp2[A]
  case class Num2[A](value: Int) extends Exp2[A]
  case class Single[A](a: A) extends Exp2[A]

  implicit val arbExp2: Arbitrary ~> λ[α => Arbitrary[Exp2[α]]] =
    new (Arbitrary ~> λ[α => Arbitrary[Exp2[α]]]) {
      def apply[α](arb: Arbitrary[α]): Arbitrary[Exp2[α]] =
        Arbitrary(Gen.oneOf(
          Gen.const(Const[α]),
          Arbitrary.arbitrary[Int].map(Num2[α](_)),
          arb.arbitrary.map(Single(_))))
    }

  def const = Fix[Exp2](Const())
  def num2(v: Int) = Fix[Exp2](Num2(v))
  def single(a: Fix[Exp2]) = Fix[Exp2](Single(a))

  implicit val Exp2Functor: Functor[Exp2] = new Functor[Exp2] {
    def map[A, B](fa: Exp2[A])(f: A => B): Exp2[B] = fa match {
      case Const()   => Const[B]()
      case Num2(v)   => Num2[B](v)
      case Single(a) => Single(f(a))
    }
  }

  implicit val Exp2Show: Show ~> λ[α => Show[Exp2[α]]] =
    new (Show ~> λ[α => Show[Exp2[α]]]) {
      def apply[α](show: Show[α]) =
        Show.show {
          case Const()   => "Const()"
          case Num2(v)   => "Num2(" + v.shows + ")"
          case Single(a) => "Single(" + show.shows(a) + ")"
        }
    }

  implicit val Exp2Equal: Equal ~> λ[α => Equal[Exp2[α]]] =
    new (Equal ~> λ[α => Equal[Exp2[α]]]) {
      def apply[α](eq: Equal[α]) =
        Equal.equal[Exp2[α]] {
          case (Const(), Const())       => true
          case (Num2(v1), Num2(v2))     => v1 ≟ v2
          case (Single(a1), Single(a2)) => eq.equal(a1, a2)
          case _                        => false
        }
    }
  implicit def Exp2Equal2[A](implicit A: Equal[A]): Equal[Exp2[A]] = Exp2Equal(A)
}


class ExpSpec extends Spec {
  import Exp._

  implicit val arbExpInt: Arbitrary[Exp[Int]] = arbExp(Arbitrary.arbInt)
  // NB: These are just a sanity check that the data structure created for the
  //     tests is lawful.
  checkAll(equal.laws[Exp[Int]])
  checkAll(traverse.laws[Exp])
}

class Exp2Spec extends Spec {
  import Exp2._

  implicit val arbExp2Int: Arbitrary[Exp2[Int]] = arbExp2(Arbitrary.arbInt)
  // NB: These are just a sanity check that the data structure created for the
  //     tests is lawful.
  checkAll(equal.laws[Exp2[Int]])
  checkAll(functor.laws[Exp2])
}

class FixplateSpecs extends Specification with ScalaCheck with ScalazMatchers {
  import Exp._

  implicit def arbFix[F[_]]:
      (Arbitrary ~> λ[α => Arbitrary[F[α]]]) => Arbitrary[Fix[F]] =
    new ((Arbitrary ~> λ[α => Arbitrary[F[α]]]) => Arbitrary[Fix[F]]) {
      def apply(FA: Arbitrary ~> λ[α => Arbitrary[F[α]]]):
          Arbitrary[Fix[F]] =
        Arbitrary(Gen.sized(size =>
          FA(
            if (size <= 0)
              Arbitrary(Gen.fail[Fix[F]])
            else
              Arbitrary(Gen.resize(size - 1, arbFix(FA).arbitrary))).arbitrary.map(Fix(_))))
    }

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
    case Num(n) => Exp2.Num2(n+1).some
    case _      => None
  }

  def addOneExpExp2ƒ[T[_[_]]]: Exp[T[Exp2]] => Exp2[T[Exp2]] =
    orDefault[Exp[T[Exp2]], Exp2[T[Exp2]]](Exp2.Const())(addOneOptExpExp2ƒ)

  def addOneOptExp2Expƒ[T[_[_]]]: Exp2[T[Exp2]] => Option[Exp[T[Exp2]]] = {
    case Exp2.Num2(n) => Num(n+1).some
    case _            => None
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
      case Num(a) => Exp2.Num2(a)
      case _      => Exp2.Single(exp2)
    }
    case _                      => Exp2.Const[T[Exp2]]
  }

  val MinusThree: Exp ~> Exp =
    new (Exp ~> Exp) {
      def apply[A](exp: Exp[A]): Exp[A] = exp match {
        case Num(x) => Num(x-3)
        case t      => t
      }
    }

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

    "transPrepro" should {
      "change literal with identity ~>" in {
        num(1).transPrepro(addOneƒ)(NaturalTransformation.refl[Exp]) must equal(num(2))
      }

      "apply ~> in original space" in {
        num(1).transPrepro(addOneƒ)(MinusThree) must equal(num(-1))
      }

      "apply ~> with change of space" in {
        num(1).transPrepro(addOneExpExp2ƒ)(MinusThree) must equal(Exp2.num2(-1))
      }
    }

    "transPostpro" should {
      "change literal with identity ~>" in {
        num(1).transPostpro(addOneƒ)(NaturalTransformation.refl[Exp]) must equal(num(2))
      }

      "apply ~> in original space" in {
        num(1).transPostpro(addOneƒ)(MinusThree) must equal(num(-1))
      }

      "apply ~> with change of space" in {
        Exp2.num2(1).transPostpro(addOneExp2Expƒ)(MinusThree) must equal(num(-1))
      }
    }

    "transPara" should {
      "project basic exp" in {
        lam('sym, num(3)).transPara(extractLambdaƒ) must equal(Exp2.num2(3))
      }

      "project basic exp recursively" in {
        lam('sym, mul(num(5), num(7))).transPara(extractLambdaƒ) must equal(Exp2.single(Exp2.const))
      }
    }

    "foldMap" should {
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

    "attribute" should {
      "work on simple algebra" in {
        val v = mul(num(1), mul(num(2), num(3)))
        val rez = Cofree[Exp, Int](6, Mul(
          Cofree(1, Num(1)),
          Cofree (6, Mul(
            Cofree(2, Num(2)),
            Cofree(3, Num(3))))))
        v.cata(eval.attribute) must equal(rez)
        v.convertTo[Mu].cata(eval.attribute) must equal(rez)
      }
    }

    "zipAlgebras" should {
      "both eval and find all constants" in {
        mul(num(5), num(2)).cata(GElgotAlgebraM.zip[Id, Id, Id, Exp].zip(eval, findConstants)) must
          equal((10, List(5, 2)))
        mul(num(5), num(2)).convertTo[Mu].cata(GElgotAlgebraM.zip[Id, Id, Id, Exp].zip(eval, findConstants)) must
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

    "coelgot" should {
      "behave like elgotCata ⋘ attributeAna" ! prop { (i: Int) =>
        i.coelgot(eval.generalizeElgot[(Int, ?)], extractFactors) must equal(
          i.attributeAna(extractFactors).elgotCata(eval.generalizeElgot[(Int, ?)]))
      }
    }

    "elgot" should {
      "behave like interpCata ⋘ elgotAna" ! prop { (i: Int) =>
        i.elgot(eval, extractFactors.generalizeElgot[Int \/ ?]) must equal(
          i.elgotAna(extractFactors.generalizeElgot[Int \/ ?]).interpretCata(eval))
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

    // TODO: Why do we need to explicitly call the implicit here? Is it because
    //      `F` is in covariant position?
    def extractFactors: Coalgebra[Exp, Int] = toCoalgebra(x =>
      if (x > 2 && x % 2 == 0) Mul(2, x/2)
      else Num(x))

    "generalizeCoalgebra" should {
      "behave like ana" ! prop { (i: Int) =>
        i.apo(extractFactors.generalize[Fix[Exp] \/ ?]) must
          equal(i.ana(extractFactors))
      }
    }

    "topDownCata" should {
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
      }
    }

    "trans" should {
      // TODO
    }

    // Evaluate as usual, but trap 0*0 as a special case
    def peval[T[_[_]]: Recursive](t: Exp[(T[Exp], Int)]): Int = t match {
      case Mul((Proj(Num(0)), _), (Proj(Num(0)), _)) => -1
      case Mul((_           , x), (_,            y)) => x * y
      case Num(x)                                    => x
      case _                                         => Predef.???
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
          distCata,
          expr => peval(expr.map(_.runEnvT))) must equal(0)
      }
    }

    def depth[T[_[_]], F[_]]: (Int, F[T[F]]) => Int = (i, _) => i + 1

    def sequential[T[_[_]], F[_]]: (Int, F[T[F]]) => State[Int, Int] =
      (_, _) => State.get[Int] <* State.modify[Int](_ + 1)

    "attributeTopDown" should {
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

    "distCata" should {
      "behave like cata" in {
        val v = mul(num(0), mul(num(0), num(1)))
        v.gcata[Id, Int](distCata, eval) must equal(v.cata(eval))
        v.convertTo[Mu].gcata[Id, Int](distCata, eval) must equal(v.cata(eval))
      }
    }

    "distPara" should {
      "behave like para" in {
        val v = mul(num(0), mul(num(0), num(1)))
        v.gcata[(Fix[Exp], ?), Int](distPara, peval[Fix]) must equal(v.para(peval[Fix]))
        v.convertTo[Mu].gcata[(Mu[Exp], ?), Int](distPara, peval[Mu]) must equal(v.convertTo[Mu].para(peval[Mu]))
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

    "distAna" should {
      "behave like ana" ! prop { (i: Int) =>
        i.gana[Fix, Exp, Id](distAna, extractFactors) must equal(i.ana(extractFactors))
      }
    }

    "hylo" should {
      "factor and then evaluate" ! prop { (i: Int) =>
        i.hylo(eval, extractFactors) must equal(i)
      }
    }

    "ghylo" should {
      "behave like hylo with distCata/distAna" ! prop { (i: Int) =>
        i.ghylo[Exp, Id, Id, Int](distCata, distAna, eval, extractFactors) must
          equal(i.hylo(eval, extractFactors))
      }

      "behave like chrono with distHisto/distFutu" ! prop { (i: Int) =>
        i.ghylo[Exp, Cofree[Exp, ?], Free[Exp, ?], Fix[Exp]](
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

    // NB: This is better done with cata, but we fake it here
    def partialEval[T[_[_]]: Corecursive: Recursive]:
        Exp[Cofree[Exp, T[Exp]]] => T[Exp] = t =>
      t match {
        case Mul(x, y) => (x.head.project, y.head.project) match {
          case (Num(a), Num(b)) => Num[T[Exp]](a * b).embed
          case _                => t.map(_.head).embed
        }
        case _ => t.map(_.head).embed
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

    // FIXME: defining this as an algebra brings up the “cyclic aliasisng” issue
    def extract2and3: Int => Exp[Free[Exp, Int]] = x =>
      // factors all the way down
      if (x > 2 && x % 2 == 0) Mul(Free.point(2), Free.point(x/2))
      // factors once and then stops
      else if (x > 3 && x % 3 == 0)
        Mul(Free.liftF(Num(3)), Free.liftF(Num(x/3)))
      else Num(x)

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

  "Holes" should {
    "holes" should {
      "find none" in {
        holes[Exp, Unit](Num(0)) must_== Num(0)
      }

      "find and replace two children" in {
        (holes(mul(num(0), num(1)).unFix) match {
          case Mul((Fix(Num(0)), f1), (Fix(Num(1)), f2)) =>
            f1(num(2)) must_== Mul(num(2), num(1))
            f2(num(2)) must_== Mul(num(0), num(2))
          case r => failure
        }): org.specs2.execute.Result
      }
    }

    "holesList" should {
      "find none" in {
        holesList[Exp, Unit](Num(0)) must_== Nil
      }

      "find and replace two children" in {
        (holesList(mul(num(0), num(1)).unFix) match {
          case (t1, f1) :: (t2, f2) :: Nil =>
            t1         must equal(num(0))
            f1(num(2)) must_== Mul(num(2), num(1))
            t2         must equal(num(1))
            f2(num(2)) must_== Mul(num(0), num(2))
          case _ => failure
        }): org.specs2.execute.Result
      }
    }

    "project" should {
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

  def expGen = Gen.resize(100, arbFix(arbExp).arbitrary)
}
