package quasar.recursionschemes

import quasar.Predef._
import quasar.{RenderTree, Terminal, NonTerminal}
import quasar.fp._
import Recursive.ops._, FunctorT.ops._, Fix._

import org.scalacheck._
import org.specs2.ScalaCheck
import org.specs2.mutable._
import org.specs2.scalaz._
import scalaz._, Scalaz._
import scalaz.scalacheck.ScalazProperties._

sealed trait Exp[+A]
object Exp {
  case class Num(value: Int) extends Exp[Nothing]
  case class Mul[A](left: A, right: A) extends Exp[A]
  case class Var(value: Symbol) extends Exp[Nothing]
  case class Lambda[A](param: Symbol, body: A) extends Exp[A]
  case class Apply[A](func: A, arg: A) extends Exp[A]
  case class Let[A](name: Symbol, value: A, inBody: A) extends Exp[A]

  implicit val arbSymbol = Arbitrary(Arbitrary.arbitrary[String].map(Symbol(_)))

  implicit val arbExp: Arbitrary ~> λ[α => Arbitrary[Exp[α]]] =
    new (Arbitrary ~> λ[α => Arbitrary[Exp[α]]]) {
      def apply[α](arb: Arbitrary[α]): Arbitrary[Exp[α]] =
        Arbitrary(Gen.oneOf(
          Arbitrary.arbitrary[Int].map(Num(_)),
          for {
            a <- arb.arbitrary
            b <- arb.arbitrary
          } yield Mul(a, b),
          Arbitrary.arbitrary[Symbol].map(Var(_)),
          for {
            a <- Arbitrary.arbitrary[Symbol]
            b <- arb.arbitrary
          } yield Lambda(a, b),
          for {
            a <- arb.arbitrary
            b <- arb.arbitrary
          } yield Apply(a, b),
          for {
            a <- Arbitrary.arbitrary[Symbol]
            b <- arb.arbitrary
            c <- arb.arbitrary
          } yield Let(a, b, c)))
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

  implicit val ExpRenderTree: RenderTree ~> λ[α => RenderTree[Exp[α]]] =
    new (RenderTree ~> λ[α => RenderTree[Exp[α]]]) {
      def apply[α](ra: RenderTree[α]) = new RenderTree[Exp[α]] {
        def render(v: Exp[α]) = v match {
          case Num(value)       => Terminal(List("Num"), Some(value.toString))
          case Mul(l, r)        => NonTerminal(List("Mul"), None, List(ra.render(l), ra.render(r)))
          case Var(sym)         => Terminal(List("Var"), Some(sym.toString))
          case Lambda(param, body) =>
            NonTerminal(List("Lambda"), Some(param.toString), List(ra.render(body)))
          case Apply(to, expr) =>
            NonTerminal(List("Apply"), None, List(ra.render(to), ra.render(expr)))
          case Let(name, form, body)  => NonTerminal(List("Let"), Some(name.toString), List(ra.render(form), ra.render(body)))
        }
      }
    }

  implicit val IntRenderTree = RenderTree.fromToString[Int]("Int")

  // NB: an unusual definition of equality, in that only the first 3 characters
  //     of variable names are significant. This is to distinguish it from `==`
  //     as well as from a derivable Equal.
  implicit val EqualExp: EqualF[Exp] = new EqualF[Exp] {
    def equal[A](e1: Exp[A], e2: Exp[A])(implicit eq: Equal[A]) = (e1, e2) match {
      case (Num(v1), Num(v2))                 => v1 == v2
      case (Mul(a1, b1), Mul(a2, b2))         => a1 ≟ a2 && b1 ≟ b2
      case (Var(s1), Var(s2))                 =>
        s1.name.substring(0, 3 min s1.name.length) == s2.name.substring(0, 3 min s2.name.length)
      case (Lambda(p1, a1), Lambda(p2, a2))   => p1 == p2 && a1 ≟ a2
      case (Apply(f1, a1), Apply(f2, a2))     => f1 ≟ f2 && a1 ≟ a2
      case (Let(n1, v1, i1), Let(n2, v2, i2)) => n1 == n2 && v1 ≟ v2 && i1 ≟ i2
      case _                                  => false
    }
  }

  implicit val ExpUnzip = new Unzip[Exp] {
    def unzip[A, B](f: Exp[(A, B)]) = (f.map(_._1), f.map(_._2))
  }

  implicit val ExpBinder: Binder[Exp] = new Binder[Exp] {
    type G[A] = Map[Symbol, A]
    val G = implicitly[Traverse[G]]

    def initial[A] = Map[Symbol, A]()

    def bindings[T[_[_]]: Recursive, A](t: Exp[T[Exp]], b: G[A])(f: Exp[T[Exp]] => A) =
      t match {
        case Let(name, value, _) => b + (name -> f(value.project))
        case _                   => b
      }

    def subst[T[_[_]]: Recursive, A](t: Exp[T[Exp]], b: G[A]) = t match {
      case Var(symbol) => b.get(symbol)
      case _           => None
    }
  }
}

class ExpSpec extends Spec {
  import Exp._

  implicit val arbExpInt: Arbitrary[Exp[Int]] = arbExp(Arbitrary.arbInt)
  checkAll(traverse.laws[Exp])
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
    case Num(v)           => Some(v)
    case Mul(left, right) => (left |@| right)(_ * _)
    case Var(v)           => None
    case Lambda(_, b)     => b
    case Apply(func, arg) => None
    case Let(_, _, i)     => i
  }

  val addOneOptƒ: Exp[Fix[Exp]] => Option[Exp[Fix[Exp]]] = {
    case Num(n) => Num(n+1).some
    case _      => None
  }

  val addOneƒ: Exp[Fix[Exp]] => Exp[Fix[Exp]] = once(addOneOptƒ)

  val simplifyƒ: Exp[Fix[Exp]] => Option[Exp[Fix[Exp]]] = {
    case Mul(Fix(Num(0)), Fix(Num(_))) => Num(0).some
    case Mul(Fix(Num(1)), Fix(Num(n))) => Num(n).some
    case Mul(Fix(Num(_)), Fix(Num(0))) => Num(0).some
    case Mul(Fix(Num(n)), Fix(Num(1))) => Num(n).some
    case _                             => None
  }

  val addOneOrSimplifyƒ: Exp[Fix[Exp]] => Exp[Fix[Exp]] = {
    case t @ Num(_)    => addOneƒ(t)
    case t @ Mul(_, _) => repeatedly(simplifyƒ).apply(t)
    case t             => t
  }

  "Fix" should {
    "isLeaf" should {
      "be true for simple literal" in {
        num(1).isLeaf must beTrue
      }

      "be false for expression" in {
        mul(num(1), num(2)).isLeaf must beFalse
      }
    }

    "children" should {
      "be empty for simple literal" in {
        num(1).children must_== Nil
      }

      "contain sub-expressions" in {
        mul(num(1), num(2)).children must_== List(num(1), num(2))
      }
    }

    "universe" should {
      "be one for simple literal" in {
        num(1).universe must_== List(num(1))
      }

      "contain root and sub-expressions" in {
        mul(num(1), num(2)).universe must_== List(mul(num(1), num(2)), num(1), num(2))
      }
    }

    "transCata" should {
      "change simple literal" in {
        num(1).transCata(addOneƒ) must_== num(2)
      }

      "change sub-expressions" in {
        mul(num(1), num(2)).transCata(addOneƒ) must_== mul(num(2), num(3))
      }

      "be bottom-up" in {
        mul(num(0), num(1)).transCata(addOneOrSimplifyƒ) must_== num(2)
        mul(num(1), num(2)).transCata(addOneOrSimplifyƒ) must_== mul(num(2), num(3))
      }
    }

    "transAna" should {
      "change simple literal" in {
        num(1).transAna(addOneƒ) must_== num(2)
      }

      "change sub-expressions" in {
        mul(num(1), num(2)).transAna(addOneƒ) must_== mul(num(2), num(3))
      }

      "be top-down" in {
        mul(num(0), num(1)).transAna(addOneOrSimplifyƒ) must_== num(0)
        mul(num(1), num(2)).transAna(addOneOrSimplifyƒ) must_== num(2)
      }
    }

    "foldMap" should {
      "fold stuff" in {
        mul(num(0), num(1)).foldMap(_ :: Nil) must_== mul(num(0), num(1)) :: num(0) :: num(1) :: Nil
      }
    }

    val eval: Exp[Int] => Int = {
      case Num(x) => x
      case Mul(x, y) => x*y
      case _ => ???
    }

    val findConstants: Exp[List[Int]] => List[Int] = {
      case Num(x) => x :: Nil
      case t      => t.fold
    }

    "cata" should {
      "evaluate simple expr" in {
        val v = mul(num(1), mul(num(2), num(3)))
        v.cata(eval) must_== 6
      }

      "find all constants" in {
        mul(num(0), num(1)).cata(findConstants) must_== List(0, 1)
      }

      "produce correct annotations for 5 * 2" in {
        mul(num(5), num(2)).cata(example1ƒ) must beSome(10)
      }
    }

    "zipAlgebras" should {
      "both eval and find all constants" in {
        mul(num(5), num(2)).cata(zipAlgebras[Exp, Id](eval, findConstants)) must_==
          ((10, List(5, 2)))
      }
    }

    "generalizeAlgebra" should {
      "behave like cata" in {
        val v = mul(num(1), mul(num(2), num(3)))
        v.para(generalizeAlgebra[(Fix[Exp], ?)](eval)) must_== v.cata(eval)
      }
    }

    def extractFactors(x: Int): Exp[Int] =
      if (x > 2 && x % 2 == 0) Mul(2, x/2)
      else Num(x)

    "generalizeCoalgebra" should {
      "behave like ana" ! prop { (i: Int) =>
        i.apo(generalizeCoalgebra[Fix[Exp] \/ ?](extractFactors)) must_==
          i.ana(extractFactors)
      }
    }

    "topDownCata" should {
      def subst(vars: Map[Symbol, Fix[Exp]], t: Fix[Exp]): (Map[Symbol, Fix[Exp]], Fix[Exp]) = t.unFix match {
        case Let(sym, value, body) => (vars + (sym -> value), body)

        case Var(sym) => (vars, vars.get(sym).getOrElse(t))

        case _ => (vars, t)
      }

      "bind vars" in {
        val v = let('x, num(1), mul(num(0), vari('x)))
        v.topDownCata(Map.empty[Symbol, Fix[Exp]])(subst) must_== mul(num(0), num(1))
      }
    }

    "trans" should {
      // TODO
    }

    // Evaluate as usual, but trap 0*0 as a special case
    def peval(t: Exp[(Fix[Exp], Int)]): Int = t match {
      case Mul((Fix(Num(0)), _), (Fix(Num(0)), _)) => -1
      case Num(x) => x
      case Mul((_, x), (_, y)) => x * y
      case _ => ???
    }

    "para" should {
      "evaluate simple expr" in {
        val v = mul(num(1), mul(num(2), num(3)))
        v.para(peval) must_== 6
      }

      "evaluate special-case" in {
        val v = mul(num(0), num(0))
        v.para(peval) must_== -1
      }

      "evaluate equiv" in {
        val v = mul(num(0), mul(num(0), num(1)))
        v.para(peval) must_== 0
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
          expr => { peval(expr.map(_.runEnvT)) }) must_== 0
      }
    }

    "distCata" should {
      "behave like cata" in {
        val v = mul(num(0), mul(num(0), num(1)))
        v.gcata[Id, Int](distCata, eval) must_== v.cata(eval)
      }
    }

    "distPara" should {
      "behave like para" in {
        val v = mul(num(0), mul(num(0), num(1)))
        v.gcata[(Fix[Exp], ?), Int](distPara, peval) must_== v.para(peval)
      }
    }

    "apomorphism" should {
      "pull out factors of two" in {
        def fM(x: Int): Option[Exp[Fix[Exp] \/ Int]] =
          if (x == 5) None else Some(f(x))
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
          else
            i.apoM[Exp, Id](f) must_==
              i.apo(f)
        }
      }
      "construct factorial" in {
        def fact(x: Int): Exp[Fix[Exp] \/ Int] =
          if (x > 1) Mul(-\/(num(x)), \/-(x-1))
          else Num(x)

        4.apo(fact) must_==
          mul(num(4), mul(num(3), mul(num(2), num(1))))
      }
    }

    "anamorphism" should {
      "pull out factors of two" in {
        "anaM" should {
          def extractFactorsM(x: Int): Option[Exp[Int]] =
            if (x == 5) None else Some(extractFactors(x))
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
          i.anaM[Exp,Id](extractFactors) must_==
            i.ana(extractFactors)
        }
      }
    }

    "distAna" should {
      "behave like ana" ! prop { (i: Int) =>
        i.gana[Exp, Id](distAna, extractFactors) must_==
          i.ana(extractFactors)
      }
    }

    "hylo" should {
      "factor and then evaluate" ! prop { (i: Int) =>

        hylo(i)(eval, extractFactors) must_== i
      }
    }

    def strings(t: Exp[(Int, String)]): String = t match {
      case Num(x) => x.toString
      case Mul((x, xs), (y, ys)) =>
        xs + " (" + x + ")" + ", " + ys + " (" + y + ")"
      case _ => ???
    }

    "zygo" should {
      "eval and strings" in {
        mul(mul(num(0), num(0)), mul(num(2), num(5))).zygo(eval, strings) must_==
        "0 (0), 0 (0) (0), 2 (2), 5 (5) (10)"
      }
    }

    "paraZygo" should {
      "peval and strings" in {
        mul(mul(num(0), num(0)), mul(num(2), num(5))).paraZygo(peval, strings) must_==
        "0 (0), 0 (0) (-1), 2 (2), 5 (5) (10)"
      }

    }

    // NB: This is better done with cata, but we fake it here
    def partialEval(t: Exp[Cofree[Exp, Fix[Exp]]]): Fix[Exp] = t match {
      case Mul(x, y) => (x.head.unFix, y.head.unFix) match {
        case (Num(a), Num(b)) => num(a * b)
        case _                => Fix(t.map(_.head))
      }
      case _ => Fix(t.map(_.head))
    }

    "histo" should {
      "eval simple literal multiplication" in {
        mul(num(5), num(10)).histo(partialEval) must_== num(50)
      }

      "partially evaluate mul in lambda" in {
        lam('foo, mul(mul(num(4), num(7)), vari('foo))).histo(partialEval) must_==
          lam('foo, mul(num(28), vari('foo)))
      }
    }

    def extract2and3(x: Int): Exp[Free[Exp, Int]] =
      // factors all the way down
      if (x > 2 && x % 2 == 0) Mul(Free.point(2), Free.point(x/2))
      // factors once and then stops
      else if (x > 3 && x % 3 == 0)
        Mul(Free.liftF(Num(3)), Free.liftF(Num(x/3)))
      else Num(x)

    "futu" should {
      "factor multiples of two" in {
        8.futu(extract2and3) must_==
          mul(num(2), mul(num(2), num(2)))
      }

      "factor multiples of three" in {
        81.futu(extract2and3) must_== mul(num(3), num(27))
      }

      "factor 3 within 2" in {
        324.futu(extract2and3) must_==
          mul(num(2), mul(num(2), mul(num(3), num(27))))
      }
    }

    "chrono" should {
      "factor and partially eval" ! prop { (i: Int) =>
        chrono(i)(partialEval, extract2and3) must_== num(i)
      }
    }

    "RenderTree" should {
      "render nodes and leaves" in {
        mul(num(0), num(1)).shows must_==
          """Fix:Mul
            |├─ Fix:Num(0)
            |╰─ Fix:Num(1)""".stripMargin
      }
    }
  }

  // NB: This really tests stuff in the fp package, but that exists for Fix,
  //     and here we have a fixpoint data type using Fix, so …
  "EqualF" should {
    "be true for same expr" in {
      mul(num(0), num(1)) ≟ mul(num(0), num(1)) must beTrue
    }

    "be false for different types" in {
      num(0) ≠ vari('x) must beTrue
    }

    "be false for different children" in {
      mul(num(0), num(1)) ≠ mul(num(2), num(3)) must beTrue
    }

    "be true for variables with matching prefixes" in {
      vari('abc1) ≟ vari('abc2) must beTrue
    }

    "be true for sub-exprs with variables with matching prefixes" in {
      mul(num(1), vari('abc1)) ≟ mul(num(1), vari('abc2)) must beTrue
    }

    "be implemented for unfixed exprs" in {
      Mul(num(1), vari('abc1)) ≟ Mul(num(1), vari('abc2)) must beTrue

      // NB: need to cast both terms to a common type
      def exp(x: Exp[Fix[Exp]]) = x
      exp(Mul(num(1), vari('abc1))) ≠ exp(Num(1)) must beTrue
    }
  }

  "Holes" should {
    "holes" should {
      "find none" in {
        holes(Num(0)) must_== Num(0)
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
        holesList(Num(0)) must_== Nil
      }

      "find and replace two children" in {
        (holesList(mul(num(0), num(1)).unFix) match {
          case (t1, f1) :: (t2, f2) :: Nil =>
            t1 must_== num(0)
            f1(num(2)) must_== Mul(num(2), num(1))
            t2 must_== num(1)
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

    "sizeF" should {
      "be 0 for flat" in {
        sizeF(Num(0)) must_== 0
      }

      "be 2 for simple expr" in {
        sizeF(mul(num(0), num(1)).unFix) must_== 2
      }

      "be non-recursive" in {
        sizeF(mul(num(0), mul(num(1), num(2))).unFix) must_== 2
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
        Recursive[Cofree[?[_], Unit]].convertTo(exp.cata(attrK(()))) must_== exp
      }
    }

    "foldMap" should {
      "zeros" ! Prop.forAll(expGen) { exp =>
        Foldable[Cofree[Exp, ?]].foldMap(exp.cata(attrK(0)))(_ :: Nil) must_== exp.universe.map(κ(0))
      }

      "selves" ! Prop.forAll(expGen) { exp =>
        Foldable[Cofree[Exp, ?]].foldMap(exp.cata(attrSelf))(_ :: Nil) must_== exp.universe
      }
    }

    "RenderTree" should {
      "render simple nested expr" in {
        mul(num(0), num(1)).cata(attrK(())).shows must_==
          """Cofree
            |├─ ()
            |╰─ Mul
            |   ├─ Cofree
            |   │  ├─ ()
            |   │  ╰─ Num(0)
            |   ╰─ Cofree
            |      ├─ ()
            |      ╰─ Num(1)""".stripMargin
      }
    }

    "zip" should {
      "tuplify simple constants" ! Prop.forAll(expGen) { exp =>
        unsafeZip2(exp.cata(attrK(0)), exp.cata(attrK(1))) must
          equal(exp.cata(attrK((0, 1))))
      }
    }

    "bound combinator" should {
      val Example2 = let('foo, num(5), mul(vari('foo), num(2)))

      "produce incorrect annotations when not used in let expression" in {
        Example2.cata(example1ƒ) must beNone
      }

      "produce correct annotations when used in let expression" in {
        Example2.boundCata(example1ƒ) must beSome(10)
      }

      val inlineMulBy1: Exp[(Fix[Exp], Fix[Exp])] => Fix[Exp] = {
        case Mul((_, Fix(Num(1))), (x, _)) => x  // actually, either `x` is the same here
        case t                             => Fix(t.map { case (src, bnd) =>
          src.unFix match {
            case Var(_) => src
            case _      => bnd
          }
        })
      }

      "rewrite with bound value" in {
        val source =
          let('x, num(1),
            let('y, mul(num(2), num(3)),
              mul(vari('x), vari('y))))
        val exp =
          let('x, num(1),
            let('y, mul(num(2), num(3)),
              vari('y)))
        source.boundPara(inlineMulBy1) must_== exp
        source.boundParaM[Id, Fix[Exp]](inlineMulBy1) must_== exp
      }

      "rewrite under a binding" in {
        val source =
          let('x, num(1),
            let('y, mul(vari('x), num(2)),
              mul(vari('y), vari('y))))
        val exp =
          let('x, num(1),
            let('y, num(2),              // Want this simplifed...
              mul(vari('y), vari('y))))  // ...but not this.
        source.boundPara(inlineMulBy1) must_== exp
        source.boundParaM[Id, Fix[Exp]](inlineMulBy1) must_== exp
      }

      "annotate source nodes using bound nodes" in {
        val exp = Cofree(
            let('foo, num(5), mul(num(5), num(2))),  // Also annotated here with the bound value.
            Let(
              'foo,
              Cofree[Exp, Fix[Exp]](
                num(5),
                Num(5)),
              Cofree(
                mul(num(5), num(2)),  // Also annotated here with the bound value.
                Mul(
                  Cofree[Exp, Fix[Exp]](
                    num(5),      // This is the point: annotated with the bound value...
                    Var('foo)),  // ... but preserves the reference.
                  Cofree[Exp, Fix[Exp]](
                    num(2),
                    Num(2))))))

        boundAttribute(Example2)(ι) must equal(exp)
      }
    }
  }

  def expGen = Gen.resize(100, arbFix(arbExp).arbitrary)
}
