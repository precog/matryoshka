package slamdata.engine.analysis

import fixplate._

import slamdata.engine.fp._
import slamdata.engine.{RenderTree, Terminal, NonTerminal}

import scalaz._
import Scalaz._

import org.specs2.ScalaCheck
import org.specs2.mutable._
import org.specs2.scalaz._
import org.scalacheck._

class FixplateSpecs extends Specification with ScalaCheck with ScalazMatchers {
  sealed trait Exp[+A]
  case class Num(value: Int) extends Exp[Nothing]
  case class Mul[A](left: A, right: A) extends Exp[A]
  case class Var(value: Symbol) extends Exp[Nothing]
  case class Lambda[A](param: Symbol, body: A) extends Exp[A]
  case class Apply[A](func: A, arg: A) extends Exp[A]
  case class Let[A](name: Symbol, value: A, inBody: A) extends Exp[A]

  def num(v: Int) = Term[Exp](Num(v))
  def mul(left: Term[Exp], right: Term[Exp]) = Term[Exp](Mul(left, right))
  def vari(v: Symbol) = Term[Exp](Var(v))
  def lam(param: Symbol, body: Term[Exp]) = Term[Exp](Lambda(param, body))
  def ap(func: Term[Exp], arg: Term[Exp]) = Term[Exp](Apply(func, arg))
  def let(name: Symbol, v: Term[Exp], inBody: Term[Exp]) = Term[Exp](Let(name, v, inBody))

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

  implicit val ExpRenderTree: RenderTree[Exp[_]] =
    new RenderTree[Exp[_]] {
      def render(v: Exp[_]) = v match {
        case Num(value)       => Terminal(List("Num"), Some(value.toString))
        case Mul(_, _)        => Terminal(List("Mul"), None)
        case Var(sym)         => Terminal(List("Var"), Some(sym.toString))
        case Lambda(param, _) => Terminal(List("Lambda"), Some(param.toString))
        case Apply(_, _)      => Terminal(List("Apply"), None)
        case Let(name, _, _)  => Terminal(List("Let"), Some(name.toString))
      }
    }

  implicit val IntRenderTree = RenderTree.fromToString[Int]("Int")

  // NB: an unusual definition of equality, in that only the first 3 characters
  //     of variable names are significant
  implicit val EqualExp: EqualF[Exp] = new EqualF[Exp] {
    def equal[A](e1: Exp[A], e2: Exp[A])(implicit eq: Equal[A]) = (e1, e2) match {
      case (Num(v1), Num(v2))                 => v1 == v2
      case (Mul(a1, b1), Mul(a2, b2))         => a1 ≟ a2 && b1 ≟ b2
      case (Var(s1), Var(s2))                 => s1.name.substring(0, 3) == s2.name.substring(0, 3)
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

    def initial[A] = Map[Symbol, A]()

    def bindings[A](t: Exp[Term[Exp]], b: G[A])(f: Exp[Term[Exp]] => A) =
      t match {
        case Let(name, value, _) => b + (name -> f(value.unFix))
        case _                   => b
      }

    def subst[A](t: Exp[Term[Exp]], b: G[A]) = t match {
      case Var(symbol) => b.get(symbol)
      case _           => None
    }
  }

  type CofreeExp[A] = Cofree[Exp, A] // Attributed expression

  val example1ƒ: Exp[Option[Int]] => Option[Int] = {
    case Num(v)           => Some(v)
    case Mul(left, right) => (left |@| right)(_ * _)
    case Var(v)           => None
    case Lambda(_, b)     => b
    case Apply(func, arg) => None
    case Let(_, _, i)     => i
  }

  val addOne: Term[Exp] => Term[Exp] = _.unFix match {
    case Num(n) => num(n+1)
    case t => Term[Exp](t)
  }

  val simplify: Term[Exp] => Term[Exp] = _.unFix match {
    case Mul(Term(Num(0)), Term(Num(_))) => num(0)
    case Mul(Term(Num(1)), Term(Num(n))) => num(n)
    case Mul(Term(Num(_)), Term(Num(0))) => num(0)
    case Mul(Term(Num(n)), Term(Num(1))) => num(n)
    case t => Term[Exp](t)
  }

  val addOneOrSimplify: Term[Exp] => Term[Exp] = t => t.unFix match {
    case Num(_)    => addOne(t)
    case Mul(_, _) => simplify(t)
    case _ => t
  }

  "Term" should {
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

    "transform" should {
      "change simple literal" in {
        num(1).transform(addOne) must_== num(2)
      }

      "change sub-expressions" in {
        mul(num(1), num(2)).transform(addOne) must_== mul(num(2), num(3))
      }

      "be bottom-up" in {
        mul(num(0), num(1)).transform(addOneOrSimplify) must_== num(2)
        mul(num(1), num(2)).transform(addOneOrSimplify) must_== mul(num(2), num(3))
      }
    }

    "topDownTransform" should {
      "change simple literal" in {
        num(1).topDownTransform(addOne) must_== num(2)
      }

      "change sub-expressions" in {
        mul(num(1), num(2)).topDownTransform(addOne) must_== mul(num(2), num(3))
      }

      "be top-down" in {
        mul(num(0), num(1)).topDownTransform(addOneOrSimplify) must_== num(0)
        mul(num(1), num(2)).topDownTransform(addOneOrSimplify) must_== num(2)
      }
    }

    "foldMap" should {
      "fold stuff" in {
        mul(num(0), num(1)).foldMap(_ :: Nil) must_== mul(num(0), num(1)) :: num(0) :: num(1) :: Nil
      }
    }

    "descend" should {
      "not apply at the root" in {
        num(0).descend(addOne) must_== num(0)
      }

      "apply at children" in {
        mul(num(0), num(1)).descend(addOne) must_== mul(num(1), num(2))
      }

      "not apply below children" in {
        mul(num(0), mul(num(1), num(2))).descend(addOne) must_== mul(num(1), mul(num(1), num(2)))
      }
    }

    // NB: unlike most of the operators `descend` is not implemented with `descendM`
    "descendM" should {
      val addOneOpt: Term[Exp] => Option[Term[Exp]] = t => Some(addOne(t))

      "not apply at the root" in {
        num(0).descendM(addOneOpt) must_== Some(num(0))
      }

      "apply at children" in {
        mul(num(0), num(1)).descendM(addOneOpt) must_== Some(mul(num(1), num(2)))
      }

      "not apply below children" in {
        mul(num(0), mul(num(1), num(2))).descendM(addOneOpt) must_== Some(mul(num(1), mul(num(1), num(2))))
      }
    }

    "rewrite" should {
      "apply more than once" in {
        val f: PartialFunction[Term[Exp], Term[Exp]] = {
          case Term(Num(2)) => num(1)
          case Term(Num(1)) => num(0)
        }

        mul(num(2), num(3)).rewrite(f.lift) must_== mul(num(0), num(3))
      }
    }

    "restructure" should {
      type E[A] = (Exp[A], Int)
      def eval(t: Exp[Term[E]]): E[Term[E]] = t match {
        case Num(x) => (t, x)
        case Mul(Term((_, c1)), Term((_, c2))) => (t, c1 * c2)
        case _ => ???
      }

      "evaluate simple expr" in {
        val v = mul(num(1), mul(num(2), num(3)))
        v.restructure(eval).unFix._2 must_== 6
      }
    }

    def eval(t: Exp[Int]): Int = t match {
      case Num(x) => x
      case Mul(x, y) => x*y
      case _ => ???
    }

    def findConstants(t: Exp[List[Int]]): List[Int] = t match {
      case Num(x) => x :: Nil
      case _      => t.fold
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

    "zipCata" should {
      "both eval and find all constants" in {
        mul(num(5), num(2)).cata(zipCata(eval, findConstants)) must_==
          (10, List(5, 2))
      }
    }

    "liftPara" should {
      "behave like cata" in {
        val v = mul(num(1), mul(num(2), num(3)))
        v.para(liftPara(eval)) must_== v.cata(eval)
      }
    }

    "liftHisto" should {
      "behave like cata" in {
        val v = mul(num(1), mul(num(2), num(3)))
        v.histo(liftHisto(eval)) must_== v.cata(eval)
      }
    }

    "liftApo" should {
      "behave like ana" ! prop { (i: Int) =>
        apo(i)(liftApo(extractFactors)) must_== ana(i)(extractFactors)
      }
    }

    "liftFutu" should {
      "behave like ana" ! prop { (i: Int) =>
        futu(i)(liftFutu(extractFactors)) must_== ana(i)(extractFactors)
      }
    }

    "topDownCata" should {
      def subst(vars: Map[Symbol, Term[Exp]], t: Term[Exp]): (Map[Symbol, Term[Exp]], Term[Exp]) = t.unFix match {
        case Let(sym, value, body) => (vars + (sym -> value), body)

        case Var(sym) => (vars, vars.get(sym).getOrElse(t))

        case _ => (vars, t)
      }

      "bind vars" in {
        val v = let('x, num(1), mul(num(0), vari('x)))
        v.topDownCata(Map.empty[Symbol, Term[Exp]])(subst) must_== mul(num(0), num(1))
      }
    }

    "trans" should {
      // TODO
    }

    // Evaluate as usual, but trap 0*0 as a special case
    def peval(t: Exp[(Term[Exp], Int)]): Int = t match {
      case Mul((Term(Num(0)), _), (Term(Num(0)), _)) => -1
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

    "distCata" should {
      "behave like cata" in {
        val v = mul(num(0), mul(num(0), num(1)))
        v.gcata[Id, Int](distCata, eval) must_== v.cata(eval)
      }
    }

    "distPara" should {
      "behave like para" in {
        val v = mul(num(0), mul(num(0), num(1)))
        v.gcata[({ type λ[α] = (Term[Exp], α) })#λ, Int](distPara, peval) must_== v.para(peval)
      }
    }

    "apo" should {
      "pull out factors of two" in {
        def f(x: Int): Exp[Term[Exp] \/ Int] =
          if (x % 2 == 0) Mul(-\/(num(2)), \/-(x/2))
          else Num(x)

        apo(12)(f) must_== mul(num(2), mul(num(2), num(3)))
      }

      "construct factorial" in {
        def fact(x: Int): Exp[Term[Exp] \/ Int] =
          if (x > 1) Mul(-\/(num(x)), \/-(x-1))
          else Num(x)

        apo(4)(fact) must_== mul(num(4), mul(num(3), mul(num(2), num(1))))
      }
    }

    def extractFactors(x: Int): Exp[Int] =
      if (x > 2 && x % 2 == 0) Mul(2, x/2)
      else Num(x)

    "ana" should {
      "pull out factors of two" in {
        ana(12)(extractFactors) must_== mul(num(2), mul(num(2), num(3)))
      }
    }

    "distAna" should {
      "behave like ana" ! prop { (i: Int) =>
        gana[Id, Exp, Int](i)(distAna, extractFactors) must_== ana(i)(extractFactors)
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
    def partialEval(t: Exp[Cofree[Exp, Term[Exp]]]): Term[Exp] = t match {
      case Mul(x, y) => (x.head.unFix, y.head.unFix) match {
        case (Num(a), Num(b)) => num(a * b)
        case _                => Term(t.map(_.head))
      }
      case _ => Term(t.map(_.head))
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
        futu(8)(extract2and3) must_== mul(num(2), mul(num(2), num(2)))
      }

      "factor multiples of three" in {
        futu(81)(extract2and3) must_== mul(num(3), num(27))
      }

      "factor 3 within 2" in {
        futu(324)(extract2and3) must_== mul(num(2), mul(num(2), mul(num(3), num(27))))
      }
    }

    "chrono" should {
      "factor and partially eval" ! prop { (i: Int) =>
        chrono(i)(partialEval, extract2and3) must_== num(i)
      }
    }

    "RenderTree" should {
      import slamdata.engine.{RenderTree}
      "render nodes and leaves" in {
        mul(num(0), num(1)).shows must_==
          """Mul
            |├─ Num(0)
            |╰─ Num(1)""".stripMargin
      }
    }

    "Equal" should {
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
        def exp(x: Exp[Term[Exp]]) = x
        exp(Mul(num(1), vari('abc1))) ≠ exp(Num(1)) must beTrue
      }
    }
  }

  "Holes" should {
    "holes" should {
      "find none" in {
        holes(Num(0)) must_== Num(0)
      }

      "find and replace two children" in {
        (holes(mul(num(0), num(1)).unFix) match {
          case Mul((Term(Num(0)), f1), (Term(Num(1)), f2)) =>
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
        universe(attrSelf(exp)) must equal(exp.universe.map(attrSelf(_)))
      }
    }

    "forget" should {
      "forget unit" ! Prop.forAll(expGen) { exp =>
        forget(attrUnit(exp)) must_== exp
      }
    }

    "foldMap" should {
      "zeros" ! Prop.forAll(expGen) { exp =>
        Foldable[CofreeExp].foldMap(attrK(exp, 0))(_ :: Nil) must_== exp.universe.map(κ(0))
      }

      "selves" ! Prop.forAll(expGen) { exp =>
        Foldable[CofreeExp].foldMap(attrSelf(exp))(_ :: Nil) must_== exp.universe
      }
    }

    "RenderTree" should {
      "render simple nested expr" in {
        implicit def RU = new RenderTree[Unit] { def render(v: Unit) = Terminal(List("()"), None) }
        attrUnit(mul(num(0), num(1))).shows must_==
          """Mul
            |├─ Annotation
            |│  ╰─ ()
            |├─ Num(0)
            |│  ╰─ Annotation
            |│     ╰─ ()
            |╰─ Num(1)
            |   ╰─ Annotation
            |      ╰─ ()""".stripMargin
      }
    }

    "zip" should {
      "tuplify simple constants" ! Prop.forAll(expGen) { exp =>
        unsafeZip2(attrK(exp, 0), attrK(exp, 1)) must
          equal(attrK(exp, (0, 1)))
      }
    }

    "bound combinator" should {
      val Example2 = let('foo, num(5), mul(vari('foo), num(2)))

      "produce incorrect annotations when not used in let expression" in {
        Example2.cata(example1ƒ) must beNone
      }

      "produce correct annotations when used in let expression" in {
        boundCata(Example2)(example1ƒ) must beSome(10)
      }
    }
  }

  def expGen: Gen[Term[Exp]] = Gen.oneOf(
    Gen.choose(0, 10).flatMap(num),
    for {
      x <- Gen.choose(0, 10)
      y <- Gen.choose(0, 10)
    } yield mul(num(x), num(y)),
    for {
      x <- Gen.choose(0, 10)
      y <- Gen.choose(0, 10)
      z <- Gen.choose(0, 10)
    } yield mul(num(x), mul(num(y), num(z))))
}
