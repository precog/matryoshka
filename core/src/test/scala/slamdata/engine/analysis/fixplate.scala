package slamdata.engine.analysis

import fixplate._

import slamdata.engine.fp._

import scalaz._
import scalaz.syntax.apply._

import scalaz.std.option._
import scalaz.std.map._
import scalaz.std.anyVal._

import org.specs2.mutable._

class FixplateSpecs extends Specification {
  sealed trait Exp[+A]
  case class Num(value: Int) extends Exp[Nothing]
  case class Mul[A](left: A, right: A) extends Exp[A]
  case class Var(value: Symbol) extends Exp[Nothing]
  case class Lambda[A](param: Symbol, body: A) extends Exp[A]
  case class Apply[A](func: A, arg: A) extends Exp[A]
  case class Let[A](name: Symbol, value: A, inBody: A) extends Exp[A]

  def int(v: Int) = Term[Exp](Num(v))
  def mul(left: Term[Exp], right: Term[Exp]) = Term[Exp](Mul(left, right))
  def vari(v: Symbol) = Term[Exp](Var(v))
  def lam(param: Symbol, body: Term[Exp]) = Term[Exp](Lambda(param, body))
  def ap(func: Term[Exp], arg: Term[Exp]) = Term[Exp](Apply(func, arg))
  def let(name: Symbol, v: Term[Exp], inBody: Term[Exp]) = Term[Exp](Let(name, v, inBody))

  implicit val ExpTraverse: Traverse[Exp] = new Traverse[Exp] {
    def traverseImpl[G[_], A, B](fa: Exp[A])(f: A => G[B])(implicit G: Applicative[G]): G[Exp[B]] = fa match {
      case Num(v) => G.point(Num(v))
      case Mul(left, right) => G.apply2(f(left), f(right))(Mul(_, _))
      case Var(v) => G.point(Var(v))
      case Lambda(p, b) => G.map(f(b))(Lambda(p, _))
      case Apply(func, arg) => G.apply2(f(func), f(arg))(Apply(_, _))
      case Let(n, v, i) => G.apply2(f(v), f(i))(Let(n, _, _))
    }
  }

  implicit val ExpShow: Show[Exp[_]] = new Show[Exp[_]] {
    override def show(v: Exp[_]): Cord = v match {
      case Num(value) => Show[Int].show(value)
      case Mul(left, right) => Cord("Mul")
      case Var(sym) => Cord(sym.toString)
      case Lambda(param, body) => Cord("Lambda(" + param.toString + ")")
      case Apply(func, arg) => Cord("Apply")
      case Let(name, _, _) => Cord("Let(" + name + ")")
    }
  }

  implicit val ExpBinder: Binder[Exp, ({type f[A] = Map[Symbol, Attr[Exp, A]]})#f] = {
    type AttrExp[A] = Attr[Exp, A] // Attributed expression
    type MapSymbol[A] = Map[Symbol, AttrExp[A]]

    new Binder[Exp, MapSymbol] {
      val bindings = new NaturalTransformation[AttrExp, MapSymbol] {
        def apply[A](attrexpa: Attr[Exp, A]): MapSymbol[A] = {
          attrexpa.unFix.unAnn match {
            case Let(name, value, _) => Map(name -> value)
            case _ => Map()
          }
        }
      }
      val subst = new NaturalTransformation[`AttrF * G`, Subst] {
        def apply[A](fa: `AttrF * G`[A]): Subst[A] = {
          val (attr, map) = fa

          attr.unFix.unAnn match {
            case Var(symbol) => map.get(symbol).map(exp => (exp, new Forall[Unsubst] { def apply[A] = { (a: A) => attrK(vari(symbol), a) } }))
            case _ => None
          }
        }
      }
    }
  }

  def ExamplePhase1[A] = Phase[Exp, A, Option[Int]] { attrfa =>
    scanCata(attrfa) { (a: A, foi: Exp[Option[Int]]) => 
      foi match {
        case Num(v) => Some(v)
        case Mul(left, right) => (left |@| right)(_ * _)
        case Var(v) => None
        case Lambda(p, b) => b
        case Apply(func, arg) => None
        case Let(n, v, i) => i
      }
    }
  }

  def bindExp[A, B](phase: Phase[Exp, A, B]): Phase[Exp, A, B] = {
    type MapSymbol[A] = Map[Symbol, Attr[Exp, A]]

    implicit val sg = Semigroup.lastSemigroup[Attr[Exp, A]]

    bound[Id.Id, Exp, MapSymbol, A, B](phase)
  }

  val Example1 = mul(int(5), int(2))
  val Example2 = let('foo, int(5), mul(vari('foo), int(2)))

  "scanCata" should {
    "produce correct annotations for 5 * 2" in {
      val rez = (ExamplePhase1[Unit])(attrUnit(Example1))

      rez.unFix.attr must beSome(10)
    }
  }

  "bound combinator" should {
    "produce incorrect annotations when not used in let expression" in {
      val rez = (ExamplePhase1[Unit])(attrUnit(Example2))

      rez.unFix.attr must beNone
    }

    "produce correct annotations when used in let expression" in {
      val rez = bindExp(ExamplePhase1[Unit])(attrUnit(Example2))

      // println(Show[Attr[Exp, Option[Int]]].show(rez))

      rez.unFix.attr must beSome(10)
    }
  }
}