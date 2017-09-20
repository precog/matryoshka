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
import scala.math.exp

/**
  * A mathematical expression of a single variable x.
  *
  * This can be differentiated using catamorphisms.
  *
  * Automatic Differentiation ported from [[https://jtobin.io/ad-via-recursion-schemes Jared Tobin's Automatic Differentiation]]
  */
sealed trait MathExprF[A]

object MathExprF {
  case class VarF[A]() extends MathExprF[A]
  case class ZeroF[A]() extends MathExprF[A]
  case class OneF[A]() extends MathExprF[A]
  case class NegateF[A](a: A) extends MathExprF[A]
  case class SumF[A](l: A, r: A) extends MathExprF[A]
  case class ProductF[A](l: A, r: A) extends MathExprF[A]
  case class ExpF[A](a: A) extends MathExprF[A]

  type Expr = Fix[MathExprF]

  implicit val exprFunctor: Functor[MathExprF] = new Functor[MathExprF] {
    def map[A, B](fa: MathExprF[A])(f: A => B): MathExprF[B] = fa match {
      case VarF() => VarF()
      case ZeroF() => ZeroF()
      case OneF() => OneF()
      case NegateF(a) => NegateF(f(a))
      case SumF(l, r) => SumF(f(l), f(r))
      case ProductF(l, r) => ProductF(f(l), f(r))
      case ExpF(a) => ExpF(f(a))
    }
  }

  val varExpr: Expr = Fix(VarF())
  val zero: Expr = Fix(ZeroF())
  val one: Expr = Fix(OneF())
  def neg(expr: Expr): Expr = Fix(NegateF(expr))
  def add(l: Expr, r: Expr): Expr = Fix(SumF( l, r ))
  def prod(l: Expr, r: Expr): Expr = Fix(ProductF( l, r ))
  def e(expr: Expr): Expr = Fix(ExpF(expr))

  def evalAlgebra(x: Double): MathExprF[Double] => Double = {
      case VarF() => x
      case ZeroF() => 0
      case OneF() => 1
      case NegateF(a) => - a
      case SumF(l, r) => l + r
      case ProductF(l, r) => l * r
      case ExpF(a) => exp(a)
  }

  def eval(x: Double, expr: Expr): Double =
    expr.cata(evalAlgebra(x))

  def showAlgebra: Algebra[MathExprF, String] = {
    case VarF() => "x"
    case ZeroF() => "0"
    case OneF() => "1"
    case NegateF(x) => s"-$x"
    case SumF(l, r) => s"($l + $r)"
    case ProductF(l, r) => s"($l * $r)"
    case ExpF(x) => s"e($x)"
  }

  def diffGAlgebra: GAlgebra[(Expr, ?), MathExprF, Fix[MathExprF]] = {
    case VarF() => one
    case ZeroF() => zero
    case OneF() => zero
    case NegateF((_, a)) => neg(a)
    case SumF((_, l), (_, r)) => add(l, r)
    case ProductF((l, ll), (r, rr)) => add(prod(l, rr),  prod(r, ll))
    case ExpF((x, xx)) => prod(e(x), xx)
  }

  def diff(expr: Expr): Expr = expr.para(diffGAlgebra)

  def adAlgebra(x: Double): Algebra[MathExprF, (Double, Double)] = {
      case VarF() => (x, 1)
      case ZeroF() => (0, 0)
      case OneF() => (1, 0)
      case NegateF((x, dx)) => (-x, -dx)
      case SumF((l, dl), (r, dr)) => (l + r, dl + dr)
      case ProductF((l, dl), (r, dr)) => (l * r, l * dr + r * dl)
      case ExpF((x, dx)) => (exp(x), exp(x) * dx)
  }

  def automaticDifferentiation(x: Double, expr: Expr): (Double, Double) =
    expr.cata(adAlgebra(x))
}

class MathExprSpec extends Specification {
  import MathExprF._

  //d exp(x*x - 1) / dx = 2*x*exp(x*x - 1)
  val expr: Expr = e(add(prod(varExpr, varExpr), neg(one)))

  "should differentiate a value automatically" >> {
    automaticDifferentiation(1.0, expr) must beEqualTo((1.0, 2.0))
  }
}
