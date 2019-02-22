/*
 * Copyright 2014–2018 SlamData Inc.
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

sealed trait Expr[A]

object Expr {

  case class Lit[A](value: Int) extends Expr[A]
  case class Add[A](x: A, y: A) extends Expr[A]
  case class Multiply[A](x: A, y: A) extends Expr[A]

  implicit val exprFunctor: Functor[Expr] = new Functor[Expr] {
    def map[A, B](expr: Expr[A])(f: A => B): Expr[B] = expr match {
      case Lit(v) => Lit(v)
      case Add(x, y) => Add(f(x), f(y))
      case Multiply(x, y) => Multiply(f(x), f(y))
    }
  }

  val evaluate: Algebra[Expr, Int] = {
    case Lit(v) => v
    case Add(x, y) => x + y
    case Multiply(x, y) => x * y
  }

  val show: Algebra[Expr, String] = {
    case Lit(v) => s"$v"
    case Add(x, y) => s"($x + $y)"
    case Multiply(x, y) => s"($x * $y)"
  }

  def lit(value: Int): Fix[Expr] = Fix(Lit(value))
  def multiply(x: Fix[Expr], y: Fix[Expr]): Fix[Expr] = Fix(Multiply(x, y))
  def add(x: Fix[Expr], y: Fix[Expr]): Fix[Expr] = Fix(Add(x, y))
}

class ExprSpec extends Specification {
  import Expr._

  val expr: Fix[Expr] = add(lit(5), multiply(lit(2), lit(4)))

  "should evaluate" >> {
    expr.cata(evaluate) should ===(13)
  }

  "should show" >> {
    expr.cata(show) must beEqualTo("(5 + (2 * 4))")
  }
}
