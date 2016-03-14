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

import matryoshka.Recursive.ops._

import java.lang.String
import scala.{Boolean, Function, Int, None, Option, Predef, Symbol, Unit}
import scala.collection.immutable.{List, Map, Nil}

import scalaz._, Scalaz._

package object exp {
  def num(v: Int) = Fix[Exp](Num(v))
  def mul(left: Fix[Exp], right: Fix[Exp]) = Fix[Exp](Mul(left, right))
  def vari(v: Symbol) = Fix[Exp](Var(v))
  def lam(param: Symbol, body: Fix[Exp]) = Fix[Exp](Lambda(param, body))
  def ap(func: Fix[Exp], arg: Fix[Exp]) = Fix[Exp](Apply(func, arg))
  def let(name: Symbol, v: Fix[Exp], inBody: Fix[Exp]) = Fix[Exp](Let(name, v, inBody))

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

  val MinusThree: Exp ~> Exp =
    new (Exp ~> Exp) {
      def apply[A](exp: Exp[A]): Exp[A] = exp match {
        case Num(x) => Num(x-3)
        case t      => t
      }
    }

  val eval: Algebra[Exp, Int] = {
    case Num(x)    => x
    case Mul(x, y) => x*y
    case _         => Predef.???
  }

  val findConstants: Exp[List[Int]] => List[Int] = {
    case Num(x) => x :: Nil
    case t      => t.fold
  }

  def extractFactors: Coalgebra[Exp, Int] = x =>
    if (x > 2 && x % 2 == 0) Mul(2, x/2)
    else Num(x)

  def subst[T[_[_]]: Recursive](vars: Map[Symbol, T[Exp]], t: T[Exp]):
      (Map[Symbol, T[Exp]], T[Exp]) = t.project match {
    case Let(sym, value, body) => (vars + ((sym, value)), body)
    case Var(sym)              => (vars, vars.get(sym).getOrElse(t))
    case _                     => (vars, t)
  }

  // Evaluate as usual, but trap 0*0 as a special case
  def peval[T[_[_]]: Recursive](t: Exp[(T[Exp], Int)]): Int = t match {
    case Mul((Proj(Num(0)), _), (Proj(Num(0)), _)) => -1
    case Mul((_           , x), (_,            y)) => x * y
    case Num(x)                                    => x
    case _                                         => Predef.???
  }

  def fact(x: Int): Exp[Fix[Exp] \/ Int] =
    if (x > 1) Mul(-\/(num(x)), \/-(x-1))
    else Num(x)

  def extractFactorsM(x: Int): Option[Exp[Int]] =
    if (x == 5) None else extractFactors(x).some

  def strings(t: Exp[(Int, String)]): String = t match {
    case Num(x) => x.toString
    case Mul((x, xs), (y, ys)) =>
      xs + " (" + x + ")" + ", " + ys + " (" + y + ")"
    case _ => Predef.???
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

  // FIXME: defining this as an algebra brings up the “cyclic aliasisng” issue
  def extract2and3: Int => Exp[Free[Exp, Int]] = x =>
  // factors all the way down
  if (x > 2 && x % 2 == 0) Mul(Free.point(2), Free.point(x/2))
  // factors once and then stops
  else if (x > 3 && x % 3 == 0)
    Mul(Free.liftF(Num(3)), Free.liftF(Num(x/3)))
  else Num(x)
}
