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

import matryoshka.Recursive.ops._
import matryoshka.algebra._

import java.lang.String
import scala.{Boolean, Function, Int, None, Option, Predef, Symbol, Unit},
  Predef.{implicitly, wrapString}
import scala.collection.immutable.{List, Map, Nil, ::}

import org.specs2.ScalaCheck
import scalaz._, Scalaz._

package object exp {
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

  def addOneƒ[T[_[_]]]: Exp[T[Exp]] => Exp[T[Exp]] = once(addOneOptƒ)

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

  def subst[T[_[_]]: Recursive](vars: Map[Symbol, T[Exp]], t: T[Exp]):
      (Map[Symbol, T[Exp]], T[Exp]) = t.project match {
    case Let(sym, value, body) => (vars + ((sym, value)), body)
    case Var(sym)              => (vars, vars.get(sym).getOrElse(t))
    case _                     => (vars, t)
  }

  val eval: Algebra[Exp, Int] = {
    case Num(x) => x
    case Mul(x, y) => x*y
    case _ => Predef.???
  }

  // Evaluate as usual, but trap 0*0 as a special case
  def peval[T[_[_]]: Recursive](t: Exp[(T[Exp], Int)]): Int = t match {
    case Mul((a, x), (b, y)) => (a.project, b.project) match {
      case (Num(0), Num(0)) => -1
      case (_,      _)      => x * y
    }
    case Num(x) => x
    case _ => Predef.???
  }

  // NB: This is better done with cata, but we fake it here
  def partialEval[T[_[_]]: Corecursive: Recursive]:
      GAlgebra[Cofree[Exp, ?], Exp, T[Exp]] = t =>
    t match {
      case Mul(x, y) => (x.head.project, y.head.project) match {
        case (Num(a), Num(b)) => Num[T[Exp]](a * b).embed
        case _                => t.map(_.head).embed
      }
      case _ => t.map(_.head).embed
    }

  val extract2and3: GCoalgebra[Free[Exp, ?], Exp, Int] = x =>
    // factors all the way down
    if (x > 2 && x % 2 == 0) Mul(Free.point(2), Free.point(x/2))
    // factors once and then stops
    else if (x > 3 && x % 3 == 0)
      Mul(Free.liftF(Num(3)), Free.liftF(Num(x/3)))
    else Num(x)


  val extractFactors: Coalgebra[Exp, Int] = x =>
    if (x > 2 && x % 2 == 0) Mul(2, x/2)
    else Num(x)

  val findConstants: Algebra[Exp, List[Int]] = {
    case Num(x) => x :: Nil
    case t      => t.fold
  }

  /** These are little recursive smart constructors that are helpful when
    * writing tests, as they build up a fixpoint structure directly. Generally
    * not useful in generic code, though.
    */
  def num(v: Int) = Fix[Exp](Num(v))
  def mul(left: Fix[Exp], right: Fix[Exp]) = Fix[Exp](Mul(left, right))
  def vari(v: Symbol) = Fix[Exp](Var(v))
  def lam(param: Symbol, body: Fix[Exp]) = Fix[Exp](Lambda(param, body))
  def ap(func: Fix[Exp], arg: Fix[Exp]) = Fix[Exp](Apply(func, arg))
  def let(name: Symbol, v: Fix[Exp], inBody: Fix[Exp]) = Fix[Exp](Let(name, v, inBody))
}
