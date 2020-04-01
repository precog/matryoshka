/*
 * Copyright 2020 Precog Data
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

import slamdata.Predef._

import matryoshka.exp._

import org.scalacheck._
import scalaz._, Scalaz._

package object helpers {
  def foldableCogen[F[_]: Foldable]: Delay[Cogen, F] =
    new Delay[Cogen, F] {
      def apply[A](cog: Cogen[A]): Cogen[F[A]] =
        Cogen((seed, value) => value.foldLeft(seed)(cog.perturb))
    }

  implicit val nonEmptyListCogen: Delay[Cogen, NonEmptyList] = foldableCogen

  implicit val optionEqualNT: Delay[Equal, Option] = new Delay[Equal, Option] {
    def apply[A](eq: Equal[A]) =
      Equal.equal {
        case (None,    None)    => true
        case (Some(a), Some(b)) => eq.equal(a, b)
        case (_,       _)       => false
      }
  }

  implicit val optionShowNT: Delay[Show, Option] = new Delay[Show, Option] {
    def apply[A](s: Show[A]) =
      Show.show(_.fold(Cord("None"))(Cord("Some(") ++ s.show(_) ++ Cord(")")))
  }

  implicit def nonEmptyListEqual: Delay[Equal, NonEmptyList] =
    new Delay[Equal, NonEmptyList] {
      def apply[A](eq: Equal[A]) = NonEmptyList.nonEmptyListEqual(eq)
    }

  def strings(t: Exp[(Int, String)]): String = t match {
    case Num(x) => x.toString
    case Mul((x, xs), (y, ys)) =>
      xs + " (" + x + ")" + ", " + ys + " (" + y + ")"
    case _ => ???
  }

  val eval: Algebra[Exp, Int] = {
    case Num(x)    => x
    case Mul(x, y) => x * y
    case _         => ???
  }

  // Evaluate as usual, but trap 0*0 as a special case
  def peval[T](t: Exp[(T, Int)])(implicit T: Recursive.Aux[T, Exp]): Int =
    t match {
      case Mul((Embed(Num(0)), _), (Embed(Num(0)), _)) => -1
      case Mul((_,             x), (_,             y)) => x * y
      case Num(x)                                      => x
      case _                                           => ???
    }

  def elgotStrings: ElgotAlgebra[(Int, ?), Exp, String] = {
    case (x, Num(xs)) if x == xs => x.toString
    case (x, Mul(xs, ys)) => "(" + xs + " * " + ys + " = " + x + ")"
    case _ => ???
  }

  def extractFactors: Coalgebra[Exp, Int] = x =>
    if (x > 2 && x % 2 == 0) Mul(2, x/2)
    else Num(x)
}
