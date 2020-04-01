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

package matryoshka.patterns

import slamdata.Predef._
import matryoshka._

import scalaz._, Scalaz._

sealed abstract class NelF[A, B] {
  def head: A = this match {
    case InitF(a, _) => a
    case LastF(a)    => a
  }

  def tailOption: Option[B] = this match {
    case InitF(_, b) => some(b)
    case LastF(_)    => none
  }
}
final case class InitF[A, B](h: A, t: B) extends NelF[A, B]
final case class LastF[A, B](a: A)       extends NelF[A, B]

object NelF extends NelFInstances {
  def nelIso[A]: AlgebraIso[NelF[A, ?], NonEmptyList[A]] =
    AlgebraIso[NelF[A, ?], NonEmptyList[A]] {
      case InitF(h, t) => h <:: t
      case LastF(a)    => NonEmptyList(a)
    } {
      case NonEmptyList(a, ICons(b, cs)) => InitF(a, NonEmptyList.nel(b, cs))
      case NonEmptyList(a,       INil()) => LastF(a)
    }

  def find[A](p: A => Boolean): Algebra[NelF[A, ?], Option[A]] =
    l => if (p(l.head)) some(l.head) else l.tailOption.join
}

sealed abstract class NelFInstances {
  implicit def traverse[A]: Traverse[NelF[A, ?]] =
    bitraverse.rightTraverse[A]

  implicit val bitraverse: Bitraverse[NelF] =
    new Bitraverse[NelF] {
      def bitraverseImpl[G[_]: Applicative, A, B, C, D](
        fab: NelF[A, B])(
        f: A => G[C], g: B => G[D]
      ) =
        fab match {
          case InitF(a, b) => (f(a) |@| g(b))(InitF(_, _))
          case LastF(a)    => f(a) map (LastF(_))
        }
    }

  implicit def equal[A: Equal]: Delay[Equal, NelF[A, ?]] =
    new Delay[Equal, NelF[A, ?]] {
      def apply[B](eql: Equal[B]) = {
        implicit val eqlB: Equal[B] = eql
        Equal.equalBy {
          case InitF(a, b) => (a, b).right[A]
          case LastF(a)    => a.left[(A, B)]
        }
      }
    }

  implicit def show[A: Show]: Delay[Show, NelF[A, ?]] =
    new Delay[Show, NelF[A, ?]] {
      def apply[B](show: Show[B]) =
        Show.show {
          case InitF(a, b) => a.show ++ Cord("::") ++ show.show(b)
          case LastF(a)    => a.show
        }
    }
}
