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

package matryoshka.patterns

import slamdata.Predef._
import matryoshka._

import monocle.Iso
import scalaz._, Scalaz._

sealed abstract class AndMaybe[A, B] {
  def head: A = this match {
    case Indeed(a, _) => a
    case Only(a)      => a
  }

  def tailOption: Option[B] = this match {
    case Indeed(_, b) => some(b)
    case Only(_)      => none
  }
}
final case class Indeed[A, B](h: A, t: B) extends AndMaybe[A, B]
final case class Only[A, B](a: A)         extends AndMaybe[A, B]

object AndMaybe extends AndMaybeInstances {
  def envTIso[A, B] = Iso[AndMaybe[A, B], EnvT[A, Option, B]] {
    case Indeed(h, t) => EnvT((h, t.some))
    case Only(h)      => EnvT((h, none))
  } (envt => envt.lower.fold[AndMaybe[A, B]](Only(envt.ask))(Indeed(envt.ask, _)))

  def nelIso[A]: AlgebraIso[AndMaybe[A, ?], NonEmptyList[A]] =
    AlgebraIso[AndMaybe[A, ?], NonEmptyList[A]] {
      case Indeed(h, t) => h <:: t
      case Only(a)    => NonEmptyList(a)
    } {
      case NonEmptyList(a, ICons(b, cs)) => Indeed(a, NonEmptyList.nel(b, cs))
      case NonEmptyList(a,       INil()) => Only(a)
    }

  def find[A](p: A => Boolean): Algebra[AndMaybe[A, ?], Option[A]] =
    l => if (p(l.head)) some(l.head) else l.tailOption.join
}

sealed abstract class AndMaybeInstances {
  implicit def traverse[A]: Traverse[AndMaybe[A, ?]] =
    bitraverse.rightTraverse[A]

  implicit val bitraverse: Bitraverse[AndMaybe] =
    new Bitraverse[AndMaybe] {
      def bitraverseImpl[G[_]: Applicative, A, B, C, D](
        fab: AndMaybe[A, B])(
        f: A => G[C], g: B => G[D]
      ) =
        fab match {
          case Indeed(a, b) => (f(a) |@| g(b))(Indeed(_, _))
          case Only(a)    => f(a) map (Only(_))
        }
    }

  implicit def equal[A: Equal]: Delay[Equal, AndMaybe[A, ?]] =
    new Delay[Equal, AndMaybe[A, ?]] {
      def apply[B](eql: Equal[B]) = {
        implicit val eqlB: Equal[B] = eql
        Equal.equalBy {
          case Indeed(a, b) => (a, b).right[A]
          case Only(a)    => a.left[(A, B)]
        }
      }
    }

  implicit def show[A: Show]: Delay[Show, AndMaybe[A, ?]] =
    new Delay[Show, AndMaybe[A, ?]] {
      def apply[B](show: Show[B]) =
        Show.show {
          case Indeed(a, b) => a.show ++ Cord("::") ++ show.show(b)
          case Only(a)    => a.show
        }
    }
}
