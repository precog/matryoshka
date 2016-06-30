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

package matryoshka.patterns

import matryoshka.Delay

import scalaz._, Scalaz._

/** Generally similar to CoEnv (Free), this has an additional `success` case
  * that indicates there’s no failure down to the leaves.
  */
sealed trait PotentialFailure[T[_[_]], F[_], E, A]
final case class Success[T[_[_]], F[_], E, A] private[patterns](v: T[F])
    extends PotentialFailure[T, F, E, A]
/** Akin to Free.point */
final case class Failure[T[_[_]], F[_], E, A] private[patterns](e: E)
    extends PotentialFailure[T, F, E, A]
/** Akin to Free.roll */
final case class PartialFailure[T[_[_]], F[_], E, A] private[patterns](v: F[A])
    extends PotentialFailure[T, F, E, A]

object PotentialFailure {
  implicit def potentialFailureEqual[T[_[_]], F[_], E: Equal](
    implicit T: Equal[T[F]], F: Delay[Equal, F]):
      Delay[Equal, PotentialFailure[T, F, E, ?]] =
    new Delay[Equal, PotentialFailure[T, F, E, ?]] {
      def apply[α](eq: Equal[α]) =
        Equal.equal {
          case (Success(v1),        Success(v2))        => T.equal(v1, v2)
          case (Failure(e1),        Failure(e2))        => e1 ≟ e2
          case (PartialFailure(v1), PartialFailure(v2)) => F(eq).equal(v1, v2)
          case (_,                  _)                  => false
        }
    }


  // TODO: implement low-prio Bifunctor and Bifoldable with looser constraint on F
  implicit def bitraverse[T[_[_]], F[_]: Traverse]:
      Bitraverse[PotentialFailure[T, F, ?, ?]] =
    new Bitraverse[PotentialFailure[T, F, ?, ?]] {
      def bitraverseImpl[G[_], A, B, C, D](
        fab: PotentialFailure[T, F, A, B])(
        f: A ⇒ G[C], g: B ⇒ G[D])(
        implicit G: Applicative[G]) =
        fab match {
          case Success(v)        => G.point(Success(v))
          case Failure(e)        => f(e) ∘ (Failure(_))
          case PartialFailure(v) => v.traverse(g) ∘ (PartialFailure(_))
        }
    }

  implicit def traverse[T[_[_]], F[_]: Traverse, E]:
      Traverse[PotentialFailure[T, F, E, ?]] =
    bitraverse[T, F].rightTraverse[E]
}
