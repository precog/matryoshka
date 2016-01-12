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

package quasar.effect

import quasar.Predef._
import quasar.fp.TaskRef

import monocle.Lens
import scalaz.{Lens => _, _}
import scalaz.concurrent.Task
import scalaz.syntax.applicative._

/** Provides the ability to request the next element of a monotonically
  * increasing numeric sequence.
  *
  * That is,
  *
  *   for {
  *     a <- next
  *     b <- next
  *   } yield a < b
  *
  * must always be true.
  */
sealed trait MonotonicSeq[A]

object MonotonicSeq {
  case object Next extends MonotonicSeq[Long]

  @SuppressWarnings(Array("org.brianmckenna.wartremover.warts.NonUnitStatements"))
  final class Ops[S[_]: Functor](implicit S: MonotonicSeqF :<: S)
    extends LiftedOps[MonotonicSeq, S] {

    def next: F[Long] =
      lift(Next)
  }

  object Ops {
    def apply[S[_]: Functor](implicit S: MonotonicSeqF :<: S): Ops[S] =
      new Ops[S]
  }

  def taskRefMonotonicSeq(initial: Long): Task[MonotonicSeq ~> Task] = {
    TaskRef(initial).map(ref =>
      new (MonotonicSeq ~> Task) {
        def apply[A](fa: MonotonicSeq[A]): Task[A] = fa match {
          case Next => ref.modifyS(n => (n+1, n))
        }
      })
  }

  /** Returns an interpreter of `MonotonicSeq` into `StateT[F, S, ?]`,
    * given a `Lens[S, Long]`.
    *
    * NB: Uses partial application of `F[_]` for better type inference, usage:
    *
    *   `stateMonotonicSeq[F](lens)`
    */
  object stateMonotonicSeq {
    def apply[F[_]]: Aux[F] =
      new Aux[F]

    final class Aux[F[_]] {
      def apply[S](l: Lens[S, Long])(implicit F: Applicative[F])
                  : MonotonicSeq ~> StateT[F, S, ?] = {
        new (MonotonicSeq ~> StateT[F, S, ?]) {
          def apply[A](seq: MonotonicSeq[A]) = seq match {
            case Next => StateT((s: S) => (l.modify(_ + 1)(s), l.get(s)).point[F])
          }
        }
      }
    }
  }
}
