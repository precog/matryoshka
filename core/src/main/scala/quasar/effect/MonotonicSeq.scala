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
import scalaz.{Lens => _, _}, Scalaz._
import scalaz.concurrent.Task

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
          case Next =>
            ref.modifyS(n => (n+1, n))
        }
      })
  }

  def stateMonotonicSeq[F[_]: Applicative, S](l: Lens[S, Long]) =
    new (MonotonicSeq ~> StateT[F, S, ?]) {
      def apply[A](fa: MonotonicSeq[A]): StateT[F, S, A] = fa match {
        case Next =>
          StateT[F, S, A](s => (l.modify(_+1)(s), l.get(s)).point[F])
      }
    }
}
