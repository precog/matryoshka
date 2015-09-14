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

package quasar

import quasar.Predef._
import quasar.fp._

import scalaz._
import scalaz.concurrent._

object Errors {
  import scalaz.stream.Process

  type ETask[E, X] = EitherT[Task, E, X]

  implicit class PrOpsETask[E, O](self: Process[ETask[E, ?], O])
      extends PrOps[ETask[E, ?], O](self)

  implicit def ETaskCatchable[E] = new Catchable[ETask[E, ?]] {
    def attempt[A](f: ETask[E, A]) =
      EitherT(f.run.attempt.map(_.fold(
        e => \/-(-\/(e)),
        _.fold(-\/(_), x => \/-(\/-(x))))))

    def fail[A](err: Throwable) = EitherT.right(Task.fail(err))
  }

  def handle[E, A, B>:A](t: ETask[E, A])(f: PartialFunction[Throwable,B]):
      ETask[E, B] =
    ETaskCatchable[E].attempt(t) flatMap {
      case -\/(e) => liftE[E](f.lift(e) map (Task.now) getOrElse Task.fail(e))
      case \/-(a) => liftE[E](Task.now(a))
    }

  def handleWith[E, A, B>:A](t: ETask[E, A])(f: PartialFunction[Throwable, ETask[E, B]]):
      ETask[E, B] =
    ETaskCatchable[E].attempt(t) flatMap {
      case -\/(e) => f.lift(e) getOrElse liftE[E](Task.fail(e))
      case \/-(a) => liftE[E](Task.now(a))
    }

  def liftE[E] = new (Task ~> ETask[E, ?]) {
    def apply[T](t: Task[T]): ETask[E, T] = EitherT.right(t)
  }

  def convertError[E, F](f: E => F) = new (ETask[E, ?] ~> ETask[F, ?]) {
    def apply[A](t: ETask[E, A]) = t.leftMap(f)
  }
}
