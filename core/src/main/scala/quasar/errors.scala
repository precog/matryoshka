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
import scalaz.syntax.monad._
import scalaz.syntax.std.option._

object Errors {
  import scalaz.stream.Process

  type ETask[E, X] = EitherT[Task, E, X]

  implicit class PrOpsETask[E, O](self: Process[ETask[E, ?], O])
      extends PrOps[ETask[E, ?], O](self)

  def handle[E, A, B>:A](t: ETask[E, A])(f: PartialFunction[Throwable, B]):
      ETask[E, B] = {
    type G[F[_], X] = EitherT[F, E, X]
    Catchable[G[Task, ?]].attempt(t) flatMap {
      case -\/(t) => f.lift(t).cata(Task.now, Task.fail(t)).liftM[G]
      case \/-(a) => Applicative[G[Task, ?]].point(a)
    }
  }

  def handleWith[E, A, B>:A](t: ETask[E, A])(f: PartialFunction[Throwable, ETask[E, B]]):
      ETask[E, B] = {
    type G[F[_], X] = EitherT[F, E, X]
    Catchable[G[Task, ?]].attempt(t) flatMap {
      case -\/(t) => f.lift(t) getOrElse liftE(Task.fail(t))
      case \/-(a) => Applicative[G[Task, ?]].point(a)
    }
  }

  def liftE[E]: (Task ~> ETask[E, ?]) = {
    type G[F[_], A] = EitherT[F, E, A]
    liftMT[Task, G]
  }

  def convertError[E, F](f: E => F) = new (ETask[E, ?] ~> ETask[F, ?]) {
    def apply[A](t: ETask[E, A]) = t.leftMap(f)
  }
}
