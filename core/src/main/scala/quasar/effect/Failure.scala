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

import scalaz._
import scalaz.syntax.monad._

/** Provides the ability to indicate a computation has failed.
  *
  * @tparam E the reason/error describing why the computation failed
  */
sealed trait Failure[E, A]

object Failure {
  final case class Fail[E, A](e: E) extends Failure[E, A]

  @SuppressWarnings(Array("org.brianmckenna.wartremover.warts.NonUnitStatements"))
  final class Ops[E, S[_]: Functor](implicit S: FailureF[E, ?] :<: S)
    extends LiftedOps[Failure[E, ?], S] {

    def attempt[A](fa: F[A]): F[E \/ A] =
      fa.foldMap(attempt0).run

    def fail[A](e: E): F[A] =
      lift(Fail(e))

    def onFinish[A](fa: F[A], f: Option[E] => F[Unit]): F[A] =
      attempt(fa).flatMap(_.fold(
        e => f(Some(e)) *> fail(e),
        a => f(None)    as a))

    def recover[A](fa: F[A], f: E => F[A]): F[A] =
      attempt(fa).flatMap(_.fold(f, _.point[F]))

    def unattempt[A](fa: F[E \/ A]): F[A] =
      fa.flatMap(_.fold(fail, _.point[F]))

    ////

    private type Err[A]      = Failure[E, A]
    private type ErrF[A]     = FailureF[E, A]
    private type G[A]        = EitherT[F, E, A]
    private type GT[X[_], A] = EitherT[X, E, A]
    private type GE[A, B]    = EitherT[F, A, B]

    private val attemptE: ErrF ~> G = {
      val g: Err ~> G = new (Err ~> G) {
        val err = MonadError[GE, E]
        def apply[A](ea: Err[A]) = ea match {
          case Fail(e) => err.raiseError[A](e)
        }
      }
      Coyoneda.liftTF(g)
    }

    private val attempt0: S ~> G = new (S ~> G) {
      def apply[A](sa: S[A]) =
        S.prj(sa) match {
          case Some(errF) => attemptE(errF)
          case None       => Free.liftF(sa).liftM[GT]
        }
    }
  }

  object Ops {
    def apply[E, S[_]: Functor](implicit S: FailureF[E, ?] :<: S): Ops[E, S] =
      new Ops[E, S]
  }
}
