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

import scalaz._, Scalaz._

/** The pattern functor for Free. */
final case class CoEnv[E, F[_], A](run: E \/ F[A])
object CoEnv {
  // TODO: Need to have lower-prio instances of Functor and Foldable, with
  //       corresponding constraints on F.
  implicit def traverse[F[_]: Traverse, A]: Traverse[CoEnv[A, F, ?]] =
    new Traverse[CoEnv[A, F, ?]] {
      def traverseImpl[G[_], R, B](
        fa: CoEnv[A, F, R])(
        f: R => G[B])(
        implicit G: Applicative[G]):
          G[CoEnv[A, F, B]] =
        fa.run.traverse(_.traverse(f)).map(CoEnv(_))
    }

  implicit def monad[F[_]: Monad: Traverse, A]: Monad[CoEnv[A, F, ?]] =
    new Monad[CoEnv[A, F, ?]] {
      def bind[B, C](fa: CoEnv[A, F, B])(f: (B) ⇒ CoEnv[A, F, C]) =
        CoEnv(fa.run.fold(_.left, _.traverse[CoEnv[A, F, ?], C](f).run.map(_.join)))
      def point[B](x: => B) = CoEnv(x.point[F].right)
    }

  implicit def monadCo[F[_]: Applicative: Comonad, A]: Monad[CoEnv[A, F, ?]] =
    new Monad[CoEnv[A, F, ?]] {
      def bind[B, C](fa: CoEnv[A, F, B])(f: (B) ⇒ CoEnv[A, F, C]) =
        fa.run.fold(a => CoEnv(a.left), fb => f(fb.copoint))
      def point[B](x: => B) = CoEnv(x.point[F].right)
    }
}
