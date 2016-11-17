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

package matryoshka.data

import matryoshka._
import matryoshka.patterns.CoEnv

import scalaz._, Scalaz._

trait FreeInstances {
  // TODO: Remove the Functor constraint when we upgrade to Scalaz 7.2
  implicit def freeRecursive[F[_]: Functor, A]: Recursive.Aux[Free[F, A], CoEnv[A, F, ?]] =
    new Recursive[Free[F, A]] {
      type Base[B] = CoEnv[A, F, B]

      def project(t: Free[F, A])(implicit BF: Functor[Base]) =
        CoEnv(t.resume.swap)
    }

  implicit def freeCorecursive[F[_]: Functor, A]: Corecursive.Aux[Free[F, A], CoEnv[A, F, ?]] =
    new Corecursive[Free[F, A]] {
      type Base[B] = CoEnv[A, F, B]

      def embed(t: CoEnv[A, F, Free[F, A]])(implicit BF: Functor[Base]) =
        t.run.fold(_.point[Free[F, ?]], Free.liftF(_).join)
    }

  implicit def freeEqual[F[_]: Functor](implicit F: Delay[Equal, F]):
      Delay[Equal, Free[F, ?]] =
    new Delay[Equal, Free[F, ?]] {
      def apply[α](eq: Equal[α]) =
        Equal.equal((a, b) => (a.resume, b.resume) match {
          case (-\/(f1), -\/(f2)) =>
            F(freeEqual[F].apply(eq)).equal(f1, f2)
          case (\/-(a1), \/-(a2)) => eq.equal(a1, a2)
          case (_,       _)       => false
        })
    }

  implicit def freeShow[F[_]: Functor](implicit F: Delay[Show, F]):
      Delay[Show, Free[F, ?]] =
    new Delay[Show, Free[F, ?]] {
      def apply[A](s: Show[A]) =
        Show.show(_.resume.fold(F(freeShow[F].apply(s)).show(_), s.show(_)))
    }
}

object free extends FreeInstances
