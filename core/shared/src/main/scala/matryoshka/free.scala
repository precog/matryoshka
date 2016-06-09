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

trait FreeInstances {
  implicit def freeCorecursive[A]: Corecursive[Free[?[_], A]] =
    new Corecursive[Free[?[_], A]] {
      def embed[F[_]: Functor](t: F[Free[F, A]]) = Free.liftF(t).join
    }

  implicit def freeTraverseT[A]: TraverseT[Free[?[_], A]] =
    new TraverseT[Free[?[_], A]] {
      def traverse[M[_]: Applicative, F[_]: Functor, G[_]: Functor](t: Free[F, A])(f: F[Free[F, A]] => M[G[Free[G, A]]]) =
        t.fold(
          _.point[Free[G, ?]].point[M],
          f(_).map(Free.liftF(_).join))
    }

  implicit def freeEqual[F[_]: Functor](
    implicit F: Equal ~> λ[α => Equal[F[α]]]):
      Equal ~> λ[α => Equal[Free[F, α]]] =
    new (Equal ~> λ[α => Equal[Free[F, α]]]) {
      def apply[α](eq: Equal[α]) =
        Equal.equal((a, b) => (a.resume, b.resume) match {
          case (-\/(f1), -\/(f2)) =>
            F(freeEqual[F].apply(eq)).equal(f1, f2)
          case (\/-(a1), \/-(a2)) => eq.equal(a1, a2)
          case (_,       _)       => false
        })
    }
}

object free extends FreeInstances
