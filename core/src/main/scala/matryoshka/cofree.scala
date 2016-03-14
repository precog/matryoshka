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

import scalaz._
import scalaz.syntax.functor._
import scalaz.syntax.show._

trait CofreeInstances {
  implicit def cofreeRecursive[A]: Recursive[Cofree[?[_], A]] =
    new Recursive[Cofree[?[_], A]] {
      def project[F[_]: Functor](t: Cofree[F, A]) = t.tail
    }

  implicit def cofreeCorecursive[A: Monoid]: Corecursive[Cofree[?[_], A]] =
    new Corecursive[Cofree[?[_], A]] {
      def embed[F[_]: Functor](t: F[Cofree[F, A]]) = Cofree(Monoid[A].zero, t)
    }

  implicit def cofreeTraverseT[A]: TraverseT[Cofree[?[_], A]] =
    new TraverseT[Cofree[?[_], A]] {
      def traverse[M[_]: Applicative, F[_]: Functor, G[_]: Functor](t: Cofree[F, A])(f: F[Cofree[F, A]] => M[G[Cofree[G, A]]]) =
        f(t.tail).map(Cofree(t.head, _))
    }

  implicit def cofreeShow[F[_], A: Show](implicit F: (Show ~> λ[α => Show[F[α]]])):
      Show[Cofree[F, A]] =
        Show.shows(cof => "(" + cof.head.show + ", " + F(cofreeShow).shows(cof.tail) + ")")
}

object cofree extends CofreeInstances
