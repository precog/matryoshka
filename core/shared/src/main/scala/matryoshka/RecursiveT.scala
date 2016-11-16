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
import simulacrum.typeclass

/** This is a workaround for a certain use case (e.g.,
  * [[matryoshka.patterns.Diff]] and [[matryoshka.patterns.PotentialFailure]]).
  * Define an instance of this rather than [[Recursive]] when possible.
  */
@typeclass trait RecursiveT[T[_[_]]] {
  def projectT[F[_]: Functor](t: T[F]): F[T[F]]
  def cataT[F[_]: Functor, A](t: T[F])(f: Algebra[F, A]): A =
    f(projectT(t) ∘ (cataT(_)(f)))
}

object RecursiveT {
  def recursive[T[_[_]]: RecursiveT, F[_]]: Recursive.Aux[T[F], F] =
    new Recursive[T[F]] {
      type Base[A] = F[A]
      def project(t: T[F])(implicit F: Functor[F]) =
        RecursiveT[T].projectT[F](t)
      override def cata[A](t: T[F])(f: Algebra[F, A])(implicit F: Functor[F]) =
        RecursiveT[T].cataT[F, A](t)(f)
    }
}
