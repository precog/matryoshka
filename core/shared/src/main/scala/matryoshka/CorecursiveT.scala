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
  * Define an instance of this rather than [[Corecursive]] when possible.
  */
@typeclass trait CorecursiveT[T[_[_]]] {
  def embedT[F[_]: Functor](t: F[T[F]]): T[F]
  def anaT[F[_]: Functor, A](a: A)(f: Coalgebra[F, A]): T[F] =
    embedT(f(a) ∘ (anaT(_)(f)))
}

object CorecursiveT {
  def corecursive[T[_[_]]: CorecursiveT, F[_]]: Corecursive.Aux[T[F], F] =
    new Corecursive[T[F]] {
      type Base[A] = F[A]
      def embed(t: F[T[F]])(implicit F: Functor[F]) =
        CorecursiveT[T].embedT[F](t)
      override def ana[A](a: A)(f: Coalgebra[F, A])(implicit F: Functor[F]) =
        CorecursiveT[T].anaT[F, A](a)(f)
    }
}
