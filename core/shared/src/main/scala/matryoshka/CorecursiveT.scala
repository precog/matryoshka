/*
 * Copyright 2014–2018 SlamData Inc.
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

/** This is a workaround for a certain use case (e.g.,
  * [[matryoshka.patterns.Diff]] and [[matryoshka.patterns.PotentialFailure]]).
  * Define an instance of this rather than [[Corecursive]] when possible.
  */
// NB: Not a `@typeclass` because we don’t want to inject these operations.
trait CorecursiveT[T[_[_]]] {
  def embedT[F[_]: Functor](t: F[T[F]]): T[F]

  def anaT[F[_]: Functor, A](a: A)(f: Coalgebra[F, A]): T[F] =
    hylo(a)(embedT[F], f)
}

object CorecursiveT {
  def apply[T[_[_]]](implicit instance: CorecursiveT[T]) = instance
}
