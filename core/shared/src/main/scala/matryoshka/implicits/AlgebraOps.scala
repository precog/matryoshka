/*
 * Copyright 2020 Precog Data
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

package matryoshka.implicits

import matryoshka._

import scalaz._, Scalaz._

sealed class AlgebraOps[F[_], A](self: Algebra[F, A]) {
  def generalize[W[_]: Comonad](implicit F: Functor[F]): GAlgebra[W, F, A] =
    node => self(node ∘ (_.copoint))

  def generalizeM[M[_]: Applicative]: AlgebraM[M, F, A] =
    node => self(node).point[M]

  def generalizeElgot[W[_]: Comonad]: ElgotAlgebra[W, F, A] =
    w => self(w.copoint)

  def generalizeElgotM[W[_]: Comonad, M[_]: Monad]: ElgotAlgebraM[W, M, F, A] =
    w => self(w.copoint).point[M]
}
