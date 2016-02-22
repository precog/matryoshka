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

sealed class AlgebraOps[F[_], A](self: Algebra[F, A]) {
  def generalize[W[_]: Comonad](implicit F: Functor[F]): GAlgebra[W, F, A] =
    algebra.generalizeAlgebra[W, F, A](self)

  def generalizeM[M[_]: Monad]: AlgebraM[M, F, A] =
    algebra.generalizeAlgebraM[M, F, A](self)

  def generalizeElgot[W[_]: Comonad]: ElgotAlgebra[W, F, A] =
    algebra.generalizeElgotAlgebra[W, F, A](self)

  def generalizeElgotM[W[_]: Comonad, M[_]: Applicative]:
      ElgotAlgebraM[W, M, F, A] =
    algebra.generalizeElgotAlgebraM[W, M, F, A](self)

  def attribute(implicit F: Functor[F]): Algebra[F, Cofree[F, A]] =
    algebra.attribute(self)
}
