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

sealed class TransformationalCoalgebraOps[T[_[_]], M[_], F[_], G[_]](
  self: GCoalgebra[M, G, T[F]]) {
  def toTransformation(implicit T: Corecursive[T], F: Functor[F]):
      F[T[F]] => G[M[T[F]]] =
    algebra.coalgebraToTransformation[T, M, F, G](self)
}
