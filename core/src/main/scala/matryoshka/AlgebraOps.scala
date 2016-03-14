/*
 * Copyright 2014 - 2015 SlamData Inc.
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

final class GAlgebraMOps[G[_], M[_], F[_], A](self: GAlgebraM[G, M, F, A]) {
  def generalizeElgot[E[_]: Comonad]: GElgotAlgebraM[E, G, M, F, A] =
    matryoshka.generalizeW[E, F[G[A]], M[A]](self)
}

final class ElgotAlgebraMOps[E[_], M[_], F[_], A](self: ElgotAlgebraM[E, M, F, A]) {
  def generalize[G[_]: Comonad](implicit EF: Functor[λ[α => E[F[α]]]]):
      GElgotAlgebraM[E, G, M, F, A] =
    matryoshka.generalizeAlgebra[λ[α => E[F[α]]], G, Id, M, F, A](self)
}

final class GElgotAlgebraOps[E[_], G[_], F[_], A](self: GElgotAlgebra[E, G, F, A]) {
  def generalizeM[M[_]: Applicative]: GElgotAlgebraM[E, G, M, F, A] =
    matryoshka.generalizeM[M, E[F[G[A]]], A](self)
}
