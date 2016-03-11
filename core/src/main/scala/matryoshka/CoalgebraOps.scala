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

import scalaz._

sealed class GCoalgebraOps[M[_], F[_], A](self: GCoalgebra[M, F, A]) {
  def generalizeM[N[_]: Applicative]:
      GCoalgebraM[M, N, F, A] =
    matryoshka.generalizeM[N, A, F[M[A]]](self)
}

sealed class ElgotCoalgebraOps[M[_], F[_], A](self: ElgotCoalgebra[M, F, A]) {
  def generalizeM[N[_]: Applicative]:
      ElgotCoalgebraM[M, N, F, A] =
    matryoshka.generalizeM[N, A, M[F[A]]](self)
}

sealed class CoalgebraMOps[M[_], F[_], A](self: CoalgebraM[M, F, A]) {
  def generalize[N[_]: Applicative](implicit M: Functor[M], F: Functor[F]):
      GCoalgebraM[N, M, F, A] =
    matryoshka.generalizeCoalgebraM[M, N, F, A](self)
}

sealed class CoalgebraOps[F[_], A](self: Coalgebra[F, A]) {
  def generalize[M[_]: Applicative](implicit F: Functor[F]):
      GCoalgebra[M, F, A] =
    matryoshka.generalizeCoalgebra[M, F, A](self)

  def generalizeM[M[_]: Applicative]: CoalgebraM[M, F, A] =
    matryoshka.generalizeM[M, A, F[A]](self)

  def generalizeElgot[M[_]: Monad]: CoalgebraM[M, F, A] =
    matryoshka.generalizeM[M, A, F[A]](self)
}
