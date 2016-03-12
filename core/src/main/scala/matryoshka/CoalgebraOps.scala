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

final class GElgotCoalgebraOps[E[_], G[_], F[_], A](self: GElgotCoalgebra[E, G, F, A]) {
  def generalizeM[M[_]: Applicative]:
      GElgotCoalgebraM[E, G, M, F, A] =
    matryoshka.generalizeM[M, A, E[F[G[A]]]](self)
}

final class ElgotCoalgebraMOps[E[_], M[_], F[_], A](self: ElgotCoalgebraM[E, M, F, A]) {
  def generalize[G[_]: Applicative](implicit MEF: Functor[λ[α => M[E[F[α]]]]]): GElgotCoalgebraM[E, G, M, F, A] =
    matryoshka.generalizeCoalgebra[λ[α => M[E[F[α]]]], G, Id, A](self)
}

final class GCoalgebraMOps[G[_], M[_], F[_], A](self: GCoalgebraM[G, M, F, A]) {
  def generalizeElgot[E[_]: Applicative](implicit M: Functor[M]): GElgotCoalgebraM[E, G, M, F, A] =
    matryoshka.generalizeCoalgebra[M, E, λ[α => F[G[α]]], A](self)
}
