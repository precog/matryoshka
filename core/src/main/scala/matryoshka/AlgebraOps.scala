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

final class GAlgebraMOps[W[_], M[_], F[_], A](self: GAlgebraM[W, M, F, A]) {
  def attribute(implicit W: Comonad[W], M: Functor[M], F: Functor[F]) =
    matryoshka.attributeGAlgebraM[W, M, F, A](self)
}

final class ElgotAlgebraMOps[W[_], M[_], F[_], A](self: ElgotAlgebraM[W, M, F, A]) {
  def attribute(implicit W: Comonad[W], M: Functor[M], F: Functor[F]) =
    matryoshka.attributeElgotAlgebraM[W, M, F, A](self)
}

final class GAlgebraOps[W[_], F[_], A](self: GAlgebra[W, F, A]) {
  def attribute(implicit W: Comonad[W], F: Functor[F]) =
    matryoshka.attributeGAlgebraM[W, Id, F, A](self)

  def generalizeM[M[_]: Applicative]: GAlgebraM[W, M, F, A] =
    matryoshka.generalizeM[M, F[W[A]], A](self)
}

final class ElgotAlgebraOps[W[_], F[_], A](self: ElgotAlgebra[W, F, A]) {
  def attribute(implicit W: Comonad[W], F: Functor[F]) =
    matryoshka.attributeElgotAlgebraM[W, Id, F, A](self)

  def generalizeM[M[_]: Applicative]: ElgotAlgebraM[W, M, F, A] =
    matryoshka.generalizeM[M, W[F[A]], A](self)
}

final class AlgebraMOps[M[_], F[_], A](self: AlgebraM[M, F, A]) {
  def attribute(implicit M: Functor[M], F: Functor[F]) =
    matryoshka.attributeAlgebraM[M, F, A](self)

  def generalize[W[_]: Comonad](implicit F: Functor[F]): GAlgebraM[W, M, F, A] =
    matryoshka.generalizeAlgebra[W, F, A, M[A]](self)

  def generalizeElgot[W[_]: Comonad]: ElgotAlgebraM[W, M, F, A] =
    matryoshka.generalizeW[W, F[A], M[A]](self)
}

final class AlgebraOps[F[_], A](self: Algebra[F, A]) {
  def attribute(implicit F: Functor[F]) =
    matryoshka.attributeAlgebra[F, A](self)

  def generalize[W[_]: Comonad](implicit F: Functor[F]): GAlgebra[W, F, A] =
    matryoshka.generalizeAlgebra[W, F, A, A](self)

  def generalizeElgot[W[_]: Comonad]: ElgotAlgebra[W, F, A] =
    matryoshka.generalizeW[W, F[A], A](self)

  def generalizeM[M[_]: Applicative](implicit F: Functor[F]):
      AlgebraM[M, F, A] =
    matryoshka.generalizeM[M, F[A], A](self)
}
