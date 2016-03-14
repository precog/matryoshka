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

sealed class IdOps[A](self: A) {
  def hylo[F[_]: Functor, B](f: Algebra[F, B], g: Coalgebra[F, A]): B =
    matryoshka.hylo(self)(f, g)
  def hyloM[M[_]: Monad, F[_]: Traverse, B](f: AlgebraM[M, F, B], g: CoalgebraM[M, F, A]):
      M[B] =
    matryoshka.hyloM(self)(f, g)
  def ghylo[F[_]: Functor, W[_]: Comonad, M[_]: Monad, B](
    w: DistributiveLaw[F, W],
    m: DistributiveLaw[M, F],
    f: GAlgebra[W, F, B],
    g: GCoalgebra[M, F, A]):
      B =
    matryoshka.ghylo(self)(w, m, f, g)

  def chrono[F[_]: Functor, B](
    g: GAlgebra[Cofree[F, ?], F, B], f: GCoalgebra[Free[F, ?], F, A]):
      B =
    matryoshka.chrono(self)(g, f)

  def attributeAna[F[_]: Functor](ψ: A => F[A]): Cofree[F, A] =
    matryoshka.attributeAna(self)(ψ)

  def attributeAnaM[M[_]: Monad, F[_]: Traverse](ψ: A => M[F[A]]):
      M[Cofree[F, A]] =
    matryoshka.attributeAnaM(self)(ψ)

  def elgotAna[F[_]: Functor, B](ψ: A => B \/ F[A]): Free[F, B] =
    matryoshka.elgotAna(self)(ψ)

  def elgot[F[_]: Functor, B](φ: Algebra[F, B], ψ: ElgotCoalgebra[B \/ ?, F, A]): B =
    matryoshka.elgot(self)(φ, ψ)

  def coelgot[F[_]: Functor, B](φ: ((A, F[B])) => B, ψ: A => F[A]): B =
    matryoshka.coelgot(self)(φ, ψ)
  def coelgotM[M[_]] = new CoelgotMPartiallyApplied[M]
  final class CoelgotMPartiallyApplied[M[_]] {
    def apply[F[_]: Traverse, B](φ: ((A, F[B])) => M[B], ψ: A => M[F[A]])(implicit M: Monad[M]):
        M[B] =
      matryoshka.coelgotM[M].apply[F, A, B](self)(φ, ψ)
  }

  def ana[T[_[_]], F[_]: Functor](f: A => F[A])(implicit T: Corecursive[T]): T[F] =
    T.ana(self)(f)
  def anaM[T[_[_]], F[_]: Traverse, M[_]: Monad](f: A => M[F[A]])(implicit T: Corecursive[T]): M[T[F]] =
    T.anaM(self)(f)
  def gana[T[_[_]], F[_]: Functor, M[_]: Monad](
    k: DistributiveLaw[M, F], f: A => F[M[A]])(
    implicit T: Corecursive[T]):
      T[F] =
    T.gana(self)(k, f)
  def apo[T[_[_]], F[_]: Functor](f: A => F[T[F] \/ A])(implicit T: Corecursive[T]): T[F] =
    T.apo(self)(f)
  def apoM[T[_[_]], F[_]: Traverse, M[_]: Monad](f: A => M[F[T[F] \/ A]])(implicit T: Corecursive[T]): M[T[F]] =
    T.apoM(self)(f)
  def postpro[T[_[_]]: Recursive, F[_]: Functor](e: F ~> F, g: A => F[A])(implicit T: Corecursive[T]): T[F] =
    T.postpro(self)(e, g)
  def gpostpro[T[_[_]]: Recursive, F[_]: Functor, M[_]: Monad](
    k: DistributiveLaw[M, F], e: F ~> F, g: A => F[M[A]])(
    implicit T: Corecursive[T]):
      T[F] =
    T.gpostpro(self)(k, e, g)
  def futu[T[_[_]], F[_]: Functor](f: A => F[Free[F, A]])(implicit T: Corecursive[T]): T[F] =
    T.futu(self)(f)
  def futuM[T[_[_]], F[_]: Traverse, M[_]: Monad](f: A => M[F[Free[F, A]]])(implicit T: Corecursive[T]):
      M[T[F]] =
    T.futuM(self)(f)
}
