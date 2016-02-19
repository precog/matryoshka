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

sealed class IdOps[A](self: A) {
  def hylo[F[_]: Functor, B](φ: F[B] => B, ψ: A => F[A]): B =
    refold.hylo(self)(φ, ψ)
  def hyloM[M[_]: Monad, F[_]: Traverse, B](φ: F[B] => M[B], ψ: A => M[F[A]]):
      M[B] =
    refold.hyloM(self)(φ, ψ)
  def ghylo[F[_]: Functor, W[_]: Comonad, M[_]: Monad, B](
    w: DistributiveLaw[F, W],
    m: DistributiveLaw[M, F],
    φ: F[W[B]] => B,
    ψ: A => F[M[A]]):
      B =
    refold.ghylo(self)(w, m, φ, ψ)

  def chrono[F[_]: Functor, B](φ: F[Cofree[F, B]] => B, ψ: A => F[Free[F, A]]):
      B =
    refold.chrono(self)(φ, ψ)

  def elgot[F[_]: Functor, B](φ: F[B] => B, ψ: A => B \/ F[A]): B =
    refold.elgot(self)(φ, ψ)

  def coelgot[F[_]: Functor, B](φ: ((A, F[B])) => B, ψ: A => F[A]): B =
    refold.coelgot(self)(φ, ψ)
  def coelgotM[M[_]] = new CoelgotMPartiallyApplied[M]
  final class CoelgotMPartiallyApplied[M[_]] {
    def apply[F[_]: Traverse, B](φ: (A, F[B]) => M[B], ψ: A => M[F[A]])(implicit M: Monad[M]):
        M[B] =
      refold.coelgotM[M, F, A, B](self)(φ, ψ)
  }

  def elgotAna[F[_]: Functor, B](ψ: A => B \/ F[A]): Free[F, B] =
    matryoshka.elgotAna[F, A, B](self)(ψ)

  def attributeAna[F[_]: Functor](ψ: A => F[A]): Cofree[F, A] =
    matryoshka.attributeAna[F, A](self)(ψ)
  def attributeAnaM[M[_]: Monad, F[_]: Traverse](ψ: A => M[F[A]]): M[Cofree[F, A]] =
    matryoshka.attributeAnaM[M, F, A](self)(ψ)

  def ana[T[_[_]], F[_]: Functor](ψ: A => F[A])(implicit T: Corecursive[T]): T[F] =
    T.ana[F, A](self)(ψ)
  def anaM[T[_[_]], F[_]: Traverse, M[_]: Monad](ψ: A => M[F[A]])(implicit T: Corecursive[T]): M[T[F]] =
    T.anaM(self)(ψ)
  def gana[T[_[_]], F[_]: Functor, M[_]: Monad](
    k: DistributiveLaw[M, F], ψ: A => F[M[A]])(
    implicit T: Corecursive[T]):
      T[F] =
    T.gana(self)(k, ψ)
  def apo[T[_[_]], F[_]: Functor](ψ: A => F[T[F] \/ A])(implicit T: Corecursive[T]): T[F] =
    T.apo(self)(ψ)
  def apoM[T[_[_]], F[_]: Traverse, M[_]: Monad](ψ: A => M[F[T[F] \/ A]])(implicit T: Corecursive[T]): M[T[F]] =
    T.apoM(self)(ψ)
  def gapo[T[_[_]], F[_]: Functor, B](f: B => F[B], g: A => F[B \/ A])(implicit T: Corecursive[T]): T[F] =
    T.gapo(self)(f, g)
  def postpro[T[_[_]]: Recursive, F[_]: Functor](e: F ~> F, ψ: A => F[A])(implicit T: Corecursive[T]): T[F] =
    T.postpro(self)(e, ψ)
  def gpostpro[T[_[_]]: Recursive, F[_]: Functor, M[_]: Monad](
    k: DistributiveLaw[M, F], e: F ~> F, ψ: A => F[M[A]])(
    implicit T: Corecursive[T]):
      T[F] =
    T.gpostpro(self)(k, e, ψ)
  def futu[T[_[_]], F[_]: Functor](ψ: A => F[Free[F, A]])(implicit T: Corecursive[T]): T[F] =
    T.futu(self)(ψ)
  def futuM[T[_[_]], F[_]: Traverse, M[_]: Monad](ψ: A => M[F[Free[F, A]]])(implicit T: Corecursive[T]):
      M[T[F]] =
    T.futuM(self)(ψ)
}
