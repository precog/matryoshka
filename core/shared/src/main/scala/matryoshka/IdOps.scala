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
  def hylo[F[_]: Functor, B](f: Algebra[F, B], g: Coalgebra[F, A]): B =
    matryoshka.hylo(self)(f, g)
  def hyloM[M[_]: Monad, F[_]: Traverse, B](f: AlgebraM[M, F, B], g: CoalgebraM[M, F, A]):
      M[B] =
    matryoshka.hyloM(self)(f, g)
    
  def ghylo[W[_]: Comonad, N[_]: Monad, F[_]: Functor, B](
    w: DistributiveLaw[F, W],
    n: DistributiveLaw[N, F],
    f: GAlgebra[W, F, B],
    g: GCoalgebra[N, F, A]):
      B =
    matryoshka.ghylo(self)(w, n, f, g)
  def ghyloM[W[_]: Comonad: Traverse, N[_]: Monad: Traverse, M[_]: Monad, F[_]: Traverse, B](
    w: DistributiveLaw[F, W],
    n: DistributiveLaw[N, F],
    f: GAlgebraM[W, M, F, B],
    g: GCoalgebraM[N, M, F, A]):
      M[B] =
    matryoshka.ghyloM(self)(w, n, f, g)

  def dyna[F[_]: Functor, B](φ: GAlgebra[Cofree[F, ?], F, B], ψ: Coalgebra[F, A]): B =
    matryoshka.dyna(self)(φ, ψ)

  def codyna[F[_]: Functor, B](φ: Algebra[F, B], ψ: GCoalgebra[Free[F, ?], F, A]): B =
    matryoshka.codyna(self)(φ, ψ)

  def codynaM[M[_]: Monad, F[_]: Traverse, B](φ: AlgebraM[M, F, B], ψ: GCoalgebraM[Free[F, ?], M, F, A]): M[B] =
    matryoshka.codynaM(self)(φ, ψ)

  def chrono[F[_]: Functor, B](
    g: GAlgebra[Cofree[F, ?], F, B], f: GCoalgebra[Free[F, ?], F, A]):
      B =
    matryoshka.chrono(self)(g, f)

  def attributeAna[F[_]: Functor](ψ: Coalgebra[F, A]): Cofree[F, A] =
    matryoshka.attributeAna(self)(ψ)

  def attributeAnaM[M[_]: Monad, F[_]: Traverse](ψ: CoalgebraM[M, F, A]):
      M[Cofree[F, A]] =
    matryoshka.attributeAnaM(self)(ψ)

  def freeAna[F[_]: Functor, B](ψ: CoalgebraM[B \/ ?, F, A]): Free[F, B] =
    matryoshka.freeAna(self)(ψ)

  def elgot[F[_]: Functor, B](φ: Algebra[F, B], ψ: CoalgebraM[B \/ ?, F, A]): B =
    matryoshka.elgot(self)(φ, ψ)

  def coelgot[F[_]: Functor, B](φ: ElgotAlgebra[(A, ?), F, B], ψ: Coalgebra[F, A]): B =
    matryoshka.coelgot(self)(φ, ψ)
  def coelgotM[M[_]] = new CoelgotMPartiallyApplied[M]
  final class CoelgotMPartiallyApplied[M[_]] {
    def apply[F[_]: Traverse, B](φ: ElgotAlgebraM[(A, ?), M, F, B], ψ: CoalgebraM[M, F, A])(implicit M: Monad[M]):
        M[B] =
      matryoshka.coelgotM[M].apply[F, A, B](self)(φ, ψ)
  }

  def ana[T[_[_]], F[_]: Functor](f: Coalgebra[F, A])(implicit T: Corecursive[T]): T[F] =
    T.ana(self)(f)
  def anaM[T[_[_]], M[_]: Monad, F[_]: Traverse](f: CoalgebraM[M, F, A])(implicit T: Corecursive[T]): M[T[F]] =
    T.anaM(self)(f)
  def gana[T[_[_]], N[_]: Monad, F[_]: Functor](
    k: DistributiveLaw[N, F], f: GCoalgebra[N, F, A])(
    implicit T: Corecursive[T]):
      T[F] =
    T.gana(self)(k, f)
  def elgotAna[T[_[_]], M[_]: Monad, F[_]: Functor](
    k: DistributiveLaw[M, F], f: CoalgebraM[M, F, A])(
    implicit T: Corecursive[T]):
      T[F] =
    T.elgotAna(self)(k, f)
  def ganaM[T[_[_]], N[_]: Monad: Traverse, M[_]: Monad, F[_]: Traverse](
    k: DistributiveLaw[N, F], f: GCoalgebraM[N, M, F, A])(
    implicit T: Corecursive[T]):
      M[T[F]] =
    T.ganaM(self)(k, f)
  def apo[T[_[_]], F[_]: Functor](f: GCoalgebra[T[F] \/ ?, F, A])(implicit T: Corecursive[T]): T[F] =
    T.apo(self)(f)
  def elgotApo[T[_[_]], F[_]: Functor](f: ElgotCoalgebra[T[F] \/ ?, F, A])(implicit T: Corecursive[T]): T[F] =
    T.elgotApo(self)(f)
  def apoM[T[_[_]], M[_]: Monad, F[_]: Traverse](f: GCoalgebraM[T[F] \/ ?, M, F, A])(implicit T: Corecursive[T]): M[T[F]] =
    T.apoM(self)(f)
  def postpro[T[_[_]]: Recursive, F[_]: Functor](e: F ~> F, g: Coalgebra[F, A])(implicit T: Corecursive[T]): T[F] =
    T.postpro(self)(e, g)
  def gpostpro[T[_[_]]: Recursive, N[_]: Monad, F[_]: Functor](
    k: DistributiveLaw[N, F], e: F ~> F, g: GCoalgebra[N, F, A])(
    implicit T: Corecursive[T]):
      T[F] =
    T.gpostpro(self)(k, e, g)
  def futu[T[_[_]], F[_]: Functor](f: GCoalgebra[Free[F, ?], F, A])(implicit T: Corecursive[T]): T[F] =
    T.futu(self)(f)
  def futuM[T[_[_]], M[_]: Monad, F[_]: Traverse](f: GCoalgebraM[Free[F, ?], M, F, A])(implicit T: Corecursive[T]):
      M[T[F]] =
    T.futuM(self)(f)
}
