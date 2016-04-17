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

import scala.Predef.implicitly

import scalaz._

sealed class IdOps[A](self: A) {
  def hylo[F[_]: Functor, B](f: F[B] => B, g: A => F[A]): B =
    matryoshka.hylo(self)(f, g)
  def hyloM[M[_]: Monad, F[_]: Traverse, B](f: F[B] => M[B], g: A => M[F[A]]):
      M[B] =
    matryoshka.hyloM(self)(f, g)

  object ghylo {
    def apply[W[_], M[_]] = new Aux[W, M]

    final class Aux[W[_], M[_]] {
      def apply[F[_]: Functor, B](
        w: DistributiveLaw[F, W],
        m: DistributiveLaw[M, F],
        f: F[W[B]] => B,
        g: A => F[M[A]])(
        implicit W: Comonad[W], M: Monad [M]):
          B =
        matryoshka.ghylo(self)(w, m, f, g)
    }
  }

  def chrono[F[_]: Functor, B](
    g: F[Cofree[F, B]] => B, f: A => F[Free[F, A]]):
      B =
    matryoshka.chrono(self)(g, f)

  def freeAna[F[_]: Functor, B](ψ: A => B \/ F[A]): Free[F, B] =
    matryoshka.freeAna(self)(ψ)

  def elgot[F[_]: Functor, B](φ: F[B] => B, ψ: A => B \/ F[A]): B =
    matryoshka.elgot(self)(φ, ψ)

  def coelgot[F[_]: Functor, B](φ: ((A, F[B])) => B, ψ: A => F[A]): B =
    matryoshka.coelgot(self)(φ, ψ)

  object coelgotM {
    def apply[M[_]] = new Aux[M]

    final class Aux[M[_]] {
      def apply[F[_]: Traverse, B](φ: ((A, F[B])) => M[B], ψ: A => M[F[A]])(implicit M: Monad[M]):
          M[B] =
        matryoshka.coelgotM[M].apply[F, A, B](self)(φ, ψ)
    }
  }

  def ana[T, F[_]](f: A => F[A])(implicit T: Corecursive.Aux[T, F]): T =
    T.ana(self)(f)

  object anaM {
    def apply[T] = new Aux[T]

    final class Aux[T] {
      def apply[M[_]: Monad, F[_]](f: A => M[F[A]])(implicit T: Corecursive.Aux[T, F], FT: Traverse[F]):
          M[T] =
        T.anaM(self)(f)(implicitly, FT)
    }
  }

  def gana[T, M[_]: Monad, F[_]](
    k: DistributiveLaw[M, F], f: A => F[M[A]])(
    implicit T: Corecursive.Aux[T, F]):
      T =
    T.gana(self)(k, f)

  def elgotAna[T, M[_]: Monad, F[_]](
    k: DistributiveLaw[M, F], f: A => M[F[A]])(
    implicit T: Corecursive.Aux[T, F]):
      T =
    T.elgotAna(self)(k, f)

  def apo[T, F[_]](f: A => F[T \/ A])(implicit T: Corecursive.Aux[T, F]): T =
    T.apo(self)(f)

  object apoM {
    def apply[T] = new Aux[T]

    final class Aux[T] {
      def apply[M[_]: Monad, F[_]](f: A => M[F[T \/ A]])(implicit T: Corecursive.Aux[T, F], FT: Traverse[F]):
          M[T] =
        T.apoM(self)(f)(implicitly, FT)
    }
  }

  def elgotApo[T, F[_]](f: A => T \/ F[A])(implicit T: Corecursive.Aux[T, F]):
      T =
    T.elgotApo(self)(f)

  def postpro[T, F[_]](
    e: F ~> F, g: A => F[A])(
    implicit TR: Recursive.Aux[T, F], TC: Corecursive.Aux[T, F]):
      T =
    TC.postpro(self)(e, g)

  def gpostpro[T, M[_]: Monad, F[_]](
    k: DistributiveLaw[M, F], e: F ~> F, g: A => F[M[A]])(
    implicit TR: Recursive.Aux[T, F], TC: Corecursive.Aux[T, F]):
      T =
    TC.gpostpro(self)(k, e, g)

  def futu[T, F[_]](f: A => F[Free[F, A]])(implicit T: Corecursive.Aux[T, F]):
      T =
    T.futu(self)(f)

  def futuM[T, M[_]: Monad, F[_]](f: A => M[F[Free[F, A]]])(implicit T: Corecursive.Aux[T, F], FT: Traverse[F]):
      M[T] =
    T.futuM(self)(f)(implicitly, FT)
}
