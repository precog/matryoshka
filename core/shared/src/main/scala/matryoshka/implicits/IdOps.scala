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

package matryoshka.implicits

import matryoshka._

import scalaz._

sealed class IdOps[A](self: A) {
  def hylo[F[_]: Functor, B](f: Algebra[F, B], g: Coalgebra[F, A]): B =
    matryoshka.hylo(self)(f, g)
  def hyloM[M[_]: Monad, F[_]: Traverse, B](f: AlgebraM[M, F, B], g: CoalgebraM[M, F, A]):
      M[B] =
    matryoshka.hyloM(self)(f, g)

  object ghylo {
    def apply[W[_], N[_]] = new Aux[W, N]

    final class Aux[W[_], N[_]] {
      def apply[F[_]: Functor, B]
        (w: DistributiveLaw[F, W],
          n: DistributiveLaw[N, F],
          f: GAlgebra[W, F, B],
          g: GCoalgebra[N, F, A])
        (implicit W: Comonad[W], N: Monad[N]) =
        matryoshka.ghylo(self)(w, n, f, g)
    }
  }

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

  def elgot[F[_]: Functor, B](φ: Algebra[F, B], ψ: ElgotCoalgebra[B \/ ?, F, A]): B =
    matryoshka.elgot(self)(φ, ψ)

  def coelgot[F[_]: Functor, B](φ: ElgotAlgebra[(A, ?), F, B], ψ: Coalgebra[F, A]): B =
    matryoshka.coelgot(self)(φ, ψ)

  object coelgotM {
    def apply[M[_]] = new Aux[M]

    final class Aux[M[_]] {
      def apply[F[_]: Traverse, B](φ: ElgotAlgebraM[(A, ?), M, F, B], ψ: CoalgebraM[M, F, A])(implicit M: Monad[M]):
          M[B] =
        matryoshka.coelgotM[M].apply[F, A, B](self)(φ, ψ)
    }
  }

  object ana {
    def apply[T] = new Aux[T]

    final class Aux[T] {
      def apply[F[_]: Functor]
        (f: Coalgebra[F, A])
        (implicit T: Corecursive.Aux[T, F])
          : T =
        T.ana(self)(f)
    }
  }

  object anaM {
    def apply[T] = new Aux[T]

    final class Aux[T] {
      def apply[M[_]: Monad, F[_]: Traverse]
        (f: CoalgebraM[M, F, A])
        (implicit T: Corecursive.Aux[T, F])
          : M[T] =
        T.anaM(self)(f)
    }
  }

  object gana {
    def apply[T] = new Aux[T]

    final class Aux[T] {
      def apply[N[_]: Monad, F[_]: Functor]
        (k: DistributiveLaw[N, F], f: GCoalgebra[N, F, A])
        (implicit T: Corecursive.Aux[T, F])
          : T =
        T.gana(self)(k, f)
    }
  }

  object ganaM {
    def apply[T] = new Aux[T]

    final class Aux[T] {
      def apply[N[_]: Monad: Traverse, M[_]: Monad, F[_]: Traverse]
        (k: DistributiveLaw[N, F], f: GCoalgebraM[N, M, F, A])
        (implicit T: Corecursive.Aux[T, F])
          : M[T] =
        T.ganaM(self)(k, f)
    }
  }

  object elgotAna {
    def apply[T] = new Aux[T]

    final class Aux[T] {
      def apply[N[_]: Monad, F[_]: Functor]
        (k: DistributiveLaw[N, F], f: ElgotCoalgebra[N, F, A])
        (implicit T: Corecursive.Aux[T, F])
          : T =
        T.elgotAna(self)(k, f)
    }
  }

  object apo {
    def apply[T] = new Aux[T]

    final class Aux[T] {
      def apply[F[_]: Functor]
        (f: GCoalgebra[T \/ ?, F, A])
        (implicit T: Corecursive.Aux[T, F])
          : T =
        T.apo(self)(f)
    }
  }

  object apoM {
    def apply[T] = new Aux[T]

    final class Aux[T] {
      def apply[M[_]: Monad, F[_]: Traverse]
        (f: GCoalgebraM[T \/ ?, M, F, A])
        (implicit T: Corecursive.Aux[T, F])
          : M[T] =
        T.apoM(self)(f)
    }
  }

  object elgotApo {
    def apply[T] = new Aux[T]

    final class Aux[T] {
      def apply[F[_]: Functor]
        (f: ElgotCoalgebra[T \/ ?, F, A])
        (implicit T: Corecursive.Aux[T, F])
          : T =
        T.elgotApo(self)(f)
    }
  }

  object postpro {
    def apply[T] = new Aux[T]

    final class Aux[T] {
      def apply[F[_]: Functor]
        (e: F ~> F, g: Coalgebra[F, A])
        (implicit TR: Recursive.Aux[T, F], TC: Corecursive.Aux[T, F])
          : T =
        TC.postpro(self)(e, g)
    }
  }

  object gpostpro {
    def apply[T] = new Aux[T]

    final class Aux[T] {
      def apply[N[_]: Monad, F[_]: Functor]
        (k: DistributiveLaw[N, F], e: F ~> F, g: GCoalgebra[N, F, A])
        (implicit TR: Recursive.Aux[T, F], TC: Corecursive.Aux[T, F])
          : T =
        TC.gpostpro(self)(k, e, g)
    }
  }

  object futu {
    def apply[T] = new Aux[T]

    final class Aux[T] {
      def apply[F[_]: Functor]
        (f: GCoalgebra[Free[F, ?], F, A])
        (implicit T: Corecursive.Aux[T, F])
          : T =
        T.futu(self)(f)
    }
  }

  def futuM[T, M[_]: Monad, F[_]: Traverse]
    (f: GCoalgebraM[Free[F, ?], M, F, A])
    (implicit T: Corecursive.Aux[T, F])
      : M[T] =
    T.futuM(self)(f)
}
