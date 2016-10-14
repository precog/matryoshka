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

import Recursive.ops._

import scala.Predef.identity
import scalaz._, Scalaz._
import simulacrum.typeclass

/** Unfolds for corecursive data types. */
@typeclass trait Corecursive[T[_[_]]] {
  def embed[F[_]: Functor](t: F[T[F]]): T[F]

  def ana[F[_]: Functor, A](a: A)(f: Coalgebra[F, A]): T[F] =
    embed((f(a): F[A]) ∘ (ana(_)(f)))

  def anaM[F[_]: Traverse, M[_]: Monad, A](a: A)(f: CoalgebraM[M, F, A]): M[T[F]] =
    f(a).flatMap(_.traverse(anaM(_)(f))) ∘ (embed(_))

  def gana[N[_], F[_]: Functor, A](
    a: A)(
    k: DistributiveLaw[N, F], f: GCoalgebra[N, F, A])(
    implicit N: Monad[N]):
      T[F] = {
    def loop(x: N[F[N[A]]]): T[F] = embed(k(x) ∘ (x => loop(N.lift(f)(x.join))))

    loop(f(a).point[N])
  }

  def ganaM[N[_]: Traverse, M[_]: Monad, F[_]: Traverse, A](
    a: A)(
    k: DistributiveLaw[N, F], f: GCoalgebraM[N, M, F, A])(
    implicit N: Monad[N]):
      M[T[F]] = {
    def loop(x: N[F[N[A]]]): M[T[F]] =
      k(x).traverse(x => N.lift(f)(x.join).sequence >>= loop) ∘ (embed(_))

    f(a) ∘ (_.point[N]) >>= loop
  }

  def elgotAna[M[_], F[_]: Functor, A](
    a: A)(
    k: DistributiveLaw[M, F], ψ: CoalgebraM[M, F, A])(
    implicit M: Monad[M]):
      T[F] = {
    def loop(x: M[F[A]]): T[F] = embed(k(x) ∘ (x => loop(M.lift(ψ)(x).join)))

    loop(ψ(a))
  }

  /** An unfold that can short-circuit certain sections.
    */
  def apo[F[_]: Functor, A](a: A)(f: GCoalgebra[T[F] \/ ?, F, A]): T[F] =
    // NB: This is not implemented with [[matryoshka.distApo]] because that
    //     would add a [[matryoshka.Recursive]] constraint.
    embed(f(a) ∘ (_.fold(identity, apo(_)(f))))

  def elgotApo[F[_]: Functor, A](a: A)(f: CoalgebraM[T[F] \/ ?, F, A]): T[F] =
    // NB: This is not implemented with [[matryoshka.distApo]] because that
    //     would add a [[matryoshka.Recursive]] constraint.
    f(a).fold(identity, fa => embed(fa ∘ (elgotApo(_)(f))))

  /** An unfold that can handle sections with a secondary unfold.
    */
  def gapo[F[_]: Functor, A, B](a: A)(ψ0: Coalgebra[F, B], ψ: GCoalgebra[B \/ ?, F, A]): T[F] =
    embed(ψ(a) ∘ (_.fold(ana(_)(ψ0), gapo(_)(ψ0, ψ))))

  def apoM[F[_]: Traverse, M[_]: Monad, A](a: A)(f: GCoalgebraM[T[F] \/ ?, M, F, A]): M[T[F]] =
    f(a).flatMap(_.traverse(_.fold(_.point[M], apoM(_)(f)))) ∘ (embed(_))

  def postpro[F[_]: Functor, A](
    a: A)(
    e: F ~> F, g: Coalgebra[F, A])(
    implicit T: Recursive[T]):
      T[F] =
    gpostpro[Id, F, A](a)(distAna, e, g)

  def gpostpro[N[_], F[_]: Functor, A](
    a: A)(
    k: DistributiveLaw[N, F], e: F ~> F, ψ: GCoalgebra[N, F, A])(
    implicit T: Recursive[T], N: Monad[N]):
      T[F] = {
    def loop(ma: N[A]): T[F] =
      embed(k(N.lift(ψ)(ma)) ∘ (x => ana(loop(x.join))(x => e(x.project))))

    loop(a.point[N])
  }

  def futu[F[_]: Functor, A](a: A)(f: GCoalgebra[Free[F, ?], F, A]): T[F] =
    gana[Free[F, ?], F, A](a)(distFutu, f)

  def elgotFutu[F[_]: Functor, A](a: A)(f: CoalgebraM[Free[F, ?], F, A]): T[F] =
    elgotAna[Free[F, ?], F, A](a)(distFutu, f)

  def futuM[M[_]: Monad, F[_]: Traverse, A](a: A)(f: GCoalgebraM[Free[F, ?], M, F, A]):
      M[T[F]] = {
    def loop(free: Free[F, A]): M[T[F]] =
      free.fold(futuM(_)(f), _.traverse(loop) ∘ (embed[F]))
    f(a).flatMap(_.traverse(loop)) ∘ (embed(_))
  }
}
