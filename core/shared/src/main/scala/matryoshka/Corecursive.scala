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

import scala.Predef

import scalaz._, Scalaz._

/** Unfolds for corecursive data types. */
trait Corecursive[T] extends Based[T] {
  def embed(t: Base[T])(implicit BF: Functor[Base]): T

  /** Roughly a default impl of `embed`, given a [[matryoshka.Recursive]]
    * instance and an overridden `ana`.
    */
  def colambek(ft: Base[T])(implicit TR: Recursive.Aux[T, Base], BF: Functor[Base]): T =
    ana(ft)(_ ∘ (TR.project(_)))

  def ana[A](a: A)(f: Coalgebra[Base, A])(implicit BF: Functor[Base]): T =
    embed(f(a) ∘ (ana(_)(f)))

  def anaM[M[_]: Monad, A]
    (a: A)
    (f: CoalgebraM[M, Base, A])
    (implicit BT: Traverse[Base])
      : M[T] =
    f(a).flatMap(_.traverse(anaM(_)(f))) ∘ (embed(_))

  def gana[N[_]: Monad, A]
    (a: A)
    (k: DistributiveLaw[N, Base], f: GCoalgebra[N, Base, A])
    (implicit BF: Functor[Base])
      : T = {
    def loop(x: N[Base[N[A]]]): T = embed(k(x) ∘ (x => loop(x.join ∘ f)))

    loop(f(a).point[N])
  }

  def ganaM[N[_]: Monad: Traverse, M[_]: Monad, A](
    a: A)(
    k: DistributiveLaw[N, Base], f: GCoalgebraM[N, M, Base, A])(
    implicit BT: Traverse[Base]):
      M[T] = {
    def loop(x: N[Base[N[A]]]): M[T] =
      k(x).traverse(_.join.traverse(f) >>= loop) ∘ (embed(_))

    f(a) ∘ (_.point[N]) >>= loop
  }

  def elgotAna[N[_]: Monad, A](
    a: A)(
    k: DistributiveLaw[N, Base], ψ: ElgotCoalgebra[N, Base, A])(
    implicit BF: Functor[Base]):
      T = {
    def loop(x: N[Base[A]]): T = embed(k(x) ∘ (x => loop(x >>= ψ)))

    loop(ψ(a))
  }

  /** An unfold that can short-circuit certain sections.
    */
  def apo[A](a: A)(f: GCoalgebra[T \/ ?, Base, A])(implicit BF: Functor[Base])
      : T =
    // NB: This is not implemented with [[matryoshka.distApo]] because that
    //     would add a [[matryoshka.Recursive]] constraint.
    embed(f(a) ∘ (_.fold(Predef.identity, apo(_)(f))))

  def elgotApo[A]
    (a: A)
    (f: ElgotCoalgebra[T \/ ?, Base, A])
    (implicit BF: Functor[Base])
      : T =
    // NB: This is not implemented with [[matryoshka.distApo]] because that
    //     would add a [[matryoshka.Recursive]] constraint.
    f(a).fold(Predef.identity, fa => embed(fa ∘ (elgotApo(_)(f))))

  /** An unfold that can handle sections with a secondary unfold.
    */
  def gapo[A, B]
    (a: A)
    (ψ0: Coalgebra[Base, B], ψ: GCoalgebra[B \/ ?, Base, A])
    (implicit BF: Functor[Base])
      : T =
    embed(ψ(a) ∘ (_.fold(ana(_)(ψ0), gapo(_)(ψ0, ψ))))

  def apoM[M[_]: Monad, A](
    a: A)(
    f: GCoalgebraM[T \/ ?, M, Base, A])(
    implicit BT: Traverse[Base]):
      M[T] =
    f(a).flatMap(_.traverse(_.fold(_.point[M], apoM(_)(f)))) ∘ embed

  def postpro[A](
    a: A)(
    e: Base ~> Base, g: Coalgebra[Base, A])(
    implicit T: Recursive.Aux[T, Base], BF: Functor[Base]):
      T =
    gpostpro[Id, A](a)(distAna, e, g)

  def gpostpro[N[_], A](
    a: A)(
    k: DistributiveLaw[N, Base], e: Base ~> Base, ψ: GCoalgebra[N, Base, A])(
    implicit T: Recursive.Aux[T, Base], BF: Functor[Base], N: Monad[N]):
      T = {
    def loop(ma: N[A]): T =
      embed(k(ma ∘ ψ) ∘ (x => ana(loop(x.join))(x => e(T.project(x)))))

    loop(a.point[N])
  }

  // Futu should probably be library-specific, so not part of the typeclass. Can
  // just use `gana` explicitly, or maybe handle injecting them somehow else.
  def futu[A]
    (a: A)
    (f: GCoalgebra[Free[Base, ?], Base, A])
    (implicit BF: Functor[Base])
      : T =
    gana[Free[Base, ?], A](a)(distFutu, f)

  def elgotFutu[A]
    (a: A)
    (f: ElgotCoalgebra[Free[Base, ?], Base, A])
    (implicit BF: Functor[Base])
      : T =
    elgotAna[Free[Base, ?], A](a)(distFutu, f)

  def futuM[M[_]: Monad, A](a: A)(f: GCoalgebraM[Free[Base, ?], M, Base, A])(
    implicit BT: Traverse[Base]):
      M[T] = {
    def loop(free: Free[Base, A]): M[T] =
      free.fold(futuM(_)(f), _.traverse(loop) ∘ (embed))
    f(a).flatMap(_.traverse(loop)) ∘ (embed(_))
  }
}
object Corecursive {
  type Aux[T, F[_]] = Corecursive[T] { type Base[A] = F[A] }

  def apply[T, F[_]](implicit instance: Aux[T, F]): Aux[T, F] = instance
}
