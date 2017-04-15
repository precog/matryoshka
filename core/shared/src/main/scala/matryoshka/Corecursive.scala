/*
 * Copyright 2014–2017 SlamData Inc.
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

  def ana[A](a: A)(f: Coalgebra[Base, A])(implicit BF: Functor[Base]): T =
    hylo(a)(embed, f)

  def anaM[M[_]: Monad, A]
    (a: A)
    (f: CoalgebraM[M, Base, A])
    (implicit BT: Traverse[Base])
      : M[T] =
    hyloM[M, Base, A, T](a)(embed(_).point[M], f)

  def gana[N[_]: Monad, A]
    (a: A)
    (k: DistributiveLaw[N, Base], f: GCoalgebra[N, Base, A])
    (implicit BF: Functor[Base])
      : T =
    ana[N[A]](a.point[N])(na => k(na.map(f)).map(_.join))

  def ganaM[N[_]: Monad: Traverse, M[_]: Monad, A](
    a: A)(
    k: DistributiveLaw[N, Base], f: GCoalgebraM[N, M, Base, A])(
    implicit BT: Traverse[Base]):
      M[T] =
    ghyloM[Id, N, M, Base, A, T](a)(distCata, k, embed(_).point[M], f)

  def elgotAna[N[_]: Monad, A](
    a: A)(
    k: DistributiveLaw[N, Base], ψ: ElgotCoalgebra[N, Base, A])(
    implicit BF: Functor[Base])
      : T =
    ana(ψ(a))(k(_) ∘ (_ >>= ψ))

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
      M[T] =
    ganaM[Free[Base, ?], M, A](a)(distFutu, f)
}

object Corecursive {
  type Aux[T, F[_]] = Corecursive[T] { type Base[A] = F[A] }

  def apply[T, F[_]](implicit instance: Aux[T, F]): Aux[T, F] = instance
}
