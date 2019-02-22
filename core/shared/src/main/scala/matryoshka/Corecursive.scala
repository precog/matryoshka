/*
 * Copyright 2014–2018 SlamData Inc.
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

/** Unfolds for corecursive data types. */
trait Corecursive[T] extends Based[T] { self =>
  implicit val corec: Corecursive.Aux[T, Base] = self

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
    hylo[λ[α => Base[T \/ α]], A, T](
      a)(
      fa => embed(fa.map(_.merge)), f)(
      BF.compose[T \/ ?])

  def elgotApo[A]
    (a: A)
    (f: ElgotCoalgebra[T \/ ?, Base, A])
    (implicit BF: Functor[Base])
      : T =
    hylo[λ[α => T \/ Base[α]], A, T](
      a)(
      _.map(embed).merge, f)(
      \/.DisjunctionInstances1 compose BF)

  /** An unfold that can handle sections with a secondary unfold.
    */
  def gapo[A, B]
    (a: A)
    (ψ0: Coalgebra[Base, B], ψ: GCoalgebra[B \/ ?, Base, A])
    (implicit BF: Functor[Base])
      : T =
    hylo[λ[α => Base[B \/ α]], A, T](
      a)(
      fa => embed(fa.map(_.leftMap(ana(_)(ψ0)).merge)), ψ)(
      BF.compose[B \/ ?])

  def apoM[M[_]: Monad, A](
    a: A)(
    f: GCoalgebraM[T \/ ?, M, Base, A])(
    implicit BT: Traverse[Base]):
      M[T] =
    hyloM[M, λ[α => Base[T \/ α]], A, T](
      a)(
      fa => embed(fa ∘ (_.merge)).point[M], f)(
      Monad[M], BT.compose[T \/ ?])

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

  def transAna[U, G[_]: Functor]
    (u: U)
    (f: G[U] => Base[U])
    (implicit U: Recursive.Aux[U, G], BF: Functor[Base])
      : T =
    ana(u)(f <<< (U.project(_)))

  def transGana[M[_]: Monad, U, G[_]: Functor]
    (u: U)
    (k: DistributiveLaw[M, Base], f: CoalgebraicGTransform[M, U, G, Base])
    (implicit U: Recursive.Aux[U, G], BF: Functor[Base])
      : T =
    gana(u)(k, f <<< (U.project(_)))

  def transApo[U, G[_]: Functor]
    (u: U)
    (f: CoalgebraicGTransform[T \/ ?, U, G, Base])
    (implicit U: Recursive.Aux[U, G], BF: Functor[Base])
      : T = {
    implicit val nested: Functor[λ[α => Base[T \/ α]]] =
      BF.compose[T \/ ?]

    transHylo[U, G, λ[α => Base[T \/ α]], T, Base](u)(_ ∘ (_.merge), f)
  }

  def transFutu[U, G[_]: Functor]
    (u: U)
    (f: CoalgebraicGTransform[Free[Base, ?], U, G, Base])
    (implicit U: Recursive.Aux[U, G], BF: Functor[Base])
      : T =
    transGana(u)(distFutu[Base], f)

  def transAnaM[M[_]: Monad, U, G[_]: Functor]
    (u: U)
    (f: TransformM[M, U, G, Base])
    (implicit U: Recursive.Aux[U, G], BT: Traverse[Base])
      : M[T] =
    anaM(u)(f <<< (U.project(_)))
}

object Corecursive {
  def fromAlgebra[T, F[_]](φ: Algebra[F, T]): Aux[T, F] = new Corecursive[T] {
    type Base[A] = F[A]
    def embed(ft: F[T])(implicit BF: Functor[Base]) = φ(ft)
  }

  type Aux[T, F[_]] = Corecursive[T] { type Base[A] = F[A] }

  def apply[T](implicit instance: Corecursive[T]): Aux[T, instance.Base] =
    instance
}
