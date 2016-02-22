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

import matryoshka.Recursive.ops._

import scala.{Function, Int, None, Option}
import scala.collection.immutable.{List, ::}

import scalaz._, Scalaz._
import simulacrum.typeclass

/** Generalized folds, unfolds, and refolds. */
package object matryoshka extends CofreeInstances with FreeInstances {

  def lambek[T[_[_]]: Corecursive: Recursive, F[_]: Functor](tf: T[F]):
      F[T[F]] =
    tf.cata[F[T[F]]](_ ∘ (_.embed))

  def colambek[T[_[_]]: Corecursive: Recursive, F[_]: Functor](ft: F[T[F]]):
      T[F] =
    ft.ana(_ ∘ (_.project))

  type GAlgebraM[W[_], M[_], F[_], A] =                    F[W[A]] => M[A]
  type GAlgebra[W[_], F[_], A] = GAlgebraM[W, Id, F, A] // F[W[A]] => A
  type AlgebraM[M[_], F[_], A] = GAlgebraM[Id, M, F, A] // F[A]    => M[A]
  type Algebra[F[_], A]        = GAlgebra[Id, F, A]     // F[A]    => A

  type ElgotAlgebraM[W[_], M[_], F[_], A] = W[F[A]] => M[A]
  type ElgotAlgebra[W[_], F[_], A] = ElgotAlgebraM[W, Id, F, A] // W[F[A]] => A

  type GCoalgebraM[N[_], M[_], F[_], A] =                      A => M[F[N[A]]]
  type GCoalgebra[N[_], F[_], A] = GCoalgebraM[N, Id, F, A] // A => F[N[A]]
  type CoalgebraM[M[_], F[_], A] = GCoalgebraM[Id, M, F, A] // A => M[F[A]] – aka, ElgotCoalgebra
  type Coalgebra[F[_], A]        = GCoalgebra[Id, F, A]     // A => F[A]

  /** A NaturalTransformation that sequences two types */
  type DistributiveLaw[F[_], G[_]] = λ[α => F[G[α]]] ~> λ[α => G[F[α]]]

  /** This is like the fold half of `coelgot`, since the `Cofree` already has
    * the attribute for each node.
    */
  def elgotCata[F[_]: Functor, A, B](t: Cofree[F, A])(φ: ((A, F[B])) => B): B =
    φ((t.head, t.tail ∘ (elgotCata(_)(φ))))

  def elgotCataM[F[_]: Traverse, M[_]: Monad, A, B](t: Cofree[F, A])(φ: ((A, F[B])) => M[B]): M[B] =
    t.tail.traverse(elgotCataM(_)(φ)) >>= (fb => φ((t.head, fb)))

  /** This is like the unfold half of `elgot`, holding the left branches in the
    * pure component of `Free`.
    */
  def elgotAna[F[_]: Functor, A, B](a: A)(ψ: A => B \/ F[A]): Free[F, B] =
    ψ(a).fold(
      _.point[Free[F, ?]],
      fb => Free.liftF(fb ∘ (elgotAna(_)(ψ))).join)

  /** A version of [[matryoshka.Corecursive.ana]] that annotates each node with
    * the `A` it was expanded from.
    */
  def attributeAna[F[_]: Functor, A](a: A)(ψ: A => F[A]): Cofree[F, A] =
    Cofree(a, ψ(a) ∘ (attributeAna(_)(ψ)))

  /** A Kleisli [[matryoshka.attributeAna]].
    */
  def attributeAnaM[M[_]: Monad, F[_]: Traverse, A](a: A)(ψ: A => M[F[A]]): M[Cofree[F, A]] =
    ψ(a).flatMap(_.traverse(attributeAnaM(_)(ψ))) ∘ (Cofree(a, _))

  implicit def toIdOps[A](a: A): IdOps[A] = new IdOps[A](a)

  implicit def toCofreeOps[F[_], A](a: Cofree[F, A]): CofreeOps[F, A] =
    new CofreeOps[F, A](a)

  implicit def toCorecursiveOps[T[_[_]]: Corecursive, F[_]](f: F[T[F]]):
      CorecursiveOps[T, F] =
    new CorecursiveOps[T, F](f)

  implicit def toAlgebraOps[F[_], A](a: Algebra[F, A]): AlgebraOps[F, A] =
    new AlgebraOps[F, A](a)

  implicit def toElgotAlgebraMOps[W[_], M[_], F[_], A](
    a: ElgotAlgebraM[W, M, F, A]):
      ElgotAlgebraMOps[W, M, F, A] =
    new ElgotAlgebraMOps[W, M, F, A](a)

  implicit def toCoalgebraOps[F[_], A](a: Coalgebra[F, A]): CoalgebraOps[F, A] =
    new CoalgebraOps[F, A](a)

  implicit def toAlgebraicTransformationOps[T[_[_]], W[_], F[_], G[_]](
    a: F[W[T[G]]] => G[T[G]]):
      AlgebraicTransformationOps[T, W, F, G] =
    new AlgebraicTransformationOps[T, W, F, G](a)

  implicit def toCoalgebraicTransformationOps[T[_[_]], M[_], F[_], G[_]](
    a: F[T[F]] => G[M[T[F]]]):
      CoalgebraicTransformationOps[T, M, F, G] =
    new CoalgebraicTransformationOps[T, M, F, G](a)

  implicit def toTransformationalAlgebraOps[T[_[_]], W[_], F[_], G[_]](
    a: GAlgebra[W, F, T[G]]):
      TransformationalAlgebraOps[T, W, F, G] =
    new TransformationalAlgebraOps[T, W, F, G](a)

  implicit def toTransformationalCoalgebraOps[T[_[_]], M[_], F[_], G[_]](
    a: GCoalgebra[M, G, T[F]]):
      TransformationalCoalgebraOps[T, M, F, G] =
    new TransformationalCoalgebraOps[T, M, F, G](a)
}
