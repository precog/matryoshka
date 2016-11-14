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

import matryoshka.implicits._

import scala.Option

import scalaz._, Scalaz._, Leibniz._

package object patterns {
  // These aliases are because these functors require a higher-order functor as
  // a parameter, and it’s likely to be the same as the “outer” one.
  // NB: Still not sure they’re a good idea.

  type DiffT[T[_[_]], F[_]] = T[Diff[T, F, ?]]
  type PotentialFailureT[T[_[_]], F[_], E] = T[PotentialFailure[T, F, E, ?]]

  def diff[T[_[_]]: RecursiveT: CorecursiveT, F[_]: Diffable: Functor: Foldable]:
      (T[F], T[F], Option[F[DiffT[T, F]]]) => DiffT[T, F] =
    ((l, r, merged) =>
      merged.fold(
        Diffable[F].diffImpl(l, r).getOrElse(Different[T, F, T[Diff[T, F, ?]]](l, r).embedT))(
        merged => {
          val children = merged.toList
          (if (children.length ≟ children.collect { case Same(_) => () }.length)
            Same[T, F, T[Diff[T, F, ?]]](l).embedT
          else
            Similar[T, F, T[Diff[T, F, ?]]](merged).embedT)
        }))

  /** Algebra transformation that allows a standard algebra to be used on a
    * CoEnv structure (given a function that converts the leaves to the result
    * type).
    */
  def interpret[F[_], A, B](f: A => B, φ: Algebra[F, B]):
      Algebra[CoEnv[A, F, ?], B] =
    interpretM[Id, F, A, B](f, φ)

  def interpretM[M[_], F[_], A, B](f: A => M[B], φ: AlgebraM[M, F, B]):
      AlgebraM[M, CoEnv[A, F, ?], B] =
    ginterpretM[Id, M, F, A, B](f, φ)

  def ginterpretM[W[_], M[_], F[_], A, B](f: A => M[B], φ: GAlgebraM[W, M, F, B]):
      GAlgebraM[W, M, CoEnv[A, F, ?], B] =
    _.run.fold(f, φ)

  /** A specialization of `interpret` where the leaves are of the result type.
    * This folds a Free that you may think of as “already partially-folded”.
    * It’s also the fold of a decomposed `elgot`.
    */
  def recover[F[_], A](φ: Algebra[F, A]): Algebra[CoEnv[A, F, ?], A] =
    interpret(x => x, φ)
}
