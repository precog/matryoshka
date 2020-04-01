/*
 * Copyright 2020 Precog Data
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

package matryoshka.patterns

import slamdata.Predef._
import matryoshka._

import scalaz._, Scalaz._
import simulacrum._

@typeclass trait Diffable[F[_]] { self =>
  private implicit def selfʹ: Diffable[F] = self

  def diffImpl[T[_[_]]: BirecursiveT](l: T[F], r: T[F]):
      Option[DiffT[T, F]]

  /** Useful when a case class has a `List[A]` that isn’t the final `A`. This is
    * because the normal comparison just walks over the children of the functor,
    * so if the lists are different lengths, the default behavior will be
    * confusing.
    * Currently also useful when the only list _is_ the final parameter, because
    * it allows you to explicitly use `Similar` rather than `LocallyDifferent`.
    */
  def diffTraverse[T[_[_]]: BirecursiveT, G[_]: Traverse](
    left: G[T[F]], right: G[T[F]])(
    implicit FF: Functor[F], FoldF: Foldable[F], FM: Merge[F]):
      G[DiffT[T, F]] = {
    val B = Birecursive[T[Diff[T, F, ?]], Diff[T, F, ?]]
    val B2 = Birecursive[T[F], F]
    if (left.toList.length < right.toList.length)
      left.zipWithR(right)((l, r) =>
        l.fold(
          B.embed(Added[T, F, T[Diff[T, F, ?]]](r)))(
          x => B2.paraMerga(x, r)(diff[T, F])))
    else
      left.zipWithL(right)((l, r) =>
        r.fold(
          B.embed(Removed[T, F, T[Diff[T, F, ?]]](l)))(
          x => B2.paraMerga(l, x)(diff[T, F])))
  }

  // TODO: create something like Equals, but that overrides G[F[_]] (where G
  //       implements Traverse) to always be equal. This should allow us to
  //       distinguish between, say, two things containing a List[F[_]] that
  //       only differ on the length of the list. So we can make them `Similar`
  //       rather than `LocallyDifferent`.

  def localDiff[T[_[_]]: BirecursiveT](
    left: F[T[F]], right: F[T[F]])(
    implicit FT: Traverse[F], FM: Merge[F]):
      DiffT[T, F] =
    Corecursive[T[Diff[T, F, ?]], Diff[T, F, ?]].embed(LocallyDifferent[T, F, T[Diff[T, F, ?]]](diffTraverse[T, F](left, right), right.void))
}
