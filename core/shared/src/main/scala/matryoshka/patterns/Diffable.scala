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

package matryoshka.patterns

import matryoshka._
import matryoshka.implicits._

import scala.Option
import scala.Predef.implicitly

import scalaz._, Scalaz._
import simulacrum._

@typeclass trait Diffable[F[_]] { self =>
  def diffImpl[T[_[_]]: RecursiveT: CorecursiveT](l: T[F], r: T[F]):
      Option[DiffT[T, F]]

  /** Useful when a case class has a `List[A]` that isn’t the final `A`. This is
    * because the normal comparison just walks over the children of the functor,
    * so if the lists are different lengths, the default behavior will be
    * confusing.
    * Currently also useful when the only list _is_ the final parameter, because
    * it allows you to explicitly use `Similar` rather than `LocallyDifferent`.
    */
  def diffTraverse[T[_[_]]: RecursiveT: CorecursiveT, G[_]: Traverse](
    left: G[T[F]], right: G[T[F]])(
    implicit FF: Functor[F], FoldF: Foldable[F], FM: Merge[F]):
      G[DiffT[T, F]] =
    if (left.toList.length < right.toList.length)
      left.zipWithR(right)((l, r) =>
        l.fold(
          Added[T, F, T[Diff[T, F, ?]]](r).embedT)(
          RecursiveT.recursive[T, F].paraMerga(_, r)(diff(RecursiveT[T], CorecursiveT[T], self, implicitly, implicitly))))
    else
      left.zipWithL(right)((l, r) =>
        r.fold(
          Removed[T, F, T[Diff[T, F, ?]]](l).embedT)(
          RecursiveT.recursive[T, F].paraMerga(l, _)(diff(RecursiveT[T], CorecursiveT[T], self, implicitly, implicitly))))

  // TODO: create something like Equals, but that overrides G[F[_]] (where G
  //       implements Traverse) to always be equal. This should allow us to
  //       distinguish between, say, two things containing a List[F[_]] that
  //       only differ on the length of the list. So we can make them `Similar`
  //       rather than `LocallyDifferent`.

  def localDiff[T[_[_]]: RecursiveT: CorecursiveT](
    left: F[T[F]], right: F[T[F]])(
    implicit FT: Traverse[F], FM: Merge[F]):
      DiffT[T, F] =
    LocallyDifferent[T, F, T[Diff[T, F, ?]]](diffTraverse[T, F](left, right), right.void).embedT
}
