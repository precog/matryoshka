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

import scala.Option

import scalaz._, Scalaz._, Leibniz._

package object patterns {
  // These aliases are because these functors require a higher-order functor as
  // a parameter, and it’s likely to be the same as the “outer” one.
  // NB: Still not sure they’re a good idea.

  type DiffT[T[_[_]], F[_]] = T[Diff[T, F, ?]]
  type PotentialFailureT[T[_[_]], F[_], E] = T[PotentialFailure[T, F, E, ?]]

  def diff[T[_[_]]: Recursive: Corecursive, F[_]: Diffable: Functor: Foldable]:
      (T[F], T[F], Option[F[DiffT[T, F]]]) => DiffT[T, F] =
    ((l, r, merged) =>
      merged.fold(
        Diffable[F].diffImpl(l, r).getOrElse(CorecursiveOps[T, Diff[T, F, ?]](Different[T, F, DiffT[T, F]](l, r)).embed))(
        merged => {
          val children = merged.toList
          CorecursiveOps[T, Diff[T, F, ?]](
            if (children.length ≟ children.collect { case Same(_) => () }.length)
              Same[T, F, DiffT[T, F]](l)
            else Similar(merged)).embed
        }))
}
