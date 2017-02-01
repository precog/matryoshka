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

package matryoshka.scalacheck

import matryoshka._
import matryoshka.data._
import matryoshka.implicits._

import org.scalacheck._
import scalaz._, Scalaz._

trait ShrinkInstancesʹ {
  implicit def delayShrink[F[_], A](implicit A: Shrink[A], F: Delay[Shrink, F]): Shrink[F[A]] =
    F(A)
}

trait ShrinkInstances extends ShrinkInstancesʹ {
  /** An instance for [[matryoshka.Recursive]] types where the [[Base]] is
    * [[scalaz.Foldable]].
    */
  def recursiveShrink[T, F[_]: Functor: Foldable]
    (implicit T: Recursive.Aux[T, F])
      : Shrink[T] =
    Shrink(_.project.toStream)

  /** An instance for [[matryoshka.Birecursive]] types where the [[Base]] has a
    * [[scalacheck.Shrink]] instance.
    */
  def shrinkCorecursiveShrink[T, F[_]: Functor]
    (implicit TR: Recursive.Aux[T, F], TC: Corecursive.Aux[T, F], F: Shrink[F[T]])
      : Shrink[T] =
    Shrink(t => F.shrink(t.project).map(_.embed))

  /** An instance for [[matryoshka.Birecursive]] types where the [[Base]] has
    * both [[scalaz.Foldable]] and [[scalacheck.Shrink]] instances.
    */
  def corecursiveShrink[T, F[_]: Functor: Foldable]
    (implicit TR: Recursive.Aux[T, F], TC: Corecursive.Aux[T, F], F: Shrink[F[T]])
      : Shrink[T] =
    Shrink(t => shrinkCorecursiveShrink[T, F].shrink(t) ++ recursiveShrink[T, F].shrink(t))

  implicit def fixShrink[F[_]: Functor: Foldable](implicit F: Shrink[F[Nu[F]]]): Shrink[Fix[F]] =
    corecursiveShrink[Fix[F], F]

  implicit def muShrink[F[_]: Functor: Foldable](implicit F: Shrink[F[Nu[F]]]): Shrink[Mu[F]] =
    corecursiveShrink[Mu[F], F]

  implicit def nuShrink[F[_]: Functor: Foldable](implicit F: Shrink[F[Nu[F]]]): Shrink[Nu[F]] =
    corecursiveShrink[Nu[F], F]
}

package object shrink extends ShrinkInstances
