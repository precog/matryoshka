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

import scalaz._, Liskov._

package object implicits
    extends Birecursive.ToBirecursiveOps
    with Merge.ToMergeOps
    with Recursive.ToRecursiveOps {

  implicit def toRecursive[T, F[_]](implicit T: Birecursive.Aux[T, F])
      : Recursive.Aux[T, F] =
    T.rec

  implicit def toCorecursive[T, F[_]](implicit T: Birecursive.Aux[T, F])
      : Corecursive.Aux[T, F] =
    T.corec

  implicit def toIdOps[A](a: A): IdOps[A] = new IdOps[A](a)

  implicit final class CorecursiveOps[T, F[_], FF[_]](
    self: F[T])(
    implicit T: Corecursive.Aux[T, FF], Sub: F[T] <~< FF[T]) {

    def embed(implicit F: Functor[FF]): T = T.embed(Sub(self))
  }

  implicit final class BirecursiveOps[T, F[_], FF[_]](
    self: F[T])(
    implicit T: Birecursive.Aux[T, FF], Sub: F[T] <~< FF[T]) {

    def colambek(implicit F: Functor[FF]): T = T.colambek(Sub(self))
  }

  implicit def toAlgebraOps[F[_], A](a: Algebra[F, A]): AlgebraOps[F, A] =
    new AlgebraOps[F, A](a)

  implicit def toCoalgebraOps[F[_], A](a: Coalgebra[F, A]): CoalgebraOps[F, A] =
    new CoalgebraOps[F, A](a)
}
