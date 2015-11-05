/*
 * Copyright 2014 - 2015 SlamData Inc.
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

package quasar.recursionschemes

import quasar.Predef._

import scalaz._, Scalaz._
import simulacrum.typeclass

@typeclass trait TraverseT[T[_[_]]] extends FunctorT[T] {
  def traverse[M[_]: Applicative, F[_], G[_]](t: T[F])(f: F[T[F]] => M[G[T[G]]]): M[T[G]]

  def map[F[_], G[_]](t: T[F])(f: F[T[F]] => G[T[G]]) = traverse[Id, F, G](t)(f)

  def transformM[F[_]: Traverse, M[_]: Monad](t: T[F])(f: T[F] => M[T[F]]): M[T[F]] =
    traverse(t)(_.traverse(transformM(_)(f))).flatMap(f)

  def topDownTransformM[F[_]: Traverse, M[_]: Monad](t: T[F])(f: T[F] => M[T[F]]): M[T[F]] =
    f(t).flatMap(traverse(_)(_.traverse(transformM(_)(f))))

  def topDownCataM[F[_]: Traverse, M[_]: Monad, A](
    t: T[F], a: A)(
    f: (A, T[F]) => M[(A, T[F])]):
      M[T[F]] =
    f(a, t).flatMap { case (a, tf) =>
      traverse(tf)(_.traverse(topDownCataM(_, a)(f)))
    }

  def transCataM[M[_]: Monad, F[_]: Traverse, G[_]](t: T[F])(f: F[T[G]] => M[G[T[G]]]): M[T[G]] =
    traverse(t)(_.traverse(transCataM(_)(f)).flatMap(f))
}
