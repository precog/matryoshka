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

import scalaz._, Scalaz._
import simulacrum._

@typeclass trait TraverseT[T[_[_]]] extends FunctorT[T] {
  def traverseT[M[_]: Applicative, F[_]: Functor, G[_]: Functor]
    (t: T[F])
    (f: F[T[F]] => M[G[T[G]]])
      : M[T[G]]

  override def mapT[F[_]: Functor, G[_]: Functor]
    (t: T[F])
    (f: F[T[F]] => G[T[G]]) =
    traverseT[Id, F, G](t)(f)

  def transCataTM[F[_]: Traverse, M[_]: Monad](t: T[F])(f: T[F] => M[T[F]])
      : M[T[F]] =
    traverseT(t)(_.traverse(transCataTM(_)(f))).flatMap(f)

  def transAnaTM[F[_]: Traverse, M[_]: Monad](t: T[F])(f: T[F] => M[T[F]])
      : M[T[F]] =
    f(t).flatMap(traverseT(_)(_.traverse(transAnaTM(_)(f))))

  def transCataM[M[_]: Monad, F[_]: Traverse, G[_]: Functor]
    (t: T[F])
    (f: AlgebraicTransformM[T, M, F, G])
      : M[T[G]] =
    traverseT(t)(_.traverse(transCataM(_)(f)).flatMap(f))

  def transAnaM[M[_]: Monad, F[_]: Functor, G[_]: Traverse]
    (t: T[F])
    (f: CoalgebraicTransformM[T, M, F, G])
      : M[T[G]] =
    traverseT(t)(f(_).flatMap(_.traverse(transAnaM(_)(f))))

  def topDownCataM[F[_]: Traverse, M[_]: Monad, A](
    t: T[F], a: A)(
    f: (A, T[F]) => M[(A, T[F])]):
      M[T[F]] =
    f(a, t).flatMap { case (a, tf) =>
      traverseT(tf)(_.traverse(topDownCataM(_, a)(f)))
    }
}

object TraverseT {
  implicit def birecursiveT[T[_[_]]: RecursiveT: CorecursiveT]: TraverseT[T] =
    new TraverseT[T] {
      def traverseT[M[_]: Applicative, F[_]: Functor, G[_]: Functor]
        (t: T[F])
        (f: F[T[F]] => M[G[T[G]]]) =
        f(RecursiveT[T].projectT(t)).map(CorecursiveT[T].embedT[G])
    }
}
