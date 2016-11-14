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

import scalaz._

trait BirecursiveT[T[_[_]]] extends RecursiveT[T] with CorecursiveT[T]

object BirecursiveT {
  def birecursive[T[_[_]]: BirecursiveT, F[_]]: Birecursive.Aux[T[F], F] =
    new Birecursive[T[F]] {
      type Base[A] = F[A]
      def project(t: T[F])(implicit F: Functor[F]) =
        RecursiveT[T].projectT[F](t)
      override def cata[A](t: T[F])(f: Algebra[F, A])(implicit F: Functor[F]) =
        RecursiveT[T].cataT[F, A](t)(f)
      def embed(t: F[T[F]])(implicit F: Functor[F]) =
        CorecursiveT[T].embedT[F](t)
      override def ana[A](a: A)(f: Coalgebra[F, A])(implicit F: Functor[F]) =
        CorecursiveT[T].anaT[F, A](a)(f)
    }
}
