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

import Recursive.ops._

import scalaz._, Scalaz._

/** This is for inductive (finite) recursive structures, models the concept of
  * “data”, aka, the “least fixed point”.
  */
final case class Mu[F[_]](unMu: λ[A => (F[A] => A)] ~> Id)
object Mu {
  implicit def recursive[F[_]]: Recursive[Mu[F]] with Corecursive[Mu[F]] =
    new Recursive[Mu[F]] with Corecursive[Mu[F]] {
      type Base[A] = F[A]

      // FIXME: ugh, shouldn’t have to redefine `lambek` in here?
      def project(t: Mu[F]) = cata[F[Mu[F]]](t)(_ ∘ embed)
      override def cata[A](t: Mu[F])(f: F[A] => A) = t.unMu(f)

      def embed(t: F[Mu[F]]) =
        Mu(new (λ[A => (F[A] => A)] ~> Id) {
          def apply[A](fa: F[A] => A): A = fa(t.map(cata(_)(fa)))
        })
    }

  implicit def equal[F[_]: Functor](implicit F: Equal ~> λ[α => Equal[Recursive[Mu[F]]#Base[α]]]):
      Equal[Mu[F]] =
    Equal.equal((a, b) => F(equal[F]).equal(a.project, b.project))

  implicit def show[F[_]: Functor](implicit  T: Recursive.Aux[Mu[F], F], F: Show ~> λ[α => Show[F[α]]]):
      Show[Mu[F]] =
    Recursive.show[Mu[F], F](T, F)
}
