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

/** This is the simplest fixpoint type, implemented with general recursion.
  */
final case class Fix[F[_]](unFix: F[Fix[F]])
object Fix {
  implicit def matryoshka[F[_]]:
      Recursive.Aux[Fix[F], F] with Corecursive.Aux[Fix[F], F] =
    new Recursive[Fix[F]] with Corecursive[Fix[F]] {
      type Base[A] = F[A]

      def project(t: Fix[F]) = t.unFix

      def embed(t: F[Fix[F]]) = Fix(t)
    }

  implicit def equal[F[_]](implicit F: Equal ~> λ[α => Equal[F[α]]]):
      Equal[Fix[F]] =
    Equal.equal((a, b) => F(equal[F]).equal(a.unFix, b.unFix))

  implicit def show[F[_]](implicit F: Show ~> λ[α => Show[F[α]]]):
      Show[Fix[F]] =
    // NB: Doesn’t use Recursive.show in order to avoid the Functor constraint.
    Show.show(f => F(show[F]).show(f.unFix))
}
