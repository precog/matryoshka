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

package matryoshka.data

import matryoshka._

import scalaz._

/** This is the simplest fixpoint type, implemented with general recursion.
  */
final case class Fix[F[_]](unFix: F[Fix[F]])
object Fix {
  implicit def recursive[F[_]]: Recursive.Aux[Fix[F], F] =
    new Recursive[Fix[F]] {
      type Base[A] = F[A]

      def project(t: Fix[F]) = t.unFix
    }

  implicit def corecursive[F[_]]: Corecursive.Aux[Fix[F], F] =
    new Corecursive[Fix[F]] {
      type Base[A] = F[A]

      def embed(t: F[Fix[F]]) = Fix(t)
    }


  implicit def equal[F[_]](implicit F: Delay[Equal, Based[Fix[F]]#Base])
      : Equal[Fix[F]] =
    Recursive.equal[Fix[F]]

  implicit def show[F[_]](implicit F: Delay[Show, Based[Fix[F]]#Base])
      : Show[Fix[F]] =
    Recursive.show[Fix[F]]
}
