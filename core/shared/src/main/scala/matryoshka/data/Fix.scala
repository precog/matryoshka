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
  implicit def recursiveT: RecursiveT[Fix] = new RecursiveT[Fix] {
    def projectT[F[_]: Functor](t: Fix[F]) = t.unFix
  }

  implicit def corecursiveT: CorecursiveT[Fix] = new CorecursiveT[Fix] {
    def embedT[F[_]: Functor](t: F[Fix[F]]) = Fix(t)
  }

  implicit def recursive[F[_]]: Recursive.Aux[Fix[F], F] =
    RecursiveT.recursive[Fix, F]

  implicit def corecursive[F[_]]: Corecursive.Aux[Fix[F], F] =
    CorecursiveT.corecursive[Fix, F]

  implicit def equal[F[_]: Functor](implicit F: Delay[Equal, F]): Equal[Fix[F]] =
    Recursive.equal[Fix[F], F]

  implicit def show[F[_]: Functor](implicit F: Delay[Show, F]): Show[Fix[F]] =
    Recursive.show[Fix[F], F]
}
