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

package matryoshka.data

import matryoshka._
import matryoshka.patterns.EnvT

import scalaz._, Scalaz._

trait CofreeInstances {
  implicit def cofreeBirecursive[F[_], A]
      : Birecursive.Aux[Cofree[F, A], EnvT[A, F, ?]] =
    Birecursive.fromAlgebraIso(
      t => Cofree(t.ask, t.lower),
      t => EnvT((t.head, t.tail)))

  implicit def cofreeEqual[F[_]: Traverse](implicit F: Delay[Equal, F]):
      Delay[Equal, Cofree[F, ?]] =
    new Delay[Equal, Cofree[F, ?]] {
      def apply[A](eq: Equal[A]) = {
        implicit val envtʹ: Delay[Equal, EnvT[A, F, ?]] = EnvT.equal(eq, F)

        Birecursive.equal[Cofree[F, A], EnvT[A, F, ?]]
      }
    }

  implicit def cofreeShow[F[_]: Functor](implicit F: Delay[Show, F]):
      Delay[Show, Cofree[F, ?]] =
    new Delay[Show, Cofree[F, ?]] {
      def apply[A](s: Show[A]) = {
        implicit val envtʹ: Delay[Show, EnvT[A, F, ?]] = EnvT.show(s, F)

        Recursive.show[Cofree[F, A], EnvT[A, F, ?]]
      }
    }
}

object cofree extends CofreeInstances
