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
import matryoshka.patterns.EnvT

import scalaz._

trait CofreeInstances {
  implicit def cofreeRecursive[F[_], A]: Recursive.Aux[Cofree[F, A], EnvT[A, F, ?]] =
    new Recursive[Cofree[F, A]] {
      type Base[B] = EnvT[A, F, B]

      def project(t: Cofree[F, A])(implicit BF: Functor[Base]) =
        EnvT((t.head, t.tail))
    }

  implicit def cofreeCorecursive[F[_], A]: Corecursive.Aux[Cofree[F, A], EnvT[A, F, ?]] =
    new Corecursive[Cofree[F, A]] {
      type Base[B] = EnvT[A, F, B]

      def embed(t: EnvT[A, F, Cofree[F, A]])(implicit BF: Functor[Base]) =
        Cofree(t.ask, t.lower)
    }

  implicit def cofreeEqual[F[_]](implicit F: Delay[Equal, F]):
      Delay[Equal, Cofree[F, ?]] =
    new Delay[Equal, Cofree[F, ?]] {
      def apply[A](eq: Equal[A]) = Equal.equal((a, b) =>
        eq.equal(a.head, b.head) && F(cofreeEqual(F)(eq)).equal(a.tail, b.tail))
    }

  implicit def cofreeShow[F[_]](implicit F: Delay[Show, F]):
      Delay[Show, Cofree[F, ?]] =
    new Delay[Show, Cofree[F, ?]] {
      def apply[A](s: Show[A]) =
        Show.show(cof => Cord("(") ++ s.show(cof.head) ++ Cord(", ") ++ F(cofreeShow(F)(s)).show(cof.tail) ++ Cord(")"))
    }
}

object cofree extends CofreeInstances
