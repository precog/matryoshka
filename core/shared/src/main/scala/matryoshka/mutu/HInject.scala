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

package matryoshka.mutu

import scala.{None, Option}

import monocle.Prism
import scalaz._, Scalaz._

abstract class HInject[F[_[_], _], G[_[_], _]] extends (F :~> G) {
  def apply[A[_], I](fa: F[A, I]): G[A, I] = inj(fa)
  def inj[A[_], I](fa: F[A, I]): G[A, I]
  def prj[A[_], I](fa: G[A, I]): Option[F[A, I]]

  def prism[A[_], I] = Prism[G[A, I], F[A, I]](prj)(inj)
}

object HInject {
  implicit def reflexive[F[_[_], _]]: HInject[F, F] = new HInject[F, F] {
    def inj[A[_], I](fa: F[A, I]) = fa
    def prj[A[_], I](fa: F[A, I]) = fa.some
  }

  implicit def leftHCoproduct[F[_[_], _], G[_[_], _]]:
      HInject[F, HCoproduct[F, G, ?[_], ?]] =
    new HInject[F, HCoproduct[F, G, ?[_], ?]] {
      def inj[A[_], I](fa: F[A, I]) = Inl(fa)
      def prj[A[_], I](co: HCoproduct[F, G, A, I]) = co match {
        case Inl(fa) => fa.some
        case Inr(_)  => None
      }
    }

  implicit def rightHCoproduct[F[_[_], _], G[_[_], _]]:
      HInject[G, HCoproduct[F, G, ?[_], ?]] =
    new HInject[G, HCoproduct[F, G, ?[_], ?]] {
      def inj[A[_], I](ga: G[A, I]) = Inr(ga)
      def prj[A[_], I](co: HCoproduct[F, G, A, I]) = co match {
        case Inl(_)  => None
        case Inr(ga) => ga.some
      }
    }
}
