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

import matryoshka.∘

import scala.{Boolean}
import scala.inline

import scalaz._
import simulacrum.typeclass

@typeclass trait KEqual[F[_]] {
  def keq[I, J](l: F[I], r: F[J]): Boolean
}

// TODO[simulacrum]: These should be typeclasses, but simulacrum doesn’t like
//                   the shape of them.

// TODO: determine if we need to “delay” this
trait EqualHF[F[_[_], _]] {
  def eqHF[G[_]: KEqual, I, J](l: F[G, I], r: F[G, J]): Boolean
}
object EqualHF {
  def apply[F[_[_], _]](implicit F: EqualHF[F]) = F

  implicit def kequal[F[_[_], _]: EqualHF, G[_]: KEqual, I, J]:
      KEqual[F[G, ?]] =
    new KEqual[F[G, ?]] {
      def keq[I, J](l: F[G, I], r: F[G, J]) = EqualHF[F].eqHF(l, r)
    }
}

trait HFunctor[H[_[_], _]] {
  // def fmap[F[_]: Functor, A, B](hfa: H[F, A])(f: A => B): H[F, B]
  def hmap[F[_], G[_]](f: F ~> G): H[F, ?] ~> H[G, ?]
}
object HFunctor {
  def apply[H[_[_], _]](implicit H: HFunctor[H]) = H

  // implicit def hfunctorFunctor[H[_[_], _]: HFunctor, F[_]: Functor]:
  //     Functor[H[F, ?]] =
  //   new Functor[H[F, ?]] {
  //     def map[A, B](fa: H[F, A])(f: A => B) = HFunctor[H].fmap(fa)(f)
  //   }
}

trait HPointed[H[_[_], _]] extends HFunctor[H] {
  def hpoint[F[_]: Functor, A](fa: F[A]): H[F, A]
}
object HPointed {
  // implicit def hpointedPointed[H[_[_], _]: HPointed, F[_]: Pointed]:
  //     Pointed[H[F, ?]] =
  //   new Pointed[H[F, ?]] {
  //     def point[A](a: A) = HPointed[H].hpoint(a.point[F])
  //   }
}

trait HCopointed[H[_[_], _]] extends HFunctor[H] {
  def hcopoint[F[_]: Functor, A](hf: H[F, A]): F[A]
}
object HCopointed {
  // implicit def hcopointedCopointed[H[_[_], _]: HCopointed, F[_]: Copointed]:
  //     Copointed[H[F, ?]] =
  //   new Copointed[H[F, ?]] {
  //     def copoint[A](hf: H[F, A]) = HCopointed[H].hcopoint(hf).copoint
  //   }
}

// trait HFoldable[H[_[_], _]] {
//   def hfold[M: Monoid]: H[K[M, ?], ?] :=> M = hfoldMap[K[M, ?], M](_.unK)

//   def hfoldMap[A[_], M](f: A :=> M)(implicit M: Monoid[M]): H[A, ?] :=> M =
//     hfoldr(l => M.append(f(l), _), M.zero)

//   def hfoldr[A[_], B](f: A :=> B => B, z: B): H[A, ?] :=> B =
//     new (H[A, ?] :=> B) {
//       def apply[I](t: H[A, I]) = appEndo(hfoldMap(Endo <<< f)(t), z)
//     }
// }

trait HTraverse[H[_[_], _]] extends HFunctor[H] // with HFoldable[H]
{
  def htraverse[F[_]: Applicative, A[_], B[_]](natM: A ~> (F ∘ B)#λ):
      H[A, ?] ~> (F ∘ H[B, ?])#λ

  def hmap[F[_], G[_]](f: F ~> G): H[F, ?] ~> H[G, ?] =
    htraverse[Scalaz.Id, F, G](f)
}
object HTraverse {
  def apply[H [_[_], _]](implicit H: HTraverse[H]) = H
}
