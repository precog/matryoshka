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

import matryoshka.distributive._

import scalaz._, Scalaz._
import simulacrum.typeclass

/** Operations that compose a fold with an unfold, without ever building up some
  * intermediate recursive structure.
  */
package object refold {
  /** Composition of an anamorphism and a catamorphism that avoids building the
    * intermediate recursive data structure.
    */
  def hylo[F[_]: Functor, A, B](a: A)(φ: F[B] => B, ψ: A => F[A]): B =
    φ(ψ(a) ∘ (hylo(_)(φ, ψ)))

  /** A Kleisli hylomorphism. */
  def hyloM[M[_]: Monad, F[_]: Traverse, A, B](
    a: A)(
    φ: AlgebraM[M, F, B], ψ: CoalgebraM[M, F, A]):
      M[B] =
    ψ(a) >>= (_.traverse(hyloM(_)(φ, ψ)) >>= φ)

  /** A generalized version of a hylomorphism that composes any coalgebra and
    * algebra.
    */
  def ghylo[F[_]: Functor, W[_]: Comonad, M[_], A, B](
    a: A)(
    w: DistributiveLaw[F, W],
    m: DistributiveLaw[M, F],
    φ: GAlgebra[W, F, B],
    ψ: GCoalgebra[M, F, A])(
    implicit M: Monad[M]):
      B = {
    def h(x: M[A]): W[B] = w(m(M.lift(ψ)(x)) ∘ (y => h(y.join).cojoin)) ∘ φ
    h(a.point[M]).copoint
  }

  /** Similar to a hylomorphism, this composes a futumorphism and a
    * histomorphism.
    */
  def chrono[F[_]: Functor, A, B](
    a: A)(
    φ: F[Cofree[F, B]] => B, ψ: A => F[Free[F, A]]):
      B =
    ghylo[F, Cofree[F, ?], Free[F, ?], A, B](a)(distHisto, distFutu, φ, ψ)

  def elgot[F[_]: Functor, A, B](a: A)(φ: F[B] => B, ψ: A => B \/ F[A]): B = {
    def h(a: A): B = ψ(a).fold(x => x, x => φ(x ∘ h))
    h(a)
  }

  def coelgot[F[_]: Functor, A, B](a: A)(φ: ((A, F[B])) => B, ψ: A => F[A]):
      B = {
    def h(a: A): B = φ((a, ψ(a) ∘ h))
    h(a)
  }

  def coelgotM[M[_]: Monad, F[_]: Traverse, A, B](
    a: A)(
    φ: (A, F[B]) => M[B], ψ: A => M[F[A]]):
      M[B] = {
    def h(a: A): M[B] = ψ(a) >>= (_.traverse(h)) >>= (φ(a, _))
    h(a)
  }
}
