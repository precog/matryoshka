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

import matryoshka.Recursive.ops._
import matryoshka.distributive._

import scala.{Function, inline, Predef}

import scalaz._, Scalaz._
import simulacrum.typeclass

/** Unfolds for corecursive data types.
  *
  * These unfolds deconstruct some value into pieces, and recursively
  * deconstruct those pieces, top-down, ending at leaf nodes.
  */
@typeclass trait Corecursive[T[_[_]]] {
  /** Wrap the exposed layer in a fixpoint operator. */
  def embed[F[_]: Functor](t: F[T[F]]): T[F]

  /** The anamorphism is the most basic unfold. */
  def ana[F[_]: Functor, A](a: A)(ψ: A => F[A]): T[F] =
    embed(ψ(a) ∘ (ana(_)(ψ)))

  /** A Kleisli version of [matryoshka.Corecursive.ana]]. */
  def anaM[F[_]: Traverse, M[_]: Monad, A](a: A)(ψ: A => M[F[A]]): M[T[F]] =
    ψ(a).flatMap(_.traverse(anaM(_)(ψ))) ∘ (embed(_))

  def gana[F[_]: Functor, M[_], A](
    a: A)(
    k: DistributiveLaw[M, F], ψ: A => F[M[A]])(
    implicit M: Monad[M]):
      T[F] = {
    def loop(x: M[F[M[A]]]): T[F] = embed(k(x) ∘ (x => loop(M.lift(ψ)(x.join))))

    loop(ψ(a).point[M])
  }

  /** The apomorphism allows the fold to “short-circuit” branches by returning a
    * full branch on the left, rather than the remaining `A` on the right.
    */
  def apo[F[_]: Functor, A](a: A)(ψ: A => F[T[F] \/ A]): T[F] =
    embed(ψ(a) ∘ (_.fold(Predef.identity, apo(_)(ψ))))

  /** A Kleisli apomorphism. */
  def apoM[F[_]: Traverse, M[_]: Monad, A](a: A)(ψ: A => M[F[T[F] \/ A]]): M[T[F]] =
    ψ(a).flatMap(_.traverse(_.fold(_.point[M], apoM(_)(ψ)))) ∘ (embed(_))

  def gapo[F[_]: Functor, A, B](a: A)(f: B => F[B], g: A => F[B \/ A]): T[F] =
    embed(g(a) ∘ (_.fold(ana(_)(f), gapo(_)(f, g))))

  /** The postpromorphism applies a natural transformation after each step of
    * the unfold.
    */
  def postpro[F[_]: Functor, A](a: A)(e: F ~> F, ψ: A => F[A])(implicit T: Recursive[T]): T[F] =
    embed(ψ(a) ∘ (x => ana(postpro(x)(e, ψ))(x => e(x.project))))

  def gpostpro[F[_]: Functor, M[_], A](
    a: A)(
    k: DistributiveLaw[M, F], e: F ~> F, ψ: A => F[M[A]])(
    implicit T: Recursive[T], M: Monad[M]):
      T[F] = {
    def loop(ma: M[A]): T[F] =
      embed(k(M.lift(ψ)(ma)) ∘ (x => ana(loop(x.join))(x => e(x.project))))

    loop(a.point[M])
  }

  /** The futumorphism can emit multiple levels of the tree on step of the
    * unfold.
    */
  def futu[F[_]: Functor, A](a: A)(ψ: A => F[Free[F, A]]): T[F] =
    gana[F, Free[F, ?], A](a)(distFutu, ψ)

  /** A Kleisli futumorphism. */
  def futuM[F[_]: Traverse, M[_]: Monad, A](a: A)(ψ: A => M[F[Free[F, A]]]):
      M[T[F]] = {
    def loop(free: Free[F, A]): M[T[F]] =
      free.fold(futuM(_)(ψ), _.traverse(loop) ∘ (embed[F]))
    ψ(a).flatMap(_.traverse(loop)) ∘ (embed(_))
  }
}

sealed class CorecursiveOps[T[_[_]], F[_]](self: F[T[F]])(implicit T: Corecursive[T]) {
  def embed(implicit F: Functor[F]): T[F] = T.embed(self)
}
