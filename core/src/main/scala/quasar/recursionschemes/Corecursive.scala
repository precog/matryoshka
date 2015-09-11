/*
 * Copyright 2014 - 2015 SlamData Inc.
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

package quasar.recursionschemes

import quasar.Predef._
import quasar.fp._
import Recursive.ops._

import scalaz._, Scalaz._
import simulacrum.typeclass

/** Unfolds for corecursive data types. */
@typeclass trait Corecursive[T[_[_]]] {
  def embed[F[_]](t: F[T[F]]): T[F]

  def ana[F[_]: Functor, A](a: A)(f: A => F[A]): T[F] =
    embed(f(a).map(ana(_)(f)))

  def gana[F[_]: Functor, M[_], A](
    a: A)(
    k: λ[α => M[F[α]]] ~> λ[α => F[M[α]]], f: A => F[M[A]])(
    implicit M: Monad[M]):
      T[F] = {
    def loop(x: M[F[M[A]]]): T[F] =
      embed(k(x).map(x => loop(M.lift(f)(x.join))))

    loop(f(a).point[M])
  }

  def apo[F[_]: Functor, A](a: A)(f: A => F[T[F] \/ A]): T[F] =
    embed(f(a).map(_.fold(ι, apo(_)(f))))

  def postpro[F[_]: Functor, A](a: A)(e: F ~> F, g: A => F[A])(implicit T: Recursive[T]): T[F] =
    embed(g(a).map(x => ana(postpro(x)(e, g))(x => e(x.project))))

  def gpostpro[F[_]: Functor, M[_], A](
    a: A)(
    k: λ[α => M[F[α]]] ~> λ[α => F[M[α]]], e: F ~> F, g: A => F[M[A]])(
    implicit T: Recursive[T], M: Monad[M]):
      T[F] = {
    def loop(ma: M[A]): T[F] =
      embed(k(M.lift(g)(ma)).map(x => ana(loop(x.join))(x => e(x.project))))

    loop(a.point[M])
  }

  def futu[F[_]: Functor, A](a: A)(f: A => F[Free[F, A]]): T[F] =
    gana[F, Free[F, ?], A](a)(distFutu, f)
}
