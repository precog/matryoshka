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

import scalaz._, Scalaz._
import simulacrum.typeclass

/** Folds for recursive data types. */
@typeclass trait Recursive[T[_[_]]] {
  def project[F[_]](t: T[F]): F[T[F]]

  def cata[F[_]: Functor, A](t: T[F])(f: F[A] => A): A =
    f(project(t).map(cata(_)(f)))

  def cataM[F[_]: Traverse, M[_]: Monad, A](t: T[F])(f: F[A] => M[A]): M[A] =
    project(t).map(cataM(_)(f)).sequence.flatMap(f)

  def gcata[F[_]: Functor, W[_]: Comonad, A](
    t: T[F])(
    k: λ[α => F[W[α]]] ~> λ[α => W[F[α]]], g: F[W[A]] => A):
      A = {
    def loop(t: T[F]): W[F[W[A]]] = k(project(t).map(loop(_).map(g).cojoin))

    g(loop(t).copoint)
  }

  def para[F[_]: Functor, A](t: T[F])(f: F[(T[F], A)] => A): A =
    f(project(t).map(t => t -> para(t)(f)))

  def paraM[F[_]: Traverse, M[_]: Monad, A](t: T[F])(f: F[(T[F], A)] => M[A]):
      M[A] =
    project(t).map(v => paraM(v)(f).map(v -> _)).sequence.flatMap(f)

  def gpara[F[_]: Functor, W[_]: Comonad, A](
    t: T[F])(
    e: λ[α => F[W[α]]] ~> λ[α => W[F[α]]], f: F[EnvT[T[F], W, A]] => A)(
    implicit T: Corecursive[T]):
      A =
    gzygo[F, W, A, T[F]](t)(T.embed(_), e, f)

  def zygo[F[_]: Functor, A, B](t: T[F])(f: F[B] => B, g: F[(B, A)] => A): A =
    gcata[F, (B, ?), A](t)(distZygo(f), g)

  def gzygo[F[_]: Functor, W[_]: Comonad, A, B](
    t: T[F])(
    f: F[B] => B,
    w: λ[α => F[W[α]]] ~> λ[α => W[F[α]]],
    g: F[EnvT[B, W, A]] => A):
      A =
    gcata[F, EnvT[B, W, ?], A](t)(distZygoT(f, w), g)

  def histo[F[_]: Functor, A](t: T[F])(f: F[Cofree[F, A]] => A): A =
    gcata[F, Cofree[F, ?], A](t)(distHisto, f)

  def ghisto[F[_]: Functor, H[_]: Functor, A](
    t: T[F])(
    g: λ[α => F[H[α]]] ~> λ[α => H[F[α]]], f: F[Cofree[H, A]] => A):
      A =
    gcata[F, Cofree[H, ?], A](t)(distGHisto(g), f)

  def paraZygo[F[_]:Functor: Unzip, A, B](
    t: T[F]) (
    f: F[(T[F], B)] => B,
    g: F[(B, A)] => A):
      A = {
    def h(t: T[F]): (B, A) =
      (project(t).map { x =>
        val (b, a) = h(x)
        ((x, b), (b, a))
      }).unfzip.bimap(f, g)

    h(t)._2
  }

  def isLeaf[F[_]: Foldable](t: T[F]): Boolean =
    !Tag.unwrap(project(t).foldMap(κ(true.disjunction)))

  def children[F[_]: Foldable](t: T[F]): List[T[F]] =
    project(t).foldMap(_ :: Nil)

  def universe[F[_]: Foldable](t: T[F]): List[T[F]] =
    t :: children(t).flatMap(universe[F])

  def transform[F[_]: Traverse](
    t: T[F])(
    f: T[F] => T[F])(
    implicit T: Corecursive[T]):
      T[F] =
    transformM[F, Free.Trampoline](
      t)(
      (v: T[F]) => f(v).pure[Free.Trampoline]).run

  def transformM[F[_]: Traverse, M[_]: Monad](
    t: T[F])(
    f: T[F] => M[T[F]])(
    implicit T: Corecursive[T]):
      M[T[F]] = {
    def loop(term: T[F]): M[T[F]] = for {
      y <- project(term).traverse(loop _)
      z <- f(T.embed(y))
    } yield z

    loop(t)
  }

  def topDownTransform[F[_]: Traverse](
    t: T[F])(
    f: T[F] => T[F])(
    implicit T: Corecursive[T]):
      T[F] =
    topDownTransformM[F, Free.Trampoline](
      t)(
      (term: T[F]) => f(term).pure[Free.Trampoline]).run

  def topDownTransformM[F[_]: Traverse, M[_]: Monad](
    t: T[F])(
    f: T[F] => M[T[F]])(
    implicit T: Corecursive[T]):
      M[T[F]] = {
    def loop(term: T[F]): M[T[F]] = for {
      x <- f(term)
      y <- project(x).traverse(loop _)
    } yield T.embed(y)

    loop(t)
  }

  def topDownCata[F[_]: Traverse, A](
    t: T[F], a: A)(
    f: (A, T[F]) => (A, T[F]))(
    implicit T: Corecursive[T]):
      T[F] =
    topDownCataM[F, Free.Trampoline, A](
      t, a)(
      (a: A, term: T[F]) => f(a, term).pure[Free.Trampoline]).run

  def topDownCataM[F[_]: Traverse, M[_]: Monad, A](
    t: T[F], a: A)(
    f: (A, T[F]) => M[(A, T[F])])(
    implicit T: Corecursive[T]):
      M[T[F]] = {
    def loop(a: A, term: T[F]): M[T[F]] = for {
      tuple   <- f(a, term)
      (a, tf) =  tuple
      rec     <- project(tf).traverse(loop(a, _))
    } yield T.embed(rec)

    loop(a, t)
  }

  def descend[F[_]: Functor](
    t: T[F])(
    f: T[F] => T[F])(
    implicit T: Corecursive[T]):
      T[F] =
    T.embed(project(t).map(f))

  def descendM[F[_]: Traverse, M[_]: Monad](
    t: T[F])(
    f: T[F] => M[T[F]])(
    implicit T: Corecursive[T]):
      M[T[F]] =
    project(t).traverse(f).map(T.embed[F])

  def rewrite[F[_]: Traverse](
    t: T[F])(
    f: T[F] => Option[T[F]])(
    implicit T: Corecursive[T]): T[F] =
    rewriteM[F, Free.Trampoline](
      t)(
      (term: T[F]) => f(term).pure[Free.Trampoline]).run

  def rewriteM[F[_]: Traverse, M[_]: Monad](
    t: T[F])(
    f: T[F] => M[Option[T[F]]])(
    implicit T: Corecursive[T]):
      M[T[F]] =
    transformM[F, M](t) { term =>
      for {
        x <- f(term)
        y <- x.traverse(rewriteM(_)(f)).map(_.getOrElse(term))
      } yield y
    }

  def restructure[F[_]: Traverse, G[_]](
    t: T[F])(
    f: F[T[G]] => G[T[G]])(
    implicit T: Corecursive[T]):
      T[G] =
    restructureM[F, Free.Trampoline, G](
      t)(
      (term: F[T[G]]) => f(term).pure[Free.Trampoline]).run

  def restructureM[F[_]: Traverse, M[_]: Monad, G[_]](
    t: T[F])(
    f: F[T[G]] => M[G[T[G]]])(
    implicit T: Corecursive[T]):
      M[T[G]] =
    for {
      x <- project(t).traverse(restructureM(_)(f))
      y <- f(x)
    } yield T.embed(y)

  def trans[F[_], G[_]: Functor](
    t: T[F])(
    f: F ~> G)(
    implicit T: Corecursive[T]):
      T[G] =
    T.embed(f(project(t)).map(trans(_)(f)))

  // Foldable
  def all[F[_]: Foldable](t: T[F])(p: T[F] ⇒ Boolean): Boolean =
    Tag.unwrap(foldMap(t)(p(_).conjunction))

  def any[F[_]: Foldable](t: T[F])(p: T[F] ⇒ Boolean): Boolean =
    Tag.unwrap(foldMap(t)(p(_).disjunction))

  def collect[F[_]: Foldable, B](t: T[F])(pf: PartialFunction[T[F], B]):
      List[B] =
    foldMap(t)(pf.lift(_).toList)

  def contains[F[_]: EqualF: Foldable](t: T[F], c: T[F])(implicit T: Equal[T[F]]):
      Boolean =
    any(t)(_ ≟ c)

  def foldMap[F[_]: Foldable, Z: Monoid](t: T[F])(f: T[F] => Z): Z =
    foldMapM[F, Free.Trampoline, Z](t)(f(_).pure[Free.Trampoline]).run

  def foldMapM[F[_]: Foldable, M[_]: Monad, Z](t: T[F])(f: T[F] => M[Z])(implicit Z: Monoid[Z]):
      M[Z] = {
    def loop(z0: Z, term: T[F]): M[Z] = {
      for {
        z1 <- f(term)
        z2 <- project(term).foldLeftM(z0 |+| z1)(loop(_, _))
      } yield z2
    }

    loop(Z.zero, t)
  }

  def forget[F[_]: Functor](t: T[F]): Fix[F] =
    Fix(project(t).map(forget[F]))
}
