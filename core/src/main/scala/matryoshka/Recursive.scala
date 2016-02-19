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

import scala.{Boolean, inline, PartialFunction}
import scala.collection.immutable.{List, Nil}

import scalaz._, Scalaz._
import simulacrum.typeclass

/** Folds for recursive data types.
  *
  * All of these take an arbitrary recursive structure and fold, starting with
  * the innermost nodes. Since these leaf nodes have no children, the `A` in
  * `F[A]` is irrelevant, because there are zero `A`s. Then, when you move down
  * to the next node, all of its children have been converted to `A`, and so on,
  * until you get to the top level.
  */
@typeclass trait Recursive[T[_[_]]] {
  // /** Expose the outermost layer of the fixpoint structure. */
  def project[F[_]: Functor](t: T[F]): F[T[F]]

  // /** A generalization of [[scalaz.Foldable.foldRight]] to any fixpoint
  //   * structure. This is the most basic fold.
  //   */
  def cata[F[_]: Functor, A](t: T[F])(φ: F[A] => A): A =
    φ(project(t) ∘ (cata(_)(φ)))

  /** A Kleisli version of [[matryoshka.Recursive.cata]]. */
  def cataM[F[_]: Traverse, M[_]: Monad, A](t: T[F])(φ: F[A] => M[A]): M[A] =
    project(t).traverse(cataM(_)(φ)).flatMap(φ)

  def gcata[F[_]: Functor, W[_]: Comonad, A](
    t: T[F])(
    k: DistributiveLaw[F, W], φ: F[W[A]] => A):
      A = {
    def loop(t: T[F]): W[F[W[A]]] = k(project(t) ∘ (loop(_).map(φ).cojoin))

    φ(loop(t).copoint)
  }

  /** The paramorphism is a fold that provides access to the unfolded children
    * as well as their folded values.
    */
  def para[F[_]: Functor, A](t: T[F])(φ: F[(T[F], A)] => A): A =
    φ(project(t) ∘ (t => (t, para(t)(φ))))

  /** A Kleisli version of [[matryoshka.Recursive.para]]. */
  def paraM[F[_]: Traverse, M[_]: Monad, A](t: T[F])(φ: F[(T[F], A)] => M[A]):
      M[A] =
    project(t).traverse(v => paraM(v)(φ) ∘ ((v, _))).flatMap(φ)

  def gpara[F[_]: Functor, W[_]: Comonad, A](
    t: T[F])(
    e: DistributiveLaw[F, W], φ: F[EnvT[T[F], W, A]] => A)(
    implicit T: Corecursive[T]):
      A =
    gzygo[F, W, A, T[F]](t)(T.embed(_), e, φ)

  /** The zygomorphism is a fold with a “helper” fold. It provides access to the
    * result of applying some other algebra to the children as well as their
    * values under the current algebra.
    */
  def zygo[F[_]: Functor, A, B](t: T[F])(f: F[B] => B, g: F[(B, A)] => A): A =
    gcata[F, (B, ?), A](t)(distZygo(f), g)

  def gzygo[F[_]: Functor, W[_]: Comonad, A, B](
    t: T[F])(
    f: F[B] => B, w: DistributiveLaw[F, W], g: F[EnvT[B, W, A]] => A):
      A =
    gcata[F, EnvT[B, W, ?], A](t)(distZygoT(f, w), g)

  /** The mutumorphism is a mutually-recursive fold. It generalizes
    * [[matryoshka.Recursive.zygo]] to allow each algebra to access the results
    * of the other.
    */
  def mutu[F[_]: Functor, A, B](t: T[F])(f: F[(A, B)] => B, g: F[(B, A)] => A):
      A =
    g(project(t) ∘ (x => (mutu(x)(g, f), mutu(x)(f, g))))

  /** The histomorphism provides access to both the unfolded structure as well
    * as the history of the results of the children to the current algebra.
    */
  def histo[F[_]: Functor, A](t: T[F])(φ: F[Cofree[F, A]] => A): A =
    gcata[F, Cofree[F, ?], A](t)(distHisto, φ)

  def ghisto[F[_]: Functor, H[_]: Functor, A](
    t: T[F])(
    g: DistributiveLaw[F, H], φ: F[Cofree[H, A]] => A):
      A =
    gcata[F, Cofree[H, ?], A](t)(distGHisto(g), φ)

  /** This variation on [[matryoshka.Recursive.zygo]] makes the “helper”
    * paramorphic.
    */
  def paraZygo[F[_]:Functor: Unzip, A, B](
    t: T[F])(
    f: F[(T[F], B)] => B,
    g: F[(B, A)] => A):
      A = {
    def h(t: T[F]): (B, A) =
      (project(t) ∘ { x =>
        val (b, a) = h(x)
        ((x, b), (b, a))
      }).unfzip.bimap(f, g)

    h(t)._2
  }

  def isLeaf[F[_]: Functor: Foldable](t: T[F]): Boolean =
    !Tag.unwrap(project[F](t).foldMap(_ => true.disjunction))

  def children[F[_]: Functor: Foldable](t: T[F]): List[T[F]] =
    project[F](t).foldMap(_ :: Nil)

  def universe[F[_]: Functor: Foldable](t: T[F]): List[T[F]] =
    t :: children(t).flatMap(universe[F])

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

  // Foldable
  def all[F[_]: Functor: Foldable](t: T[F])(p: T[F] ⇒ Boolean): Boolean =
    Tag.unwrap(foldMap(t)(p(_).conjunction))

  def any[F[_]: Functor: Foldable](t: T[F])(p: T[F] ⇒ Boolean): Boolean =
    Tag.unwrap(foldMap(t)(p(_).disjunction))

  def collect[F[_]: Functor: Foldable, B](t: T[F])(pf: PartialFunction[T[F], B]):
      List[B] =
    foldMap(t)(pf.lift(_).toList)

  def contains[F[_]: Functor: Foldable](t: T[F], c: T[F])(implicit T: Equal[T[F]]):
      Boolean =
    any(t)(_ ≟ c)

  def foldMap[F[_]: Functor: Foldable, Z: Monoid](t: T[F])(f: T[F] => Z): Z =
    foldMapM[F, Free.Trampoline, Z](t)(f(_).pure[Free.Trampoline]).run

  def foldMapM[F[_]: Functor: Foldable, M[_]: Monad, Z: Monoid](t: T[F])(f: T[F] => M[Z]):
      M[Z] = {
    def loop(z0: Z, term: T[F]): M[Z] = {
      for {
        z1 <- f(term)
        z2 <- project[F](term).foldLeftM(z0 ⊹ z1)(loop(_, _))
      } yield z2
    }

    loop(Monoid[Z].zero, t)
  }

  // /** This function converts a structure using one fixpoint operator to a
  //   * structure using another fixpoint operator.
  //   */
  def convertTo[F[_]: Functor, R[_[_]]: Corecursive](t: T[F]): R[F] =
    cata(t)(Corecursive[R].embed[F])
}
