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

import Merge.ops._
import matryoshka.patterns.EnvT

import scala.{Boolean, Option, PartialFunction}
import scala.collection.immutable.{List, Nil}

import scalaz._, Scalaz._
import simulacrum.typeclass

/** Folds for recursive data types. */
@typeclass trait Recursive[T[_[_]]] {
  def project[F[_]: Functor](t: T[F]): F[T[F]]

  def cata[F[_]: Functor, A](t: T[F])(f: Algebra[F, A]): A =
    f(project(t) ∘ (cata(_)(f)))

  /** A Kleisli catamorphism. */
  def cataM[F[_]: Traverse, M[_]: Monad, A](t: T[F])(f: AlgebraM[M, F, A]): M[A] =
    project(t).traverse(cataM(_)(f)).flatMap(f)

  /** A catamorphism generalized with a comonad in inside the functor. */
  def gcata[W[_]: Comonad, F[_]: Functor, A](
    t: T[F])(
    k: DistributiveLaw[F, W], g: GAlgebra[W, F, A]):
      A = {
    def loop(t: T[F]): W[F[W[A]]] = k(project(t) ∘ (loop(_).map(g).cojoin))

    g(loop(t).copoint)
  }

  def gcataM[W[_]: Comonad: Traverse, M[_]: Monad, F[_]: Traverse, A](
    t: T[F])(
    k: DistributiveLaw[F, W], g: GAlgebraM[W, M, F, A]):
      M[A] = {
    def loop(t: T[F]): M[W[F[W[A]]]] =
      project(t).traverse(loop(_) >>= (_.traverse(g) ∘ (_.cojoin))) ∘ (k(_))

    loop(t) ∘ (_.copoint) >>= g
  }

  /** A catamorphism generalized with a comonad outside the functor. */
  def elgotCata[W[_]: Comonad, F[_]: Functor, A](
    t: T[F])(
    k: DistributiveLaw[F, W], g: ElgotAlgebra[W, F, A]):
      A = {
    def loop(t: T[F]): W[F[A]] = k(project(t) ∘ (loop(_).cojoin.map(g)))

    g(loop(t))
  }

  def para[F[_]: Functor, A](t: T[F])(f: GAlgebra[(T[F], ?), F, A]): A =
    // NB: This is not implemented with [[matryoshka.distPara]] because that
    //     would add a [[matryoshka.Corecursive]] constraint.
    f(project(t) ∘ (t => (t, para(t)(f))))

  def elgotPara[F[_]: Functor, A](t: T[F])(f: ElgotAlgebra[(T[F], ?), F, A]): A =
    // NB: This is not implemented with [[matryoshka.distPara]] because that
    //     would add a [[matryoshka.Corecursive]] constraint.
    f((t, project(t) ∘ (elgotPara(_)(f))))

  def paraM[F[_]: Traverse, M[_]: Monad, A](t: T[F])(f: GAlgebraM[(T[F], ?), M, F, A]):
      M[A] =
    project(t).traverse(v => paraM(v)(f) ∘ ((v, _))).flatMap(f)

  def gpara[F[_]: Functor, W[_]: Comonad, A](
    t: T[F])(
    e: DistributiveLaw[F, W], f: GAlgebra[EnvT[T[F], W, ?], F, A])(
    implicit T: Corecursive[T]):
      A =
    gzygo[F, W, A, T[F]](t)(T.embed(_), e, f)

  def zygo[F[_]: Functor, A, B](t: T[F])(f: Algebra[F, B], g: GAlgebra[(B, ?), F, A]): A =
    gcata[(B, ?), F, A](t)(distZygo(f), g)

  def elgotZygo[F[_]: Functor, A, B](t: T[F])(f: Algebra[F, B], g: ElgotAlgebra[(B, ?), F, A]):
      A =
    elgotCata[(B, ?), F, A](t)(distZygo(f), g)

  def gzygo[F[_]: Functor, W[_]: Comonad, A, B](
    t: T[F])(
    f: Algebra[F, B], w: DistributiveLaw[F, W], g: GAlgebra[EnvT[B, W, ?], F, A]):
      A =
    gcata[EnvT[B, W, ?], F, A](t)(distZygoT(f, w), g)

  def gElgotZygo[F[_]: Functor, W[_]: Comonad, A, B](
    t: T[F])(
    f: F[B] => B, w: DistributiveLaw[F, W], g: ElgotAlgebra[EnvT[B, W, ?], F, A]):
      A =
    elgotCata[EnvT[B, W, ?], F, A](t)(distZygoT(f, w), g)

  /** Mutually-recursive fold. */
  def mutu[F[_]: Functor, A, B](t: T[F])(f: GAlgebra[(A, ?), F, B], g: GAlgebra[(B, ?), F, A]):
      A =
    g(project(t) ∘ (x => (mutu(x)(g, f), mutu(x)(f, g))))

  def prepro[F[_]: Functor, A](t: T[F])(e: F ~> F, f: Algebra[F, A])(implicit T: Corecursive[T]): A =
    f(project(t) ∘ (x => prepro(cata[F, T[F]](x)(c => T.embed(e(c))))(e, f)))

  def gprepro[F[_]: Functor, W[_]: Comonad, A](
    t: T[F])(
    k: DistributiveLaw[F, W], e: F ~> F, f: GAlgebra[W, F, A])(
    implicit T: Corecursive[T]):
      A = {
    def loop(t: T[F]): W[A] =
      k(project(t) ∘ (x => loop(cata[F, T[F]](x)(c => T.embed(e(c)))).cojoin)) ∘ f

    loop(t).copoint
  }

  def histo[F[_]: Functor, A](t: T[F])(f: GAlgebra[Cofree[F, ?], F, A]): A =
    gcata[Cofree[F, ?], F, A](t)(distHisto, f)

  def elgotHisto[F[_]: Functor, A](t: T[F])(f: ElgotAlgebra[Cofree[F, ?], F, A]): A =
    elgotCata[Cofree[F, ?], F, A](t)(distHisto, f)

  def ghisto[F[_]: Functor, H[_]: Functor, A](
    t: T[F])(
    g: DistributiveLaw[F, H], f: GAlgebra[Cofree[H, ?], F, A]):
      A =
    gcata[Cofree[H, ?], F, A](t)(distGHisto(g), f)

  def paraZygo[F[_]:Functor: Unzip, A, B](
    t: T[F]) (
    f: GAlgebra[(T[F], ?), F, B],
    g: GAlgebra[(B, ?), F, A]):
      A = {
    def h(t: T[F]): (B, A) =
      (project(t) ∘ { x =>
        val (b, a) = h(x)
        ((x, b), (b, a))
      }).unfzip.bimap(f, g)

    h(t)._2
  }

  // TODO: figure out how to represent this as a elgotHylo with mergeTuple
  /** Combines two functors that may fail to merge, also providing access to the
    * inputs at each level. This is akin to an Elgot, not generalized, fold.
    */
  def paraMerga[F[_]: Functor: Merge, A](t: T[F], that: T[F])(
    f: (T[F], T[F], Option[F[A]]) => A):
      A =
    f(t, that, project(t).mergeWith(project(that))(paraMerga(_, _)(f)))

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

  /** Attribute a tree via an algebra starting from the root. */
  def attributeTopDown[F[_]: Functor, A](t: T[F], z: A)(f: (A, F[T[F]]) => A):
      Cofree[F, A] = {
    val ft = project(t)
    val a = f(z, ft)
    Cofree(a, ft ∘ (attributeTopDown(_, a)(f)))
  }
  /** Kleisli variant of attributeTopDown */
  def attributeTopDownM[F[_]: Traverse, M[_]: Monad, A](
    t: T[F], z: A)(
    f: (A, F[T[F]]) => M[A]):
      M[Cofree[F, A]] = {
    val ft = project(t)
    f(z, ft) >>=
      (a => ft.traverse(attributeTopDownM(_, a)(f)) ∘ (Cofree(a, _)))
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

  def convertTo[F[_]: Functor, R[_[_]]: Corecursive](t: T[F]): R[F] =
    cata(t)(Corecursive[R].embed[F])
}

object Recursive {
  def equalT[T[_[_]]](implicit T: Recursive[T]): EqualT[T] =
    new EqualT[T] {
      def equal[F[_]: Functor](tf1: T[F], tf2: T[F])(implicit del: Delay[Equal, F]) =
        del(equalT[F](del)).equal(T.project(tf1), T.project(tf2))
    }

  def showT[T[_[_]]](implicit T: Recursive[T]): ShowT[T] =
    new ShowT[T] {
      override def show[F[_]: Functor](tf: T[F])(implicit del: Delay[Show, F]): Cord =
        T.cata(tf)(del(Cord.CordShow).show)
    }
}
