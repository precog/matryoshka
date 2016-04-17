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

import scala.{Boolean, inline, PartialFunction}
import scala.collection.immutable.{List, Nil}

import scalaz._, Scalaz._
import simulacrum.typeclass

/** Folds for recursive data types. */
@typeclass trait Recursive[T] extends Based[T] {
  // TODO: This works around a bug in Simulacrum (#55). Delete once that is fixed.
  type BaseT[A] = Base[A]

  def project(t: T): BaseT[T]

  /** Roughly a default impl of `project`, given a [[matryoshka.Corecursive]]
    * instance and an overridden `cata`.
    */
  def lambek(tf: T)(implicit T: Corecursive.Aux[T, Base]): Base[T] =
    cata[Base[T]](tf)(_ ∘ (_.embed))

  def cata[A](t: T)(f: Base[A] => A): A = f(project(t) ∘ (cata(_)(f)))

  /** A Kleisli catamorphism. */
  def cataM[M[_]: Monad, A](t: T)(f: Base[A] => M[A])(implicit BT: Traverse[Base]):
      M[A] =
    project(t).traverse(cataM(_)(f)).flatMap(f)

  /** A catamorphism generalized with a comonad inside the functor. */
  def gcata[W[_]: Comonad, A](
    t: T)(
    k: DistributiveLaw[Base, W], g: Base[W[A]] => A):
      A = {
    def loop(t: T): W[Base[W[A]]] = k(project(t) ∘ (loop(_).map(g).cojoin))

    g(loop(t).copoint)
  }

  /** A catamorphism generalized with a comonad outside the functor. */
  def elgotCata[W[_]: Comonad, A](
    t: T)(
    k: DistributiveLaw[Base, W], g: ElgotAlgebra[W, Base, A]):
      A = {
    def loop(t: T): W[Base[A]] = k(project(t) ∘ (loop(_).cojoin.map(g)))

    g(loop(t))
  }

  def para[A](t: T)(f: Base[(T, A)] => A): A =
    // NB: This is not implemented with [[matryoshka.distPara]] because that
    //     would add a [[matryoshka.Corecursive]] constraint.
    f(project(t) ∘ (t => (t, para(t)(f))))

  def elgotPara[A](t: T)(f: ((T, Base[A])) => A): A =
    // NB: This is not implemented with [[matryoshka.distPara]] because that
    //     would add a [[matryoshka.Corecursive]] constraint.
    f((t, project(t) ∘ (elgotPara(_)(f))))

  def paraM[M[_]: Monad, A](
    t: T)(
    f: Base[(T, A)] => M[A])(
    implicit BT: Traverse[Base]):
      M[A] =
    project(t).traverse(v => paraM(v)(f) ∘ ((v, _))).flatMap(f)

  def gpara[W[_]: Comonad, A](
    t: T)(
    e: DistributiveLaw[Base, W], f: Base[EnvT[T, W, A]] => A)(
    implicit T: Corecursive.Aux[T, Base]):
      A =
    gzygo[W, A, T](t)(T.embed(_), e, f)

  def zygo[A, B](t: T)(f: Base[B] => B, g: Base[(B, A)] => A): A =
    gcata[(B, ?), A](t)(distZygo(f), g)

  def elgotZygo[A, B](t: T)(f: Base[B] => B, g: ElgotAlgebra[(B, ?), Base, A]):
      A =
    elgotCata[(B, ?), A](t)(distZygo(f), g)

  def gzygo[W[_]: Comonad, A, B](
    t: T)(
    f: Base[B] => B, w: DistributiveLaw[Base, W], g: Base[EnvT[B, W, A]] => A):
      A =
    gcata[EnvT[B, W, ?], A](t)(distZygoT(f, w), g)

  /** Mutually-recursive fold. */
  def mutu[A, B](t: T)(f: Base[(A, B)] => B, g: Base[(B, A)] => A): A =
    g(project(t) ∘ (x => (mutu(x)(g, f), mutu(x)(f, g))))

  def prepro[A](t: T)(e: Base ~> Base, f: Base[A] => A)(implicit T: Corecursive.Aux[T, Base]):
      A =
    f(project(t) ∘ (x => prepro(cata[T](x)(c => T.embed(e(c))))(e, f)))

  def gprepro[W[_]: Comonad, A](
    t: T)(
    k: DistributiveLaw[Base, W], e: Base ~> Base, f: Base[W[A]] => A)(
    implicit T: Corecursive.Aux[T, Base]):
      A = {
    def loop(t: T): W[A] =
      k(project(t) ∘ (x => loop(cata[T](x)(c => T.embed(e(c)))).cojoin)) ∘ f

    loop(t).copoint
  }

  def histo[A](t: T)(f: Base[Cofree[Base, A]] => A): A =
    gcata[Cofree[Base, ?], A](t)(distHisto, f)

  def elgotHisto[A](t: T)(f: Cofree[Base, Base[A]] => A): A =
    elgotCata[Cofree[Base, ?], A](t)(distHisto, f)

  def ghisto[H[_]: Functor, A](
    t: T)(
    g: DistributiveLaw[Base, H], f: Base[Cofree[H, A]] => A):
      A =
    gcata[Cofree[H, ?], A](t)(distGHisto(g), f)

  def paraZygo[A, B](
    t: T)(
    f: Base[(T, B)] => B, g: Base[(B, A)] => A)(
    implicit BU: Unzip[Base]):
      A = {
    def h(t: T): (B, A) =
      (project(t) ∘ { x =>
        val (b, a) = h(x)
        ((x, b), (b, a))
      }).unfzip.bimap(f, g)

    h(t)._2
  }

  def isLeaf(t: T)(implicit B: Foldable[Base]): Boolean =
    !Tag.unwrap(project(t).foldMap(_ => true.disjunction))

  def children(t: T)(implicit B: Foldable[Base]): List[T] =
    project(t).foldMap(_ :: Nil)

  def universe(t: T)(implicit B: Foldable[Base]): List[T] =
    t :: children(t).flatMap(universe)

  def topDownCataM[M[_]: Monad, A](
    t: T, a: A)(
    f: (A, T) => M[(A, T)])(
    implicit T: Corecursive.Aux[T, Base], BT: Traverse[Base]):
      M[T] = {
    def loop(a: A, term: T): M[T] = for {
      tuple   <- f(a, term)
      (a, tf) =  tuple
      rec     <- project(tf).traverse(loop(a, _))
    } yield T.embed(rec)

    loop(a, t)
  }

  /** Attribute a tree via an algebra starting from the root. */
  def attributeTopDown[A](t: T, z: A)(f: (A, Base[T]) => A):
      Cofree[Base, A] = {
    val ft = project(t)
    val a = f(z, ft)
    Cofree(a, ft ∘ (attributeTopDown(_, a)(f)))
  }

  /** Kleisli variant of attributeTopDown */
  def attributeTopDownM[M[_]: Monad, A](
    t: T, z: A)(
    f: (A, Base[T]) => M[A])(
    implicit BT: Traverse[Base]):
      M[Cofree[Base, A]] = {
    val ft = project(t)
    f(z, ft) >>=
    (a => ft.traverse(attributeTopDownM(_, a)(f)) ∘ (Cofree(a, _)))
  }

  // Foldable
  def all(t: T)(p: T ⇒ Boolean)(implicit B: Foldable[Base]): Boolean =
    Tag.unwrap(foldMap(t)(p(_).conjunction))

  def any(t: T)(p: T ⇒ Boolean)(implicit B: Foldable[Base]): Boolean =
    Tag.unwrap(foldMap(t)(p(_).disjunction))

  def collect[B](t: T)(pf: PartialFunction[T, B])(implicit B: Foldable[Base]):
      List[B] =
    foldMap(t)(pf.lift(_).toList)

  def contains(t: T, c: T)(implicit T: Equal[T], B: Foldable[Base]): Boolean =
    any(t)(_ ≟ c)

  def foldMap[Z: Monoid](t: T)(f: T => Z)(implicit B: Foldable[Base]): Z =
    foldMapM[Free.Trampoline, Z](t)(f(_).pure[Free.Trampoline]).run

  def foldMapM[M[_]: Monad, Z: Monoid](t: T)(f: T => M[Z])(implicit B: Foldable[Base]):
      M[Z] = {
    def loop(z0: Z, term: T): M[Z] = {
      for {
        z1 <- f(term)
        z2 <- project(term).foldLeftM(z0 ⊹ z1)(loop(_, _))
      } yield z2
    }

    loop(Monoid[Z].zero, t)
  }

  def convertTo[R](t: T)(implicit R: Corecursive.Aux[R, Base]): R =
    cata(t)(R.embed)
}

object Recursive {
  type Aux[T, F[_]] = Recursive[T] { type Base[A] = F[A] }

  implicit def show[T, F[_]](
    implicit T: Recursive.Aux[T, F], BS: Show ~> λ[α => Show[F[α]]]):
      Show[T] =
    Show.show(T.cata(_)(BS(Cord.CordShow).show))
}
