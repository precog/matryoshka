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

/** Folds for recursive data types. */
trait Recursive[T] extends Based[T] {
  // TODO: This works around a bug in Simulacrum (#55). Delete once that is fixed.
  type BaseT[A] = Base[A]

  def project(t: T): BaseT[T]

  /** Roughly a default impl of `project`, given a [[matryoshka.Corecursive]]
    * instance and an overridden `cata`.
    */
  def lambek(tf: T)(implicit T: Corecursive.Aux[T, Base]): Base[T] =
    cata[Base[T]](tf)(_ ∘ (_.embed))

  def cata[A](t: T)(f: Algebra[Base, A]): A = f(project(t) ∘ (cata(_)(f)))

  /** A Kleisli catamorphism. */
  def cataM[M[_]: Monad, A](t: T)(f: AlgebraM[M, Base, A])(implicit BT: Traverse[Base]):
      M[A] =
    project(t).traverse(cataM(_)(f)).flatMap(f)

  /** A catamorphism generalized with a comonad inside the functor. */
  def gcata[W[_]: Comonad, A](
    t: T)(
    k: DistributiveLaw[Base, W], g: GAlgebra[W, Base, A]):
      A = {
    def loop(t: T): W[Base[W[A]]] = k(project(t) ∘ (loop(_).map(g).cojoin))

    g(loop(t).copoint)
  }

  def gcataM[W[_]: Comonad: Traverse, M[_]: Monad, A](
    t: T)(
    k: DistributiveLaw[Base, W], g: GAlgebraM[W, M, Base, A])(
    implicit BT: Traverse[Base]):
      M[A] = {
    def loop(t: T): M[W[Base[W[A]]]] =
      project(t).traverse(loop(_) >>= (_.traverse(g) ∘ (_.cojoin))) ∘ (k(_))

    loop(t) ∘ (_.copoint) >>= g
  }

  /** A catamorphism generalized with a comonad outside the functor. */
  def elgotCata[W[_]: Comonad, A](
    t: T)(
    k: DistributiveLaw[Base, W], g: ElgotAlgebra[W, Base, A]):
      A = {
    def loop(t: T): W[Base[A]] = k(project(t) ∘ (loop(_).cojoin.map(g)))

    g(loop(t))
  }

  def para[A](t: T)(f: GAlgebra[(T, ?), Base, A]): A =
    // NB: This is not implemented with [[matryoshka.distPara]] because that
    //     would add a [[matryoshka.Corecursive]] constraint.
    f(project(t) ∘ (t => (t, para(t)(f))))

  def elgotPara[A](t: T)(f: ElgotAlgebra[(T, ?), Base, A]): A =
    // NB: This is not implemented with [[matryoshka.distPara]] because that
    //     would add a [[matryoshka.Corecursive]] constraint.
    f((t, project(t) ∘ (elgotPara(_)(f))))

  def paraM[M[_]: Monad, A](
    t: T)(
    f: GAlgebraM[(T, ?), M, Base, A])(
    implicit BT: Traverse[Base]):
      M[A] =
    project(t).traverse(v => paraM(v)(f) ∘ ((v, _))).flatMap(f)

  def gpara[W[_]: Comonad, A](
    t: T)(
    e: DistributiveLaw[Base, W], f: GAlgebra[EnvT[T, W, ?], Base, A])(
    implicit T: Corecursive.Aux[T, Base]):
      A =
    gzygo[W, A, T](t)(T.embed(_), e, f)

  def zygo[A, B](t: T)(f: Algebra[Base, B], g: GAlgebra[(B, ?), Base, A]): A =
    gcata[(B, ?), A](t)(distZygo(f), g)

  def elgotZygo[A, B](t: T)(f: Algebra[Base, B], g: ElgotAlgebra[(B, ?), Base, A]):
      A =
    elgotCata[(B, ?), A](t)(distZygo(f), g)

  def gzygo[W[_]: Comonad, A, B](
    t: T)(
    f: Algebra[Base, B], w: DistributiveLaw[Base, W], g: GAlgebra[EnvT[B, W, ?], Base, A]):
      A =
    gcata[EnvT[B, W, ?], A](t)(distZygoT(f, w), g)

  def gElgotZygo[W[_]: Comonad, A, B](
    t: T)(
    f: Algebra[Base, B], w: DistributiveLaw[Base, W], g: ElgotAlgebra[EnvT[B, W, ?], Base, A]):
      A =
    elgotCata[EnvT[B, W, ?], A](t)(distZygoT(f, w), g)

  /** Mutually-recursive fold. */
  def mutu[A, B](t: T)(f: GAlgebra[(A, ?), Base, B], g: GAlgebra[(B, ?), Base, A])
      : A =
    g(project(t) ∘ (x => (mutu(x)(g, f), mutu(x)(f, g))))

  def prepro[A](t: T)(e: Base ~> Base, f: Algebra[Base, A])(implicit T: Corecursive.Aux[T, Base]):
      A =
    f(project(t) ∘ (x => prepro(cata[T](x)(c => T.embed(e(c))))(e, f)))

  def gprepro[W[_]: Comonad, A](
    t: T)(
    k: DistributiveLaw[Base, W], e: Base ~> Base, f: GAlgebra[W, Base, A])(
    implicit T: Corecursive.Aux[T, Base]):
      A = {
    def loop(t: T): W[A] =
      k(project(t) ∘ (x => loop(cata[T](x)(c => T.embed(e(c)))).cojoin)) ∘ f

    loop(t).copoint
  }

  def histo[A](t: T)(f: GAlgebra[Cofree[Base, ?], Base, A]): A =
    gcata[Cofree[Base, ?], A](t)(distHisto, f)

  def elgotHisto[A](t: T)(f: ElgotAlgebra[Cofree[Base, ?], Base, A]): A =
    elgotCata[Cofree[Base, ?], A](t)(distHisto, f)

  def ghisto[H[_]: Functor, A](
    t: T)(
    g: DistributiveLaw[Base, H], f: GAlgebra[Cofree[H, ?], Base, A]):
      A =
    gcata[Cofree[H, ?], A](t)(distGHisto(g), f)

  def paraZygo[A, B](
    t: T)(
    f: GAlgebra[(T, ?), Base, B],
    g: GAlgebra[(B, ?), Base, A])(
    implicit BU: Unzip[Base]):
      A = {
    def h(t: T): (B, A) =
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
  def paraMerga[A](t: T, that: T)(
    f: (T, T, Option[Base[A]]) => A)(implicit BM: Merge[Base]):
      A =
    f(t, that, project(t).mergeWith(project(that))(paraMerga(_, _)(f)))

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

  def equal[T](implicit T: Recursive[T], BE: Delay[Equal, Based[T]#Base])
      : Equal[T] =
    Equal.equal((a, b) => BE(equal[T]).equal(T.project(a), T.project(b)))

  def show[T](implicit T: Recursive[T], BS: Delay[Show, Based[T]#Base])
      : Show[T] =
    Show.show(T.cata(_)(BS(Cord.CordShow).show))

  // NB: The rest of this is what would be generated by simulacrum, except this
  //     type class is too complicated to take advantage of that.

  def apply[T, F[_]](implicit instance: Aux[T, F]): Aux[T, F] = instance

  trait Ops[T, F[_]] {
    def typeClassInstance: Aux[T, F]
    def self: T

    type BaseT[A] = Aux[T, F]#Base[A]
    def project: BaseT[T] = typeClassInstance.project(self)
    def lambek(implicit T: Corecursive.Aux[T, BaseT]): BaseT[T] =
      typeClassInstance.lambek(self)
    def cata[A](f: Algebra[BaseT, A]): A = typeClassInstance.cata[A](self)(f)
    def cataM[M[_]: Monad, A](f: AlgebraM[M, BaseT, A])(implicit BT: Traverse[BaseT]): M[A] =
      typeClassInstance.cataM[M, A](self)(f)
    def gcata[W[_]: Comonad, A](k: DistributiveLaw[BaseT, W], g: GAlgebra[W, BaseT, A]): A =
      typeClassInstance.gcata[W, A](self)(k, g)
    def gcataM[W[_]: Comonad: Traverse, M[_]: Monad, A]
      (k: DistributiveLaw[BaseT, W], g: GAlgebraM[W, M, BaseT, A])
      (implicit BT: Traverse[BaseT])
        : M[A] =
      typeClassInstance.gcataM[W, M, A](self)(k, g)
    def elgotCata[W[_]: Comonad, A]
      (k: DistributiveLaw[BaseT, W], g: ElgotAlgebra[W, BaseT, A])
        : A =
      typeClassInstance.elgotCata[W, A](self)(k, g)
    def para[A](f: GAlgebra[(T, ?), BaseT, A]): A =
      typeClassInstance.para[A](self)(f)
    def elgotPara[A](f: ElgotAlgebra[(T, ?), BaseT, A]): A =
      typeClassInstance.elgotPara[A](self)(f)
    def paraM[M[_]: Monad, A]
      (f: GAlgebraM[(T, ?), M, BaseT, A])
      (implicit BT: Traverse[BaseT])
        : M[A] =
      typeClassInstance.paraM[M, A](self)(f)
    def gpara[W[_]: Comonad, A]
      (e: DistributiveLaw[BaseT, W], f: GAlgebra[EnvT[T, W, ?], BaseT, A])
      (implicit T: Corecursive.Aux[T, BaseT])
        : A =
      typeClassInstance.gpara[W, A](self)(e, f)
    def zygo[A, B](f: Algebra[BaseT, B], g: GAlgebra[(B, ?), BaseT, A]): A =
      typeClassInstance.zygo[A, B](self)(f, g)
    def elgotZygo[A, B](f: Algebra[BaseT, B], g: ElgotAlgebra[(B, ?), BaseT, A])
        : A =
      typeClassInstance.elgotZygo[A, B](self)(f, g)
    def gzygo[W[_]: Comonad, A, B]
      (f: Algebra[BaseT, B],
        w: DistributiveLaw[BaseT, W],
        g: GAlgebra[EnvT[B, W, ?], BaseT, A])
        : A =
      typeClassInstance.gzygo[W, A, B](self)(f, w, g)
    def gElgotZygo[W[_]: Comonad, A, B]
      (f: Algebra[BaseT, B],
        w: DistributiveLaw[BaseT, W],
        g: ElgotAlgebra[EnvT[B, W, ?], BaseT, A])
        : A =
      typeClassInstance.gElgotZygo [W, A, B](self)(f, w, g)
    def mutu[A, B](f: GAlgebra[(A, ?), BaseT, B], g: GAlgebra[(B, ?), BaseT, A])
        : A =
      typeClassInstance.mutu[A, B](self)(f, g)
    def prepro[A]
      (e: BaseT ~> BaseT, f: Algebra[BaseT, A])
      (implicit T: Corecursive.Aux[T, BaseT])
        : A =
      typeClassInstance.prepro[A](self)(e, f)
    def gprepro[W[_]: Comonad, A]
      (k: DistributiveLaw[BaseT, W], e: BaseT ~> BaseT, f: GAlgebra[W, BaseT, A])
      (implicit T: Corecursive.Aux[T, BaseT])
        : A =
      typeClassInstance.gprepro[W, A](self)(k, e, f)
    def histo[A](f: GAlgebra[Cofree[BaseT, ?], BaseT, A]): A =
      typeClassInstance.histo(self)(f)
    def elgotHisto[A](f: ElgotAlgebra[Cofree[BaseT, ?], BaseT, A]): A =
      typeClassInstance.elgotHisto(self)(f)
    def ghisto[H[_]: Functor, A]
      (g: DistributiveLaw[BaseT, H], f: GAlgebra[Cofree[H, ?], BaseT, A])
        : A =
      typeClassInstance.ghisto(self)(g, f)
    def paraZygo[A, B]
      (f: GAlgebra[(T, ?), BaseT, B], g: GAlgebra[(B, ?), BaseT, A])
      (implicit BU: Unzip[BaseT])
        : A =
      typeClassInstance.paraZygo[A, B](self)(f, g)
    def paraMerga[A]
      (that: T)
      (f: (T, T, Option[BaseT[A]]) => A)
      (implicit BM: Merge[BaseT])
        : A =
      typeClassInstance.paraMerga[A](self, that)(f)
    def isLeaf(implicit B: Foldable[BaseT]): Boolean =
      typeClassInstance.isLeaf(self)
    def children(implicit B: Foldable[BaseT]): List[T] =
      typeClassInstance.children(self)
    def universe(implicit B: Foldable[BaseT]): List[T] =
      typeClassInstance.universe(self)
    def topDownCataM[M[_]: Monad, A]
      (a: A)
      (f: (A, T) => M[(A, T)])
      (implicit T: Corecursive.Aux[T, BaseT], BT: Traverse[BaseT])
        : M[T] =
      typeClassInstance.topDownCataM[M, A](self, a)(f)
    def attributeTopDown[A](z: A)(f: (A, BaseT[T]) => A): Cofree[BaseT, A] =
      typeClassInstance.attributeTopDown[A](self, z)(f)
    def attributeTopDownM[M[_]: Monad, A]
      (z: A)
      (f: (A, BaseT[T]) => M[A])
      (implicit BT: Traverse[BaseT])
        : M[Cofree[BaseT, A]] =
      typeClassInstance.attributeTopDownM[M, A](self, z)(f)
    def all(p: T ⇒ Boolean)(implicit B: Foldable[BaseT]): Boolean =
      typeClassInstance.all(self)(p)
    def any(p: T ⇒ Boolean)(implicit B: Foldable[BaseT]): Boolean =
      typeClassInstance.any(self)(p)
    def collect[B](pf: PartialFunction[T, B])(implicit B: Foldable[BaseT])
        : List[B] =
      typeClassInstance.collect[B](self)(pf)
    def contains(c: T)(implicit T: Equal[T], B: Foldable[BaseT]): Boolean =
      typeClassInstance.contains(self, c)
    def foldMap[Z: Monoid](f: T => Z)(implicit B: Foldable[BaseT]): Z =
      typeClassInstance.foldMap[Z](self)(f)
    def foldMapM[M[_]: Monad, Z: Monoid]
      (f: T => M[Z])
      (implicit B: Foldable[BaseT])
        : M[Z] =
      typeClassInstance.foldMapM[M, Z](self)(f)
    def convertTo[R](implicit R: Corecursive.Aux[R, BaseT]): R =
      typeClassInstance.convertTo[R](self)
  }

  trait ToRecursiveOps {
    implicit def toRecursiveOps[T, F[_]](target: T)(implicit tc: Aux[T, F])
        : Ops[T, F] =
      new Ops[T, F] {
        val self = target
        val typeClassInstance = tc
      }
  }

  object nonInheritedOps extends ToRecursiveOps

  trait AllOps[T, F[_]] extends Ops[T, F] {
    def typeClassInstance: Aux[T, F]
  }

  object ops {
    implicit def toAllRecursiveOps[T, F[_]](target: T)(implicit tc: Aux[T, F])
        : AllOps[T, F] =
      new AllOps[T, F] {
        val self = target
        val typeClassInstance = tc
      }
  }
}
