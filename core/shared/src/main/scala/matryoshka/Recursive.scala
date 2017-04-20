/*
 * Copyright 2014–2017 SlamData Inc.
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

import slamdata.Predef._
import matryoshka.implicits._
import matryoshka.patterns.EnvT

import scala.Predef.identity

import scalaz._, Scalaz._

/** Folds for recursive data types. */
trait Recursive[T] extends Based[T] {
  // TODO: This works around a bug in Simulacrum (#55). Delete once that is fixed.
  type BaseT[A] = Base[A]

  def project(t: T)(implicit BF: Functor[Base]): BaseT[T]

  def cata[A](t: T)(f: Algebra[Base, A])(implicit BF: Functor[Base]): A =
    hylo(t)(f, project)

  /** A Kleisli catamorphism. */
  def cataM[M[_]: Monad, A](t: T)(f: AlgebraM[M, Base, A])(implicit BT: Traverse[Base]):
      M[A] =
    cata[M[A]](t)(_.sequence >>= f)

  /** A catamorphism generalized with a comonad inside the functor. */
  def gcata[W[_]: Comonad, A]
    (t: T)
    (k: DistributiveLaw[Base, W], g: GAlgebra[W, Base, A])
    (implicit BF: Functor[Base])
      : A =
    cata[W[A]](t)(fwa => k(fwa.map(_.cojoin)).map(g)).copoint

  def gcataM[W[_]: Comonad: Traverse, M[_]: Monad, A]
    (t: T)
    (w: DistributiveLaw[Base, W], g: GAlgebraM[W, M, Base, A])
    (implicit BT: Traverse[Base])
      : M[A] =
    cataM[M, W[A]](t)(fwa => w(fwa.map(_.cojoin)).traverse(g)) ∘ (_.copoint)

  /** A catamorphism generalized with a comonad outside the functor. */
  def elgotCata[W[_]: Comonad, A](
    t: T)(
    k: DistributiveLaw[Base, W], g: ElgotAlgebra[W, Base, A])
    (implicit BF: Functor[Base])
      : A =
    g(cata[W[Base[A]]](t)(fwfa => k(fwfa ∘ (_.cobind(g)))))

  def elgotCataM[W[_]: Comonad : Traverse, M[_]: Monad, A]
    (t: T)
    (k: DistributiveLaw[Base, (M ∘ W)#λ], g: ElgotAlgebraM[W, M, Base, A])
    (implicit BT: Traverse[Base])
      : M[A] =
    cataM[M, W[Base[A]]](t)(fwfa => k(fwfa ∘ (_.cojoin.traverse(g)))) >>= g

  def para[A](t: T)(f: GAlgebra[(T, ?), Base, A])(implicit BF: Functor[Base])
      : A =
    // NB: This is not implemented with [[matryoshka.distPara]] because that
    //     would add a [[matryoshka.Corecursive]] constraint.
    f(project(t) ∘ (t => (t, para(t)(f))))

  def elgotPara[A]
    (t: T)
    (f: ElgotAlgebra[(T, ?), Base, A])
    (implicit BF: Functor[Base])
      : A =
    // NB: This is not implemented with [[matryoshka.distPara]] because that
    //     would add a [[matryoshka.Corecursive]] constraint.
    f((t, project(t) ∘ (elgotPara(_)(f))))

  def paraM[M[_]: Monad, A](
    t: T)(
    f: GAlgebraM[(T, ?), M, Base, A])(
    implicit BT: Traverse[Base]):
      M[A] =
    project(t).traverse(v => paraM(v)(f) ∘ ((v, _))).flatMap(f)

  def zygo[A, B]
    (t: T)
    (f: Algebra[Base, B], g: GAlgebra[(B, ?), Base, A])
    (implicit BF: Functor[Base])
      : A =
    gcata[(B, ?), A](t)(distZygo(f), g)

  def zygoM[A, B, M[_]: Monad]
    (t: T)
    (f: AlgebraM[M, Base, B], g: GAlgebraM[(B, ?), M, Base, A])
    (implicit BT: Traverse[Base])
      : M[A] =
    gcataM[(M[B], ?), M, A](
      t)(
      distZygo(_.sequence >>= f),
        _.traverse(Bitraverse[(?, ?)].leftTraverse.sequence[M, B]) >>= g)

  def elgotZygo[A, B]
    (t: T)
    (f: Algebra[Base, B], g: ElgotAlgebra[(B, ?), Base, A])
    (implicit BF: Functor[Base])
      : A =
    elgotCata[(B, ?), A](t)(distZygo(f), g)

  def elgotZygoM[A, B, M[_]: Monad]
    (t: T)
    (f: AlgebraM[M, Base, B], g: ElgotAlgebraM[(B, ?), M, Base, A])
    (implicit BT: Traverse[Base])
      : M[A] =
    elgotCataM[(B, ?), M, A](t)(distZygoM(f, distApplicative[Base, M]), g)

  def gzygo[W[_]: Comonad, A, B](
    t: T)(
    f: Algebra[Base, B], w: DistributiveLaw[Base, W], g: GAlgebra[EnvT[B, W, ?], Base, A])
    (implicit BF: Functor[Base]):
      A =
    gcata[EnvT[B, W, ?], A](t)(distZygoT(f, w), g)

  def gElgotZygo[W[_]: Comonad, A, B](
    t: T)(
    f: Algebra[Base, B], w: DistributiveLaw[Base, W], g: ElgotAlgebra[EnvT[B, W, ?], Base, A])
    (implicit BF: Functor[Base]):
      A =
    elgotCata[EnvT[B, W, ?], A](t)(distZygoT(f, w), g)

  /** Mutually-recursive fold. */
  def mutu[A, B]
    (t: T)
    (f: GAlgebra[(A, ?), Base, B], g: GAlgebra[(B, ?), Base, A])
    (implicit BF: Functor[Base])
      : A =
    g(project(t) ∘ (x => (mutu(x)(g, f), mutu(x)(f, g))))

  def histo[A]
    (t: T)
    (f: GAlgebra[Cofree[Base, ?], Base, A])
    (implicit BF: Functor[Base])
      : A =
    gcata[Cofree[Base, ?], A](t)(distHisto, f)

  def elgotHisto[A]
    (t: T)
    (f: ElgotAlgebra[Cofree[Base, ?], Base, A])
    (implicit BF: Functor[Base])
      : A =
    elgotCata[Cofree[Base, ?], A](t)(distHisto, f)

  def ghisto[H[_]: Functor, A](
    t: T)(
    g: DistributiveLaw[Base, H], f: GAlgebra[Cofree[H, ?], Base, A])
    (implicit BF: Functor[Base]):
      A =
    gcata[Cofree[H, ?], A](t)(distGHisto(g), f)

  def gcataZygo[W[_]: Comonad, A, B]
    (t: T)
    (k: DistributiveLaw[Base, W], f: GAlgebra[W, Base, B], g: GAlgebra[(B, ?), Base, A])
    (implicit BF: Functor[Base], BU: Unzip[Base]) =
    gcata[(W[B], ?), A](
      t)(
      distZygo(fwa => k(fwa.map(_.cojoin)).map(f)),
        fwa => g(fwa.map(_.leftMap(_.copoint))))

  // TODO: Remove `Unzip` constraint
  def paraZygo[A, B](
    t: T)(
    f: GAlgebra[(T, ?), Base, B],
    g: GAlgebra[(B, ?), Base, A])(
    implicit BF: Functor[Base], BU: Unzip[Base]):
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
  def paraMerga[A]
    (t: T, that: T)
    (f: (T, T, Option[Base[A]]) => A)
    (implicit BF: Functor[Base], BM: Merge[Base])
      : A =
    f(t, that, project(t).mergeWith(project(that))(paraMerga(_, _)(f)))

  def isLeaf(t: T)(implicit BF: Functor[Base], B: Foldable[Base]): Boolean =
    project(t).foldRight(true)((e, a) => false)

  def children(t: T)(implicit BF: Functor[Base], B: Foldable[Base]): List[T] =
    project(t).toList

  def universe(t: T)(implicit BF: Functor[Base], B: Foldable[Base])
      : NonEmptyList[T] =
    NonEmptyList.nel(t, children(t).map(universe(_)).unite.toIList)

  /** Attribute a tree via an algebra starting from the root. */
  def attributeTopDown[A]
    (t: T, z: A)
    (f: (A, Base[T]) => A)
    (implicit BF: Functor[Base])
      : Cofree[Base, A] = {
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
  def all(t: T)(p: T ⇒ Boolean)(implicit BF: Functor[Base], B: Foldable[Base]): Boolean =
    Tag.unwrap(foldMap(t)(p(_).conjunction))

  def any(t: T)(p: T ⇒ Boolean)(implicit BF: Functor[Base], B: Foldable[Base]): Boolean =
    Tag.unwrap(foldMap(t)(p(_).disjunction))

  def collect[B]
    (t: T)
    (pf: PartialFunction[T, B])
    (implicit BF: Functor[Base], B: Foldable[Base])
      : List[B] =
    foldMap(t)(pf.lift(_).toList)

  def contains
    (t: T, c: T)
    (implicit T: Equal[T], BF: Functor[Base], B: Foldable[Base])
      : Boolean =
    any(t)(_ ≟ c)

  def foldMap[Z: Monoid]
    (t: T)
    (f: T => Z)
    (implicit BF: Functor[Base], B: Foldable[Base])
      : Z =
    foldMapM[Free.Trampoline, Z](t)(f(_).pure[Free.Trampoline]).run

  def foldMapM[M[_]: Monad, Z: Monoid]
    (t: T)
    (f: T => M[Z])
    (implicit BF: Functor[Base], B: Foldable[Base])
      : M[Z] = {
    def loop(z0: Z, term: T): M[Z] = {
      for {
        z1 <- f(term)
        z2 <- project(term).foldLeftM(z0 ⊹ z1)(loop(_, _))
      } yield z2
    }

    loop(Monoid[Z].zero, t)
  }

  def convertTo[R]
    (t: T)
    (implicit R: Corecursive.Aux[R, Base], BF: Functor[Base])
      : R =
    cata[R](t)(R.embed(_))

  def mapR[U, G[_]: Functor]
    (t: T)
    (f: Base[T] => G[U])
    (implicit U: Corecursive.Aux[U, G], BF: Functor[Base])
      : U =
    f(project(t)).embed

  def traverseR[M[_]: Functor, U, G[_]: Functor]
    (t: T)
    (f: Base[T] => M[G[U]])
    (implicit U: Corecursive.Aux[U, G], BF: Functor[Base])
      : M[U] =
    f(project(t)) ∘ (_.embed)

  def transCata[U, G[_]: Functor]
    (t: T)
    (f: Base[U] => G[U])
    (implicit U: Corecursive.Aux[U, G], BF: Functor[Base])
      : U =
    cata(t)(f >>> (U.embed(_)))

  def transAna[U, G[_]: Functor]
    (t: T)
    (f: Base[T] => G[T])
    (implicit U: Corecursive.Aux[U, G], BF: Functor[Base])
      : U =
    U.ana(t)(f <<< project)

  def transPostpro[U, G[_]: Functor]
    (t: T)
    (e: G ~> G, f: Transform[T, Base, G])
    (implicit U: Birecursive.Aux[U, G], BF: Functor[Base])
      : U =
    mapR(t)(f(_) ∘ (x => U.transAna(transPostpro(x)(e, f))(e)))

  def transPara[U, G[_]: Functor]
    (t: T)
    (f: AlgebraicGTransform[(T, ?), U, Base, G])
    (implicit U: Corecursive.Aux[U, G], BF: Functor[Base])
      : U =
    mapR(t)(ft => f(ft.map(tf => (tf, transPara(tf)(f)))))

  def transApo[U, G[_]: Functor]
    (t: T)
    (f: CoalgebraicGTransform[(U \/ ?), T, Base, G])
    (implicit U: Corecursive.Aux[U, G], BF: Functor[Base])
      : U =
    mapR(t)(f(_).map(_.fold(identity, transApo(_)(f))))

  def transHylo[G[_]: Functor, U, H[_]: Functor]
    (t: T)
    (φ: G[U] => H[U], ψ: Base[T] => G[T])
    (implicit U: Corecursive.Aux[U, H], BF: Functor[Base])
      : U =
    mapR(t)(ft => φ(ψ(ft) ∘ (transHylo(_)(φ, ψ))))

  def transCataM[M[_]: Monad, U, G[_]: Functor]
    (t: T)
    (f: TransformM[M, U, Base, G])
    (implicit U: Corecursive.Aux[U, G], BT: Traverse[Base])
      : M[U] =
    cataM(t)(f(_) ∘ (U.embed(_)))

  def transAnaM[M[_]: Monad, U, G[_]: Traverse]
    (t: T)
    (f: TransformM[M, T, Base, G])
    (implicit U: Corecursive.Aux[U, G], BF: Functor[Base])
      : M[U] =
    U.anaM(t)(f <<< project)
}

object Recursive {
  def equal[T, F[_]: Functor]
    (implicit T: Recursive.Aux[T, F], F: Delay[Equal, F])
      : Equal[T] =
    Equal.equal((a, b) => F(equal[T, F]).equal(T.project(a), T.project(b)))

  def show[T, F[_]: Functor](implicit T: Recursive.Aux[T, F], F: Delay[Show, F])
      : Show[T] =
    Show.show(T.cata(_)(F(Cord.CordShow).show))

  // NB: The rest of this is what would be generated by simulacrum, except this
  //     type class is too complicated to take advantage of that.

  type Aux[T, F[_]] = Recursive[T] { type Base[A] = F[A] }

  def apply[T, F[_]](implicit instance: Aux[T, F]): Aux[T, F] = instance

  trait Ops[T, F[_]] {
    def typeClassInstance: Aux[T, F]
    def self: T

    def project(implicit BF: Functor[F]): F[T] = typeClassInstance.project(self)
    def cata[A](f: Algebra[F, A])(implicit BF: Functor[F]): A =
      typeClassInstance.cata[A](self)(f)
    def cataM[M[_]: Monad, A](f: AlgebraM[M, F, A])(implicit BT: Traverse[F])
        : M[A] =
      typeClassInstance.cataM[M, A](self)(f)
    def gcata[W[_]: Comonad, A]
      (k: DistributiveLaw[F, W], g: GAlgebra[W, F, A])
      (implicit BF: Functor[F])
        : A =
      typeClassInstance.gcata[W, A](self)(k, g)
    def gcataM[W[_]: Comonad: Traverse, M[_]: Monad, A]
      (w: DistributiveLaw[F, W], g: GAlgebraM[W, M, F, A])
      (implicit BT: Traverse[F])
        : M[A] =
      typeClassInstance.gcataM[W, M, A](self)(w, g)
    def elgotCata[W[_]: Comonad, A]
      (k: DistributiveLaw[F, W], g: ElgotAlgebra[W, F, A])
      (implicit BF: Functor[F])
        : A =
      typeClassInstance.elgotCata[W, A](self)(k, g)
    def elgotCataM[W[_]: Comonad : Traverse, M[_]: Monad, A]
      (k: DistributiveLaw[F, (M ∘ W)#λ], g: ElgotAlgebraM[W, M, F, A])
      (implicit BT: Traverse[F])
        : M[A] =
      typeClassInstance.elgotCataM[W, M, A](self)(k, g)
    def para[A](f: GAlgebra[(T, ?), F, A])(implicit BF: Functor[F]): A =
      typeClassInstance.para[A](self)(f)
    def elgotPara[A]
      (f: ElgotAlgebra[(T, ?), F, A])
      (implicit BF: Functor[F])
        : A =
      typeClassInstance.elgotPara[A](self)(f)
    def paraM[M[_]: Monad, A]
      (f: GAlgebraM[(T, ?), M, F, A])
      (implicit BT: Traverse[F])
        : M[A] =
      typeClassInstance.paraM[M, A](self)(f)
    def zygo[A, B]
      (f: Algebra[F, B], g: GAlgebra[(B, ?), F, A])
      (implicit BF: Functor[F])
        : A =
      typeClassInstance.zygo[A, B](self)(f, g)
    def zygoM[A, B, M[_]: Monad]
      (f: AlgebraM[M, F, B], g: GAlgebraM[(B, ?), M, F, A])
      (implicit BT: Traverse[F])
        : M[A] =
      typeClassInstance.zygoM[A, B, M](self)(f, g)
    def elgotZygo[A, B]
      (f: Algebra[F, B], g: ElgotAlgebra[(B, ?), F, A])
      (implicit BF: Functor[F])
        : A =
      typeClassInstance.elgotZygo[A, B](self)(f, g)
    def elgotZygoM[A, B, M[_]: Monad]
      (f: AlgebraM[M, F, B], g: ElgotAlgebraM[(B, ?), M, F, A])
      (implicit BT: Traverse[F])
        : M[A] =
      typeClassInstance.elgotZygoM[A, B, M](self)(f, g)
    def gzygo[W[_]: Comonad, A, B]
      (f: Algebra[F, B],
        w: DistributiveLaw[F, W],
        g: GAlgebra[EnvT[B, W, ?], F, A])
      (implicit BF: Functor[F])
        : A =
      typeClassInstance.gzygo[W, A, B](self)(f, w, g)
    def gElgotZygo[W[_]: Comonad, A, B]
      (f: Algebra[F, B],
        w: DistributiveLaw[F, W],
        g: ElgotAlgebra[EnvT[B, W, ?], F, A])
      (implicit BF: Functor[F])
        : A =
      typeClassInstance.gElgotZygo [W, A, B](self)(f, w, g)
    def mutu[A, B]
      (f: GAlgebra[(A, ?), F, B], g: GAlgebra[(B, ?), F, A])
      (implicit BF: Functor[F])
        : A =
      typeClassInstance.mutu[A, B](self)(f, g)
    def histo[A]
      (f: GAlgebra[Cofree[F, ?], F, A])
      (implicit BF: Functor[F])
        : A =
      typeClassInstance.histo(self)(f)
    def elgotHisto[A]
      (f: ElgotAlgebra[Cofree[F, ?], F, A])
      (implicit BF: Functor[F])
        : A =
      typeClassInstance.elgotHisto(self)(f)
    def ghisto[H[_]: Functor, A]
      (g: DistributiveLaw[F, H], f: GAlgebra[Cofree[H, ?], F, A])
      (implicit BF: Functor[F])
        : A =
      typeClassInstance.ghisto(self)(g, f)
    def gcataZygo[W[_]: Comonad, A, B]
      (w: DistributiveLaw[F, W], f: GAlgebra[W, F, B], g: GAlgebra[(B, ?), F, A])
      (implicit BF: Functor[F], BU: Unzip[F])
        : A =
      typeClassInstance.gcataZygo[W, A, B](self)(w, f, g)
    def paraZygo[A, B]
      (f: GAlgebra[(T, ?), F, B], g: GAlgebra[(B, ?), F, A])
      (implicit BF: Functor[F], BU: Unzip[F])
        : A =
      typeClassInstance.paraZygo[A, B](self)(f, g)
    def paraMerga[A]
      (that: T)
      (f: (T, T, Option[F[A]]) => A)
      (implicit BF: Functor[F], BM: Merge[F])
        : A =
      typeClassInstance.paraMerga[A](self, that)(f)
    def isLeaf(implicit BT: Traverse[F]): Boolean =
      typeClassInstance.isLeaf(self)
    def children(implicit BT: Traverse[F]): List[T] =
      typeClassInstance.children(self)
    def universe(implicit BF: Functor[F], B: Foldable[F]): NonEmptyList[T] =
      typeClassInstance.universe(self)
    def attributeTopDown[A]
      (z: A)
      (f: (A, F[T]) => A)
      (implicit BF: Functor[F])
        : Cofree[F, A] =
      typeClassInstance.attributeTopDown[A](self, z)(f)
    def attributeTopDownM[M[_]: Monad, A]
      (z: A)
      (f: (A, F[T]) => M[A])
      (implicit BT: Traverse[F])
        : M[Cofree[F, A]] =
      typeClassInstance.attributeTopDownM[M, A](self, z)(f)
    def all(p: T ⇒ Boolean)(implicit BF: Functor[F], B: Foldable[F])
        : Boolean =
      typeClassInstance.all(self)(p)
    def any(p: T ⇒ Boolean)(implicit BF: Functor[F], B: Foldable[F])
        : Boolean =
      typeClassInstance.any(self)(p)
    def collect[B]
      (pf: PartialFunction[T, B])
      (implicit BF: Functor[F], B: Foldable[F])
        : List[B] =
      typeClassInstance.collect[B](self)(pf)
    def contains
      (c: T)
      (implicit T: Equal[T], BF: Functor[F], B: Foldable[F])
        : Boolean =
      typeClassInstance.contains(self, c)
    def foldMap[Z: Monoid]
      (f: T => Z)
      (implicit BF: Functor[F], B: Foldable[F])
        : Z =
      typeClassInstance.foldMap[Z](self)(f)
    def foldMapM[M[_]: Monad, Z: Monoid]
      (f: T => M[Z])
      (implicit BF: Functor[F], B: Foldable[F])
        : M[Z] =
      typeClassInstance.foldMapM[M, Z](self)(f)
    def convertTo[R](implicit R: Corecursive.Aux[R, F], BF: Functor[F])
        : R =
      typeClassInstance.convertTo[R](self)
    def mapR[U, G[_]: Functor]
      (f: F[T] => G[U])
      (implicit U: Corecursive.Aux[U, G], BF: Functor[F])
        : U =
      typeClassInstance.mapR(self)(f)
    def traverseR[M[_]: Functor, U, G[_]: Functor]
      (f: F[T] => M[G[U]])
      (implicit U: Corecursive.Aux[U, G], BF: Functor[F])
        : M[U] =
      typeClassInstance.traverseR(self)(f)

    object transCata {
      def apply[U] = new PartiallyApplied[U]
      final class PartiallyApplied[U] {
        def apply[G[_]: Functor]
          (f: F[U] => G[U])
          (implicit U: Corecursive.Aux[U, G], BF: Functor[F])
            : U =
          typeClassInstance.transCata(self)(f)
      }
    }

    object transAna {
      def apply[U] = new PartiallyApplied[U]
      final class PartiallyApplied[U] {
        def apply[G[_]: Functor]
          (f: F[T] => G[T])
          (implicit U: Corecursive.Aux[U, G], BF: Functor[F])
            : U =
          typeClassInstance.transAna(self)(f)
      }
    }

    object transPostpro {
      def apply[U] = new PartiallyApplied[U]
      final class PartiallyApplied[U] {
        def apply[G[_]: Functor]
          (e: G ~> G, f: Transform[T, F, G])
          (implicit U: Birecursive.Aux[U, G], BF: Functor[F])
            : U =
          typeClassInstance.transPostpro(self)(e, f)
      }
    }

    object transPara {
      def apply[U] = new PartiallyApplied[U]
      final class PartiallyApplied[U] {
        def apply[G[_]: Functor]
          (f: AlgebraicGTransform[(T, ?), U, F, G])
          (implicit U: Corecursive.Aux[U, G], BF: Functor[F])
            : U =
          typeClassInstance.transPara(self)(f)
      }
    }

    def transApo[U, G[_]: Functor]
      (f: CoalgebraicGTransform[(U \/ ?), T, F, G])
      (implicit U: Corecursive.Aux[U, G], BF: Functor[F])
        : U =
      typeClassInstance.transApo(self)(f)
    def transHylo[G[_]: Functor, U, H[_]: Functor]
      (φ: G[U] => H[U], ψ: F[T] => G[T])
      (implicit U: Corecursive.Aux[U, H], BF: Functor[F])
        : U =
      typeClassInstance.transHylo(self)(φ, ψ)
    def transCataM[M[_]: Monad, U, G[_]: Functor]
      (f: TransformM[M, U, F, G])
      (implicit U: Corecursive.Aux[U, G], BT: Traverse[F])
        : M[U] =
      typeClassInstance.transCataM(self)(f)
    def transAnaM[M[_]: Monad, U, G[_]: Traverse]
      (f: TransformM[M, T, F, G])
      (implicit U: Corecursive.Aux[U, G], BF: Functor[F])
        : M[U] =
      typeClassInstance.transAnaM(self)(f)
  }

  trait ToRecursiveOps {
    implicit def toRecursiveOps[T, F[_]](target: T)(implicit tc: Aux[T, F]): Ops[T, F] =
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
    implicit def toAllRecursiveOps[T, F[_]](target: T)(implicit tc: Aux[T, F]): AllOps[T, F] =
      new AllOps[T, F] {
        val self = target
        val typeClassInstance = tc
      }
  }
}
