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

import matryoshka.implicits._
import matryoshka.patterns.EnvT

import scala.{Boolean, Option, PartialFunction}
import scala.Predef.identity
import scala.collection.immutable.{List, Nil}

import scalaz._, Scalaz._

/** Folds for recursive data types. */
trait Recursive[T] extends Based[T] {
  // TODO: This works around a bug in Simulacrum (#55). Delete once that is fixed.
  type BaseT[A] = Base[A]

  def project(t: T)(implicit BF: Functor[Base]): BaseT[T]

  def cata[A](t: T)(f: Algebra[Base, A])(implicit BF: Functor[Base]): A =
    f(project(t) ∘ (cata(_)(f)))

  /** A Kleisli catamorphism. */
  def cataM[M[_]: Monad, A](t: T)(f: AlgebraM[M, Base, A])(implicit BT: Traverse[Base]):
      M[A] =
    project(t).traverse(cataM(_)(f)).flatMap(f)

  /** A catamorphism generalized with a comonad inside the functor. */
  def gcata[W[_]: Comonad, A]
    (t: T)
    (k: DistributiveLaw[Base, W], g: GAlgebra[W, Base, A])
    (implicit BF: Functor[Base])
      : A = {
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
    k: DistributiveLaw[Base, W], g: ElgotAlgebra[W, Base, A])
    (implicit BF: Functor[Base]):
      A = {
    def loop(t: T): W[Base[A]] = k(project(t) ∘ (loop(_).cojoin.map(g)))

    g(loop(t))
  }

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

  def elgotZygo[A, B]
    (t: T)
    (f: Algebra[Base, B], g: ElgotAlgebra[(B, ?), Base, A])
    (implicit BF: Functor[Base])
      : A =
    elgotCata[(B, ?), A](t)(distZygo(f), g)

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
    !Tag.unwrap(project(t).foldMap(_ => true.disjunction))

  def children(t: T)(implicit BF: Functor[Base], B: Foldable[Base]): List[T] =
    project(t).foldMap(_ :: Nil)

  def universe(t: T)(implicit BF: Functor[Base], B: Foldable[Base]): List[T] =
    t :: children(t).flatMap(universe)

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
    mapR(t)(ft => f(ft.map(transCata(_)(f))))

  def transAna[U, G[_]: Functor]
    (t: T)
    (f: Base[T] => G[T])
    (implicit U: Corecursive.Aux[U, G], BF: Functor[Base])
      : U =
    mapR(t)(f(_).map(transAna(_)(f)))

  def transPostpro[U, G[_]: Functor]
    (t: T)
    (e: G ~> G, f: Transform[T, Base, G])
    (implicit UR: Recursive.Aux[U, G], UC: Corecursive.Aux[U, G], BF: Functor[Base])
      : U =
    mapR(t)(f(_) ∘ (x => UR.transAna(transPostpro(x)(e, f))(e)))

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
    traverseR(t)(_.traverse(transCataM(_)(f)).flatMap(f))

  def transAnaM[M[_]: Monad, U, G[_]: Traverse]
    (t: T)
    (f: TransformM[M, T, Base, G])
    (implicit U: Corecursive.Aux[U, G], BF: Functor[Base])
      : M[U] =
    traverseR(t)(f(_).flatMap(_.traverse(transAnaM(_)(f))))

  // TODO: Move these operations to `Birecursive` once #44 is fixed.

  /** Roughly a default impl of `project`, given a [[matryoshka.Corecursive]]
    * instance and an overridden `cata`.
    */
  def lambek(tf: T)(implicit T: Corecursive.Aux[T, Base], BF: Functor[Base])
      : Base[T] =
    cata[Base[T]](tf)(_ ∘ (_.embed))

  def gpara[W[_]: Comonad, A](
    t: T)(
    e: DistributiveLaw[Base, W], f: GAlgebra[EnvT[T, W, ?], Base, A])(
    implicit T: Corecursive.Aux[T, Base], BF: Functor[Base]):
      A =
    gzygo[W, A, T](t)(T.embed(_), e, f)

  def prepro[A]
    (t: T)
    (e: Base ~> Base, f: Algebra[Base, A])
    (implicit T: Corecursive.Aux[T, Base], BF: Functor[Base])
      : A =
    f(project(t) ∘ (x => prepro(cata[T](x)(c => T.embed(e(c))))(e, f)))

  def gprepro[W[_]: Comonad, A](
    t: T)(
    k: DistributiveLaw[Base, W], e: Base ~> Base, f: GAlgebra[W, Base, A])(
    implicit T: Corecursive.Aux[T, Base], BF: Functor[Base]):
      A = {
    def loop(t: T): W[A] =
      k(project(t) ∘ (x => loop(cata[T](x)(c => T.embed(e(c)))).cojoin)) ∘ f

    loop(t).copoint
  }

  def topDownCata[A]
    (t: T, a: A)
    (f: (A, T) => (A, T))
    (implicit T: Corecursive.Aux[T, Base], BF: Functor[Base])
      : T = {
    val (a0, tf) = f(a, t)
    mapR(tf)(_.map(topDownCata(_, a0)(f)))
  }

  def topDownCataM[M[_]: Monad, A](
    t: T, a: A)(
    f: (A, T) => M[(A, T)])(
    implicit T: Corecursive.Aux[T, Base], BT: Traverse[Base]):
      M[T] =
    f(a, t).flatMap { case (a, tf) =>
      traverseR(tf)(_.traverse(topDownCataM(_, a)(f)))
    }

  def transPrepro[U, G[_]: Functor]
    (t: T)
    (e: Base ~> Base, f: Transform[U, Base, G])
    (implicit T: Corecursive.Aux[T, Base], U: Corecursive.Aux[U, G], BF: Functor[Base])
      : U =
    mapR(t)(ft => f(ft ∘ (x => transPrepro(transCata[T, Base](x)(e(_)))(e, f))))

  def transCataT
    (t: T)
    (f: T => T)
    (implicit T: Corecursive.Aux[T, Base], BF: Functor[Base])
      : T =
    f(mapR(t)(_.map(transCataT(_)(f))))

  /** This behaves like [[matryoshka.Recursive.elgotPara]]`, but it’s harder to
    * see from the types that in the tuple, `_2` is the result so far and `_1`
    * is the original structure.
    */
  def transParaT
    (t: T)
    (f: ((T, T)) => T)
    (implicit T: Corecursive.Aux[T, Base], BF: Functor[Base])
      : T =
    f((t, mapR(t)(_.map(transParaT(_)(f)))))

  def transAnaT
    (t: T)
    (f: T => T)
    (implicit T: Corecursive.Aux[T, Base], BF: Functor[Base])
      : T =
    mapR(f(t))(_.map(transAnaT(_)(f)))

  /** This behaves like [[matryoshka.Corecursive.elgotApo]]`, but it’s harder to
    * see from the types that in the disjunction, `-\/` is the final result for
    * this node, while `\/-` means to keep processing the children.
    */
  def transApoT
    (t: T)
    (f: T => T \/ T)
    (implicit T: Corecursive.Aux[T, Base], BF: Functor[Base])
      : T =
    f(t).fold(identity, mapR(_)(_.map(transApoT(_)(f))))

  def transCataTM[M[_]: Monad]
    (t: T)
    (f: T => M[T])
    (implicit T: Corecursive.Aux[T, Base], BF: Traverse[Base])
      : M[T] =
    traverseR(t)(_.traverse(transCataTM(_)(f))).flatMap(f)

  def transAnaTM[M[_]: Monad]
    (t: T)
    (f: T => M[T])
    (implicit T: Corecursive.Aux[T, Base], BF: Traverse[Base])
      : M[T] =
    f(t).flatMap(traverseR(_)(_.traverse(transAnaTM(_)(f))))
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
    def lambek(implicit T: Corecursive.Aux[T, F], BF: Functor[F]): F[T] =
      typeClassInstance.lambek(self)
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
      (k: DistributiveLaw[F, W], g: GAlgebraM[W, M, F, A])
      (implicit BT: Traverse[F])
        : M[A] =
      typeClassInstance.gcataM[W, M, A](self)(k, g)
    def elgotCata[W[_]: Comonad, A]
      (k: DistributiveLaw[F, W], g: ElgotAlgebra[W, F, A])
      (implicit BF: Functor[F])
        : A =
      typeClassInstance.elgotCata[W, A](self)(k, g)
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
    def gpara[W[_]: Comonad, A]
      (e: DistributiveLaw[F, W], f: GAlgebra[EnvT[T, W, ?], F, A])
      (implicit T: Corecursive.Aux[T, F], BF: Functor[F])
        : A =
      typeClassInstance.gpara[W, A](self)(e, f)
    def zygo[A, B]
      (f: Algebra[F, B], g: GAlgebra[(B, ?), F, A])
      (implicit BF: Functor[F])
        : A =
      typeClassInstance.zygo[A, B](self)(f, g)
    def elgotZygo[A, B]
      (f: Algebra[F, B], g: ElgotAlgebra[(B, ?), F, A])
      (implicit BF: Functor[F])
        : A =
      typeClassInstance.elgotZygo[A, B](self)(f, g)
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
    def prepro[A]
      (e: F ~> F, f: Algebra[F, A])
      (implicit T: Corecursive.Aux[T, F], BF: Functor[F])
        : A =
      typeClassInstance.prepro[A](self)(e, f)
    def gprepro[W[_]: Comonad, A]
      (k: DistributiveLaw[F, W], e: F ~> F, f: GAlgebra[W, F, A])
      (implicit T: Corecursive.Aux[T, F], BF: Functor[F])
        : A =
      typeClassInstance.gprepro[W, A](self)(k, e, f)
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
    def universe(implicit BT: Traverse[F]): List[T] =
      typeClassInstance.universe(self)
    def topDownCata[A]
      (a: A)
      (f: (A, T) => (A, T))
      (implicit T: Corecursive.Aux[T, F], BF: Functor[F])
        : T =
      typeClassInstance.topDownCata[A](self, a)(f)
    def topDownCataM[M[_]: Monad, A]
      (a: A)
      (f: (A, T) => M[(A, T)])
      (implicit T: Corecursive.Aux[T, F], BT: Traverse[F])
        : M[T] =
      typeClassInstance.topDownCataM[M, A](self, a)(f)
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

    object transPrepro {
      def apply[U] = new PartiallyApplied[U]
      final class PartiallyApplied[U] {
        def apply[G[_]: Functor]
          (e: F ~> F, f: Transform[U, F, G])
          (implicit T: Corecursive.Aux[T, F], U: Corecursive.Aux[U, G], BF: Functor[F])
            : U =
          typeClassInstance.transPrepro(self)(e, f)
      }
    }

    object transPostpro {
      def apply[U] = new PartiallyApplied[U]
      final class PartiallyApplied[U] {
        def apply[G[_]: Functor]
          (e: G ~> G, f: Transform[T, F, G])
          (implicit UR: Recursive.Aux[U, G], UC: Corecursive.Aux[U, G], BF: Functor[F])
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
    def transCataT(f: T => T)(implicit T: Corecursive.Aux[T, F], BF: Functor[F]): T =
      typeClassInstance.transCataT(self)(f)

    def transParaT(f: ((T, T)) => T)(implicit T: Corecursive.Aux[T, F], BF: Functor[F]): T =
      typeClassInstance.transParaT(self)(f)

    def transAnaT(f: T => T)(implicit T: Corecursive.Aux[T, F], BF: Functor[F]): T =
      typeClassInstance.transAnaT(self)(f)

    def transApoT(f: T => T \/ T)(implicit T: Corecursive.Aux[T, F], BF: Functor[F]): T =
      typeClassInstance.transApoT(self)(f)

    def transCataTM[M[_]: Monad](f: T => M[T])(implicit T: Corecursive.Aux[T, F], BF: Traverse[F])
        : M[T] =
      typeClassInstance.transCataTM(self)(f)

    def transAnaTM[M[_]: Monad](f: T => M[T])(implicit T: Corecursive.Aux[T, F], BF: Traverse[F])
        : M[T] =
      typeClassInstance.transAnaTM(self)(f)

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
