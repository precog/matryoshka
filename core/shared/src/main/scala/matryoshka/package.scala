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

import matryoshka.Recursive.ops._
import matryoshka.patterns.{CoEnv, EnvT}

import scala.{Boolean, Function, Int, None, Option, Unit}
import scala.collection.immutable.{List, ::}

import monocle._
import scalaz._, Liskov._, Scalaz._

/** Generalized folds, unfolds, and refolds.
  *
  * @groupname algebras Algebras & Coalgebras
  * @groupdesc algebras Generic algebras that operate over most functors.
  * @groupname algtrans Algebra Transformations
  * @groupdesc algtrans Operations that modify algebras in various ways to make them easier to combine with others.
  * @groupname refolds Refolds
  * @groupdesc refolds Traversals that apply an unfold and a fold in a single pass.
  * @groupname dist Distributive Laws
  * @groupdesc dist Natural transformations required for generalized folds and unfolds.
  */
package object matryoshka {

  /** Fold a structure `F` containing values in `W`, to a value `A`, 
    * accumulating effects in the monad `M`.
    * @group algebras 
    */
  type GAlgebraM[W[_], M[_], F[_], A] = F[W[A]] => M[A]
  /** Fold a structure `F` containing values in `W`, to a value `A`.
    * @group algebras 
    */
  type GAlgebra[W[_], F[_], A]        = F[W[A]] => A    // GAlgebraM[W, Id, F, A]
  /** Fold a structure `F` to a value `A`, accumulating effects in the monad `M`.
    * @group algebras 
    */
  type AlgebraM[M[_], F[_], A]        = F[A]    => M[A] // GAlgebraM[Id, M, F, A]
  /** Fold a structure `F` to a value `A`.
    * @group algebras 
    */
  type Algebra[F[_], A]               = F[A] => A       // GAlgebra[Id, F, A]
  /** Fold a structure `F` (usually a `Functor`) contained in `W` (usually a 
    * `Comonad`), to a value `A`, accumulating effects in the monad `M`.
    * @group algebras 
    */
  type ElgotAlgebraM[W[_], M[_], F[_], A] =                        W[F[A]] => M[A]
  /** Fold a structure `F` (usually a `Functor`) contained in `W` (usually a 
    * `Comonad`), to a value `A`.
    * @group algebras 
    */
  type ElgotAlgebra[W[_], F[_], A] = ElgotAlgebraM[W, Id, F, A] // W[F[A]] => A

  /** Unfold a value `A` to a structure `F` containing values in `N`, 
    * accumulating effects in the monad `M`.
    * @group algebras 
    */
  type GCoalgebraM[N[_], M[_], F[_], A] = A => M[F[N[A]]]
  /** Unfold a value `A` to a structure `F` containing values in `N`.
    * @group algebras 
    */
  type GCoalgebra[N[_], F[_], A]        = A => F[N[A]] // GCoalgebraM[N, Id, F, A]
  /** Unfold a value `A` to a structure `F`, accumulating effects in the monad 
    * `M`.
    * @group algebras 
    */
  type CoalgebraM[M[_], F[_], A]        = A => M[F[A]] // GCoalgebraM[Id, M, F, A]
  /** Unfold a value `A` to a structure `F`.
    * @group algebras 
    */
  type Coalgebra[F[_], A]               = A => F[A]    // GCoalgebra[Id, F, A]
  /** Unfold a value `A` to a structure `F` (usually a `Functor`), contained in 
    * `E`, accumulating effects in the monad `M`.
    * @group algebras 
    */
  type ElgotCoalgebraM[E[_], M[_], F[_], A] = A => M[E[F[A]]]
  /** Unfold a value `A` to a structure `F` (usually a `Functor`), contained in 
    * `E`.
    * @group algebras 
    */
  type ElgotCoalgebra[E[_], F[_], A]        = A => E[F[A]] // ElgotCoalgebraM[E, Id, F, A]

  /** Transform a structure `F` containing values in `W`, to a structure `G`, 
    * in bottom-up fashion, accumulating effects in the monad `M`.
    * @group algebras 
    */
  type GAlgebraicTransformM[T[_[_]], W[_], M[_], F[_], G[_]] = F[W[T[G]]] => M[G[T[G]]]
  /** Transform a structure `F`, to a structure `G`, in bottom-up fashion, 
    * accumulating effects in the monad `M`.
    * @group algebras 
    */
  type AlgebraicTransformM[T[_[_]], M[_], F[_], G[_]]        = F[T[G]] => M[G[T[G]]] // GAlgebraicTransformM[T, Id, M, F, G]
  /** Transform a structure `F` containing values in `W`, to a structure `G`, 
    * in bottom-up fashion.
    * @group algebras 
    */
  type GAlgebraicTransform[T[_[_]], W[_], F[_], G[_]]        = F[W[T[G]]] => G[T[G]] // GAlgebraicTransformM[T, W, Id, F, G]
  /** Transform a structure `F` to a structure `G`, in bottom-up fashion.
    * @group algebras 
    */
  type AlgebraicTransform[T[_[_]], F[_], G[_]]               = F[T[G]] => G[T[G]]    // GAlgebraicTransformM[T, Id, Id, F, G]

  /** Transform a structure `F` to a structure `G` containing values in `N`, 
    * in top-down fashion, accumulating effects in the monad `M`.
    * @group algebras 
    */
  type GCoalgebraicTransformM[T[_[_]], N[_], M[_], F[_], G[_]] = F[T[F]] => M[G[N[T[F]]]]
  /** Transform a structure `F` to a structure `G`, in top-down fashion, 
    * accumulating effects in the monad `M`.
    * @group algebras 
    */
  type CoalgebraicTransformM[T[_[_]], M[_], F[_], G[_]]        = F[T[F]] => M[G[T[F]]] // GCoalgebraicTransformM[T, Id, M, F, G]
  /** Transform a structure `F` to a structure `G` containing values in `N`, 
    * in top-down fashion.
    * @group algebras 
    */
  type GCoalgebraicTransform[T[_[_]], N[_], F[_], G[_]]        = F[T[F]] => G[N[T[F]]] // GCoalgebraicTransformM[T, N, Id, F, G]
  /** Transform a structure `F` to a structure `G`, in top-down fashion.
    * @group algebras 
    */
  type CoalgebraicTransform[T[_[_]], F[_], G[_]]               = F[T[F]] => G[T[F]]    // GCoalgebraicTransformM[T, Id, Id, F, G]

  /** @group algtrans */
  def transformToAlgebra[T[_[_]], W[_], M[_]: Functor, F[_], G[_]: Functor]
    (self: GAlgebraicTransformM[T, W, M, F, G])
    (implicit T: Corecursive.Aux[T[G], G])
      : GAlgebraM[W, M, F, T[G]] =
    self(_) ∘ (_.embed)

  /** An algebra and its dual form an isomorphism.
    */
  type GAlgebraIso[W[_], N[_], F[_], A] = PIso[F[W[A]], F[N[A]], A, A]
  object GAlgebraIso {
    def apply[W[_], N[_], F[_], A](φ: GAlgebra[W, F, A])(ψ: GCoalgebra[N, F, A]):
        GAlgebraIso[W, N, F, A] =
      PIso(φ)(ψ)
  }

  type AlgebraIso[F[_], A] = GAlgebraIso[Id, Id, F, A]
  object AlgebraIso {
    def apply[F[_], A](φ: Algebra[F, A])(ψ: Coalgebra[F, A]):
        AlgebraIso[F, A] =
      Iso(φ)(ψ)
  }

  type AlgebraPrism[F[_], A] = Prism[F[A], A]
  object AlgebraPrism {
    def apply[F[_], A](φ: AlgebraM[Option, F, A])(ψ: Coalgebra[F, A]):
        AlgebraPrism[F, A] =
      Prism(φ)(ψ)
  }

  type CoalgebraPrism[F[_], A] = Prism[A, F[A]]
  object CoalgebraPrism {
    def apply[F[_], A](ψ: CoalgebraM[Option, F, A])(φ: Algebra[F, A]):
        CoalgebraPrism[F, A] =
      Prism(ψ)(φ)
  }

  def birecursiveIso[T, F[_]]
    (implicit TR: Recursive.Aux[T, F], TC: Corecursive.Aux[T, F]) =
    AlgebraIso[F, T](_.embed)(_.project)

  def bilambekIso[T, F[_]]
    (implicit TR: Recursive.Aux[T, F], TC: Corecursive.Aux[T, F]) =
    AlgebraIso[F, T](_.colambek)(_.lambek)

  /** There is a fold/unfold isomorphism for any AlgebraIso.
    */
  def foldIso[T, F[_], A]
    (alg: AlgebraIso[F, A])
    (implicit TR: Recursive.Aux[T, F], TC: Corecursive.Aux[T, F]) =
    Iso[T, A](_.cata(alg.get))(_.ana[T](alg.reverseGet))

  /** There is a fold prism for any AlgebraPrism.
    */
  def foldPrism[T, F[_]: Traverse, A]
    (alg: AlgebraPrism[F, A])
    (implicit TR: Recursive.Aux[T, F], TC: Corecursive.Aux[T, F]) =
    Prism[T, A](TR.cataM(_)(alg.getOption))(_.ana[T](alg.reverseGet))

  /** There is an unfold prism for any CoalgebraPrism.
    */
  def unfoldPrism[T, F[_]: Traverse, A]
    (coalg: CoalgebraPrism[F, A])
    (implicit TR: Recursive.Aux[T, F], TC: Corecursive.Aux[T, F]) =
    Prism[A, T](_.anaM[T](coalg.getOption))(_.cata(coalg.reverseGet))

  /** A NaturalTransformation that sequences two types
    *
    * @group dist
    */
  type DistributiveLaw[F[_], G[_]] = (F ∘ G)#λ ~> (G ∘ F)#λ

  /** Composition of an anamorphism and a catamorphism that avoids building the
    * intermediate recursive data structure.
    *
    * @group refolds
    */
  def hylo[F[_]: Functor, A, B](a: A)(f: Algebra[F, B], g: Coalgebra[F, A]): B =
    f(g(a) ∘ (hylo(_)(f, g)))

  /** A Kleisli hylomorphism.
    *
    * @group refolds
    */
  def hyloM[M[_]: Monad, F[_]: Traverse, A, B](a: A)(f: AlgebraM[M, F, B], g: CoalgebraM[M, F, A]):
      M[B] =
    g(a) >>= (_.traverse(hyloM(_)(f, g)) >>= f)

  /** `histo ⋘ ana`
    *
    * @group refolds
    */
  def dyna[F[_]: Functor, A, B](a: A)(φ: F[Cofree[F, B]] => B, ψ: Coalgebra[F, A]): B =
    ghylo[Cofree[F, ?], Id, F, A, B](a)(distHisto, distAna, φ, ψ)

  /** `cata ⋘ futu`
    *
    * @group refolds
    */
  def codyna[F[_]: Functor, A, B](a: A)(φ: Algebra[F, B], ψ: GCoalgebra[Free[F, ?], F, A]): B =
    ghylo[Id, Free[F, ?], F, A, B](a)(distCata, distFutu, φ, ψ)

  /** `cataM ⋘ futuM`
    *
    * @group refolds
    */
  def codynaM[M[_]: Monad, F[_]: Traverse, A, B](a: A)(φ: AlgebraM[M, F, B], ψ: GCoalgebraM[Free[F, ?], M, F, A]): M[B] =
    ghyloM[Id, Free[F, ?], M, F, A, B](a)(distCata, distFutu, φ, ψ)

  /** A generalized version of a hylomorphism that composes any coalgebra and
    * algebra. (`gcata ⋘ gana`)
    *
    * @group refolds
    */
  def ghylo[W[_]: Comonad, N[_], F[_]: Functor, A, B](
    a: A)(
    w: DistributiveLaw[F, W],
    n: DistributiveLaw[N, F],
    f: GAlgebra[W, F, B],
    g: GCoalgebra[N, F, A])(
    implicit N: Monad[N]):
      B = {
    def h(x: N[A]): W[B] = w(n(N.lift(g)(x)) ∘ (y => h(y.join).cojoin)) ∘ f
    h(a.point[N]).copoint
  }

  /** A Kleisli `ghylo` (`gcataM ⋘ ganaM`)
    *
    * @group refolds
    */
  def ghyloM[W[_]: Comonad: Traverse, N[_]: Traverse, M[_]: Monad, F[_]: Traverse, A, B](
    a: A)(
    w: DistributiveLaw[F, W],
    m: DistributiveLaw[N, F],
    f: GAlgebraM[W, M, F, B],
    g: GCoalgebraM[N, M, F, A])(
    implicit N: Monad[N]):
      M[B] = {
    def h(x: N[A]): M[W[B]] =
      (N.lift(g)(x).sequence >>=
        (m(_: N[F[N[A]]]).traverse(y => h(y.join) ∘ (_.cojoin)))) ∘
        (w(_)) >>=
        (_.traverse(f))
    h(a.point[N]) ∘ (_.copoint)
  }

  /** Similar to a hylomorphism, this composes a futumorphism and a
    * histomorphism.
    *
    * @group refolds
    */
  def chrono[F[_]: Functor, A, B](
    a: A)(
    g: GAlgebra[Cofree[F, ?], F, B], f: GCoalgebra[Free[F, ?], F, A]):
      B =
    ghylo[Cofree[F, ?], Free[F, ?], F, A, B](a)(distHisto, distFutu, g, f)

  /** `cata ⋘ elgotGApo`
    *
    * @group refolds
    */
  def elgot[F[_]: Functor, A, B](a: A)(φ: Algebra[F, B], ψ: ElgotCoalgebra[B \/ ?, F, A]): B = {
    def h: A => B =
      (((x: B) => x) ||| ((x: F[A]) => φ(x ∘ h))) ⋘ ψ
    h(a)
  }

  /** `cataM ⋘ elgotGApoM`
    *
    * @group refolds
    */
  def elgotM[M[_]: Monad, F[_]: Traverse, A, B](a: A)(φ: AlgebraM[M, F, B], ψ: ElgotCoalgebraM[B \/ ?, M, F, A]):
      M[B] = {
    def h(a: A): M[B] = ψ(a) >>= (_.traverse(_.traverse(h) >>= φ).map(_.merge))
    h(a)
  }

  /** `elgotZygo ⋘ ana`
    *
    * @group refolds
    */
  def coelgot[F[_]: Functor, A, B](a: A)(φ: ElgotAlgebra[(A, ?), F, B], ψ: Coalgebra[F, A]):
      B = {
    def h: A => B =
      φ ⋘ (((x: A) => x) &&& (((x: F[A]) => x ∘ h) ⋘ ψ))
    h(a)
  }

  /** `elgotZygoM ⋘ anaM`
    *
    * @group refolds
    */
  def coelgotM[M[_]] = new CoelgotMPartiallyApplied[M]
  final class CoelgotMPartiallyApplied[M[_]] {
    def apply[F[_]: Traverse, A, B](a: A)(φ: ElgotAlgebraM[(A, ?), M, F, B], ψ: CoalgebraM[M, F, A])(implicit M: Monad[M]):
        M[B] = {
      def h(a: A): M[B] = ψ(a) >>= (_.traverse(h)) >>= (x => φ((a, x)))
      h(a)
    }
  }

  /**
    *
    * @group dist
    */
  def distPara[T](implicit T: Corecursive[T]): DistributiveLaw[T.Base, (T, ?)] =
    distZygo[T.Base, T](T.embed(_))(T.BF)

  /**
    *
    * @group dist
    */
  def distParaT[T, W[_]: Comonad, F[_]: Functor]
    (t: DistributiveLaw[F, W])
    (implicit T: Corecursive.Aux[T, F])
      : DistributiveLaw[F, EnvT[T, W, ?]] =
    distZygoT[F, W, T](_.embed, t)

  /**
    *
    * @group dist
    */
  def distCata[F[_]]: DistributiveLaw[F, Id] = NaturalTransformation.refl

  /**
    *
    * @group dist
    */
  def distZygo[F[_]: Functor, B](g: Algebra[F, B]) =
    new DistributiveLaw[F, (B, ?)] {
      def apply[α](m: F[(B, α)]) = (g(m ∘ (_._1)), m ∘ (_._2))
    }

  /**
    *
    * @group dist
    */
  def distZygoT[F[_], W[_]: Comonad, B](
    g: Algebra[F, B], k: DistributiveLaw[F, W])(
    implicit F: Functor[F]) =
    new DistributiveLaw[F, EnvT[B, W, ?]] {
      def apply[α](fe: F[EnvT[B, W, α]]) =
        EnvT((
          g(F.lift[EnvT[B, W, α], B](_.ask)(fe)),
          k(F.lift[EnvT[B, W, α], W[α]](_.lower)(fe))))
    }

  /**
    *
    * @group dist
    */
  def distHisto[F[_]: Functor] =
    new DistributiveLaw[F, Cofree[F, ?]] {
      def apply[α](m: F[Cofree[F, α]]) =
        distGHisto[F, F](NaturalTransformation.refl[λ[α => F[F[α]]]]).apply(m)
    }

  /**
    *
    * @group dist
    */
  def distGHisto[F[_],  H[_]](
    k: DistributiveLaw[F, H])(
    implicit F: Functor[F], H: Functor[H]) =
    new DistributiveLaw[F, Cofree[H, ?]] {
      def apply[α](m: F[Cofree[H, α]]) =
        Cofree.unfold(m)(as => (
          F.lift[Cofree[H, α], α](_.copure)(as),
          k(F.lift[Cofree[H, α], H[Cofree[H, α]]](_.tail)(as))))
    }

  /**
    *
    * @group dist
    */
  def distAna[F[_]]: DistributiveLaw[Id, F] = NaturalTransformation.refl

  /**
    *
    * @group dist
    */
  def distApo[T](implicit T: Recursive[T]): DistributiveLaw[T \/ ?, T.Base] =
    distGApo[T.Base, T](T.project(_))(T.BF)

  /**
    *
    * @group dist
    */
  def distGApo[F[_]: Functor, B](g: Coalgebra[F, B]) =
    new DistributiveLaw[B \/ ?, F] {
      def apply[α](m: B \/ F[α]) = m.bitraverse(g(_), x => x)
    }

  /** Allows for more complex unfolds, like
    * `futuGApo(φ0: Coalgebra[F, B], φ: GCoalgebra[λ[α => EitherT[Free[F, ?], B, α]], F, A])`
    *
    * @group dist
    */
  def distGApoT[F[_]: Functor, M[_]: Functor, B](
    g: Coalgebra[F, B], k: DistributiveLaw[M, F]) =
    new DistributiveLaw[EitherT[M, B, ?], F] {
      def apply[α](m: EitherT[M, B, F[α]]) =
        k(m.run.map(distGApo(g).apply(_))).map(EitherT(_))
    }

  /**
    *
    * @group dist
    */
  def distFutu[F[_]: Functor] =
    new DistributiveLaw[Free[F, ?], F] {
      def apply[α](m: Free[F, F[α]]) =
        distGFutu[F, F](NaturalTransformation.refl[λ[α => F[F[α]]]]).apply(m)
    }

  /**
    *
    * @group dist
    */
  def distGFutu[H[_], F[_]](
    k: DistributiveLaw[H, F])(
    implicit H: Functor[H], F: Functor[F]): DistributiveLaw[Free[H, ?], F] =
    new DistributiveLaw[Free[H, ?], F] {
      def apply[α](m: Free[H, F[α]]) =
        m.fold(
          F.lift(Free.point[H, α](_)),
          as => F.lift(Free.liftF(_: H[Free[H, α]]).join)(k(H.lift(distGFutu(k)(H, F)(_: Free[H, F[α]]))(as))))
    }

  sealed trait Hole
  val Hole = new Hole{}

  def holes[F[_]: Traverse, A](fa: F[A]): F[(A, Coalgebra[F, A])] =
    (fa.mapAccumL(0) {
      case (i, x) =>
        val h: Coalgebra[F, A] = { y =>
          val g: (Int, A) => (Int, A) = (j, z) => (j + 1, if (i == j) y else z)

          fa.mapAccumL(0)(g)._2
        }

        (i + 1, (x, h))
    })._2

  def holesList[F[_]: Traverse, A](fa: F[A]): List[(A, Coalgebra[F, A])] =
    holes(fa).toList

  def builder[F[_]: Traverse, A, B](fa: F[A], children: List[B]): F[B] = {
    (fa.mapAccumL(children) {
      case (x :: xs, _) => (xs, x)
      case _ => scala.sys.error("Not enough children")
    })._2
  }

  def project[F[_]: Foldable, A](index: Int, fa: F[A]): Option[A] =
   if (index < 0) None
   else fa.toList.drop(index).headOption

  /** Turns any F-algebra, into an identical one that attributes the tree with
    * the results for each node.
    *
    * @group algtrans
    */
  def attributeAlgebraM[F[_]: Functor, M[_]: Functor, A](f: AlgebraM[M, F, A]):
      AlgebraM[M, F, Cofree[F, A]] =
    fa => f(fa ∘ (_.head)) ∘ (Cofree(_, fa))

  /**
    *
    * @group algtrans
    */
  def attributeAlgebra[F[_]: Functor, A](f: Algebra[F, A]): Algebra[F, Cofree[F, A]] =
    attributeAlgebraM[F, Id, A](f)

  /**
    *
    * @group algtrans
    */
  def attributeCoalgebra[F[_], B](ψ: Coalgebra[F, B]):
      Coalgebra[EnvT[B, F, ?], B] =
    b => EnvT[B, F, B]((b, ψ(b)))

  /** Useful for ignoring the annotation when folding a cofree. */
  def deattribute[F[_], A, B](φ: Algebra[F, B]): Algebra[EnvT[A, F, ?], B] =
    ann => φ(ann.lower)

  // def ignoreAttribute[T[_[_]], F[_], G[_], A](φ: F[T[G]] => G[T[G]]):
  //     EnvT[A, F, T[EnvT[A, G, ?]]] => EnvT[A, G, T[EnvT[A, G, ?]]] =
  //   ???

  /**
    *
    * @group algtrans
    */
  def attrK[F[_]: Functor, A](k: A) = attributeAlgebra[F, A](Function.const(k))

  /**
    *
    * @group algtrans
    */
  def attrSelf[T](implicit T: Corecursive[T]) =
    attributeAlgebra[T.Base, T](T.embed(_))(T.BF)

  /** A function to be called like `attributeElgotM[M](myElgotAlgebraM)`.
    *
    * @group algtrans
    */
  object attributeElgotM {
    def apply[W[_], M[_]] = new Aux[W, M]

    final class Aux[W[_], M[_]] {
      def apply[F[_]: Functor, A](f: ElgotAlgebraM[W, M, F, A])(implicit W: Comonad[W], M: Functor[M]):
          ElgotAlgebraM[W, M, F, Cofree[F, A]] =
        node => f(node ∘ (_ ∘ (_.head))) ∘ (Cofree(_, node.copoint))
    }
  }

  /** Makes it possible to use ElgotAlgebras on EnvT.
    */
  def liftT[F[_], A, B](φ: ElgotAlgebra[(A, ?), F, B]):
      Algebra[EnvT[A, F, ?], B] =
    ann => φ(ann.run)

  /** Makes it possible to use ElgotAlgebras on EnvT.
    */
  def liftTM[M[_], F[_], A, B](φ: ElgotAlgebraM[(A, ?), M, F, B]):
      AlgebraM[M, EnvT[A, F, ?], B] =
    ann => φ(ann.run)

  def runT[F[_], A, B](ψ: ElgotCoalgebra[A \/ ?, F, B])
      : Coalgebra[CoEnv[A, F, ?], B] =
    b => CoEnv(ψ(b))

  /**
    *
    * @group algtrans
    */
  implicit def GAlgebraZip[W[_]: Functor, F[_]: Functor]:
      Zip[GAlgebra[W, F, ?]] =
    new Zip[GAlgebra[W, F, ?]] {
      def zip[A, B](a: ⇒ GAlgebra[W, F, A], b: ⇒ GAlgebra[W, F, B]) =
        node => (a(node ∘ (_ ∘ (_._1))), b(node ∘ (_ ∘ (_._2))))
    }
  /**
    *
    * @group algtrans
    */
  implicit def AlgebraZip[F[_]: Functor] = GAlgebraZip[Id, F]

  /**
    *
    * @group algtrans
    */
  implicit def ElgotAlgebraMZip[W[_]: Functor, M[_]: Applicative, F[_]: Functor]:
      Zip[ElgotAlgebraM[W, M, F, ?]] =
    new Zip[ElgotAlgebraM[W, M, F, ?]] {
      def zip[A, B](a: ⇒ ElgotAlgebraM[W, M, F, A], b: ⇒ ElgotAlgebraM[W, M, F, B]) =
        w => Bitraverse[(?, ?)].bisequence((a(w ∘ (_ ∘ (_._1))), b(w ∘ (_ ∘ (_._2)))))
    }
  /**
    *
    * @group algtrans
    */
  implicit def ElgotAlgebraZip[W[_]: Functor, F[_]: Functor] =
    ElgotAlgebraMZip[W, Id, F]
    // (ann, node) => node.unfzip.bimap(f(ann, _), g(ann, _))

  /** Repeatedly applies the function to the result as long as it returns Some.
    * Finally returns the last non-None value (which may be the initial input).
    *
    * @group algtrans
    */
  def repeatedly[A](f: A => Option[A]): A => A =
    expr => f(expr).fold(expr)(repeatedly(f))

  /** Converts a failable fold into a non-failable, by simply returning the
    * argument upon failure.
    *
    * @group algtrans
    */
  def orOriginal[A](f: A => Option[A]): A => A = expr => f(expr).getOrElse(expr)

  /** Converts a failable fold into a non-failable, by returning the default
    * upon failure.
    *
    * @group algtrans
    */
  def orDefault[A, B](default: B)(f: A => Option[B]): A => B =
    expr => f(expr).getOrElse(default)

  /** Count the instinces of `form` in the structure.
    *
    * @group algebras
    */
  def count[T, F[_]: Functor: Foldable]
    (form: T)
    (implicit T: Recursive.Aux[T, F])
      : GAlgebra[(T, ?), F, Int] =
    e => e.foldRight(if (e ∘ (_._1) == form.project) 1 else 0)(_._2 + _)

  /** The number of nodes in this structure.
    *
    * @group algebras
    */
  def size[F[_]: Foldable]: Algebra[F, Int] = _.foldRight(1)(_ + _)

  /** The largest number of hops from a node to a leaf.
    *
    * @group algebras
    */
  def height[F[_]: Foldable]: Algebra[F, Int] = _.foldRight(-1)(_ max _) + 1

  /** Combines a tuple of zippable functors.
    *
    * @group algebras
    */
  def zipTuple[T, F[_]: Zip](implicit T: Recursive.Aux[T, F])
      : Coalgebra[F, (T, T)] =
    p => Zip[F].zip[T, T](p._1.project, p._2.project)

  /** Aligns “These” into a single structure, short-circuting when we hit a
    * “This” or “That”.
    *
    * @group algebras
    */
  def alignThese[T, F[_]: Align](implicit T: Recursive.Aux[T, F])
      : ElgotCoalgebra[T \/ ?, F, T \&/ T] =
    _.fold(_.left, _.left, (a, b) => a.project.align(b.project).right)

  /** Merges a tuple of functors, if possible.
    *
    * @group algebras
    */
  def mergeTuple[T, F[_]: Merge](implicit T: Recursive.Aux[T, F])
      : CoalgebraM[Option, F, (T, T)] =
    p => Merge[F].merge[T, T](p._1.project, p._2.project)

  /** Generates an infinite sequence from two seed values.
    *
    * @group algebras
    */
  def binarySequence[A](relation: (A, A) => A): Coalgebra[(A, ?), (A, A)] = {
    case (a1, a2) =>
      val c = relation(a1, a2)
      (c, (a2, c))
  }

  /** Converts a fixed-point structure into a generic Tree.
    * One use of this is using `.cata(toTree).drawTree` rather than `.show` to
    * get a pretty-printed tree.
    *
    * @group algebras
    */
  def toTree[F[_]: Functor: Foldable]: Algebra[F, Tree[F[Unit]]] =
    x => Tree.Node(x.void, x.toStream)

  /** Returns (on the left) the first element that passes `f`.
    */
  def find[T](f: T => Boolean): T => T \/ T =
    tf => if (f(tf)) tf.left else tf.right

  /** Replaces all instances of `original` in the structure with `replacement`.
    *
    * @group algebras
    */
  def substitute[T](original: T, replacement: T)(implicit T: Equal[T])
      : T => T \/ T =
    find[T](_ ≟ original)(_).leftMap(_ => replacement)

  sealed abstract class ∘[F[_], G[_]] { type λ[A] = F[G[A]] }

  /** To avoid diverging implicits with fixed-point types, we need to defer the
    * lookup. We do this with a `NaturalTransformation` (although there
    * are more type class-y solutions available now).
    */
  type Delay[F[_], G[_]] = F ~> (F ∘ G)#λ

  /** This implicit allows Delay implicits to be found when searching for a
    * traditionally-defined instance.
    */
  implicit def delayEqual[F[_], A](implicit A: Equal[A], F: Delay[Equal, F]):
      Equal[F[A]] =
    F(A)

  /** See `delayEqual`.
    */
  implicit def delayShow[F[_], A](implicit A: Show[A], F: Delay[Show, F]):
      Show[F[A]] =
    F(A)

  implicit def toIdOps[A](a: A): IdOps[A] = new IdOps[A](a)

  implicit final class CorecursiveOps[T, F[_], FF[_]](
    self: F[T])(
    implicit T: Corecursive.Aux[T, FF], Sub: F[T] <~< FF[T]) {

    val embed: T = T.embed(Sub(self))
    def colambek(implicit TR: Recursive.Aux[T, FF]): T = T.colambek(Sub(self))
  }

  implicit final class CorecursiveTOps[T[_[_]], F[_], FF[_]: Functor](
    self: F[T[FF]])(
    implicit T: CorecursiveT[T], Sub: F[T[FF]] <~< FF[T[FF]]) {

    val embedT: T[FF] = T.embedT[FF](Sub(self))
  }

  implicit def toAlgebraOps[F[_], A](a: Algebra[F, A]): AlgebraOps[F, A] =
    new AlgebraOps[F, A](a)

  implicit def toCoalgebraOps[F[_], A](a: Coalgebra[F, A]): CoalgebraOps[F, A] =
    new CoalgebraOps[F, A](a)
}
