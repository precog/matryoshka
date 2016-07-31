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
import matryoshka.patterns.EnvT

import scala.{Function, Int, None, Option, Unit}
import scala.collection.immutable.{List, ::}

import monocle._
import scalaz._, Scalaz._

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
package object matryoshka extends CofreeInstances with FreeInstances {

  def lambek[T[_[_]]: Corecursive: Recursive, F[_]: Functor](tf: T[F]):
      F[T[F]] =
    tf.cata[F[T[F]]](_ ∘ (_.embed))

  def colambek[T[_[_]]: Corecursive: Recursive, F[_]: Functor](ft: F[T[F]]):
      T[F] =
    ft.ana(_ ∘ (_.project))

  /** @group algebras */
  type GAlgebraM[W[_], M[_], F[_], A] =                    F[W[A]] => M[A]
  /** @group algebras */
  type GAlgebra[W[_], F[_], A] = GAlgebraM[W, Id, F, A] // F[W[A]] => A
  /** @group algebras */
  type AlgebraM[M[_], F[_], A] = GAlgebraM[Id, M, F, A] // F[A]    => M[A]
  /** @group algebras */
  type Algebra[F[_], A]        = GAlgebra[Id, F, A]     // F[A]    => A
  /** @group algebras */
  type ElgotAlgebraM[W[_], M[_], F[_], A] = W[F[A]] => M[A]
  /** @group algebras */
  type ElgotAlgebra[W[_], F[_], A] = ElgotAlgebraM[W, Id, F, A] // W[F[A]] => A

  /** @group algebras */
  type GCoalgebraM[N[_], M[_], F[_], A] =                      A => M[F[N[A]]]
  /** @group algebras */
  type GCoalgebra[N[_], F[_], A] = GCoalgebraM[N, Id, F, A] // A => N[F[A]]
  /** @group algebras */
  type CoalgebraM[M[_], F[_], A] = GCoalgebraM[Id, M, F, A] // A => F[M[A]]
  /** @group algebras */
  type Coalgebra[F[_], A]        = GCoalgebra[Id, F, A]     // A => F[A]
  /** @group algebras */
  type ElgotCoalgebraM[E[_], M[_], F[_], A] = A => M[E[F[A]]]
  /** @group algebras */
  type ElgotCoalgebra[E[_], F[_], A] = ElgotCoalgebraM[E, Id, F, A] // A => E[F[A]]

  /** @group algebras */
  type GAlgebraicTransformM[T[_[_]], W[_], M[_], F[_], G[_]] = F[W[T[G]]] => M[G[T[G]]]
  /** @group algebras */
  type AlgebraicTransformM[T[_[_]], M[_], F[_], G[_]] = GAlgebraicTransformM[T, Id, M, F, G]
  /** @group algebras */
  type GAlgebraicTransform[T[_[_]], W[_], F[_], G[_]] = GAlgebraicTransformM[T, W, Id, F, G]
  /** @group algebras */
  type AlgebraicTransform[T[_[_]], F[_], G[_]] = GAlgebraicTransformM[T, Id, Id, F, G]

  /** @group algebras */
  type GCoalgebraicTransformM[T[_[_]], M[_], N[_], F[_], G[_]] = F[T[F]] => N[G[M[T[F]]]]
  /** @group algebras */
  type CoalgebraicTransformM[T[_[_]], N[_], F[_], G[_]] = GCoalgebraicTransformM[T, Id, N, F, G]
  /** @group algebras */
  type GCoalgebraicTransform[T[_[_]], M[_], F[_], G[_]] = GCoalgebraicTransformM[T, M, Id, F, G]
  /** @group algebras */
  type CoalgebraicTransform[T[_[_]], F[_], G[_]] = GCoalgebraicTransformM[T, Id, Id, F, G]

  /**
    *
    * @group algtrans
    */
  def transformToAlgebra[T[_[_]]: Corecursive, W[_], M[_]: Functor, F[_], G[_]: Functor](
    self: GAlgebraicTransformM[T, W, M, F, G]):
      GAlgebraM[W, M, F, T[G]] =
    self(_) ∘ (_.embed)

  /** An algebra and its dual form an isomorphism.
    */
  type GAlgebraIso[W[_], M[_], F[_], A] = PIso[F[W[A]], F[M[A]], A, A]
  object GAlgebraIso {
    def apply[W[_], M[_], F[_], A](φ: F[W[A]] => A)(ψ: A => F[M[A]]):
        GAlgebraIso[W, M, F, A] =
      PIso(φ)(ψ)
  }

  type AlgebraIso[F[_], A] = GAlgebraIso[Id, Id, F, A]
  object AlgebraIso {
    def apply[F[_], A](φ: F[A] => A)(ψ: A => F[A]):
        AlgebraIso[F, A] =
      Iso(φ)(ψ)
  }

  type AlgebraPrism[F[_], A] = Prism[F[A], A]
  object AlgebraPrism {
    def apply[F[_], A](φ: F[A] => Option[A])(ψ: A => F[A]):
        AlgebraPrism[F, A] =
      Prism(φ)(ψ)
  }

  type CoalgebraPrism[F[_], A] = Prism[A, F[A]]
  object CoalgebraPrism {
    def apply[F[_], A](ψ: A => Option[F[A]])(φ: F[A] => A):
        CoalgebraPrism[F, A] =
      Prism(ψ)(φ)
  }

  def recCorecIso[T[_[_]]: Recursive: Corecursive, F[_]: Functor] =
    AlgebraIso[F, T[F]](_.embed)(_.project)

  def lambekIso[T[_[_]]: Corecursive: Recursive, F[_]: Functor] =
    AlgebraIso[F, T[F]](colambek)(lambek)

  /** There is a fold/unfold isomorphism for any AlgebraIso.
    */
  def foldIso[T[_[_]]: Corecursive: Recursive, F[_]: Functor, A](alg: AlgebraIso[F, A]) =
    Iso[T[F], A](_.cata(alg.get))(_.ana(alg.reverseGet))

  /** There is a fold prism for any AlgebraPrism.
    */
  def foldPrism[T[_[_]]: Corecursive: Recursive, F[_]: Traverse, A](alg: AlgebraPrism[F, A]) =
    Prism[T[F], A](Recursive[T].cataM(_)(alg.getOption))(_.ana(alg.reverseGet))

  /** There is an unfold prism for any CoalgebraPrism.
    */
  def unfoldPrism[T[_[_]]: Corecursive: Recursive, F[_]: Traverse, A](coalg: CoalgebraPrism[F, A]) =
    Prism[A, T[F]](_.anaM(coalg.getOption))(_.cata(coalg.reverseGet))

  /** A NaturalTransformation that sequences two types
    *
    * @group dist
    */
  type DistributiveLaw[F[_], G[_]] = (F ∘ G)#λ ~> (G ∘ F)#λ

  /** This folds a Free that you may think of as “already partially-folded”.
    * It’s also the fold of a decomposed `elgot`.
    */
  def interpretCata[F[_]: Functor, A](t: Free[F, A])(φ: F[A] => A): A =
    t.fold(x => x, f => φ(f ∘ (interpretCata(_)(φ))))

  /** The fold of a decomposed `coelgot`, since the Cofree already has the
    * attribute for each node.
    */
  def cofCata[F[_]: Functor, A, B](
    t: Cofree[F, A])(
    φ: ElgotAlgebra[(A, ?), F, B]):
      B =
    φ((t.head, t.tail ∘ (cofCata(_)(φ))))

  def cofCataM[F[_]: Traverse, M[_]: Monad, A, B](
    t: Cofree[F, A])(
    φ: ElgotAlgebraM[(A, ?), M, F, B]):
      M[B] =
    t.tail.traverse(cofCataM(_)(φ)) >>= (fb => φ((t.head, fb)))

  /** A version of [[matryoshka.Corecursive.ana]] that annotates each node with
    * the `A` it was expanded from. This is also the unfold from a decomposed
    * `coelgot`.
    */
  def attributeAna[F[_]: Functor, A](a: A)(ψ: A => F[A]): Cofree[F, A] =
    Cofree(a, ψ(a) ∘ (attributeAna(_)(ψ)))

  /** A Kleisli [[matryoshka.attributeAna]]. */
  def attributeAnaM[M[_]: Monad, F[_]: Traverse, A](a: A)(ψ: A => M[F[A]]): M[Cofree[F, A]] =
    ψ(a).flatMap(_.traverse(attributeAnaM(_)(ψ))) ∘ (Cofree(a, _))

  /** The unfold from a decomposed `elgot`. */
  def freeAna[F[_]: Functor, A, B](a: A)(ψ: A => B \/ F[A]): Free[F, B] =
    ψ(a).fold(
      _.point[Free[F, ?]],
      fb => Free.liftF(fb ∘ (freeAna(_)(ψ))).join)

  def cofParaM[M[_]] = new CofParaMPartiallyApplied[M]
  final class CofParaMPartiallyApplied[M[_]] { self =>
    def apply[T[_[_]]: Corecursive, S[_]: Traverse, A, B](t: Cofree[S, A])(f: (A, S[(T[S], B)]) => M[B])(implicit M: Monad[M]): M[B] =
      t.tail.traverse(cs => self(cs)(f) ∘ ((Recursive[Cofree[?[_], A]].convertTo[S, T](cs), _))) >>= (f(t.head, _))
  }

  /** Composition of an anamorphism and a catamorphism that avoids building the
    * intermediate recursive data structure.
    *
    * @group refolds
    */
  def hylo[F[_]: Functor, A, B](a: A)(f: F[B] => B, g: A => F[A]): B =
    f(g(a) ∘ (hylo(_)(f, g)))

  /** A Kleisli hylomorphism.
    *
    * @group refolds
    */
  def hyloM[M[_]: Monad, F[_]: Traverse, A, B](a: A)(f: F[B] => M[B], g: A => M[F[A]]):
      M[B] =
    g(a) >>= (_.traverse(hyloM(_)(f, g)) >>= f)

  /** `histo ⋘ ana`
    *
    * @group refolds
    */
  def dyna[F[_]: Functor, A, B](a: A)(φ: F[Cofree[F, B]] => B, ψ: A => F[A]): B =
    ghylo[Cofree[F, ?], Id, F, A, B](a)(distHisto, distAna, φ, ψ)

  /** `cata ⋘ futu`
    *
    * @group refolds
    */
  def codyna[F[_]: Functor, A, B](a: A)(φ: F[B] => B, ψ: A => F[Free[F, A]]): B =
    ghylo[Id, Free[F, ?], F, A, B](a)(distCata, distFutu, φ, ψ)

  /** `cataM ⋘ futuM`
    *
    * @group refolds
    */
  def codynaM[M[_]: Monad, F[_]: Traverse, A, B](a: A)(φ: F[B] => M[B], ψ: A => M[F[Free[F, A]]]): M[B] =
    ghyloM[Id, Free[F, ?], M, F, A, B](a)(distCata, distFutu, φ, ψ)

  /** A generalized version of a hylomorphism that composes any coalgebra and
    * algebra. (`gcata ⋘ gana`)
    *
    * @group refolds
    */
  def ghylo[W[_]: Comonad, M[_], F[_]: Functor, A, B](
    a: A)(
    w: DistributiveLaw[F, W],
    m: DistributiveLaw[M, F],
    f: F[W[B]] => B,
    g: A => F[M[A]])(
    implicit M: Monad[M]):
      B = {
    def h(x: M[A]): W[B] = w(m(M.lift(g)(x)) ∘ (y => h(y.join).cojoin)) ∘ f
    h(a.point[M]).copoint
  }

  /** A Kleisli `ghylo` (`gcataM ⋘ ganaM`)
    *
    * @group refolds
    */
  def ghyloM[W[_]: Comonad: Traverse, M[_]: Traverse, N[_]: Monad, F[_]: Traverse, A, B](
    a: A)(
    w: DistributiveLaw[F, W],
    m: DistributiveLaw[M, F],
    f: F[W[B]] => N[B],
    g: A => N[F[M[A]]])(
    implicit M: Monad[M]):
      N[B] = {
    def h(x: M[A]): N[W[B]] =
      (M.lift(g)(x).sequence >>=
        (m(_: M[F[M[A]]]).traverse(y => h(y.join) ∘ (_.cojoin)))) ∘
        (w(_)) >>=
        (_.traverse(f))
    h(a.point[M]) ∘ (_.copoint)
  }

  /** Similar to a hylomorphism, this composes a futumorphism and a
    * histomorphism.
    *
    * @group refolds
    */
  def chrono[F[_]: Functor, A, B](
    a: A)(
    g: F[Cofree[F, B]] => B, f: A => F[Free[F, A]]):
      B =
    ghylo[Cofree[F, ?], Free[F, ?], F, A, B](a)(distHisto, distFutu, g, f)

  /** `cata ⋘ elgotGApo`
    *
    * @group refolds
    */
  def elgot[F[_]: Functor, A, B](a: A)(φ: F[B] => B, ψ: A => B \/ F[A]): B = {
    def h: A => B =
      (((x: B) => x) ||| ((x: F[A]) => φ(x ∘ h))) ⋘ ψ
    h(a)
  }

  /** `cataM ⋘ elgotGApoM`
    *
    * @group refolds
    */
  def elgotM[M[_]: Monad, F[_]: Traverse, A, B](a: A)(φ: F[B] => M[B], ψ: A => M[B \/ F[A]]):
      M[B] = {
    def h(a: A): M[B] = ψ(a) >>= (_.traverse(_.traverse(h) >>= φ).map(_.merge))
    h(a)
  }

  /** `elgotZygo ⋘ ana`
    *
    * @group refolds
    */
  def coelgot[F[_]: Functor, A, B](a: A)(φ: ((A, F[B])) => B, ψ: A => F[A]):
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
    def apply[F[_]: Traverse, A, B](a: A)(φ: ((A, F[B])) => M[B], ψ: A => M[F[A]])(implicit M: Monad[M]):
        M[B] = {
      def h(a: A): M[B] = ψ(a) >>= (_.traverse(h)) >>= (x => φ((a, x)))
      h(a)
    }
  }

  /**
    *
    * @group dist
    */
  def distPara[T[_[_]]: Corecursive, F[_]: Functor]:
      DistributiveLaw[F, (T[F], ?)] =
    distZygo(_.embed)

  /**
    *
    * @group dist
    */
  def distParaT[T[_[_]], F[_]: Functor, W[_]: Comonad](
    t: DistributiveLaw[F, W])(
    implicit T: Corecursive[T]):
      DistributiveLaw[F, EnvT[T[F], W, ?]] =
    distZygoT(_.embed, t)

  /**
    *
    * @group dist
    */
  def distCata[F[_]]: DistributiveLaw[F, Id] = NaturalTransformation.refl

  /**
    *
    * @group dist
    */
  def distZygo[F[_]: Functor, B](g: F[B] => B) =
    new DistributiveLaw[F, (B, ?)] {
      def apply[α](m: F[(B, α)]) = (g(m ∘ (_._1)), m ∘ (_._2))
    }

  /**
    *
    * @group dist
    */
  def distZygoT[F[_], W[_]: Comonad, B](
    g: F[B] => B, k: DistributiveLaw[F, W])(
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
  def distApo[T[_[_]]: Recursive, F[_]: Functor]:
      DistributiveLaw[T[F] \/ ?, F] =
    distGApo(_.project)

  /**
    *
    * @group dist
    */
  def distGApo[F[_]: Functor, B](g: B => F[B]) =
    new DistributiveLaw[B \/ ?, F] {
      def apply[α](m: B \/ F[α]) = m.fold(g(_) ∘ (_.left), _ ∘ (_.right))
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

  def holes[F[_]: Traverse, A](fa: F[A]): F[(A, A => F[A])] =
    (fa.mapAccumL(0) {
      case (i, x) =>
        val h: A => F[A] = { y =>
          val g: (Int, A) => (Int, A) = (j, z) => (j + 1, if (i == j) y else z)

          fa.mapAccumL(0)(g)._2
        }

        (i + 1, (x, h))
    })._2

  def holesList[F[_]: Traverse, A](fa: F[A]): List[(A, A => F[A])] =
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
  def attributeAlgebraM[F[_]: Functor, M[_]: Functor, A](f: F[A] => M[A]):
      F[Cofree[F, A]] => M[Cofree[F, A]] =
    fa => f(fa ∘ (_.head)) ∘ (Cofree(_, fa))

  /**
    *
    * @group algtrans
    */
  def attributeAlgebra[F[_]: Functor, A](f: F[A] => A) =
    attributeAlgebraM[F, Id, A](f)

  /**
    *
    * @group algtrans
    */
  def attributeCoalgebra[F[_], B](ψ: Coalgebra[F, B]):
      Coalgebra[EnvT[B, F, ?], B] =
    b => EnvT[B, F, B]((b, ψ(b)))

  /**
    *
    * @group algtrans
    */
  def attrK[F[_]: Functor, A](k: A) = attributeAlgebra[F, A](Function.const(k))

  /**
    *
    * @group algtrans
    */
  def attrSelf[T[_[_]]: Corecursive, F[_]: Functor] =
    attributeAlgebra[F, T[F]](_.embed)

  /** NB: Since Cofree carries the functor, the resulting algebra is a cata, not
    *     a para.
    *
    * @group algtrans
    */
  def attributePara[T[_[_]]: Corecursive, F[_]: Functor, A](f: F[(T[F], A)] => A):
      F[Cofree[F, A]] => Cofree[F, A] =
    fa => Cofree(f(fa ∘ (x => (Recursive[Cofree[?[_], A]].convertTo[F, T](x), x.head))), fa)

  /** A function to be called like `attributeElgotM[M](myElgotAlgebraM)`.
    *
    * @group algtrans
    */
  object attributeElgotM {
    def apply[W[_], M[_]] = new Aux[W, M]

    final class Aux[W[_], M[_]] {
      def apply[F[_]: Functor, A](f: ElgotAlgebraM[W, M, F, A])(implicit W: Comonad[W], M: Functor[M]):
          W[F[Cofree[F, A]]] => M[Cofree[F, A]] =
        node => f(node ∘ (_ ∘ (_.head))) ∘ (Cofree(_, node.copoint))
    }
  }

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
  def count[T[_[_]]: Recursive, F[_]: Functor: Foldable](form: T[F]):
      F[(T[F], Int)] => Int =
    e => e.foldRight(if (e ∘ (_._1) == form.project) 1 else 0)(_._2 + _)

  /** The number of nodes in this structure.
    *
    * @group algebras
    */
  def size[F[_]: Foldable]: F[Int] => Int = _.foldRight(1)(_ + _)

  /** The largest number of hops from a node to a leaf.
    *
    * @group algebras
    */
  def height[F[_]: Foldable]: F[Int] => Int = _.foldRight(-1)(_ max _) + 1

  /** Combines a tuple of zippable functors.
    *
    * @group algebras
    */
  def zipTuple[T[_[_]]: Recursive, F[_]: Functor: Zip]:
      Coalgebra[F, (T[F], T[F])] =
    p => Zip[F].zip[T[F], T[F]](p._1.project, p._2.project)

  /** Aligns “These” into a single structure, short-circuting when we hit a
    * “This” or “That”.
    *
    * @group algebras
    */
  def alignThese[T[_[_]]: Recursive, F[_]: Align]:
      ElgotCoalgebra[T[F] \/ ?, F, T[F] \&/ T[F]] =
    _.fold(_.left, _.left, (a, b) => a.project.align(b.project).right)

  /** Merges a tuple of functors, if possible.
    *
    * @group algebras
    */
  def mergeTuple[T[_[_]]: Recursive, F[_]: Functor: Merge]:
      CoalgebraM[Option, F, (T[F], T[F])] =
    p => Merge[F].merge[T[F], T[F]](p._1.project, p._2.project)

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

  /** Replaces all instances of `original` in the structure with `replacement`.
    *
    * @group algebras
    */
  def substitute[T[_[_]], F[_]](original: T[F], replacement: T[F])(implicit T: Equal[T[F]]):
      T[F] => T[F] \/ T[F] =
   tf => if (tf ≟ original) replacement.left else tf.right


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

  implicit final class CorecursiveOps[T[_[_]], F[_]](
    self: F[T[F]])(
    implicit T: Corecursive[T]) {

    def embed(implicit F: Functor[F]): T[F] = T.embed(self)
  }

  implicit def toAlgebraOps[F[_], A](a: Algebra[F, A]): AlgebraOps[F, A] =
    new AlgebraOps[F, A](a)

  implicit def toCoalgebraOps[F[_], A](a: Coalgebra[F, A]): CoalgebraOps[F, A] =
    new CoalgebraOps[F, A](a)

  implicit def toCofreeOps[F[_], A](a: Cofree[F, A]): CofreeOps[F, A] =
    new CofreeOps[F, A](a)

  implicit def toFreeOps[F[_], A](a: Free[F, A]): FreeOps[F, A] =
    new FreeOps[F, A](a)

  implicit def equalTEqual[T[_[_]], F[_]: Functor](implicit T: EqualT[T], F: Delay[Equal, F]):
      Equal[T[F]] =
    T.equalT[F](F)

  implicit def showTShow[T[_[_]], F[_]: Functor](implicit T: ShowT[T], F: Delay[Show, F]):
      Show[T[F]] =
    T.showT[F](F)
}
