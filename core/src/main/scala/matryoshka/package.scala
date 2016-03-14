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

import matryoshka.Recursive.ops._

import scala.{Function, Int, None, Option}
import scala.collection.immutable.{List, ::}

import scalaz._, Scalaz._
import simulacrum.typeclass

/** Generalized folds, unfolds, and refolds. */
package object matryoshka extends CofreeInstances with FreeInstances {

  def lambek[T[_[_]]: Corecursive: Recursive, F[_]: Functor](tf: T[F]):
      F[T[F]] =
    tf.cata[F[T[F]]](_ ∘ (_.embed))

  def colambek[T[_[_]]: Corecursive: Recursive, F[_]: Functor](ft: F[T[F]]):
      T[F] =
    ft.ana(_ ∘ (_.project))

  type GAlgebraM[W[_], M[_], F[_], A] =                    F[W[A]] => M[A]
  type GAlgebra[W[_], F[_], A] = GAlgebraM[W, Id, F, A] // F[W[A]] => A
  type AlgebraM[M[_], F[_], A] = GAlgebraM[Id, M, F, A] // F[A]    => M[A]
  type Algebra[F[_], A]        = GAlgebra[Id, F, A]     // F[A]    => A
  type ElgotAlgebraM[W[_], M[_], F[_], A] = W[F[A]] => M[A]
  type ElgotAlgebra[W[_], F[_], A] = ElgotAlgebraM[W, Id, F, A] // W[F[A]] => A

  type GCoalgebraM[N[_], M[_], F[_], A] =                      A => M[F[N[A]]]
  type GCoalgebra[N[_], F[_], A] = GCoalgebraM[N, Id, F, A] // A => N[F[A]]
  type CoalgebraM[M[_], F[_], A] = GCoalgebraM[Id, M, F, A] // A => F[M[A]]
  type Coalgebra[F[_], A]        = GCoalgebra[Id, F, A]     // A => F[A]


  /** A NaturalTransformation that sequences two types */
  type DistributiveLaw[F[_], G[_]] = λ[α => F[G[α]]] ~> λ[α => G[F[α]]]

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
      t.tail.traverseU(cs => self(cs)(f) ∘ ((Recursive[Cofree[?[_], A]].convertTo[S, T](cs), _))) >>= (f(t.head, _))
  }

  /** Composition of an anamorphism and a catamorphism that avoids building the
    * intermediate recursive data structure.
    */
  def hylo[F[_]: Functor, A, B](a: A)(f: F[B] => B, g: A => F[A]): B =
    // f(g(a) ∘ (hylo(_)(f, g)))
    ghylo[Id, Id, F, A, B](a)(distCata, distAna, f, g)

  /** A Kleisli hylomorphism. */
  def hyloM[M[_]: Monad, F[_]: Traverse, A, B](a: A)(f: F[B] => M[B], g: A => M[F[A]]):
      M[B] =
    g(a) >>= (_.traverse(hyloM(_)(f, g)) >>= (f))

  /** A generalized version of a hylomorphism that composes any coalgebra and
    * algebra.
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

  /** Similar to a hylomorphism, this composes a futumorphism and a
    * histomorphism.
    */
  def chrono[F[_]: Functor, A, B](
    a: A)(
    g: F[Cofree[F, B]] => B, f: A => F[Free[F, A]]):
      B =
    ghylo[Cofree[F, ?], Free[F, ?], F, A, B](a)(distHisto, distFutu, g, f)

  def elgot[F[_]: Functor, A, B](a: A)(φ: F[B] => B, ψ: A => B \/ F[A]): B = {
    def h: A => B =
      (((x: B) => x) ||| ((x: F[A]) => φ(x ∘ h))) ⋘ ψ
    h(a)
  }

  def coelgot[F[_]: Functor, A, B](a: A)(φ: ((A, F[B])) => B, ψ: A => F[A]):
      B = {
    def h: A => B =
      φ ⋘ (((x: A) => x) &&& (((x: F[A]) => x ∘ h) ⋘ ψ))
    h(a)
  }

  def coelgotM[M[_]] = new CoelgotMPartiallyApplied[M]
  final class CoelgotMPartiallyApplied[M[_]] {
    def apply[F[_]: Traverse, A, B](a: A)(φ: ((A, F[B])) => M[B], ψ: A => M[F[A]])(implicit M: Monad[M]):
        M[B] = {
      def h(a: A): M[B] = ψ(a) >>= (_.traverse(h)) >>= (x => φ((a, x)))
      h(a)
    }
  }

  def distPara[T[_[_]], F[_]: Functor](implicit T: Corecursive[T]):
      DistributiveLaw[F, (T[F], ?)] =
    distZygo(_.embed)

  def distParaT[T[_[_]], F[_]: Functor, W[_]: Comonad](
    t: DistributiveLaw[F, W])(
    implicit T: Corecursive[T]):
      DistributiveLaw[F, EnvT[T[F], W, ?]] =
    distZygoT(_.embed, t)

  def distCata[F[_]]: DistributiveLaw[F, Id] = NaturalTransformation.refl

  def distZygo[F[_]: Functor, B](g: F[B] => B) =
    new DistributiveLaw[F, (B, ?)] {
      def apply[α](m: F[(B, α)]) = (g(m ∘ (_._1)), m ∘ (_._2))
    }

  def distZygoT[F[_], W[_]: Comonad, B](
    g: F[B] => B, k: DistributiveLaw[F, W])(
    implicit F: Functor[F]) =
    new DistributiveLaw[F, EnvT[B, W, ?]] {
      def apply[α](fe: F[EnvT[B, W, α]]) =
        EnvT((
          g(F.lift[EnvT[B, W, α], B](_.ask)(fe)),
          k(F.lift[EnvT[B, W, α], W[α]](_.lower)(fe))))
    }

  def distHisto[F[_]: Functor] =
    new DistributiveLaw[F, Cofree[F, ?]] {
      def apply[α](m: F[Cofree[F, α]]) =
        distGHisto[F, F](NaturalTransformation.refl[λ[α => F[F[α]]]]).apply(m)
    }

  def distGHisto[F[_],  H[_]](
    k: DistributiveLaw[F, H])(
    implicit F: Functor[F], H: Functor[H]) =
    new DistributiveLaw[F, Cofree[H, ?]] {
      def apply[α](m: F[Cofree[H, α]]) =
        Cofree.unfold(m)(as => (
          F.lift[Cofree[H, α], α](_.copure)(as),
          k(F.lift[Cofree[H, α], H[Cofree[H, α]]](_.tail)(as))))
    }

  def distAna[F[_]]: DistributiveLaw[Id, F] = NaturalTransformation.refl

  def distFutu[F[_]: Functor] =
    new DistributiveLaw[Free[F, ?], F] {
      def apply[α](m: Free[F, F[α]]) =
        distGFutu[F, F](NaturalTransformation.refl[λ[α => F[F[α]]]]).apply(m)
    }

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
    * the results for each node. */
  def attributeM[F[_]: Functor, M[_]: Functor, A](f: F[A] => M[A]):
      F[Cofree[F, A]] => M[Cofree[F, A]] =
    fa => f(fa ∘ (_.head)) ∘ (Cofree(_, fa))

  def attribute[F[_]: Functor, A](f: F[A] => A) = attributeM[F, Id, A](f)

  def attrK[F[_]: Functor, A](k: A) = attribute[F, A](Function.const(k))

  def attrSelf[T[_[_]]: Corecursive, F[_]: Functor] =
    attribute[F, T[F]](_.embed)

  /** NB: Since Cofree carries the functor, the resulting algebra is a cata, not
    *     a para. */
  def attributePara[T[_[_]]: Corecursive, F[_]: Functor, A](f: F[(T[F], A)] => A):
      F[Cofree[F, A]] => Cofree[F, A] =
    fa => Cofree(f(fa ∘ (x => (Recursive[Cofree[?[_], A]].convertTo[F, T](x), x.head))), fa)

  def attributeCoelgotM[M[_]] = new AttributeCoelgotMPartiallyApplied[M]
  final class AttributeCoelgotMPartiallyApplied[M[_]] {
    def apply[F[_]: Functor, A, B](f: (A, F[B]) => M[B])(implicit M: Functor[M]):
        (A, F[Cofree[F, B]]) => M[Cofree[F, B]] =
      (a, node) => f(a, node ∘ (_.head)) ∘ (Cofree(_, node))
  }

  implicit def GAlgebraZip[W[_]: Functor, F[_]: Functor]:
      Zip[GAlgebra[W, F, ?]] =
    new Zip[GAlgebra[W, F, ?]] {
      def zip[A, B](a: ⇒ GAlgebra[W, F, A], b: ⇒ GAlgebra[W, F, B]) =
        node => (a(node ∘ (_ ∘ (_._1))), b(node ∘ (_ ∘ (_._2))))
    }
  implicit def AlgebraZip[F[_]: Functor] = GAlgebraZip[Id, F]

  implicit def ElgotAlgebraMZip[W[_]: Functor, M[_]: Applicative, F[_]: Functor]:
      Zip[ElgotAlgebraM[W, M, F, ?]] =
    new Zip[ElgotAlgebraM[W, M, F, ?]] {
      def zip[A, B](a: ⇒ ElgotAlgebraM[W, M, F, A], b: ⇒ ElgotAlgebraM[W, M, F, B]) =
        w => Bitraverse[(?, ?)].bisequence((a(w ∘ (_ ∘ (_._1))), b(w ∘ (_ ∘ (_._2)))))
    }
  implicit def ElgotAlgebraZip[W[_]: Functor, F[_]: Functor] =
    ElgotAlgebraMZip[W, Id, F]
    // (ann, node) => node.unfzip.bimap(f(ann, _), g(ann, _))

  /** Repeatedly applies the function to the result as long as it returns Some.
    * Finally returns the last non-None value (which may be the initial input).
    */
  def repeatedly[A](f: A => Option[A]): A => A =
    expr => f(expr).fold(expr)(repeatedly(f))

  /** Converts a failable fold into a non-failable, by simply returning the
    * argument upon failure.
    */
  def orOriginal[A](f: A => Option[A]): A => A = expr => f(expr).getOrElse(expr)

  /** Converts a failable fold into a non-failable, by returning the default
   * upon failure.
    */
  def orDefault[A, B](default: B)(f: A => Option[B]): A => B =
    expr => f(expr).getOrElse(default)

  /** Count the instinces of `form` in the structure.
    */
  def count[T[_[_]]: Recursive, F[_]: Functor: Foldable](form: T[F]): F[(T[F], Int)] => Int =
    e => e.foldRight(if (e ∘ (_._1) == form.project) 1 else 0)(_._2 + _)

  implicit def ToIdOps[A](a: A): IdOps[A] = new IdOps[A](a)

  implicit def ToCorecursiveOps[T[_[_]]: Corecursive, F[_]](f: F[T[F]]):
      CorecursiveOps[T, F] =
    new CorecursiveOps[T, F](f)

  implicit def ToAlgebraOps[F[_], A](a: Algebra[F, A]): AlgebraOps[F, A] =
    new AlgebraOps[F, A](a)

  implicit def ToCoalgebraOps[F[_], A](a: Coalgebra[F, A]): CoalgebraOps[F, A] =
    new CoalgebraOps[F, A](a)

  implicit def ToCofreeOps[F[_], A](a: Cofree[F, A]): CofreeOps[F, A] =
    new CofreeOps[F, A](a)

  implicit def ToFreeOps[F[_], A](a: Free[F, A]): FreeOps[F, A] =
    new FreeOps[F, A](a)
}
