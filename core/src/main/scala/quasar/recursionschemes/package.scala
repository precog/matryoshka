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

package quasar

import quasar.Predef._
import quasar.fp._
import quasar.recursionschemes.Recursive.ops._
import quasar.recursionschemes.cofree._

import scala.Function0

import scalaz.{Tree => ZTree, Node => _, _}, Id.Id, Scalaz._
import simulacrum.typeclass

/** Generalized folds, unfolds, and refolds. */
package object recursionschemes {

  def cofCataM[S[_]: Traverse, M[_]: Monad, A, B](t: Cofree[S, A])(f: (A, S[B]) => M[B]): M[B] =
    t.tail.traverse(cofCataM(_)(f)).flatMap(f(t.head, _))

  def cofParaM[M[_]] = new CofParaMPartiallyApplied[M]
  final class CofParaMPartiallyApplied[M[_]] { self =>
    def apply[T[_[_]]: Corecursive, S[_]: Traverse, A, B](t: Cofree[S, A])(f: (A, S[(T[S], B)]) => M[B])(implicit M: Monad[M]): M[B] =
      t.tail.traverseU(cs => self(cs)(f).map((Recursive[Cofree[?[_], A]].convertTo[S, T](cs), _))).flatMap(f(t.head, _))
  }

  // refolds

  def hylo[F[_]: Functor, A, B](a: A)(f: F[B] => B, g: A => F[A]): B =
    f(g(a).map(hylo(_)(f, g)))

  def hyloM[M[_]: Monad, F[_]: Traverse, A, B](a: A)(f: F[B] => M[B], g: A => M[F[A]]):
      M[B] =
    g(a).flatMap(_.traverse(hyloM(_)(f, g)).flatMap(f))

  def ghylo[F[_]: Functor, W[_]: Comonad, M[_], A, B](
    a: A)(
    w: λ[α => F[W[α]]] ~> λ[α => W[F[α]]],
    m: λ[α => M[F[α]]] ~> λ[α => F[M[α]]],
    f: F[W[B]] => B,
    g: A => F[M[A]])(
    implicit M: Monad[M]):
      B = {
    def h(x: M[A]): W[B] = w(m(M.lift(g)(x)).map(y => h(y.join).cojoin)).map(f)
    h(a.point[M]).copoint
  }

  def chrono[F[_]: Functor, A, B](
    a: A)(
    g: F[Cofree[F, B]] => B,
    f: A => F[Free[F, A]]):
      B =
    ghylo[F, Cofree[F, ?], Free[F, ?], A, B](a)(distHisto, distFutu, g, f)

  def elgot[F[_]: Functor, A, B](a: A)(φ: F[B] => B, ψ: A => B \/ F[A]): B = {
    def h: A => B = (ι ||| ((x: F[A]) => φ(x.map(h)))) ⋘ ψ
    h(a)
  }

  def coelgot[F[_]: Functor, A, B](a: A)(φ: (A, F[B]) => B, ψ: A => F[A]):
      B = {
    def h: A => B = φ.tupled ⋘ (ι &&& (((x: F[A]) => x.map(h)) ⋘ ψ))
    h(a)
  }

  def coelgotM[M[_]] = new CoelgotMPartiallyApplied[M]
  final class CoelgotMPartiallyApplied[M[_]] {
    def apply[F[_]: Traverse, A, B](a: A)(φ: (A, F[B]) => M[B], ψ: A => M[F[A]])(implicit M: Monad[M]):
        M[B] = {
      def h(a: A): M[B] = ψ(a).flatMap(_.traverse(h)).flatMap(φ(a, _))
      h(a)
    }
  }

  // distributive algebras

  def distPara[T[_[_]], F[_]: Functor](implicit T: Corecursive[T]):
      λ[α => F[(T[F], α)]] ~> λ[α => (T[F], F[α])] =
    distZygo(T.embed)

  def distParaT[T[_[_]], F[_]: Functor, W[_]: Comonad](
    t: λ[α => F[W[α]]] ~> λ[α => W[F[α]]])(
    implicit T: Corecursive[T]):
      (λ[α => F[EnvT[T[F], W, α]]] ~> λ[α => EnvT[T[F], W, F[α]]]) =
    distZygoT(T.embed, t)

  def distCata[F[_]]: λ[α => F[Id[α]]] ~> λ[α => Id[F[α]]] =
    NaturalTransformation.refl

  def distZygo[F[_]: Functor, B](g: F[B] => B) =
    new (λ[α => F[(B, α)]] ~> λ[α => (B,  F[α])]) {
      def apply[α](m: F[(B, α)]) = (g(m.map(_._1)), m.map(_._2))
    }

  def distZygoT[F[_], W[_]: Comonad, B](
    g: F[B] => B, k: λ[α => F[W[α]]] ~> λ[α => W[F[α]]])(
    implicit F: Functor[F]) =
    new (λ[α => F[EnvT[B, W, α]]] ~> λ[α => EnvT[B, W, F[α]]]) {
      def apply[α](fe: F[EnvT[B, W, α]]) =
        EnvT((
          g(F.lift[EnvT[B, W, α], B](_.ask)(fe)),
          k(F.lift[EnvT[B, W, α], W[α]](_.lower)(fe))))
    }

  def distHisto[F[_]: Functor] =
    new (λ[α => F[Cofree[F, α]]] ~> λ[α => Cofree[F, F[α]]]) {
      def apply[α](m: F[Cofree[F, α]]) =
        distGHisto[F, F](NaturalTransformation.refl[λ[α => F[F[α]]]]).apply(m)
    }

  def distGHisto[F[_],  H[_]](
    k: λ[α => F[H[α]]] ~> λ[α => H[F[α]]])(
    implicit F: Functor[F], H: Functor[H]) =
    new (λ[α => F[Cofree[H, α]]] ~> λ[α => Cofree[H, F[α]]]) {
      def apply[α](m: F[Cofree[H, α]]) =
        Cofree.unfold(m)(as => (
          F.lift[Cofree[H, α], α](_.copure)(as),
          k(F.lift[Cofree[H, α], H[Cofree[H, α]]](_.tail)(as))))
    }

  def distAna[F[_]: Functor, A]: λ[α => Id[F[α]]] ~> λ[α => F[Id[α]]] =
    NaturalTransformation.refl

  def distFutu[F[_]: Functor] =
    new (λ[α => Free[F, F[α]]] ~> λ[α => F[Free[F, α]]]) {
      def apply[α](m: Free[F, F[α]]) =
        distGFutu[F, F](NaturalTransformation.refl[λ[α => F[F[α]]]]).apply(m)
    }

  def distGFutu[H[_], F[_]](
    k: λ[α => H[F[α]]] ~> λ[α => F[H[α]]])(
    implicit H: Functor[H], F: Functor[F]):
      (λ[α => Free[H, F[α]]] ~> λ[α => F[Free[H, α]]]) =
    new (λ[α => Free[H, F[α]]] ~> λ[α => F[Free[H, α]]]) {
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

  def sizeF[F[_]: Foldable, A](fa: F[A]): Int = fa.foldLeft(0)((a, _) => a + 1)

  /** Turns any F-algebra, into an identical one that attributes the tree with
    * the results for each node. */
  def attributeM[F[_]: Functor, M[_]: Functor, A](f: F[A] => M[A]):
      F[Cofree[F, A]] => M[Cofree[F, A]] =
    fa => f(fa.map(_.head)).map(Cofree(_, fa))

  def attribute[F[_]: Functor, A](f: F[A] => A) = attributeM[F, Id, A](f)

  def attrK[F[_]: Functor, A](k: A) = attribute[F, A](κ(k))

  def attrSelf[T[_[_]]: Corecursive, F[_]: Functor] =
    attribute[F, T[F]](Corecursive[T].embed)

  /** NB: Since Cofree carries the functor, the resulting algebra is a cata, not
    *     a para. */
  def attributePara[T[_[_]]: Corecursive, F[_]: Functor, A](f: F[(T[F], A)] => A):
      F[Cofree[F, A]] => Cofree[F, A] =
    fa => Cofree(f(fa.map(x => (Recursive[Cofree[?[_], A]].convertTo[F, T](x), x.head))), fa)

  def attributeCoelgotM[M[_]] = new AttributeCoelgotMPartiallyApplied[M]
  final class AttributeCoelgotMPartiallyApplied[M[_]] {
    def apply[F[_]: Functor, A, B](f: (A, F[B]) => M[B])(implicit M: Functor[M]):
        (A, F[Cofree[F, B]]) => M[Cofree[F, B]] =
      (a, node) => f(a, node.map(_.head)).map(Cofree(_, node))
  }

  // These lifts are largely useful when you want to zip a cata (or ana) with
  // some more complicated algebra.

  def generalizeAlgebra[W[_]] = new GeneralizeAlgebraPartiallyApplied[W]
  final class GeneralizeAlgebraPartiallyApplied[W[_]] {
    def apply[F[_]: Functor, A](f: F[A] => A)(implicit W: Comonad[W]):
        F[W[A]] => A =
      node => f(node.map(_.copoint))
  }

  def generalizeCoelgot[A] = new GeneralizeCoelgotPartiallyApplied[A]
  final class GeneralizeCoelgotPartiallyApplied[A] {
    def apply[F[_], B](f: F[B] => B): (A, F[B]) => B = (a, node) => f(node)
  }

  def generalizeCoalgebra[M[_]] = new GeneralizeCoalgebraPartiallyApplied[M]
  final class GeneralizeCoalgebraPartiallyApplied[M[_]] {
    def apply[F[_]: Functor, A](f: A => F[A])(implicit M: Monad[M]):
        A => F[M[A]] =
      f(_).map(_.point[M])
  }

  def zipAlgebras[F[_], W[_]] = new ZipAlgebrasPartiallyApplied[F, W]
  final class ZipAlgebrasPartiallyApplied[F[_], W[_]] {
    def apply[A, B](f: F[W[A]] => A, g: F[W[B]] => B)(implicit F: Functor[F], W: Functor[W]):
        F[W[(A, B)]] => (A, B) =
      node => (f(node.map(_.map(_._1))), g(node.map(_.map(_._2))))
  }

  // NB: There are potentially two versions of this function – one as below, and
  //     one where the annotation parameter should become a tuple, too.
  def zipCoelgot[F[_]: Unzip, A, B, C](f: (A, F[B]) => B, g: (A, F[C]) => C):
      (A, F[(B, C)]) => (B, C) =
    (ann, node) => node.unfzip.bimap(f(ann, _), g(ann, _))

  def zipCoelgotM[M[_]] = new ZipCoelgotMPartiallyApplied[M]
  final class ZipCoelgotMPartiallyApplied[M[_]] {
    def apply[F[_]: Functor, A, B, C](f: (A, F[B]) => M[B], g: (A, F[C]) => M[C])(implicit M: Applicative[M]):
      (A, F[(B, C)]) => M[(B, C)] =
    (ann, node) => Bitraverse[(?, ?)].bisequence((f(ann, node.map(_._1)), g(ann, node.map(_._2))))
  }

  /** Repeatedly applies the function to the result as long as it returns Some.
    * Finally returns the last non-None value (which may be the initial input).
    */
  def repeatedly[A](f: A => Option[A]): A => A =
    expr => f(expr).fold(expr)(repeatedly(f))

  /** Converts a failable fold into a non-failable, by simply returning the
    * argument upon failure.
    */
  def once[A](f: A => Option[A]): A => A = expr => f(expr).getOrElse(expr)

  def count[T[_[_]]: Recursive, F[_]: Functor: Foldable](form: T[F]): F[(T[F], Int)] => Int =
    e => e.foldRight(if (e.map(_._1) == form.project) 1 else 0)(_._2 + _)

  // Inherited: inherit, inherit2, inherit3, inheritM, inheritM_
  def inherit[F[_]: Functor, A, B](tree: Cofree[F, A], b: B)(f: (B, Cofree[F, A]) => B): Cofree[F, B] = {
    val b2 = f(b, tree)
    Cofree[F, B](b2, tree.tail.map(inherit(_, b2)(f)))
  }

  // TODO: Top down folds

  def transform[F[_]: Functor, A](attrfa: Cofree[F, A])(f: A => Option[Cofree[F, A]]): Cofree[F, A] = {
    val a = attrfa.head
    f(a).map(transform(_)(f))
      .getOrElse(Cofree(a, attrfa.tail.map(transform(_)(f))))
  }

  def swapTransform[F[_]: Functor, A, B](attrfa: Cofree[F, A])(f: A => B \/ Cofree[F, B]): Cofree[F, B] =
    f(attrfa.head).fold(Cofree(_, attrfa.tail.map(swapTransform(_)(f))), ι)

  /**
   * Zips two attributed nodes together. This is unsafe in the sense that the
   * user is responsible for ensuring both left and right parameters have the
   * same shape (i.e. represent the same tree).
   */
  def unsafeZip2[F[_]: Traverse, A, B](left: Cofree[F, A], right: Cofree[F, B]):
      Cofree[F, (A, B)] = {
    val lunAnn: F[Cofree[F, A]] = left.tail
    val lunAnnL: List[Cofree[F, A]] = lunAnn.toList

    val runAnnL: List[Cofree[F, B]] = right.tail.toList

    val abs: List[Cofree[F, (A, B)]] = lunAnnL.zip(runAnnL).map { case ((a, b)) => unsafeZip2(a, b) }

    val fabs : F[Cofree[F, (A, B)]] = builder(lunAnn, abs)

    Cofree((left.head, right.head), fabs)
  }

  @typeclass trait Binder[F[_]] {
    type G[A]

    def initial[A]: G[A]

    // Extracts bindings from a node:
    def bindings[A](t: F[Fix[F]], b: G[A])(f: F[Fix[F]] => A): G[A]

    // Possibly binds a free term to its definition:
    def subst[A](t: F[Fix[F]], b: G[A]): Option[A]
  }

  def boundCata[F[_]: Functor, A](t: Fix[F])(f: F[A] => A)(implicit B: Binder[F]): A = {
    def loop(t: F[Fix[F]], b: B.G[A]): A = {
      val newB = B.bindings(t, b)(loop(_, b))
      B.subst(t, newB).getOrElse(f(t.map(x => loop(x.unFix, newB))))
    }

    loop(t.unFix, B.initial)
  }

  def boundPara[F[_]: Functor, A](t: Fix[F])(f: F[(Fix[F], A)] => A)(implicit B: Binder[F]): A = {
    def loop(t: F[Fix[F]], b: B.G[A]): A = {
      val newB = B.bindings(t, b)(loop(_, b))
      B.subst(t, newB).getOrElse(f(t.map(x => (x, loop(x.unFix, newB)))))
    }

    loop(t.unFix, B.initial)
  }

  /**
   Annotate (the original nodes of) a tree, by applying a function to the
   "bound" nodes. The function is also applied to the bindings themselves
   to determine their annotation.
   */
  def boundAttribute[F[_]: Functor, A](t: Fix[F])(f: Fix[F] => A)(implicit B: Binder[F]): Cofree[F, A] = {
    def loop(t: F[Fix[F]], b: B.G[(Fix[F], Cofree[F, A])]): (Fix[F], Cofree[F, A]) = {
      val newB = B.bindings(t, b)(loop(_, b))
      B.subst(t, newB).fold {
        val m: F[(Fix[F], Cofree[F, A])] = t.map(x => loop(x.unFix, newB))
        val t1 = Fix(m.map(_._1))
        (t1, Cofree(f(t1), m.map(_._2)))
      } { case (x, _) => (x, Fix(t).cata(attrK(f(x)))) }
    }
    loop(t.unFix, B.initial)._2
  }
}
