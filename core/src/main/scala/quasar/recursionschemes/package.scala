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

import scala.Function0

import scalaz._, Scalaz._
import simulacrum.typeclass

/** Generalized folds, unfolds, and refolds. */
package object recursionschemes extends CofreeInstances with FreeInstances {

  def lambek[T[_[_]]: Corecursive: Recursive, F[_]: Functor](tf: T[F]):
      F[T[F]] =
    tf.cata[F[T[F]]](_.map(Corecursive[T].embed[F]))

  def colambek[T[_[_]]: Corecursive: Recursive, F[_]: Functor](ft: F[T[F]]):
      T[F] =
    Corecursive[T].ana(ft)(_.map(_.project))

  type GAlgebraM[W[_], M[_], F[_], A] =                    F[W[A]] => M[A]
  type GAlgebra[W[_], F[_], A] = GAlgebraM[W, Id, F, A] // F[W[A]] => A
  type AlgebraM[M[_], F[_], A] = GAlgebraM[Id, M, F, A] // F[A]    => M[A]
  type Algebra[F[_], A]        = GAlgebra[Id, F, A]     // F[A]    => A

  type GCoalgebraM[N[_], M[_], F[_], A] =                      A => M[F[N[A]]]
  type GCoalgebra[N[_], F[_], A] = GCoalgebraM[N, Id, F, A] // A => N[F[A]]
  type CoalgebraM[M[_], F[_], A] = GCoalgebraM[Id, M, F, A] // A => F[M[A]]
  type Coalgebra[F[_], A]        = GCoalgebra[Id, F, A]     // A => F[A]

  type ElgotAlgebraM[M[_], F[_], A, B] = A => M[B \/ F[A]]
  type ElgotAlgebra[F[_], A, B] = ElgotAlgebraM[Id, F, A, B] // A => B \/ F[A]

  type CoelgotAlgebraM[M[_], F[_], A, B] = (A, F[B]) => M[B]
  type CoelgotAlgebra[F[_], A, B] = CoelgotAlgebraM[Id, F, A, B] // (A, F[B]) => B

  /** A NaturalTransformation that sequences two types */
  type DistributiveLaw[F[_], G[_]] = λ[α => F[G[α]]] ~> λ[α => G[F[α]]]

  def cofCataM[S[_]: Traverse, M[_]: Monad, A, B](t: Cofree[S, A])(f: (A, S[B]) => M[B]): M[B] =
    t.tail.traverse(cofCataM(_)(f)).flatMap(f(t.head, _))

  def cofParaM[M[_]] = new CofParaMPartiallyApplied[M]
  final class CofParaMPartiallyApplied[M[_]] { self =>
    def apply[T[_[_]]: Corecursive, S[_]: Traverse, A, B](t: Cofree[S, A])(f: (A, S[(T[S], B)]) => M[B])(implicit M: Monad[M]): M[B] =
      t.tail.traverseU(cs => self(cs)(f).map((Recursive[Cofree[?[_], A]].convertTo[S, T](cs), _))).flatMap(f(t.head, _))
  }

  /** Composition of an anamorphism and a catamorphism that avoids building the
    * intermediate recursive data structure.
    */
  def hylo[F[_]: Functor, A, B](a: A)(f: F[B] => B, g: A => F[A]): B =
    f(g(a).map(hylo(_)(f, g)))

  /** A Kleisli hylomorphism. */
  def hyloM[M[_]: Monad, F[_]: Traverse, A, B](a: A)(f: F[B] => M[B], g: A => M[F[A]]):
      M[B] =
    g(a).flatMap(_.traverse(hyloM(_)(f, g)).flatMap(f))

  /** A generalized version of a hylomorphism that composes any coalgebra and
    * algebra.
    */
  def ghylo[F[_]: Functor, W[_]: Comonad, M[_], A, B](
    a: A)(
    w: DistributiveLaw[F, W],
    m: DistributiveLaw[M, F],
    f: F[W[B]] => B,
    g: A => F[M[A]])(
    implicit M: Monad[M]):
      B = {
    def h(x: M[A]): W[B] = w(m(M.lift(g)(x)).map(y => h(y.join).cojoin)).map(f)
    h(a.point[M]).copoint
  }

  /** Similar to a hylomorphism, this composes a futumorphism and a
    * histomorphism.
    */
  def chrono[F[_]: Functor, A, B](
    a: A)(
    g: F[Cofree[F, B]] => B, f: A => F[Free[F, A]]):
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

  def distPara[T[_[_]], F[_]: Functor](implicit T: Corecursive[T]):
      DistributiveLaw[F, (T[F], ?)] =
    distZygo(T.embed[F])

  def distParaT[T[_[_]], F[_]: Functor, W[_]: Comonad](
    t: DistributiveLaw[F, W])(
    implicit T: Corecursive[T]):
      DistributiveLaw[F, EnvT[T[F], W, ?]] =
    distZygoT(T.embed[F], t)

  def distCata[F[_]]: DistributiveLaw[F, Id] = NaturalTransformation.refl

  def distZygo[F[_]: Functor, B](g: F[B] => B) =
    new DistributiveLaw[F, (B, ?)] {
      def apply[α](m: F[(B, α)]) = (g(m.map(_._1)), m.map(_._2))
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

  implicit def GAlgebraZip[W[_]: Functor, F[_]: Functor]:
      Zip[GAlgebra[W, F, ?]] =
    new Zip[GAlgebra[W, F, ?]] {
      def zip[A, B](a: ⇒ GAlgebra[W, F, A], b: ⇒ GAlgebra[W, F, B]) =
        node => (a(node.map(_.map(_._1))), b(node.map(_.map(_._2))))
    }
  implicit def AlgebraZip[F[_]: Functor] = GAlgebraZip[Id, F]

  implicit def CoelgotAlgebraMZip[M[_]: Applicative, F[_]: Functor, C]:
      Zip[CoelgotAlgebraM[M, F, C, ?]] =
    new Zip[CoelgotAlgebraM[M, F, C, ?]] {
      def zip[A, B](a: ⇒ CoelgotAlgebraM[M, F, C, A], b: ⇒ CoelgotAlgebraM[M, F, C, B]) =
        (ann, node) => Bitraverse[(?, ?)].bisequence((a(ann, node.map(_._1)), b(ann, node.map(_._2))))
    }
  implicit def CoelgotAlgebraZip[F[_]: Functor, C] =
    CoelgotAlgebraMZip[Id, F, C]
    // (ann, node) => node.unfzip.bimap(f(ann, _), g(ann, _))

  /** Repeatedly applies the function to the result as long as it returns Some.
    * Finally returns the last non-None value (which may be the initial input).
    */
  def repeatedly[A](f: A => Option[A]): A => A =
    expr => f(expr).fold(expr)(repeatedly(f))

  /** Converts a failable fold into a non-failable, by simply returning the
    * argument upon failure.
    */
  def once[A](f: A => Option[A]): A => A = expr => f(expr).getOrElse(expr)

  /** Count the instinces of `form` in the structure.
    */
  def count[T[_[_]]: Recursive, F[_]: Functor: Foldable](form: T[F]): F[(T[F], Int)] => Int =
    e => e.foldRight(if (e.map(_._1) == form.project) 1 else 0)(_._2 + _)

  /** Zips two attributed nodes together. This is unsafe in the sense that the
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
    def G: Traverse[G]

    def initial[A]: G[A]

    // Extracts bindings from a node:
    def bindings[T[_[_]]: Recursive, A](t: F[T[F]], b: G[A])(f: F[T[F]] => A): G[A]

    // Possibly binds a free term to its definition:
    def subst[T[_[_]], A](t: F[T[F]], b: G[A]): Option[A]
  }

  /** Annotate (the original nodes of) a tree, by applying a function to the
    * "bound" nodes. The function is also applied to the bindings themselves
    * to determine their annotation.
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

  sealed class IdOps[A](self: A) {
    def hylo[F[_]: Functor, B](f: F[B] => B, g: A => F[A]): B =
      recursionschemes.hylo(self)(f, g)
    def hyloM[M[_]: Monad, F[_]: Traverse, B](f: F[B] => M[B], g: A => M[F[A]]):
        M[B] =
      recursionschemes.hyloM(self)(f, g)
    def ghylo[F[_]: Functor, W[_]: Comonad, M[_]: Monad, B](
      w: DistributiveLaw[F, W],
      m: DistributiveLaw[M, F],
      f: F[W[B]] => B,
      g: A => F[M[A]]):
        B =
      recursionschemes.ghylo(self)(w, m, f, g)

    def chrono[F[_]: Functor, B](
      g: F[Cofree[F, B]] => B, f: A => F[Free[F, A]]):
        B =
      recursionschemes.chrono(self)(g, f)

    def elgot[F[_]: Functor, B](φ: F[B] => B, ψ: A => B \/ F[A]): B =
      recursionschemes.elgot(self)(φ, ψ)

    def coelgot[F[_]: Functor, B](φ: (A, F[B]) => B, ψ: A => F[A]): B =
      recursionschemes.coelgot(self)(φ, ψ)
    def coelgotM[M[_]] = new CoelgotMPartiallyApplied[M]
    final class CoelgotMPartiallyApplied[M[_]] {
      def apply[F[_]: Traverse, B](φ: (A, F[B]) => M[B], ψ: A => M[F[A]])(implicit M: Monad[M]):
          M[B] =
        recursionschemes.coelgotM[M].apply[F, A, B](self)(φ, ψ)
    }

    // def ana[T[_[_]], F[_]: Functor](f: A => F[A])(implicit T: Corecursive[T]): T[F] =
    //   T.ana(self)(f)
    // def anaM[T[_[_]], F[_]: Traverse, M[_]: Monad](f: A => M[F[A]])(implicit T: Corecursive[T]): M[T[F]] =
    //   T.anaM(self)(f)
    // def gana[T[_[_]], F[_]: Functor, M[_]: Monad](
    //   k: DistributiveLaw[M, F], f: A => F[M[A]])(
    //   implicit T: Corecursive[T]):
    //     T[F] =
    //   T.gana(self)(k, f)
    // def apo[T[_[_]], F[_]: Functor](f: A => F[T[F] \/ A])(implicit T: Corecursive[T]): T[F] =
    //   T.apo(self)(f)
    // def apoM[T[_[_]], F[_]: Traverse, M[_]: Monad](f: A => M[F[T[F] \/ A]])(implicit T: Corecursive[T]): M[T[F]] =
    //   T.apoM(self)(f)
    // def postpro[T[_[_]]: Recursive, F[_]: Functor](e: F ~> F, g: A => F[A])(implicit T: Corecursive[T]): T[F] =
    //   T.postpro(self)(e, g)
    // def gpostpro[T[_[_]]: Recursive, F[_]: Functor, M[_]: Monad](
    //   k: DistributiveLaw[M, F], e: F ~> F, g: A => F[M[A]])(
    //   implicit T: Corecursive[T]):
    //       T[F] =
    //   T.gpostpro(self)(k, e, g)
    // def futu[T[_[_]], F[_]: Functor](f: A => F[Free[F, A]])(implicit T: Corecursive[T]): T[F] =
    //   T.futu(self)(f)
    // def futuM[T[_[_]], F[_]: Traverse, M[_]: Monad](f: A => M[F[Free[F, A]]])(implicit T: Corecursive[T]):
    //     M[T[F]] =
    //   T.futuM(self)(f)
  }
  implicit def ToIdOps[A](a: A): IdOps[A] = new IdOps[A](a)

  sealed class CorecursiveIdOps[T[_[_]], A](self: A)(implicit T: Corecursive[T]) {
    def ana[F[_]: Functor](f: A => F[A]): T[F] =
      T.ana(self)(f)
    def anaM[F[_]: Traverse, M[_]: Monad](f: A => M[F[A]]): M[T[F]] =
      T.anaM(self)(f)
    def gana[F[_]: Functor, M[_]: Monad](
      k: DistributiveLaw[M, F], f: A => F[M[A]]):
        T[F] =
      T.gana(self)(k, f)
    def apo[F[_]: Functor](f: A => F[T[F] \/ A]): T[F] =
      T.apo(self)(f)
    def apoM[F[_]: Traverse, M[_]: Monad](f: A => M[F[T[F] \/ A]]): M[T[F]] =
      T.apoM(self)(f)
    def postpro[F[_]: Functor](e: F ~> F, g: A => F[A])(implicit R: Recursive[T]): T[F] =
      T.postpro(self)(e, g)
    def gpostpro[F[_]: Functor, M[_]](
        k: DistributiveLaw[M, F], e: F ~> F, g: A => F[M[A]])(
        implicit R: Recursive[T], M: Monad[M]):
          T[F] =
      T.gpostpro(self)(k, e, g)
    def futu[F[_]: Functor](f: A => F[Free[F, A]]): T[F] =
      T.futu(self)(f)
    def futuM[F[_]: Traverse, M[_]: Monad](f: A => M[F[Free[F, A]]]):
        M[T[F]] =
      T.futuM(self)(f)
  }
  implicit def ToFixIdOps[A](a: A): CorecursiveIdOps[Fix, A] =
    new CorecursiveIdOps[Fix, A](a)
}
