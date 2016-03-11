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

/** Generalized folds, unfolds, and refolds.
  *
  * @groupname Algebra [Co]Algebras
  * @groupname Fold Folds
  * @groupdesc Fold
  *            These are folds that don’t fit under [[matryoshka.Recursive]]
  *            because they fold from a specific fixed point operator.
  * @groupname Unfold Unfolds
  * @groupdesc Unfold
  *            These are unfolds that don’t fit under [[matryoshka.Corecursive]]
  *            because they unfold to a specific fixed point operator.
  * @groupname Refold Refolds
  * @groupdesc Refold
  *            These operations apply a function both on the way to the leaves
  *            of the structure as well as on the way back, so they avoid ever
  *            constructing an intermediate fixed point structure.
  * @groupname Dist DistributiveLaws
  */
package object matryoshka extends CofreeInstances with FreeInstances {

  def lambek[T[_[_]]: Corecursive: Recursive, F[_]: Functor](tf: T[F]):
      F[T[F]] =
    tf.cata[F[T[F]]](_ ∘ (_.embed))

  def colambek[T[_[_]]: Corecursive: Recursive, F[_]: Functor](ft: F[T[F]]):
      T[F] =
    ft.ana(_ ∘ (_.project))

  type GAlgebraM[W[_], M[_], F[_], A] = F[W[A]] => M[A]
  object GAlgebraM {
    implicit def toOps[W[_], M[_], F[_], A](a: GAlgebraM[W, M, F, A]):
        GAlgebraMOps[W, M, F, A] =
      new GAlgebraMOps[W, M, F, A](a)
  }

  type GAlgebra[W[_], F[_], A] = GAlgebraM[W, Id, F, A] // F[W[A]] => A
  object GAlgebra {
    implicit def zip[W[_]: Functor, F[_]: Functor]: Zip[GAlgebra[W, F, ?]] =
      new Zip[GAlgebra[W, F, ?]] {
        def zip[A, B](a: ⇒ GAlgebra[W, F, A], b: ⇒ GAlgebra[W, F, B]) =
          node => (a(node ∘ (_ ∘ (_._1))), b(node ∘ (_ ∘ (_._2))))
      }

    implicit def toOps[W[_], F[_], A](a: GAlgebra[W, F, A]): GAlgebraOps[W, F, A] =
      new GAlgebraOps[W, F, A](a)
  }

  type AlgebraM[M[_], F[_], A] = GAlgebraM[Id, M, F, A] // F[A] => M[A]
  object AlgebraM {
    implicit def toOps[M[_], F[_], A](a: AlgebraM[M, F, A]):
        AlgebraMOps[M, F, A] =
      new AlgebraMOps[M, F, A](a)
  }

  type Algebra[F[_], A] = GAlgebra[Id, F, A] // F[A] => A
  object Algebra {
    implicit def zip[F[_]: Functor] = GAlgebra.zip[Id, F]

    implicit def toOps[F[_], A](a: Algebra[F, A]): AlgebraOps[F, A] =
      new AlgebraOps[F, A](a)
  }

  type ElgotAlgebraM[W[_], M[_], F[_], A] = W[F[A]] => M[A]
  object ElgotAlgebraM {
    implicit def zip[W[_]: Functor, M[_]: Applicative, F[_]: Functor]:
        Zip[ElgotAlgebraM[W, M, F, ?]] =
      new Zip[ElgotAlgebraM[W, M, F, ?]] {
        def zip[A, B](a: ⇒ ElgotAlgebraM[W, M, F, A], b: ⇒ ElgotAlgebraM[W, M, F, B]) =
          w => Bitraverse[(?, ?)].bisequence((a(w ∘ (_ ∘ (_._1))), b(w ∘ (_ ∘ (_._2)))))
      }

    implicit def toOps[W[_], M[_], F[_], A](a: ElgotAlgebraM[W, M, F, A]):
        ElgotAlgebraMOps[W, M, F, A] =
      new ElgotAlgebraMOps[W, M, F, A](a)
  }

  type ElgotAlgebra[W[_], F[_], A] = ElgotAlgebraM[W, Id, F, A] // W[F[A]] => A
  object ElgotAlgebra {
    implicit def zip[W[_]: Functor, F[_]: Functor] = ElgotAlgebraM.zip[W, Id, F]
      // (ann, node) => node.unfzip.bimap(f(ann, _), g(ann, _))

    implicit def toOps[W[_], F[_], A](a: ElgotAlgebra[W, F, A]):
        ElgotAlgebraOps[W, F, A] =
      new ElgotAlgebraOps[W, F, A](a)
  }

  /** A coalgebra with two monads, one handled by the fold and the other in the
    * result. `N` is the fold’s monad, whereas `M` is the Kleisli.
    */
  type GCoalgebraM[N[_], M[_], F[_], A] = A => M[F[N[A]]]

  type GCoalgebra[N[_], F[_], A] = GCoalgebraM[N, Id, F, A] // A => F[N[A]]
  object GCoalgebra {
    implicit def toOps[M[_], F[_], A](a: GCoalgebra[M, F, A]):
        GCoalgebraOps[M, F, A] =
      new GCoalgebraOps[M, F, A](a)
  }

  type CoalgebraM[M[_], F[_], A] = GCoalgebraM[Id, M, F, A] // A => M[F[A]]
  object CoalgebraM {
    implicit def toOps[M[_], F[_], A](a: CoalgebraM[M, F, A]):
        CoalgebraMOps[M, F, A] =
      new CoalgebraMOps[M, F, A](a)
  }

  type Coalgebra[F[_], A]        = GCoalgebra[Id, F, A] // A => F[A]
  object Coalgebra {
    implicit def toOps[F[_], A](a: Coalgebra[F, A]): CoalgebraOps[F, A] =
      new CoalgebraOps[F, A](a)
  }

  type ElgotCoalgebraM[N[_], M[_], F[_], A] = A => M[N[F[A]]]

  /** Identical to [[matryoshka.CoalgebraM]] in structure, this signifies a
    * different intent – the dual of [[matryoshka.ElgotAlgebra]].
    */
  type ElgotCoalgebra[N[_], F[_], A] = ElgotCoalgebraM[N, Id, F, A] // A => N[F[A]]
  object ElgotCoalgebra {
    implicit def toOps[M[_], F[_], A](a: ElgotCoalgebra[M, F, A]):
        ElgotCoalgebraOps[M, F, A] =
      new ElgotCoalgebraOps[M, F, A](a)
  }

  /** A `NaturalTransformation` that sequences two types.
    *
    * @group Dist
    */
  type DistributiveLaw[F[_], G[_]] = λ[α => F[G[α]]] ~> λ[α => G[F[α]]]

  /** This folds a Free that you may think of as “already partially-folded”.
    * It’s also the fold of a decomposed `elgot`.
    *
    * @group Fold
    */
  def interpretCata[F[_]: Functor, A](t: Free[F, A])(φ: F[A] => A): A =
    t.fold(x => x, f => φ(f ∘ (interpretCata(_)(φ))))

  /** The fold of a decomposed `coelgot`, since the Cofree already has the
    * attribute for each node.
    *
    * @group Fold
    */
  def elgotCata[F[_]: Functor, A, B](t: Cofree[F, A])(φ: ((A, F[B])) => B): B =
    φ((t.head, t.tail ∘ (elgotCata(_)(φ))))

  /** A Kleisli version of [[matryoshka.elgotCata]].
    *
    * @group Fold
    */
  def elgotCataM[F[_]: Traverse, M[_]: Monad, A, B](
    t: Cofree[F, A])(
    φ: ((A, F[B])) => M[B]):
      M[B] =
    t.tail.traverse(elgotCataM(_)(φ)) >>= (fb => φ((t.head, fb)))

  /** A version of [[matryoshka.Corecursive.ana]] that annotates each node with
    * the `A` it was expanded from. This is also the unfold from a decomposed
    * `coelgot`.
    *
    * @group Unfold
    */
  def attributeAna[F[_]: Functor, A](a: A)(ψ: A => F[A]): Cofree[F, A] =
    Cofree(a, ψ(a) ∘ (attributeAna(_)(ψ)))

  /** A Kleisli [[matryoshka.attributeAna]].
    *
    * @group Unfold
    */
  def attributeAnaM[M[_]: Monad, F[_]: Traverse, A](a: A)(ψ: A => M[F[A]]):
      M[Cofree[F, A]] =
    ψ(a).flatMap(_.traverse(attributeAnaM(_)(ψ))) ∘ (Cofree(a, _))

  /** The unfold from a decomposed `elgot`.
    *
    * @group Unfold
    */
  def elgotAna[F[_]: Functor, A, B](a: A)(ψ: A => B \/ F[A]): Free[F, B] =
    ψ(a).fold(
      _.point[Free[F, ?]],
      fb => Free.liftF(fb ∘ (elgotAna(_)(ψ))).join)

  /** The “second” fold from `elgotZygo`, with the “helper value” coming from
    * `Cofree` rather than from another algebra.
    *
    * @group Fold
    */
  def cofCataM[S[_]: Traverse, M[_]: Monad, A, B](t: Cofree[S, A])(f: (A, S[B]) => M[B]):
      M[B] =
    t.tail.traverse(cofCataM(_)(f)) >>= (f(t.head, _))

  /** This is the “second” fold from `paraElgotZygo`, with the comonad
    * structures coming from `Cofree` rather than from another algebra.
    *
    * @group Fold
    */
  def cofParaM[M[_]] = new CofParaMPartiallyApplied[M]
  final class CofParaMPartiallyApplied[M[_]] { self =>
    def apply[T[_[_]]: Corecursive, S[_]: Traverse, A, B](t: Cofree[S, A])(f: (A, S[(T[S], B)]) => M[B])(implicit M: Monad[M]): M[B] =
      t.tail.traverseU(cs => self(cs)(f) ∘ ((Recursive[Cofree[?[_], A]].convertTo[S, T](cs), _))) >>= (f(t.head, _))
  }

  /** A hylomorphism is a composition of [[matryoshka.Corecursive.ana]] and
    * [[matryoshka.Recursive.cata]] that avoids building the intermediate
    * recursive data structure.
    *
    * @group Refold
    */
  def hylo[F[_]: Functor, A, B](a: A)(f: F[B] => B, g: A => F[A]): B =
    f(g(a) ∘ (hylo(_)(f, g)))

  /** A Kleisli [[matryoshka.hylo]].
    *
    * @group Refold
    */
  def hyloM[M[_]: Monad, F[_]: Traverse, A, B](a: A)(f: F[B] => M[B], g: A => M[F[A]]):
      M[B] =
    g(a) >>= (_.traverse(hyloM(_)(f, g)) >>= (f))

  /** A generalized version of [[matryoshka.hylo]] that composes any coalgebra
    * and algebra.
    *
    * @group Refold
    */
  def ghylo[F[_]: Functor, W[_]: Comonad, M[_], A, B](
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

  /** Similar to [[matryoshka.hylo]], this composes
    * [[matryoshka.Corecursive.futu]] and [[matryoshka.Recursive.histo]].
    *
    * @group Refold
    */
  def chrono[F[_]: Functor, A, B](
    a: A)(
    g: F[Cofree[F, B]] => B, f: A => F[Free[F, A]]):
      B =
    ghylo[F, Cofree[F, ?], Free[F, ?], A, B](a)(distHisto, distFutu, g, f)

  /** Similar to [[matryoshka.hylo]], `elgot` allows the unfold to short-circuit
    * by returning the final value for a branch directly.
    *
    * @group Refold
    */
  def elgot[F[_]: Functor, A, B](a: A)(φ: F[B] => B, ψ: A => B \/ F[A]): B = {
    def h: A => B =
      (((x: B) => x) ||| ((x: F[A]) => φ(x ∘ h))) ⋘ ψ
    h(a)
  }

  /** The dual of [[matryoshka.elgot]], `coelgot` has access to the pre-unfolded
    * value for each node.
    *
    * @group Refold
    */
  def coelgot[F[_]: Functor, A, B](a: A)(φ: ((A, F[B])) => B, ψ: A => F[A]):
      B = {
    def h: A => B =
      φ ⋘ (((x: A) => x) &&& (((x: F[A]) => x ∘ h) ⋘ ψ))
    h(a)
  }

  /** A Kleisli version of [[matryoshka.coelgot]].
    *
    * @group Refold
    */
  def coelgotM[M[_]] = new CoelgotMPartiallyApplied[M]
  final class CoelgotMPartiallyApplied[M[_]] {
    def apply[F[_]: Traverse, A, B](a: A)(φ: ((A, F[B])) => M[B], ψ: A => M[F[A]])(implicit M: Monad[M]):
        M[B] = {
      def h(a: A): M[B] = ψ(a) >>= (_.traverse(h)) >>= (x => φ((a, x)))
      h(a)
    }
  }

  /** @group Dist */
  def distPara[T[_[_]], F[_]: Functor](implicit T: Corecursive[T]):
      DistributiveLaw[F, (T[F], ?)] =
    distZygo(_.embed)

  /** @group Dist */
  def distParaT[T[_[_]], F[_]: Functor, W[_]: Comonad](
    t: DistributiveLaw[F, W])(
    implicit T: Corecursive[T]):
      DistributiveLaw[F, EnvT[T[F], W, ?]] =
    distZygoT(_.embed, t)

  /** @group Dist */
  def distCata[F[_]]: DistributiveLaw[F, Id] = NaturalTransformation.refl

  /** @group Dist */
  def distZygo[F[_]: Functor, B](g: F[B] => B) =
    new DistributiveLaw[F, (B, ?)] {
      def apply[α](m: F[(B, α)]) = (g(m ∘ (_._1)), m ∘ (_._2))
    }

  /** @group Dist */
  def distZygoT[F[_], W[_]: Comonad, B](
    g: F[B] => B, k: DistributiveLaw[F, W])(
    implicit F: Functor[F]) =
    new DistributiveLaw[F, EnvT[B, W, ?]] {
      def apply[α](fe: F[EnvT[B, W, α]]) =
        EnvT((
          g(F.lift[EnvT[B, W, α], B](_.ask)(fe)),
          k(F.lift[EnvT[B, W, α], W[α]](_.lower)(fe))))
    }

  /** @group Dist */
  def distHisto[F[_]: Functor] =
    new DistributiveLaw[F, Cofree[F, ?]] {
      def apply[α](m: F[Cofree[F, α]]) =
        distGHisto[F, F](NaturalTransformation.refl[λ[α => F[F[α]]]]).apply(m)
    }

  /** @group Dist */
  def distGHisto[F[_],  H[_]](
    k: DistributiveLaw[F, H])(
    implicit F: Functor[F], H: Functor[H]) =
    new DistributiveLaw[F, Cofree[H, ?]] {
      def apply[α](m: F[Cofree[H, α]]) =
        Cofree.unfold(m)(as => (
          F.lift[Cofree[H, α], α](_.copure)(as),
          k(F.lift[Cofree[H, α], H[Cofree[H, α]]](_.tail)(as))))
    }

  /** @group Dist */
  def distAna[F[_]]: DistributiveLaw[Id, F] = NaturalTransformation.refl

  /** @group Dist */
  def distFutu[F[_]: Functor] =
    new DistributiveLaw[Free[F, ?], F] {
      def apply[α](m: Free[F, F[α]]) =
        distGFutu[F, F](NaturalTransformation.refl[λ[α => F[F[α]]]]).apply(m)
    }

  /** @group Dist */
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
  def attributeAlgebraM[M[_]: Functor, F[_]: Functor, A](f: AlgebraM[M, F, A]):
      AlgebraM[M, F, Cofree[F, A]] =
    fa => f(fa ∘ (_.head)) ∘ (Cofree(_, fa))

  def attributeAlgebra[F[_]: Functor, A](f: Algebra[F, A]) =
    attributeAlgebraM[Id, F, A](f)

  def attrK[F[_]: Functor, A](k: A) = attributeAlgebra[F, A](Function.const(k))

  def attrSelf[T[_[_]]: Corecursive, F[_]: Functor] =
    attributeAlgebra[F, T[F]](_.embed)

  /** NB: Since Cofree carries the functor, the resulting algebra is a cata, not
    *     a para. */
  def attributePara[T[_[_]]: Corecursive, F[_]: Functor, A](f: F[(T[F], A)] => A):
      F[Cofree[F, A]] => Cofree[F, A] =
    fa => Cofree(f(fa ∘ (x => (Recursive[Cofree[?[_], A]].convertTo[F, T](x), x.head))), fa)

  def attributeGAlgebraM[W[_]: Comonad, M[_]: Functor, F[_]: Functor, A](
    f: GAlgebraM[W, M, F, A]):
      GAlgebraM[W, M, F, Cofree[F, A]] =
    node => f(node ∘ (_ ∘ (_.head))) ∘ (Cofree(_, node ∘ (_.copoint)))

  def attributeElgotAlgebraM[W[_]: Comonad, M[_]: Functor, F[_]: Functor, A](
    f: ElgotAlgebraM[W, M, F, A]):
      ElgotAlgebraM[W, M, F, Cofree[F, A]] =
    node => f(node ∘ (_ ∘ (_.head))) ∘ (Cofree(_, node.copoint))

  def generalizeM[M[_]: Applicative, A, B](f: A => B): A => M[B] = f(_).point[M]
  def generalizeW[W[_]: Comonad, A, B](f: A => B): W[A] => B = w => f(w.copoint)
  def generalizeAlgebra[W[_]: Comonad, F[_]: Functor, A, B](f: F[A] => B):
      F[W[A]] => B =
    node => f(node ∘ (_.copoint))
  def generalizeCoalgebra[M[_]: Applicative, F[_]: Functor, A](f: A => F[A]):
      A => F[M[A]] =
    f(_).map(_.point[M])
  def generalizeCoalgebraM[M[_]: Functor, N[_]: Applicative, F[_]: Functor, A](
    f: A => M[F[A]]):
      A => M[F[N[A]]] =
    f(_).map(_.map(_.point[N]))

  /** Repeatedly applies the function to the result as long as it returns Some.
    * Finally returns the last non-None value (which may be the initial input).
    */
  def repeatedly[A](f: A => Option[A]): A => A =
    expr => f(expr).fold(expr)(repeatedly(f))

  /** Converts a failable fold into a non-failable, by returning the default
    * upon failure.
    */
  def orDefault[A, B](default: B)(f: A => Option[B]): A => B =
    f(_).getOrElse(default)

  /** Converts a failable fold into a non-failable, by simply returning the
    * argument upon failure.
    */
  def orOriginal[A](f: A => Option[A]): A => A =
    expr => orDefault(expr)(f)(expr)

  /** Count the instinces of `form` in the structure.
    *
    * @group Algebra
    */
  def count[T[_[_]]: Recursive, F[_]: Functor: Foldable](form: T[F]):
      F[(T[F], Int)] => Int =
    e => e.foldRight(if (e ∘ (_._1) == form.project) 1 else 0)(_._2 + _)

  implicit def ToIdOps[A](a: A): IdOps[A] = new IdOps[A](a)

  implicit def ToCorecursiveOps[T[_[_]]: Corecursive, F[_]](f: F[T[F]]):
      CorecursiveOps[T, F] =
    new CorecursiveOps[T, F](f)
}
