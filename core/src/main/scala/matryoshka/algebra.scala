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

package matryoshka

import scala.Function1

import scalaz._, Leibniz._, Scalaz._

/** The most general algebra, using both generalized and Elgot comonads as
  * well as a monad.
  */
final class GElgotAlgebraM[E[_], G[_], M[_], F[_], A](f: E[F[G[A]]] => M[A])
    extends Function1[E[F[G[A]]], M[A]] {
  def apply(v1: E[F[G[A]]]) = f(v1)

  def attribute(implicit E: Comonad[E], G: Comonad[G], M: Functor[M], F: Functor[F]) =
    matryoshka.attribute[E, G, M, F, A](this)

  def generalizeElgot[EE[_]: Comonad](
    implicit ev: GElgotAlgebraM[E, G, M, F, A] === GAlgebraM[G, M, F, A]):
      GElgotAlgebraM[EE, G, M, F, A] =
    matryoshka.generalizeW[EE, F[G[A]], M[A]](ev(this).apply)

  def generalize[GG[_]: Comonad](implicit EF: Functor[λ[α => E[F[α]]]], ev: GElgotAlgebraM[E, G, M, F, A] === ElgotAlgebraM[E, M, F, A]):
      GElgotAlgebraM[E, GG, M, F, A] =
    matryoshka.generalizeAlgebra[λ[α => E[F[α]]], GG, Id, M, F, A](ev(this).apply)

  def generalizeM[MM[_]: Applicative](
    implicit ev: GElgotAlgebraM[E, G, M, F, A] === GElgotAlgebra[E, G, F, A]):
      GElgotAlgebraM[E, G, MM, F, A] =
    matryoshka.generalizeM[MM, E[F[G[A]]], A](ev(this).apply)

  def zip[B](
    b: ⇒ GElgotAlgebraM[E, G, M, F, B])(
    implicit E: Functor[E], G: Functor[G], M: Applicative[M], F: Functor[F]):
      GElgotAlgebraM[E, G, M, F, (A, B)]=
    node => (this.f(node ∘ (_ ∘ (_ ∘ (_._1)))) ⊛ b(node ∘ (_ ∘ (_ ∘ (_._2)))))((_, _))
}
object GElgotAlgebraM {
  implicit def gElgotAlgebraMZip[E[_]: Functor, G[_]: Functor, M[_]: Applicative, F[_]: Functor]:
      Zip[GElgotAlgebraM[E, G, M, F, ?]] =
    new Zip[GElgotAlgebraM[E, G, M, F, ?]] {
      def zip[A, B](
        a: ⇒ GElgotAlgebraM[E, G, M, F, A],
        b: ⇒ GElgotAlgebraM[E, G, M, F, B]) =
        a.zip(b)
    }
}

sealed class GElgotCoalgebraM[E[_], G[_], M[_], F[_], A](f: A => M[E[F[G[A]]]])
    extends Function1[A, M[E[F[G[A]]]]] {
  def apply(v1: A) = f(v1)

  def generalizeM[MM[_]: Applicative](implicit ev: GElgotCoalgebraM[E, G, M, F, A] === GElgotCoalgebra[E, G, F, A]):
      GElgotCoalgebraM[E, G, MM, F, A] =
    matryoshka.generalizeM[MM, A, E[F[G[A]]]](ev(this).apply)

  def generalizeElgot[EE[_]: Applicative](
    implicit M: Functor[M],
             ev: GElgotCoalgebraM[E, G, M, F, A] === GCoalgebraM[G, M, F, A]):
      GElgotCoalgebraM[EE, G, M, F, A] =
    matryoshka.generalizeCoalgebra[M, EE, λ[α => F[G[α]]], A](ev(this).apply)

  def generalize[GG[_]: Applicative](implicit MEF: Functor[λ[α => M[E[F[α]]]]], ev: GElgotCoalgebraM[E, G, M, F, A] === ElgotCoalgebraM[E, M, F, A]):
      GElgotCoalgebraM[E, GG, M, F, A] =
    matryoshka.generalizeCoalgebra[λ[α => M[E[F[α]]]], GG, Id, A](ev(this).apply)
}

// NB: This class is needed to avoid the `Id[Id[_]]` “cyclic alias” issue.
final class GCoalgebra[G[_], F[_], A](f: A => F[G[A]])
    extends GElgotCoalgebraM[Id, G, Id, F, A](f)

sealed trait ZeroIdInstances {
  implicit def toGElgotAlgebraM[E[_], G[_], M[_], F[_], A](f: E[F[G[A]]] => M[A]): GElgotAlgebraM[E, G, M, F, A] =
    new GElgotAlgebraM[E, G, M, F, A](f)

  implicit def toGElgotCoalgebraM[E[_], G[_], M[_], F[_], A](f: A => M[E[F[G[A]]]]): GElgotCoalgebraM[E, G, M, F, A] =
    new GElgotCoalgebraM[E, G, M, F, A](f)
}

sealed trait OneIdInstances extends ZeroIdInstances {
}

sealed trait TwoIdInstances extends OneIdInstances {
  // FIXME: somehow this causes an ambiguous implicit with a lower priority
  //        implicit.
  // implicit def toElgotAlgebra[E[_], F[_], A](f: E[F[A]] => A):
  //     ElgotAlgebra[E, F, A] =
  //   new GElgotAlgebraM[E, Id, Id, F, A](f)
  // Poor man’s unapply trick
  implicit def toElgotAlgebraU[E[_[_], _], F[_], A, X[_]](f: E[X, F[A]] => A):
      ElgotAlgebra[E[X, ?], F, A] =
    new GElgotAlgebraM[E[X, ?], Id, Id, F, A](f)

  implicit def toElgotCoalgebra[E[_], F[_], A](f: A => E[F[A]]):
      ElgotCoalgebra[E, F, A] =
    new GElgotCoalgebraM[E, Id, Id, F, A](f)
  // Poor man’s unapply trick
  implicit def toElgotCoalgebraU[E[_[_], _], F[_], A, X[_]](f: A => E[X, F[A]]):
      ElgotCoalgebra[E[X, ?], F, A] =
    new GElgotCoalgebraM[E[X, ?], Id, Id, F, A](f)

  implicit def toGAlgebra[G[_], F[_], A](f: F[G[A]] => A): GAlgebra[G, F, A] =
    new GElgotAlgebraM[Id, G, Id, F, A](f)
  // Poor man’s unapply trick
  implicit def toGAlgebraU[G[_[_], _], F[_], A, X[_]](f: F[G[X, A]] => A):
      GAlgebra[G[X, ?], F, A] =
    new GElgotAlgebraM[Id, G[X, ?], Id, F, A](f)

  implicit def toGCoalgebra[G[_], F[_], A](f: A => F[G[A]]):
      GCoalgebra[G, F, A] =
    new GCoalgebra[G, F, A](f)
  // Poor man’s unapply trick
  implicit def toGCoalgebraU[G[_[_], _], F[_], A, X[_]](f: A => F[G[X, A]]):
      GCoalgebra[G[X, ?], F, A] =
    new GCoalgebra[G[X, ?], F, A](f)

  implicit def toAlgebraM[M[_], F[_], A](f: F[A] => M[A]): AlgebraM[M, F, A] =
    new GElgotAlgebraM[Id, Id, M, F, A](f)
  // Poor man’s unapply trick
  implicit def toAlgebraMU[M[_[_], _], F[_], A, X[_]](f: F[A] => M[X, A]):
      AlgebraM[M[X, ?], F, A] =
    new GElgotAlgebraM[Id, Id, M[X, ?], F, A](f)

  implicit def toCoalgebraM[M[_], F[_], A](f: A => M[F[A]]):
      CoalgebraM[M, F, A] =
    new GElgotCoalgebraM[Id, Id, M, F, A](f)
  // Poor man’s unapply trick
  implicit def toCoalgebraMU[M[_[_], _], F[_], A, X[_]](f: A => M[X, F[A]]):
      CoalgebraM[M[X, ?], F, A] =
    new GElgotCoalgebraM[Id, Id, M[X, ?], F, A](f)
}

trait ThreeIdInstances extends TwoIdInstances {
  implicit def toAlgebra[F[_], A](f: F[A] => A): Algebra[F, A] =
    new GElgotAlgebraM[Id, Id, Id, F, A](f)

  implicit def toCoalgebra[F[_], A](f: A => F[A]): Coalgebra[F, A] =
    new GCoalgebra[Id, F, A](f)
}
