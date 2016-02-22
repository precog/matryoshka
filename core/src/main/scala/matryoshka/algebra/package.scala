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

import Recursive.ops._

import scala.{Function, Int, Option}

import scalaz._, Scalaz._

/** Operations on algebras, as well as generic algebras. */
package object algebra {
  /** Turns any F-algebra, into an identical one that attributes the tree with
    * the results for each node. */
  def attributeM[F[_]: Functor, M[_]: Functor, A](φ: F[A] => M[A]):
      F[Cofree[F, A]] => M[Cofree[F, A]] =
    fa => φ(fa ∘ (_.head)) ∘ (Cofree(_, fa))

  def attribute[F[_]: Functor, A](φ: F[A] => A) = attributeM[F, Id, A](φ)

  def attrK[F[_]: Functor, A](k: A) = attribute[F, A](Function.const(k))

  def attrSelf[T[_[_]]: Corecursive, F[_]: Functor] =
    attribute[F, T[F]](_.embed)

  /** NB: Since Cofree carries the functor, the resulting algebra is a cata, not
    *     a para. */
  def attributePara[T[_[_]]: Corecursive, F[_]: Functor, A](φ: F[(T[F], A)] => A):
      F[Cofree[F, A]] => Cofree[F, A] =
    fa => Cofree(φ(fa ∘ (x => (Recursive[Cofree[?[_], A]].convertTo[F, T](x), x.head))), fa)

  def attributeElgotM[W[_]: Comonad, M[_]: Functor, F[_]: Functor, A](
    f: W[F[A]] => M[A]):
      W[F[Cofree[F, A]]] => M[Cofree[F, A]] =
    node => f(node ∘ (_ ∘ (_.head))) ∘ (Cofree(_, node.copoint))

  def generalizeAlgebra[W[_]: Comonad, F[_]: Functor, A](a: Algebra[F, A]):
      GAlgebra[W, F, A] =
    node => a(node ∘ (_.copoint))

  def generalizeAlgebraM[M[_]: Monad, F[_], A](a: Algebra[F, A]):
      AlgebraM[M, F, A] =
    a(_).point[M]

  def generalizeElgotAlgebra[W[_]: Comonad, F[_], A](a: Algebra[F, A]):
      ElgotAlgebra[W, F, A] =
    node => a(node.copoint)

  def generalizeElgotAlgebraM[W[_]: Comonad, M[_]: Applicative, F[_], A](
    a: Algebra[F, A]):
      ElgotAlgebraM[W, M, F, A] =
    node => a(node.copoint).point[M]

  def generalizeCoalgebra[M[_]: Monad, F[_]: Functor, A](c: Coalgebra[F, A]):
      GCoalgebra[M, F, A] =
    c(_).map(_.map(_.point[M]))

  def generalizeCoalgebraM[M[_]: Monad, F[_], A](c: Coalgebra[F, A]):
      CoalgebraM[M, F, A] =
    c(_).point[M]

  /** This is identical to `generalizeCoalgebraM`, but is defined for
    * consistency with generalizeElgotAlgebra.
    */
  def generalizeElgotCoalgebra[M[_]: Monad, F[_], A](c: Coalgebra[F, A]):
      CoalgebraM[M, F, A] =
    generalizeCoalgebraM[M, F, A](c)

  /** Converts any GAlgebra into a bottom-up transformation. However, note that
    * the `T` still needs to have a `Recursive` instance, not just `FunctorT`.
    */
  def algebraToTransformation[T[_[_]]: Recursive, W[_], F[_], G[_]: Functor](
    self: GAlgebra[W, F, T[G]]):
      F[W[T[G]]] => G[T[G]] =
    self(_).project

  /** Converts any GCoalgebra into a top-down transformation. However, note that
    * the `T` still needs to have a `Corecursive` instance, not just `FunctorT`.
    */
  def coalgebraToTransformation[T[_[_]]: Corecursive, M[_], F[_]: Functor, G[_]](
    self: GCoalgebra[M, G, T[F]]):
      F[T[F]] => G[M[T[F]]] =
    f => self(f.embed)

  /** Converts any bottom-up transformation into a [[matryoshka.GAlgebra]].
    * However, note that `T` now needs a [[matryoshka.Corecursive]] instance,
    * not just [[matryoshka.FunctorT]], and since it’s an algebra, folding over
    * it will probably also require a [[matryoshka.Recursive]] instance.
    */
  def transformationToAlgebra[T[_[_]]: Corecursive, W[_], F[_], G[_]: Functor](
    self: F[W[T[G]]] => G[T[G]]):
      GAlgebra[W, F, T[G]] =
    self(_).embed

  /** Converts any top-down transformation into a [[matryoshka.GCoalgebra]].
    * However, note that `T` now needs a [[matryoshka.Recursive]] instance, not
    * just [[matryoshka.FunctorT]], and since it’s an algebra, folding over it
    * will probably also require a [[matryoshka.Corecursive]] instance.
    */
  def transformationToCoalgebra[T[_[_]]: Recursive, M[_], F[_]: Functor, G[_]](
    self: F[T[F]] => G[M[T[F]]]):
      GCoalgebra[M, G, T[F]] =
    f => self(f.project)

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
        node => Bitraverse[(?, ?)].bisequence((a(node ∘ (_ ∘ (_._1))), b(node ∘ (_ ∘ (_._2)))))
    }
  implicit def ElgotAlgebraZip[W[_]: Functor, F[_]: Functor] =
    ElgotAlgebraMZip[W, Id, F]

  /** Repeatedly applies the function to the result as long as it returns Some.
    * Finally returns the last non-None value (which may be the initial input).
    */
  def repeatedly[A](f: A => Option[A]): A => A =
    expr => f(expr).fold(expr)(repeatedly(f))

  /** Converts a failable fold into a non-failable, by simply returning the
    * argument upon failure.
    */
  def once[A](f: A => Option[A]): A => A = expr => f(expr).getOrElse(expr)

  /** Count the instinces of `form` in the structure. */
  def count[T[_[_]]: Recursive, F[_]: Functor: Foldable](form: T[F]):
      F[(T[F], Int)] => Int =
    e => e.foldRight(if (e ∘ (_._1) == form.project) 1 else 0)(_._2 + _)
}
