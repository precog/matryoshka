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

import scalaz._, Scalaz._

/** The most general algebra, using both generalized and Elgot comonads as
  * well as a monad.
  */
final class GElgotAlgebraM[E[_], G[_], M[_], F[_], A](f: E[F[G[A]]] => M[A])
    extends Function1[E[F[G[A]]], M[A]] {
  def apply(v1: E[F[G[A]]]) = f(v1)
}
object GElgotAlgebraM {
  implicit def zip[E[_]: Functor, G[_]: Functor, M[_]: Applicative, F[_]: Functor]:
      Zip[GElgotAlgebraM[E, G, M, F, ?]] =
    new Zip[GElgotAlgebraM[E, G, M, F, ?]] {
      def zip[A, B](
        a: ⇒ GElgotAlgebraM[E, G, M, F, A],
        b: ⇒ GElgotAlgebraM[E, G, M, F, B]) =
        node => (a(node ∘ (_ ∘ (_ ∘ (_._1)))) ⊛ b(node ∘ (_ ∘ (_ ∘ (_._2)))))((_, _))
    }
}

sealed class GElgotCoalgebraM[E[_], G[_], M[_], F[_], A](f: A => M[E[F[G[A]]]])
    extends Function1[A, M[E[F[G[A]]]]] {
  def apply(v1: A) = f(v1)
}

// NB: This class is needed to avoid the `Id[Id[_]]` “cyclic alias” issue.
final class GCoalgebra[G[_], F[_], A](f: A => F[G[A]])
    extends GElgotCoalgebraM[Id, G, Id, F, A](f)

sealed trait ZeroIdInstances {
  implicit def toGElgotAlgebraM[E[_], G[_], M[_], F[_], A](f: E[F[G[A]]] => M[A]): GElgotAlgebraM[E, G, M, F, A] =
    new GElgotAlgebraM[E, G, M, F, A](f)

  implicit def toGElgotAlgebraMOps[E[_], G[_], M[_], F[_], A](
    a: GElgotAlgebraM[E, G, M, F, A]):
      GElgotAlgebraMOps[E, G, M, F, A] =
    new GElgotAlgebraMOps[E, G, M, F, A](a)

  implicit def toGElgotCoalgebraM[E[_], G[_], M[_], F[_], A](f: A => M[E[F[G[A]]]]): GElgotCoalgebraM[E, G, M, F, A] =
    new GElgotCoalgebraM[E, G, M, F, A](f)

}

sealed trait OneIdInstances extends ZeroIdInstances {
  implicit def toGElgotAlgebraOps[E[_], G[_], F[_], A](
    a: GElgotAlgebra[E, G, F, A]):
      GElgotAlgebraOps[E, G, F, A] =
    new GElgotAlgebraOps[E, G, F, A](a)

  implicit def toElgotAlgebraMOps[E[_], M[_], F[_], A](
    a: ElgotAlgebraM[E, M, F, A]):
      ElgotAlgebraMOps[E, M, F, A] =
    new ElgotAlgebraMOps[E, M, F, A](a)

  implicit def toGAlgebraMOps[G[_], M[_], F[_], A](a: GAlgebraM[G, M, F, A]):
      GAlgebraMOps[G, M, F, A] =
    new GAlgebraMOps[G, M, F, A](a)


  implicit def toGElgotCoalgebraOps[E[_], G[_], F[_], A](
    a: GElgotCoalgebra[E, G, F, A]):
      GElgotCoalgebraOps[E, G, F, A] =
    new GElgotCoalgebraOps[E, G, F, A](a)

  implicit def toElgotCoalgebraMOps[E[_], M[_], F[_], A](
    a: ElgotCoalgebraM[E, M, F, A]):
      ElgotCoalgebraMOps[E, M, F, A] =
    new ElgotCoalgebraMOps[E, M, F, A](a)

  implicit def toGCoalgebraMOps[G[_], M[_], F[_], A](a: GCoalgebraM[G, M, F, A]):
      GCoalgebraMOps[G, M, F, A] =
    new GCoalgebraMOps[G, M, F, A](a)
}

sealed trait TwoIdInstances extends OneIdInstances {
  // implicit def toElgotAlgebraOps[W[_], F[_], A](a: ElgotAlgebra[W, F, A]):
  //     ElgotAlgebraOps[W, F, A] =
  //   new ElgotAlgebraOps[W, F, A](a)

  // // implicit def toGAlgebraOps[W[_], F[_], A](a: GAlgebra[W, F, A]):
  // //     GAlgebraOps[W, F, A] =
  // //   new GAlgebraOps[W, F, A](a)

  // implicit def toAlgebraMOps[M[_], F[_], A](a: AlgebraM[M, F, A]):
  //     AlgebraMOps[M, F, A] =
  //   new AlgebraMOps[M, F, A](a)
}

trait ThreeIdInstances extends TwoIdInstances {
  implicit def toAlgebra[F[_], A](f: F[A] => A): Algebra[F, A] =
    new GElgotAlgebraM[Id, Id, Id, F, A](f)

  implicit def toCoalgebra[F[_], A](f: A => F[A]): Coalgebra[F, A] =
    new GCoalgebra[Id, F, A](f)
}
