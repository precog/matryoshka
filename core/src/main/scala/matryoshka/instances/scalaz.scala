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

package matryoshka.instances.scalaz

import matryoshka._

import scalaz._, Scalaz._

trait IdInstances {
  /** This is a single (low-priority) instance to provide folds/unfolds for all
    * all non-recursive data types.
    */
  // NB: This should really be available even without an additional dependency,
  //     but [[scalaz.Const]] only exists in Scalaz (and Cats).
  def idMatryoshka[A]: Recursive[A] with Corecursive[A] =
    new Recursive[A] with Corecursive[A] {
      type Base[B] = Const[A, B]
      def project(t: A) = Const(t)
      def embed(t: Const[A, A]) = t.getConst
    }
}

object id extends IdInstances

trait MaybeInstances {
  implicit def maybeMatryoshka[A]:
      Recursive[Maybe[A]] with Corecursive[Maybe[A]] =
    id.idMatryoshka[Maybe[A]]
}

object maybe extends MaybeInstances

trait EitherInstances {
  implicit def eitherMatryoshka[A, B]:
      Recursive[A \/ B] with Corecursive[A \/ B] =
    id.idMatryoshka[A \/ B]
}

object either extends EitherInstances

trait IListInstances {
  implicit def ilistMatryoshka[A]:
      Recursive[IList[A]] with Corecursive[IList[A]] =
    new Recursive[IList[A]] with Corecursive[IList[A]] {
      type Base[B] = ListF[A, B]
      def project(t: IList[A]) = t match {
        case ICons(h, t) => ConsF(h, t)
        case INil()      => NilF[A, IList[A]]()
      }
      def embed(t: ListF[A, IList[A]]) = t match {
        case ConsF(h, t) => ICons(h, t)
        case NilF()      => INil[A]
      }
    }
}

object ilist extends IListInstances

trait CofreeInstances {
  implicit def cofreeMatryoshka[F[_], A]:
      Recursive[Cofree[F, A]] with Corecursive[Cofree[F, A]] =
    new Recursive[Cofree[F, A]] with Corecursive[Cofree[F, A]] {
      type Base[B] = EnvT[A, F, B]
      def project(t: Cofree[F, A]) = EnvT((t.head, t.tail))
      def embed(t: EnvT[A, F, Cofree[F, A]]) = Cofree(t.ask, t.lower)
    }

  implicit def envTShow[F[_], A: Show, B](
    implicit F: Show ~> λ[α => Show[F[α]]]):
      Show ~> λ[α => Show[EnvT[A, F, α]]] =
    new (Show ~> λ[α => Show[EnvT[A, F, α]]]) {
      def apply[α](show: Show[α]) =
        Show.shows(e => "(" + e.ask.shows + ", " + F(show).shows(e.lower) + ")")
    }

  // implicit def cofreeShow[F[_], A: Show](
  //   implicit F: (Show ~> λ[α => Show[F[α]]])):
  //     Show[Cofree[F, A]] =
  //   Recursive.show[Cofree[F, A], EnvT[A, F, ?]]
}

object cofree extends CofreeInstances

trait FreeInstances {
  // TODO: Remove the Functor constraint when we upgrade to Scalaz 7.2
  implicit def freeMatryoshka[F[_]: Functor, A]:
      Recursive[Free[F, A]] with Corecursive[Free[F, A]] =
    new Recursive[Free[F, A]] with Corecursive[Free[F, A]] {
      type Base[B] = CoEnv[A, F, B]
      def project(t: Free[F, A]) = CoEnv(t.resume.swap)
      def embed(t: CoEnv[A, F, Free[F, A]]) =
        t.run.fold(_.point[Free[F, ?]], Free.liftF(_).join)
    }
}

object free extends FreeInstances

object scalazMeh extends MaybeInstances with EitherInstances with IListInstances with CofreeInstances with FreeInstances
