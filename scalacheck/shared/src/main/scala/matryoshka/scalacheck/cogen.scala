/*
 * Copyright 2014–2018 SlamData Inc.
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

package matryoshka.scalacheck

import slamdata.Predef._
import matryoshka._
import matryoshka.data._
import matryoshka.implicits._
import matryoshka.patterns._

import org.scalacheck._
import scalaz._

trait CogenInstancesʹ {
  implicit def delayCogen[F[_], A](implicit F: Delay[Cogen, F], A: Cogen[A]): Cogen[F[A]] =
    F(A)
}

trait CogenInstances extends CogenInstancesʹ {
  // TODO: Define this using a fold rather than `project`
  @SuppressWarnings(Array("org.wartremover.warts.Recursion"))
  def recursiveCogen[T, F[_]: Functor]
    (implicit T: Recursive.Aux[T, F], F: Delay[Cogen, F])
      : Cogen[T] =
    Cogen((seed, value) => F(recursiveCogen[T, F]).perturb(seed, value.project))

  implicit def fixCogen[F[_]: Functor](implicit F: Delay[Cogen, F]): Cogen[Fix[F]] =
    recursiveCogen[Fix[F], F]
  implicit def muCogen[F[_]: Functor](implicit F: Delay[Cogen, F]): Cogen[Mu[F]] =
    recursiveCogen[Mu[F], F]
  implicit def nuCogen[F[_]: Functor](implicit F: Delay[Cogen, F]): Cogen[Nu[F]] =
    recursiveCogen[Nu[F], F]

  implicit val optionCogen: Delay[Cogen, Option] =
    new Delay[Cogen, Option] {
      def apply[A](a: Cogen[A]) =
        Cogen((seed, value) => value.fold(seed.next)(a.perturb(seed, _)))
    }

  implicit def coEnvCogen[F[_], A: Cogen](implicit F: Delay[Cogen, F]): Delay[Cogen, CoEnv[A, F, ?]] =
    new Delay[Cogen, CoEnv[A, F, ?]] {
      def apply[B](b: Cogen[B]) =
        Cogen((seed, value) => value.run.fold(Cogen[A].perturb(seed, _), F(b).perturb(seed.next, _)))
    }

  implicit def envTCogen[F[_], A: Cogen](implicit F: Delay[Cogen, F]): Delay[Cogen, EnvT[A, F, ?]] =
    new Delay[Cogen, EnvT[A, F, ?]] {
      def apply[B](b: Cogen[B]) =
        Cogen((seed, value) => F(b).perturb(Cogen[A].perturb(seed, value.ask), value.lower))
    }

  implicit def freeCogen[F[_]: Functor](implicit F: Delay[Cogen, F]): Delay[Cogen, Free[F, ?]] =
    new Delay[Cogen, Free[F, ?]] {
      def apply[A](a: Cogen[A]) = {
        implicit val aʹ: Cogen[A] = a
        recursiveCogen[Free[F, A], CoEnv[A, F, ?]]
      }
    }

  implicit def cofreeCogen[F[_]: Functor](implicit F: Delay[Cogen, F]): Delay[Cogen, Cofree[F, ?]] =
    new Delay[Cogen, Cofree[F, ?]] {
      def apply[A](a: Cogen[A]) = {
        implicit val aʹ: Cogen[A] = a
        recursiveCogen[Cofree[F, A], EnvT[A, F, ?]]
      }
    }
}

package object cogen extends CogenInstances
