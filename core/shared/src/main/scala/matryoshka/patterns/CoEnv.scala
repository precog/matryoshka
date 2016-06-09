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

package matryoshka.patterns

import matryoshka._

import scalaz._, Scalaz._

/** The pattern functor for Free. */
final case class CoEnv[E, F[_], A](run: E \/ F[A])
object CoEnv extends CoEnvInstances {
  def coEnv[E, F[_], A](v: E \/ F[A]): CoEnv[E, F, A] = CoEnv(v)

  def freeIso[E, F[_]: Functor] = AlgebraIso[CoEnv[E, F, ?], Free[F, E]](
    coe => coe.run.fold(_.point[Free[F, ?]], Free.roll))(
    fr => CoEnv(fr.fold(_.left, _.right)))
}

sealed abstract class CoEnvInstances extends CoEnvInstances0 {
  implicit def equal[E: Equal, F[_]](
    implicit F: Equal ~> λ[α => Equal[F[α]]]):
      Equal ~> λ[α => Equal[CoEnv[E, F, α]]] =
    new (Equal ~> λ[α => Equal[CoEnv[E, F, α]]]) {
      def apply[α](arb: Equal[α]) = {
        Equal.equal((a, b) => (a.run, b.run) match {
          case (-\/(e1), -\/(e2)) => e1 ≟ e2
          case (\/-(f1), \/-(f2)) => F(arb).equal(f1, f2)
          case (_,       _)       => false
        })
      }
    }

  // TODO: Need to have lower-prio instances of Functor and Foldable, with
  //       corresponding constraints on F.
  implicit def traverse[F[_]: Traverse, A]: Traverse[CoEnv[A, F, ?]] =
    new Traverse[CoEnv[A, F, ?]] {
      def traverseImpl[G[_]: Applicative, R, B](
        fa: CoEnv[A, F, R])(
        f: R => G[B]):
          G[CoEnv[A, F, B]] =
        fa.run.traverse(_.traverse(f)).map(CoEnv(_))
    }

  // TODO: write a test to ensure the two monad instances are identical
  // implicit def monadCo[F[_]: Applicative: Comonad, A]: Monad[CoEnv[A, F, ?]] =
  //   new Monad[CoEnv[A, F, ?]] {
  //     def bind[B, C](fa: CoEnv[A, F, B])(f: (B) ⇒ CoEnv[A, F, C]) =
  //       CoEnv(fa.run >>= (fb => f(fb.copoint).run))
  //     def point[B](x: => B) = CoEnv(x.point[F].right)
  //   }
}

sealed abstract class CoEnvInstances0 {
  // implicit def monad[F[_]: Monad: Traverse, A]: Monad[CoEnv[A, F, ?]] =
  //   new Monad[CoEnv[A, F, ?]] {
  //     def bind[B, C](fa: CoEnv[A, F, B])(f: (B) ⇒ CoEnv[A, F, C]) =
  //       CoEnv(fa.run >>= (_.traverse[CoEnv[A, F, ?], C](f).run.map(_.join)))
  //     def point[B](x: => B) = CoEnv(x.point[F].right)
  //   }
}
