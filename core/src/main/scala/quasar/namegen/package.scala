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

import scalaz._, Scalaz._

package object namegen {

  final case class NameGen(nameGen: Int)

  implicit val NameGenMonoid: Monoid[NameGen] = new Monoid[NameGen] {
    def zero = NameGen(0)
    def append(f1: NameGen, f2: => NameGen) = NameGen(f1.nameGen max f2.nameGen)
  }

  def freshName(label: String): State[NameGen, String] = for {
    n <- State((s: NameGen) => s.copy(nameGen = s.nameGen + 1) -> s.nameGen)
  } yield "__" + label + n.toString

  type NameT[M[_], A] = StateT[M, NameGen, A]
  type NameDisj[E, A] = NameT[E \/ ?, A]

  class LiftHelper[F[_]] {
    def apply[A](v: F[A])(implicit F: Functor[F]) =
      StateT[F, NameGen, A](s => F.map(v)(s -> _))
  }

  def lift[F[_]] = new LiftHelper[F]

  def emit[F[_]: Applicative, A](v: A): NameT[F, A] =
    lift(v.point[F])
  def emitName[F[_]: Applicative, A](v: State[NameGen, A]): NameT[F, A] =
    StateT[F, NameGen, A](s => v.run(s).point[F])
}
