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

package quasar.fp

import quasar.Predef._

import scalaz._
import Scalaz._

/**
 * This is the transformer for the (,) comonad.
 */
final case class EnvT[E, W[_], A](run: (E, W[A])) { self =>
  import EnvT._

  def runEnvT: (E, W[A]) = run

  def ask: E = run._1

  def lower: W[A] = run._2

  def map[B](f: A => B)(implicit W: Functor[W]): EnvT[E, W, B] =
    envT((run._1, run._2.map(f)))
}

object EnvT extends EnvTInstances with EnvTFunctions

sealed abstract class EnvTInstances0 {
  implicit def envTFunctor[E, W[_]](implicit W0: Functor[W]): Functor[EnvT[E, W, ?]] = new EnvTFunctor[E, W] {
    implicit def W = W0
  }
}

sealed abstract class EnvTInstances extends EnvTInstances0 {
  implicit def envTComonad[E, W[_]](implicit W0: Comonad[W]):
      Comonad[EnvT[E, W, ?]] =
    new EnvTComonad[E, W] { implicit def W = W0 }
}

trait EnvTFunctions {
  def envT[E, W[_], A](v: (E, W[A])): EnvT[E, W, A] = EnvT(v)
}

//
// Type class implementation traits
//

private trait EnvTFunctor[E, W[_]] extends Functor[EnvT[E, W, ?]] {
  implicit def W: Functor[W]

  override def map[A, B](fa: EnvT[E, W, A])(f: A => B) = fa map f
}

private trait EnvTComonad[E, W[_]] extends Comonad[EnvT[E, W, ?]] with EnvTFunctor[E, W] {
  implicit def W: Comonad[W]

  override def cojoin[A](fa: EnvT[E, W, A]): EnvT[E, W, EnvT[E, W, A]] =
    EnvT((fa.ask, fa.lower.cobind(x => EnvT((fa.ask, x)))))

  def cobind[A, B](fa: EnvT[E, W, A])(f: EnvT[E, W, A] => B): EnvT[E, W, B] =
    cojoin(fa).map(f)

  def copoint[A](p: EnvT[E, W, A]): A = p.lower.copoint

}
