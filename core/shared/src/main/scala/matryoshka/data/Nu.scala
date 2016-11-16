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

package matryoshka.data

import matryoshka._

import scalaz._, Scalaz._

/** This is for coinductive (potentially infinite) recursive structures, models
  * the concept of “codata”, aka, the “greatest fixed point”.
  */
sealed abstract class Nu[F[_]] {
  type A
  val a: A
  val unNu: Coalgebra[F, A]
}
object Nu {
  def apply[F[_], B](f: Coalgebra[F, B], b: B): Nu[F] =
    new Nu[F] {
      type A = B
      val a = b
      val unNu = f
    }

  implicit def recursiveT: RecursiveT[Nu] = new RecursiveT[Nu] {
    def projectT[F[_]: Functor](t: Nu[F]) = t.unNu(t.a).map(Nu(t.unNu, _))
  }

  implicit def corecursiveT: CorecursiveT[Nu] = new CorecursiveT[Nu] {
    // FIXME: ugh, shouldn’t have to redefine `colambek` in here?
    def embedT[F[_]: Functor](t: F[Nu[F]]) = anaT(t)(_ ∘ recursiveT.projectT[F])
    override def anaT[F[_]: Functor, A](a: A)(f: A => F[A]) = Nu(f, a)
  }

  implicit def recursive[F[_]]: Recursive.Aux[Nu[F], F] =
    RecursiveT.recursive[Nu, F]

  implicit def corecursive[F[_]]: Corecursive.Aux[Nu[F], F] =
    CorecursiveT.corecursive[Nu, F]

  implicit def equal[F[_]: Functor](implicit F: Delay[Equal, F]): Equal[Nu[F]] =
    Recursive.equal[Nu[F], F]

  implicit def show[F[_]: Functor](implicit F: Delay[Show, F]): Show[Nu[F]] =
    Recursive.show[Nu[F], F]
}
