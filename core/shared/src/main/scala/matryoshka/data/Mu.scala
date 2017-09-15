/*
 * Copyright 2014–2017 SlamData Inc.
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

import scala.Unit

import scalaz._, Scalaz._

/** This is for inductive (finite) recursive structures, models the concept of
  * “data”, aka, the “least fixed point”.
  */
final case class Mu[F[_]](unMu: Algebra[F, ?] ~> Id)

object Mu {
  implicit def birecursiveT: BirecursiveT[Mu] = new BirecursiveT[Mu] {
    // FIXME: ugh, shouldn’t have to redefine `lambek` in here?
    def projectT[F[_]: Functor](t: Mu[F]) =
      cataT[F, F[Mu[F]]](t)(_ ∘ embedT[F])
    override def cataT[F[_]: Functor, A](t: Mu[F])(f: Algebra[F, A]) = t.unMu(f)

    def embedT[F[_]: Functor](t: F[Mu[F]]) =
      Mu(new (Algebra[F, ?] ~> Id) {
        def apply[A](fa: Algebra[F, A]): A = fa(t.map(cataT(_)(fa)))
      })
  }

  implicit val equalT: EqualT[Mu] = EqualT.recursiveT

  // TODO: Use OrderT
  implicit def order[F[_]: Traverse](implicit F: Order[F[Unit]]): Order[Mu[F]] =
    Birecursive.order[Mu[F], F]

  implicit val showT: ShowT[Mu] = ShowT.recursiveT
}
