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

import scalaz._, Scalaz._

final case class Nu[F[_]](unNu: Id ~> F, a: Id[_])
object Nu {
  implicit val recursive: Recursive[Nu] = new Recursive[Nu] {
    def project[F[_]: Functor](t: Nu[F]) = t.unNu(t.a).map(Nu(t.unNu, _))
  }

  implicit val corecursive: Corecursive[Nu] = new Corecursive[Nu] {
    def embed[F[_]: Functor](t: F[Nu[F]]) = colambek(t)
    // FIXME: there are two different `A`s here. How to reconcile?
    // override def ana[F[_]: Functor, A](a: A)(f: A => F[A]) = Nu(new (Id ~> F) {
    //   def apply[A](fa: A): F[A] = f(fa)
    // }, a)
  }

  // implicit def show[F[_]](implicit F: Show ~> λ[α => Show[F[α]]]):
  //     Show[Nu[F]] =
  //   Show.show(f => F(show[F]).show(f.unFix))

  implicit def equal[F[_]: Functor](implicit F: Equal ~> λ[α => Equal[F[α]]]):
      Equal[Nu[F]] =
    Equal.equal((a, b) => a.convertTo[Fix] ≟ b.convertTo[Fix])
}
