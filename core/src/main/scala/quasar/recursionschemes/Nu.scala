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

package quasar.recursionschemes

import quasar.Predef._
// import quasar.RenderTree
import Recursive.ops._

import scalaz._, Scalaz._

final case class Nu[F[_]](unNu: Id ~> F, a: Id[_])
object Nu {
  implicit val nuRecursive: Recursive[Nu] = new Recursive[Nu] {
    def project[F[_]: Functor](t: Nu[F]) = t.unNu(t.a).map(Nu(t.unNu, _))
  }

  implicit val nuCorecursive: Corecursive[Nu] = new Corecursive[Nu] {
    def embed[F[_]: Functor](t: F[Nu[F]]) = colambek(t)
    // FIXME: there are two different `A`s here. How to reconcile?
    // override def ana[F[_]: Functor, A](a: A)(f: A => F[A]) = Nu(new (Id ~> F) {
    //   def apply[A](fa: A): F[A] = f(fa)
    // }, a)
  }

  // implicit def fixRenderTree[F[_]](implicit RF: RenderTree ~> λ[α => RenderTree[F[α]]]):
  //     RenderTree[Fix[F]] =
  //   new RenderTree[Fix[F]] {
  //     def render(v: Fix[F]) =
  //       RF(fixRenderTree[F]).render(v.unFix).retype {
  //         case h :: t => ("Fix:" + h) :: t
  //         case Nil    => "Fix" :: Nil
  //       }
  //   }

  // implicit def fixShow[F[_]](implicit F: Show ~> λ[α => Show[F[α]]]):
  //     Show[Fix[F]] =
  //   Show.show(f => F(fixShow[F]).show(f.unFix))

  implicit def nuEqual[F[_]: Functor](implicit F: Equal ~> λ[α => Equal[F[α]]]):
      Equal[Nu[F]] =
    Equal.equal((a, b) => a.convertTo[Fix] ≟ b.convertTo[Fix])
}
