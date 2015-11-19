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
package recursionschemes

import scalaz._

final case class Fix[F[_]](unFix: F[Fix[F]]) {
  override def toString = unFix.toString
}

object Fix {
  implicit val fixRecursive: Recursive[Fix] = new Recursive[Fix] {
    def project[F[_]](t: Fix[F]) = t.unFix
  }

  implicit val fixCorecursive: Corecursive[Fix] = new Corecursive[Fix] {
    def embed[F[_]](t: F[Fix[F]]) = Fix(t)
  }

  implicit def fixRenderTree[F[_]](implicit RF: RenderTree ~> λ[α => RenderTree[F[α]]]):
      RenderTree[Fix[F]] =
    new RenderTree[Fix[F]] {
      def render(v: Fix[F]) =
        RF(fixRenderTree[F]).render(v.unFix).relabel("Fix(" + _ + ")")
    }

  implicit def fixShow[F[_]](implicit F: Show ~> λ[α => Show[F[α]]]):
      Show[Fix[F]] =
    Show.show(f => F(fixShow[F]).show(f.unFix))

  implicit def fixEqual[F[_]](implicit F: Equal ~> λ[α => Equal[F[α]]]):
      Equal[Fix[F]] =
    Equal.equal((a, b) => F(fixEqual[F]).equal(a.unFix, b.unFix))
}
