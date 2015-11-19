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

import quasar.Predef._
import RenderTree.ops._

import scalaz._
import scalaz.syntax.functor._

trait CofreeInstances {
  implicit def cofreeRecursive[A]: Recursive[Cofree[?[_], A]] =
    new Recursive[Cofree[?[_], A]] {
      def project[F[_]](t: Cofree[F, A]) = t.tail
    }

  implicit def cofreeTraverseT[A]: TraverseT[Cofree[?[_], A]] =
    new TraverseT[Cofree[?[_], A]] {
      def traverse[M[_]: Applicative, F[_], G[_]](t: Cofree[F, A])(f: F[Cofree[F, A]] => M[G[Cofree[G, A]]]) =
        f(t.tail).map(Cofree(t.head, _))
    }

  implicit def cofreeRenderTree[F[_], A: RenderTree](implicit RF: RenderTree ~> λ[α => RenderTree[F[α]]]):
      RenderTree[Cofree[F, A]] =
    new RenderTree[Cofree[F, A]] {
      def render(t: Cofree[F, A]) = {
        NonTerminal(List("Cofree"), None, List(t.head.render, RF(cofreeRenderTree[F, A]).render(t.tail)))
      }
    }
}

object cofree extends CofreeInstances
