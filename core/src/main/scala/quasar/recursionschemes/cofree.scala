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

object cofree {
  implicit def cofreeRecursive[A]: Recursive[Cofree[?[_], A]] =
    new Recursive[Cofree[?[_], A]] {
      def project[F[_]](t: Cofree[F, A]) = t.tail
    }

  implicit def cofreeTraverseT[A]: TraverseT[Cofree[?[_], A]] =
    new TraverseT[Cofree[?[_], A]] {
      def traverse[M[_]: Applicative, F[_], G[_]](t: Cofree[F, A])(f: F[Cofree[F, A]] => M[G[Cofree[G, A]]]) =
        f(t.tail).map(Cofree(t.head, _))
    }

  implicit def cofreeRenderTree[F[_]: Foldable, A: RenderTree](implicit RF: RenderTree[F[_]]) =
    new RenderTree[Cofree[F, A]] {
      def render(attr: Cofree[F, A]) = {
        val term = RF.render(attr.tail)
        val ann = attr.head.render
        NonTerminal(term.nodeType, term.label,
          (if (ann.children.isEmpty)
            NonTerminal(List("Annotation"), None, ann :: Nil)
          else ann.copy(label=None, nodeType=List("Annotation"))) ::
            Recursive[Cofree[?[_], A]].children(attr).map(render(_)))
      }
    }
}
