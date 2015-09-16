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

package quasar.analysis

import quasar.Predef._
import quasar.{RenderTree, Terminal, NonTerminal, RenderedTree}

import scalaz.{Tree => ZTree, Validation, Semigroup, NonEmptyList, Foldable1, Show, Cord}

sealed trait AnnotatedTree[N, A] extends Tree[N] { self =>
  def attr(node: N): A
}

trait AnnotatedTreeInstances {
  implicit def AnnotatedTreeRenderTree[N, A](implicit RN: RenderTree[N], RA: RenderTree[A]) = new RenderTree[AnnotatedTree[N, A]] {
    def render(t: AnnotatedTree[N, A]) = {
      def renderNode(n: N): RenderedTree = {
        val r = RN.render(n)
        NonTerminal(r.nodeType, r.label,
          RA.render(t.attr(n)).copy(nodeType=List("Annotation"), label=None) ::
            t.children(n).map(renderNode(_)))
      }

      renderNode(t.root)
    }
  }
}

object AnnotatedTree extends AnnotatedTreeInstances {
  def unit[N](root0: N, children0: N => List[N]): AnnotatedTree[N, Unit] = const(root0, children0, ())

  def const[N, A](root0: N, children0: N => List[N], const: A): AnnotatedTree[N, A] = new AnnotatedTree[N, A] {
    def root: N = root0

    def children(node: N): List[N] = children0(node)

    def attr(node: N): A = const
  }

  def apply[N, A](root0: N, children0: N => List[N], attr0: N => A): AnnotatedTree[N, A] = new AnnotatedTree[N, A] {
    def root: N = root0

    def children(node: N): List[N] = children0(node)

    def attr(node: N): A = attr0(node)
  }
}
