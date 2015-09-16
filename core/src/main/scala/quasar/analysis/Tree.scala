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
import quasar.fp._

import scalaz.{Tree => ZTree, Validation, Semigroup, NonEmptyList, Foldable1, Show, Cord}
import scalaz.Validation.FlatMap._
import scalaz.std.vector._
import scalaz.syntax.foldable1._
import scalaz.syntax.traverse._

trait Tree[N] { self =>
  def root: N

  def children(node: N): List[N]

  final def parent(node: N): Option[N] = Option(parentMap.get(node))

  final def siblings(node: N): List[N] = Option(siblingMap.get(node)).getOrElse(Nil)

  final def isLeaf(node: N): Boolean = children(node).isEmpty

  final def fork[Z, E: Semigroup](initial: Z)(f: (Z, N) => Validation[E, Z]): Validation[E, Vector[Z]] = {
    def fork0(acc0: Z, node: N): Validation[E, Vector[Z]] = {
      f(acc0, node).fold(
        Validation.failure,
        acc => {
          val children = self.children(node)

          if (children.length == 0) Validation.success(Vector(acc))
          else children.toVector.map(child => fork0(acc, child)).sequence[({type V[X] = Validation[E, X]})#V, Vector[Z]].map(_.flatten)
        }
      )
    }

    fork0(initial, root)
  }

  final def join[Z: Semigroup, E: Semigroup](initial: Z)(f: (Z, N) => Validation[E, Z]): Validation[E, Z] = {
    def join0: Z => N => Validation[E, Z] = (acc: Z) => (node: N) => {
      val children = self.children(node)

      (children.headOption.map { head =>
        val children2 = NonEmptyList.nel(head, children.drop(1))

        children2.foldMap1(join0(acc)).flatMap(f(_, node))
      }).getOrElse(f(acc, node))
    }

    join0(initial)(root)
  }

  final def subtree(node: N) = new Tree[N] {
    def root = node

    def children(node: N) = self.children(node)
  }

  final def foldDown[Z](acc: Z)(f: (Z, N) => Z): Z = {
    def foldDown0(acc: Z, node: N): Z = children(node).foldLeft(f(acc, node))(foldDown0 _)

    foldDown0(acc, root)
  }

  final def foldUp[Z](acc: Z)(f: (Z, N) => Z): Z = {
    def foldUp0(acc: Z, node: N): Z = f(children(node).foldLeft(acc)(foldUp0 _), node)

    foldUp0(acc, root)
  }

  final def collect[Z](f: PartialFunction[N, Z]): List[Z] = {
    val lifted = f.lift

    foldUp(List.empty[Z]) {
      case (acc, n) => lifted(n).map(_ :: acc).getOrElse(acc)
    }
  }

  lazy val nodes: List[N] = foldDown(List.empty[N])((acc, n) => n :: acc)

  final def annotate[B](f: N => B): AnnotatedTree[N, B] = AnnotatedTree[N, B](root, self.children _, f)

  lazy val leaves: List[N] = nodes.filter(v => !isLeaf(v)).toList

  // TODO: Change from identity map once we implement Node properly...
  private lazy val parentMap: java.util.Map[N, N] = (foldDown(new java.util.IdentityHashMap[N, N]) {
    case (parentMap, parentNode) =>
      self.children(parentNode).foldLeft(parentMap) {
        case (parentMap, childNode) =>
          ignore(parentMap.put(childNode, parentNode))
          parentMap
      }
  })

  // TODO: Ditto
  private lazy val siblingMap: java.util.Map[N, List[N]] = foldDown(new java.util.IdentityHashMap[N, List[N]]) {
    case (siblingMap, parentNode) =>
      val children = self.children(parentNode)

      children.foldLeft(siblingMap) {
        case (siblingMap, childNode) =>
          ignore(siblingMap.put(childNode, children.filter(_ != childNode)))
          siblingMap
      }
  }
}

trait TreeInstances {
  implicit def ShowTree[N: Show] = new Show[Tree[N]] {
    override def show(v: Tree[N]) = {
      def toTree(node: N): ZTree[N] = ZTree.node(node, v.children(node).toStream.map(toTree _))

      Cord(toTree(v.root).drawTree)
    }
  }
}

object Tree extends TreeInstances {
  def apply[N](root0: N, children0: N => List[N]): Tree[N] = new Tree[N] {
    def root: N = root0

    def children(node: N): List[N] = children0(node)
  }
}
