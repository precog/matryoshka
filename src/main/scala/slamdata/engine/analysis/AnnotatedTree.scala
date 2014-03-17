package slamdata.engine.analysis

import scalaz.{Tree => ZTree, Validation, Semigroup, NonEmptyList, Foldable1, Show, Cord}

import scalaz.syntax.traverse._

import scalaz.std.vector._
import scalaz.std.list._
import scalaz.std.tuple._

import scala.collection.JavaConverters._

sealed trait AnnotatedTree[N, A] extends Tree[N] { self =>
  def attr(node: N): A
}

trait AnnotatedTreeInstances {
  implicit def ShowAnnotatedTree[N: Show, A: Show] = new Show[AnnotatedTree[N, A]] {
    override def show(v: AnnotatedTree[N, A]) = {
      def toTree(node: N): ZTree[(N, A)] = ZTree.node((node, v.attr(node)), v.children(node).toStream.map(toTree _))

      Cord(toTree(v.root).drawTree)
    }
  }
}

object AnnotatedTree extends AnnotatedTreeInstances {
  def unit[N](root0: N, children0: N => List[N]): AnnotatedTree[N, Unit] = const(root0, children0, Unit)

  def const[N, A](root0: N, children0: N => List[N], const: A): AnnotatedTree[N, A] = new AnnotatedTree[N, A] {
    def root: N = root0

    def children(node: N): List[N] = children0(node)

    def attr(node: N): A = const
  }

  def apply[N, A](root0: N, children0: N => List[N], attr: N => A): AnnotatedTree[N, A] = new AnnotatedTree[N, A] {
    def root: N = root0

    def children(node: N): List[N] = children0(node)

    def attr(node: N): A = attr(node)
  }
}