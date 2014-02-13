package slamdata.engine.analysis

import scalaz._

import scalaz.syntax.traverse._

import scalaz.std.vector._
import scalaz.std.list._

sealed trait AnnotatedTree[N, A] { self =>
  def root: N

  def children(node: N): List[N]

  def attr(node: N): A

  final def parent(node: N): Option[N] = parentMap.get(node)

  final def siblings(node: N): List[N] = siblingMap.get(node).getOrElse(Nil)

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
        val children2 = NonEmptyList.nel(head, children.tail)

        Foldable1[NonEmptyList].foldMap1(children2)(join0(acc)).flatMap(acc => f(acc, node))
      }).getOrElse(f(acc, node))
    }

    join0(initial)(root)
  }

  final def foldDown[Z](acc: Z)(f: (Z, N) => Z): Z = {
    def foldDown0(acc: Z, node: N): Z = children(node).foldLeft(f(acc, node))(foldDown0 _)

    foldDown0(acc, root)
  }

  final def foldUp[Z](acc: Z)(f: (Z, N) => Z): Z = {
    def foldUp0(acc: Z, node: N): Z = f(children(node).foldLeft(acc)(foldUp0 _), node)

    foldUp0(acc, root)
  }

  lazy val nodes: List[N] = foldDown(List.empty[N])((acc, n) => n :: acc)

  final def annotate[B](f: N => B): AnnotatedTree[N, B] = new AnnotatedTree[N, B] {
    def root = self.root

    def children(node: N): List[N] = self.children(node)

    def attr(node: N): B = f(node)
  }

  lazy val leaves: List[N] = nodes.filter(v => !isLeaf(v)).toList

  private lazy val parentMap: Map[N, N] = foldDown(Map.empty[N, N]) {
    case (parentMap, parentNode) =>
      self.children(parentNode).foldLeft(parentMap) {
        case (parentMap, childNode) =>
          parentMap + (parentNode -> childNode)
      }
  }

  private lazy val siblingMap: Map[N, List[N]] = foldDown(Map.empty[N, List[N]]) {
    case (siblingMap, parentNode) =>
      val children = self.children(parentNode)

      children.foldLeft(siblingMap) {
        case (siblingMap, childNode) =>
          siblingMap + (childNode -> children.filter(_ != childNode))
      }
  }
}

object AnnotatedTree {
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