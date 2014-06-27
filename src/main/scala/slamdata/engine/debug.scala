package slamdata.engine

import scalaz._

sealed trait RenderedTree {
  def label: Cord

  def relabel(label: Cord) = this match {
    case Terminal(_)                     => Terminal(label)
    case NonTerminal(oldLabel, children) => NonTerminal(label, children)
  }
}
case class Terminal(label: Cord) extends RenderedTree
case class NonTerminal(label: Cord, children: List[RenderedTree]) extends RenderedTree

trait RenderTree[A] {
  def render(a: A): RenderedTree
}

object RenderTree {
  def apply[A](implicit RA: RenderTree[A]) = RA

  def show[A](a: A)(implicit RA: RenderTree[A]): Cord = {
    val ShowLabel = new Show[RenderedTree] {
      override def show(v: RenderedTree) = v match {
        case Terminal(label)       => label
        case NonTerminal(label, _) => label
      }
    }

    def asTree(node: RenderedTree): Tree[RenderedTree] = node match {
      case Terminal(_)              => Tree.leaf(node)
      case NonTerminal(_, children) => Tree.node(node, children.toStream.map(asTree))
    }

    Cord(asTree(RA.render(a)).drawTree(ShowLabel))
  }
}
