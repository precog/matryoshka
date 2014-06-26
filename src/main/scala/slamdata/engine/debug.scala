package slamdata.engine

import scalaz._

sealed trait RenderedNode {
  def label: Cord
}
case class Terminal(label: Cord) extends RenderedNode
case class NonTerminal(label: Cord, children: List[RenderedNode]) extends RenderedNode

object RenderedNode {
  def relabel(node: RenderedNode, label: Cord) = node match {
    case Terminal(_)                     => Terminal(label)
    case NonTerminal(oldLabel, children) => NonTerminal(label, children)
  }
}


trait NodeRenderer[A] {
  def render(a: A): RenderedNode
}

object ShowTree {
  def showTree[A: NodeRenderer](a: A): Cord = {
    val ShowLabel = new Show[RenderedNode] {
      override def show(v: RenderedNode) = v match {
        case Terminal(label)       => label
        case NonTerminal(label, _) => label
      }
    }

    def asTree(node: RenderedNode): Tree[RenderedNode] = node match {
      case Terminal(_)              => Tree.leaf(node)
      case NonTerminal(_, children) => Tree.node(node, children.toStream.map(asTree))
    }

    val nr = implicitly[NodeRenderer[A]]

    Cord(asTree(nr.render(a)).drawTree(ShowLabel))
  }
}
