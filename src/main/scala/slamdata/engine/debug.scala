package slamdata.engine

import scalaz._

sealed trait RenderedNode {
  def asTree: Tree[RenderedNode]
  
  def showTree: Cord = Cord(asTree.drawTree(RenderedNode.ShowRenderedNode))
}
case class Terminal(label: Cord) extends RenderedNode {
  override def asTree = Tree.leaf(this)
}
case class NonTerminal(label: Cord, children: List[RenderedNode]) extends RenderedNode {
  override def asTree: Tree[RenderedNode] = Tree.node(this, children.toStream.map(_.asTree))
}

object RenderedNode {
  val ShowRenderedNode = new Show[RenderedNode] {
    override def show(v: RenderedNode) = v match {
      case Terminal(label)       => label
      case NonTerminal(label, _) => label
    }
  }
}

trait NodeRenderer {
  def showTree: Cord
}

