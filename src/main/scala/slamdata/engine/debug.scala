package slamdata.engine

import scalaz._
import Scalaz._

sealed trait RenderedTree {
  def label: String

  def relabel(label: String): RenderedTree = relabel(_ => label)

  def relabel(f: String => String): RenderedTree = this match {
    case Terminal(_)                     => Terminal(f(label))
    case NonTerminal(oldLabel, children) => NonTerminal(f(label), children)
  }

  /**
   A tree that describes differences between two trees:
   - If the two trees are identical, the result is the same as (either) input.
   - If the trees differ only in the labels on nodes, then the result has those
      nodes decorated with "[Changed] old -> new".
   - If a single node is unmatched on either side, it is decorated with "[Added]"
      or "[Deleted]".
   As soon as a difference is found and decorated, the subtree(s) beneath the
   decorated nodes are not inspected.
   */
  def diff(that: RenderedTree): RenderedTree = (this, that) match {
    case (Terminal(l1), Terminal(l2)) if l1 != l2 => Terminal("[Changed] " + l1 + " -> " + l2)
    case (Terminal(l1), Terminal(l2))             => this

    case (NonTerminal(l1, children1), NonTerminal(l2, children2)) => {
      val label = if (l1 != l2) "[Changed] " + l1 + " -> " + l2 else l1
      def matchChildren(children1: List[RenderedTree], children2: List[RenderedTree]): List[RenderedTree] = (children1, children2) match {
        case (Nil, Nil)     => Nil
        case (x :: xs, Nil) => x.relabel("[Deleted] " + _) :: matchChildren(xs, Nil)
        case (Nil, x :: xs) => x.relabel("[Added] " + _) :: matchChildren(Nil, xs)

        case (a :: as, b :: bs)        if a.label == b.label  => a.diff(b) :: matchChildren(as, bs)
        case (a1 :: a2 :: as, b :: bs) if a2.label == b.label => a1.relabel("[Deleted] " + _) :: a2.diff(b) :: matchChildren(as, bs)
        case (a :: as, b1 :: b2 :: bs) if a.label == b2.label => b1.relabel("[Added] " + _) :: a.diff(b2) :: matchChildren(as, bs)

        case (Terminal(al) :: as, Terminal(bl) :: bs) => Terminal("[Changed] " + al + " -> " + bl) :: matchChildren(as, bs)
        case (NonTerminal(al, ac) :: as, NonTerminal(bl, bc) :: bs) if ac == bc => NonTerminal("[Changed] " + al + " -> " + bl, ac) :: matchChildren(as, bs)

        // Note: will get here if more than one node is added/deleted:
        case (a :: as, b :: bs) => a.relabel("[Deleted] " + _) :: b.relabel("[Added] " + _) :: matchChildren(as, bs)
      }
      NonTerminal(label, matchChildren(children1, children2))
    }

    // Terminal/non-terminal mis-match (currently not handled well):
    case (l, r) => NonTerminal("[Unmatched]", l.relabel("[Old] " + _) :: r.relabel("[New] " + _) :: Nil)
  }
}
object RenderedTree {
  implicit val RenderedTreeShow = new Show[RenderedTree] {
    override def show(t: RenderedTree) = {
      def asTree(node: RenderedTree): Tree[RenderedTree] = node match {
        case Terminal(_)              => Tree.leaf(node)
        case NonTerminal(_, children) => Tree.node(node, children.toStream.map(asTree))
      }

      val ShowLabel = new Show[RenderedTree] {
        override def show(v: RenderedTree) = v match {
          case Terminal(label)       => label
          case NonTerminal(label, _) => label
        }
      }

      Cord(asTree(t).drawTree(ShowLabel))
    }
  }
}
case class Terminal(label: String) extends RenderedTree
case class NonTerminal(label: String, children: List[RenderedTree]) extends RenderedTree

trait RenderTree[A] {
  def render(a: A): RenderedTree
}
object RenderTree {
  def apply[A](implicit RA: RenderTree[A]) = RA

  def show[A](a: A)(implicit RA: RenderTree[A]): Cord = Show[RenderedTree].show(RA.render(a))

  def showGraphviz[A](a: A)(implicit RA: RenderTree[A]): Cord = {
    def nodeName: State[Int, String] =
      for {
        i <- get
        _ <- put(i+1)
      } yield "n" + i

    case class Node(name: String, dot: Cord)

    def render(t: RenderedTree): State[Int, Node] = {
      def escape(str: String) = str.replace("\\\\", "\\\\").replace("\"", "\\\"")
      
      def decl(name: String) = Cord("  ") ++ name ++ "[label=\"" ++ escape(t.label.toString) ++ "\"];\n"

      for {
        n <- nodeName
        cc <- t match {
          case Terminal(_) => state[Int, Cord](Cord(""))
          case NonTerminal(_, children) => {
            for {
              nodes <- children.map(render(_)).sequenceU
            } yield nodes.map(cn => Cord("  ") ++ n ++ " -> " ++ cn.name ++ ";\n" ++ cn.dot).reduce(_++_)
          }
        }
      } yield Node(n, decl(n) ++ cc)
    }

    val tree = RA.render(a)
    
    val program = for {
      foo <- render(tree)
    } yield Cord("digraph G {\n") ++ foo.dot ++ Cord("}")
    
    program.eval(0)
  }
}
