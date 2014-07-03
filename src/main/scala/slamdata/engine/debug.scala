package slamdata.engine

import scalaz._
import Scalaz._

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
  
  def showGraphviz[A](a: A)(implicit RA: RenderTree[A]): Cord = {
    /*
    digraph G {
      n1 [label="foo, bar"];
    
      n1 -> n2;
    }
    */

    def nodeName: State[Int, String] =
      for {
        i <- get
        _ <- put(i+1)
      } yield "n" + i

    case class Foo(name: String, dot: Cord)
    
    def render(t: RenderedTree): State[Int, Foo] = {
      def sequence(lst: List[State[Int, Foo]]): State[Int, List[Foo]] = 
        
      
      def decl(name: String) = Cord(name) ++ "[label=\"" ++ t.label ++ "\"];\n"

      for {
        n <- nodeName
        c <- t match {
          case Terminal(_) => state[Int, Cord](Cord(""))
          case NonTerminal(_, children) => {
            val sts1: List[State[Int, Foo]] = children.map(render(_))
            val sts: State[Int, List[Foo]] = sequence(sts1)
            // state[Int, Cord](Cord("<children>\n"))
          }
        }
      } yield Foo(n, decl(n) ++ c)
    }

    val tree = RA.render(a)
    val program = for {
      foo <- render(tree)
    } yield Cord("digraph G {\n") ++ foo.dot ++ Cord("}")
    
    program.eval(0)
  }
}
