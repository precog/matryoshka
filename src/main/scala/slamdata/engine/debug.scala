package slamdata.engine

import scalaz._
import Scalaz._

import argonaut._
import Argonaut._

case class RenderedTree(label: String, children: List[RenderedTree], nodeType: List[String]) {
  def relabel(label: String): RenderedTree = relabel(_ => label)

  def relabel(f: String => String): RenderedTree = copy(label = f(label))

  def simpleType: Option[String] = nodeType.lastOption

  /**
   A tree that describes differences between two trees:
   - If the two trees are identical, the result is the same as (either) input.
   - If the trees differ only in the labels on nodes, then the result has those
      nodes decorated with "[Changed] old -> new".
   - If a single node is unmatched on either side, it is decorated with "[Added]"
      or "[Deleted]".
   As soon as a difference is found and decorated, the subtree(s) beneath the
   decorated nodes are not inspected.

   Node types are not compared or necessarily preserved.
   */
  def diff(that: RenderedTree): RenderedTree = {
    def prefixedType(t: RenderedTree, p: String): List[String] = t.nodeType.reverse match {
      case last :: rest => ((p + " " + last) :: rest).reverse
      case Nil          => p :: Nil
    }

    def prefixType(t: RenderedTree, p: String): RenderedTree = t.copy(nodeType = prefixedType(t, p))

    (this, that) match {
      case (RenderedTree(l1, children1, nodeType1), RenderedTree(l2, children2, nodeType2)) => {
        val (newLabel, newType) = if (l1 != l2) ((l1 + " -> " + l2) -> prefixedType(this, "[Changed]")) else (label -> nodeType1)
        def matchChildren(children1: List[RenderedTree], children2: List[RenderedTree]): List[RenderedTree] = (children1, children2) match {
          case (Nil, Nil)     => Nil
          case (x :: xs, Nil) => prefixType(x, "[Deleted]") :: matchChildren(xs, Nil)
          case (Nil, x :: xs) => prefixType(x, "[Added]") :: matchChildren(Nil, xs)

          case (a :: as, b :: bs)        if a.label == b.label  => a.diff(b) :: matchChildren(as, bs)
          case (a1 :: a2 :: as, b :: bs) if a2.label == b.label => prefixType(a1, "[Deleted]") :: a2.diff(b) :: matchChildren(as, bs)
          case (a :: as, b1 :: b2 :: bs) if a.label == b2.label => prefixType(b1, "[Added]") :: a.diff(b2) :: matchChildren(as, bs)

          case (RenderedTree(al, Nil, _) :: as, RenderedTree(bl, Nil, _) :: bs)           => RenderedTree(al + " -> " + bl, Nil, "[Changed]" :: Nil) :: matchChildren(as, bs)
          case (RenderedTree(al, ac, _) :: as, RenderedTree(bl, bc, _) :: bs) if ac == bc => RenderedTree(al + " -> " + bl, ac, "[Changed]" :: Nil) :: matchChildren(as, bs)

          // Note: will get here if more than one node is added/deleted:
          case (a :: as, b :: bs) => prefixType(a, "[Deleted]") :: prefixType(b, "[Added]") :: matchChildren(as, bs)
        }
        RenderedTree(newLabel, matchChildren(children1, children2), newType)
      }

      // Terminal/non-terminal mis-match (currently not handled well):
      case (l, r) => RenderedTree("[Unmatched]", prefixType(l, "[Old]") :: prefixType(r, "[New]") :: Nil, Nil)
    }
  }

  /** 
  A 2D String representation of this Tree, separated into lines. Based on 
  scalaz Tree's show, but improved to use a single line per node, use 
  unicode box-drawing glyphs, and to handle newlines in the rendered 
  nodes.
  */
  def draw: Stream[String] = {
    def drawSubTrees(s: List[RenderedTree]): Stream[String] = s match {
      case Nil      => Stream.Empty
      case t :: Nil => shift("╰─ ", "   ", t.draw)
      case t :: ts  => shift("├─ ", "│  ", t.draw) append drawSubTrees(ts)
    }
    def shift(first: String, other: String, s: Stream[String]): Stream[String] =
      (first #:: Stream.continually(other)).zip(s).map {
        case (a, b) => a + b
      }

    typeAndLabel.split("\n").toStream ++ drawSubTrees(children)
  }
  
  def typeAndLabel: String = (simpleType, label) match {
    case (None, label) => label
    case (Some(simpleType), "") => simpleType
    case (Some(simpleType), label) => simpleType + "(" + label + ")"
  }
}
object RenderedTree {
  implicit val RenderedTreeShow = new Show[RenderedTree] {
    override def show(t: RenderedTree) = {
      t.draw.mkString("\n")
    }
  }

  implicit val RenderedTreeEncodeJson: EncodeJson[RenderedTree] = EncodeJson {
    case RenderedTree(label, children, nodeType) =>
      Json.obj((
        (nodeType match {
          case Nil => None
          case _   => Some("type" := nodeType.mkString("/"))
        }) ::
        Some("label" := label) ::
        {
          if (children.empty) None
          else Some("children" := children.map(RenderedTreeEncodeJson.encode(_)))
        } ::
        Nil).flatten: _*)
  }
}
object Terminal {
  def apply(label: String, nodeType: List[String] = Nil): RenderedTree = RenderedTree(label, Nil, nodeType)
}
object NonTerminal {
  def apply(label: String, children: List[RenderedTree], nodeType: List[String] = Nil): RenderedTree = RenderedTree(label, children, nodeType)
}

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
      def escapeHtml(str: String) = str.replace("&", "&amp;").replace("\"", "&quot;").replace("<", "&lt;").replace(">", "&gt;")
      
      def decl(name: String) = {
        val formatted = t.nodeType match {
          case Nil => "\"" + escape(t.label.toString) + "\""
          case _ => "<" + "<font color=\"#777777\">" + escapeHtml(t.nodeType.mkString("/")) + "</font><br/>" + escapeHtml(t.label.toString) + ">"
        }
        Cord("  ") ++ name ++ "[label=" ++ formatted ++ "];\n"
      }

      for {
        n <- nodeName
        cc <- t match {
          case RenderedTree(_, Nil, _) => state[Int, Cord](Cord(""))
          case RenderedTree(_, children, _) => {
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
