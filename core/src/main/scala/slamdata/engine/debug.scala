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
    val deleted = ">>>"
    val added = "<<<"

    (this, that) match {
      case (RenderedTree(l1, children1, nodeType1), RenderedTree(l2, children2, nodeType2)) => {
        if (nodeType1 != nodeType2 || l1 != l2)
          RenderedTree("", 
            prefixType(this, deleted) ::
            prefixType(that, added) ::
            Nil,
            List("[Root differs]"))
        else {
          def matchChildren(children1: List[RenderedTree], children2: List[RenderedTree]): List[RenderedTree] = (children1, children2) match {
            case (Nil, Nil)     => Nil
            case (x :: xs, Nil) => prefixType(x, deleted) :: matchChildren(xs, Nil)
            case (Nil, x :: xs) => prefixType(x, added) :: matchChildren(Nil, xs)

            case (a :: as, b :: bs)        if a.typeAndLabel == b.typeAndLabel  => a.diff(b) :: matchChildren(as, bs)
            case (a1 :: a2 :: as, b :: bs) if a2.typeAndLabel == b.typeAndLabel => prefixType(a1, deleted) :: a2.diff(b) :: matchChildren(as, bs)
            case (a :: as, b1 :: b2 :: bs) if a.typeAndLabel == b2.typeAndLabel => prefixType(b1, added) :: a.diff(b2) :: matchChildren(as, bs)

            case (a :: as, b :: bs) => prefixType(a, deleted) :: prefixType(b, added) :: matchChildren(as, bs)
          }
          RenderedTree(l1, matchChildren(children1, children2), nodeType1)
        }
      }
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
    def mapParts[A, B](as: Stream[A])(f: (A, Boolean, Boolean) => B): Stream[B] = {
      def loop(as: Stream[A], first: Boolean): Stream[B] =
        if (as.isEmpty)           Stream.empty
        else if (as.tail.isEmpty) f(as.head, first, true) #:: Stream.empty
        else                      f(as.head, first, false) #:: loop(as.tail, false)
      loop(as, true)
    }

    val (prefix, body, suffix) = (simpleType, label) match {
      case (None, label)             => ("", label, "")
      case (Some(simpleType), "")    => ("", simpleType, "")
      case (Some(simpleType), label) => (simpleType + "(",  label, ")")
    }
    val indent = " " * (prefix.length-2)
    val lines = body.split("\n").toStream
    mapParts(lines) { (a, first, last) => 
      val pre = if (first) prefix else indent
      val suf = if (last) suffix else ""
      pre + a + suf
    } ++ drawSubTrees(children)
  }
  
  private def typeAndLabel: String = (simpleType, label) match {
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
  
  /**
   (Effectfully) adds the given object(s) to a crude UI which shows them as trees 
   that can be interactively explored. Can be used with `unsafeTap` aka `<|` to capture
   a value from the middle of some expression.
   */
  def showSwing[A](as: A*)(implicit RA: RenderTree[A]): Unit = {
    import javax.swing._
    import java.awt.event._
    import javax.swing.tree._
    
    val roots = as.toList.map { a => RA.render(a) }
    
    trait Node {
      def children: List[TreeNode]
    }
    case object RootNode extends Node { 
      val children = roots.map(new TreeNode(_))
    }
    // Not a case class, because JTree gets confused if there are multiple
    // equal nodes, but yes, that's a serious drag.
    class TreeNode(val t: RenderedTree) extends Node {
      val children = t.children.map(new TreeNode(_))
      override def toString = t.label
    }
    
    class RenderedTreeModel(roots: List[RenderedTree]) extends TreeModel {
      def addTreeModelListener(l: javax.swing.event.TreeModelListener): Unit = ()
      def getChild(parent: Any, index: Int): Object = children(parent)(index)
      def getChildCount(parent: Any): Int = children(parent).length
      def getIndexOfChild(parent: Any, child: Any): Int = children(parent).indexOf(child)
      def getRoot(): Object = RootNode
      def isLeaf(node: Any): Boolean = children(node).isEmpty
      def removeTreeModelListener(l: javax.swing.event.TreeModelListener): Unit = ()
      def valueForPathChanged(path: javax.swing.tree.TreePath, newValue: Any): Unit = ()
      
      private def children(node: Any): List[TreeNode] = node match {
        case n: Node => n.children
        case _       => Nil
      }
    }
    
    class RenderedTreeCellRenderer extends DefaultTreeCellRenderer {
      override def getTreeCellRendererComponent(tree: JTree,
                                     value: Any,
                                     selected: Boolean,
                                     expanded: Boolean,
                                     leaf: Boolean,
                                     row: Int,
                                     hasFocus: Boolean): java.awt.Component = {
        val comp = super.getTreeCellRendererComponent(tree, value, selected, expanded, leaf, row, hasFocus)
        
        (value, comp) match {
          case (n: TreeNode, label: JLabel) =>
            val typ = n.t.nodeType.lastOption.getOrElse("")
            label.setText(s"<html><i>${typ}</i> (${n.t.label})</html>")
        }
        
        comp
      }
    }
    
    implicit def toActionListener(l: ActionEvent => Unit): ActionListener = 
      new ActionListener { def actionPerformed(evt: ActionEvent): Unit = l(evt) }
    
    val m = new RenderedTreeModel(roots)
    
    val jt = new JTree()
    jt.setModel(m)
    jt.setCellRenderer(new RenderedTreeCellRenderer)
    jt.setRootVisible(false)
    jt.setShowsRootHandles(true)
    
    val sc = new JScrollPane(jt)
    
    
    // Dumb search field at the top of the frame. Hit enter to search.
    val s = new JTextField(10)
    s.addActionListener((evt: ActionEvent) => {
      val pattern = s.getText.trim
      
      implicit def toTreePath(l: List[TreeNode]): TreePath = new TreePath((m.getRoot :: l).toArray)
      
      def paths: List[List[TreeNode]] = {
        def loop(node: Any): List[List[TreeNode]] = {
          for {
            i <- (0 until m.getChildCount(node)).toList
            child = m.getChild(node, i).asInstanceOf[TreeNode]
            p <- (List() :: loop(child)).map(child :: _)
          } yield p
        }
        loop(m.getRoot)
      }
            
      val matches = paths.filter(path => {
        def strMatch(str: String): Boolean = str.toLowerCase.contains(pattern.toLowerCase)

        path.lastOption.fold(false) { node => 
          strMatch(node.t.label) || node.t.nodeType.lastOption.map(strMatch).getOrElse(false)
        }
      })
      
      for (p <- paths.reverse) {
        jt.collapsePath(p)
        jt.removeSelectionPath(p)
      }
      
      for (p <- matches) {
        jt.makeVisible(p)
        jt.addSelectionPath(p)
      }
    })
    
    val sl = new JLabel("Search:")
    val sp = new JPanel()
    sp.add(sl)
    sp.add(s)
    
    val f = new JFrame("RenderedTree - " + new java.text.SimpleDateFormat("HH:mm:ss.SSS").format(new java.util.Date()))
    f.setDefaultCloseOperation(WindowConstants.DISPOSE_ON_CLOSE)
    f.setSize(400, 600)
    val count = windowCount.incrementAndGet
    f.setLocation(count*20, count*20)
  
    f.getContentPane().add(sp, "North")
    f.getContentPane().add(sc)

    java.awt.EventQueue.invokeLater(new Runnable { def run = {
      f.setVisible(true)
    }})
  }
  
  val windowCount = new java.util.concurrent.atomic.AtomicInteger()
}
