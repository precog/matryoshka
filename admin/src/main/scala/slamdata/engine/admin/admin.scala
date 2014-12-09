package slamdata.engine.admin

import java.io.File

import scala.swing._
import scala.swing.event._
import scalaswingcontrib.tree._
import java.awt.Color

import scalaz.{Tree => _, _}
import Scalaz._
import scalaz.concurrent._

import slamdata.engine.{Backend, Mounter}
import slamdata.engine.fs._
import slamdata.engine.config._

object Main extends SimpleSwingApplication {
  var configPath: Option[String] = None

  override def startup(args: Array[String]) {
    LookAndFeel.init

    configPath = Some(args.headOption.getOrElse(defaultConfigPath))
    configPath.map(p => println("Configuration file: " + p))

    super.startup(args)
  }

  def defaultConfigPath = Option(System.getProperty("user.home")).getOrElse(".") + "/.slamdata/config.json"

  def top = new AdminUI(configPath.get).mainFrame
}

class AdminUI(configPath: String) {
  import SwingUtils._

  // val undoAction = Action("Undo") {
  //   println("Undo/Redo!")
  //   // TODO
  // }
  // val cutAction = Action("Cut") {
  //   println("Cut!")
  //   // TODO
  // }
  // val copyAction = Action("Copy") {
  //   println("Copy!")
  //   // TODO
  // }
  // val pasteAction = Action("Paste") {
  //   println("Paste!")
  //   // TODO
  // }
  // pasteAction.enabled = false

  var currentConfig: Option[Config] = None

  def fsTable = currentConfig.map(Mounter.mount(_).run)

  lazy val fsTree = new Tree[Path] {
    model = fsTreeModel

    // NB: override type to avoid bug with the internal `hiddenRoot`
    renderer = Tree.Renderer[Any, Any](_ match {
      case p: Path => p.asDir.dir.lastOption.map(_.value).getOrElse(p)
      case n => n
    })
    // TODO: icons?
  }

  lazy val queryArea = new TextArea {
    text = "select *\n  from \"/zips\"\n  where city like 'B%'\n  and pop > 1000"
    font = new java.awt.Font("Monospaced", 0, 11)

    peer.setEnabled(false)
  }

  // lazy val resultTable =
  //   new Table {
  //     model = dummyTableModel
  //
  //     peer.setEnabled(false)
  //   }

  lazy val mainFrame: MainFrame = new MainFrame {
    title = "SlamEngine Admin Console"

    // menuBar = new MenuBar {
    //   import javax.swing.KeyStroke.getKeyStroke
    //   import java.awt.event.KeyEvent._
    //
    //   val menuKeyMask = java.awt.Toolkit.getDefaultToolkit().getMenuShortcutKeyMask()
    //
    //   contents += new Menu("Edit") {
    //     contents += new MenuItem(undoAction)  { peer.setAccelerator(getKeyStroke(VK_Z, menuKeyMask)) }
    //     contents += new Separator
    //     contents += new MenuItem(cutAction)   { peer.setAccelerator(getKeyStroke(VK_X, menuKeyMask)) }
    //     contents += new MenuItem(copyAction)  { peer.setAccelerator(getKeyStroke(VK_C, menuKeyMask)) }
    //     contents += new MenuItem(pasteAction) { peer.setAccelerator(getKeyStroke(VK_V, menuKeyMask)) }
    //   }
    // }

    contents = new GridBagPanel {
      import GridBagPanel._

      layout(new Label { icon = loadImage("graphic-slamdata-logo-medium.png") }) =
        new Constraints { gridx = 0; gridy = 0; gridheight = 3; insets = new Insets(0, 0, 10, 20) }

      // layout(new Label { text = "SlamEngine URL: http://localhost:8080/"}) = // TODO
      //   new Constraints { gridx = 1; gridy = 1; anchor = Anchor.West; insets = new Insets(5, 2, 5, 2) }
      // layout(new Label { text = "X MongoDB databases are mounted with no errors" }) = // TODO
      //   new Constraints { gridx = 1; gridy = 2; weightx = 1; anchor = Anchor.West; insets = new Insets(5, 2, 5, 2) }
      layout(new Button(configAction)) =
        new Constraints { gridx = 2; gridy = 2; anchor = Anchor.East; insets = new Insets(2, 2, 2, 10) }

      layout(browser) =
        new Constraints { gridx = 0; gridy = 3; gridwidth = 3; weightx = 1; weighty = 1; fill = Fill.Both }
    }
    preferredSize = new Dimension(600, 400)

    reactions += {
      case WindowOpened(_) => async(ConfigDialog.loadAndTestConfig(configPath))(_.fold(
        err => configAction.apply,
        config => { currentConfig = Some(config); syncFsTree }
      ))
    }

    // apply size variants:
    javax.swing.SwingUtilities.updateComponentTreeUI(peer)
  }

  lazy val configAction = Action("Configure") {
    val dialog = new ConfigDialog(mainFrame, configPath)

    // NB: this call blocks, since the dialog is modal, but the main frame will
    // continue to process repaint events and so on
    dialog.open

    dialog.config.map { cfg =>
      currentConfig = Some(cfg)
      syncFsTree
    }
  }

  def browser = {
    val resultLabel = new Label { text = "Elapsed: 0.2s" }

    import GridBagPanel._

    new GridBagPanel {
      // trait SmallButton extends Button {
      //   peer.putClientProperty("JComponent.sizeVariant", "mini")
      // }

      layout(new Label { text = "FileSystem" }) =
        new Constraints { gridx = 0; gridy = 0; weightx = 1; anchor = Anchor.West; insets = new Insets(5, 2, 5, 2) }

      layout(new ScrollPane(fsTree) {
        minimumSize = new Dimension(300, 200)
        preferredSize = new Dimension(300, 200)
      }) = new Constraints { gridx = 0; gridy = 1; gridheight = 3; weightx = 1; weighty = 1; fill = Fill.Both }

      layout(new Label { text = "Test Query" }) =
        new Constraints { gridx = 1; gridy = 0; anchor = Anchor.West; insets = new Insets(5, 2, 5, 2) }
      layout(new ScrollPane(queryArea) {
        minimumSize = new Dimension(600, 200)
        preferredSize = new Dimension(600, 200)
      }) = new Constraints { gridx = 1; gridy = 1; weightx = 2; weighty = 1; fill = Fill.Both }

      layout(new Label { text = "Result" }) =
        new Constraints { gridx = 1; gridy = 2; anchor = Anchor.West; insets = new Insets(5, 2, 5, 2) }
      layout(new ScrollPane(new BorderPanel) {
        minimumSize = new Dimension(600, 200)
        preferredSize = new Dimension(600, 200)
      }) = new Constraints { gridx = 1; gridy = 3; weightx = 2; weighty = 2; fill = Fill.Both }
      //   },
      //   new GridBagPanel {
      //     layout(resultLabel) = new Constraints { gridx = 0; gridy = 0; anchor = Anchor.West; insets = new Insets(2, 2, 2, 2) }
      //     layout(new Button("Save...")) = new Constraints { gridx = 1; gridy = 0; anchor = Anchor.East; insets = new Insets(2, 2, 2, 2) }
      //
      //     layout(new ScrollPane(resultTable) {
      //       minimumSize = new Dimension(100, 100)
      //       preferredSize = new Dimension(500, 300)
      //     }) = new Constraints { gridx = 0; gridy = 1; gridwidth = 2; weightx = 1; weighty = 1; fill = Fill.Both }
      //   }
      // ))
    }
  }

  var paths = Map(Path.Root -> List[Path]())

  def syncFsTree = {
    // NB: this is work that should really be done somewhere else (FSTable?)
    def lsTree: Task[Map[Path, List[Path]]] = {
      def children(p: Path): Task[List[Path]] =
        fsTable.map { fs =>
          val files = fs.lookup(p).map { case (backend, mountPath, relPath) =>
            backend.dataSource.ls(relPath).map(_.map(p ++ _))
          }.getOrElse(Task.now(Nil))
          val mounts = fs.children(p).filterNot(_ == Path("./")).map(p ++ _)
          files.map(_ ++ mounts)
        }.getOrElse(Task.now(Nil))

      def loop(p: Path): Task[Map[Path, List[Path]]] =
        children(p).flatMap(ps => ps.map(loop(_)).sequenceU.map(_.foldLeft(Map[Path, List[Path]]())(_ ++ _)).map(Map(p -> ps) ++ _))

      loop(Path.Root)
    }

   async(lsTree)(_.fold(
      err => println("error loading paths: " + err),
      ps => {
        paths = ps
        FsTreeModel.reload(fsTreeModel, ps)
        fsTree.expandPath(Vector(Path.Root))
      }))
  }

  lazy val fsTreeModel = FsTreeModel.apply

  syncFsTree
}

object FsTreeModel {
  def apply = InternalTreeModel(Path.Root)((p: Path) => Nil)

  def reload(tm: InternalTreeModel[Path], contents: Map[Path, List[Path]]) = {
    def loop(p: Vector[Path]): Unit = {
      val oldChildren = tm.getChildrenOf(p)
      val newChildren = contents.get(p.last).getOrElse(Nil)

      for (c <- oldChildren) {
        if (!(newChildren contains c)) tm.remove(p :+ c)
      }
      for (c <- newChildren) {
        if (!(oldChildren contains c)) tm.insertUnder(p, c, tm.getChildrenOf(p).length)
        loop(p :+ c)
      }
    }

    loop(Vector(Path.Root))
  }
}

trait SwingUtils {
  /** Run on a worker thread, and handle the result on Swing's event thread. */
  def async[A](t: Task[A])(f: Throwable \/ A => Unit): Unit = Task.fork(t).runAsync(v => Swing.onEDT { f(v) } )

  def loadImage(relPath: String) = new javax.swing.ImageIcon(getClass.getResource(relPath))

  def errorAlert(parent: Component, detail: String): Unit =
    Dialog.showMessage(parent, detail, javax.swing.UIManager.getString("OptionPane.messageDialogTitle"), Dialog.Message.Error)
}
object SwingUtils extends SwingUtils
