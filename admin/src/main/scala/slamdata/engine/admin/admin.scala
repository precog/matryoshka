package slamdata.engine.admin

import java.io.File

import scala.swing._
import Swing._
import scala.swing.event._
import scalaswingcontrib.tree._
import java.awt.Color

import scalaz.{Tree => _, _}
import Scalaz._
import scalaz.concurrent._
import scalaz.stream.Process

import slamdata.engine.{Backend, Mounter, QueryRequest, PhaseResult, ResultPath}
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

  def defaultConfigPath = {
    import scala.util.Properties._

    val commonPath = "SlamData/slamengine-config.json"

    if (isMac)      propOrElse("user.home", ".") + "/Library/Application Support/" + commonPath
    else if (isWin) envOrNone("LOCALAPPDATA").map(_ + commonPath)
                      .getOrElse(propOrElse("user.home", ".") + commonPath)
    else            propOrElse("user.home", ".") + "/.config/" + commonPath
  }

  def top = new AdminUI(configPath.get).mainFrame
}

class AdminUI(configPath: String) {
  import SwingUtils._

  var currentConfig: Option[Config] = None

  def fsTable = currentConfig.map(Mounter.mount(_).run)

  lazy val fsTree = new Tree[Path] {
    model = fsTreeModel

    // NB: override type to avoid bug with the internal `hiddenRoot`
    renderer = Tree.Renderer[Any, Any](_ match {
      case p: Path => p.asDir.dir.lastOption.map(_.value).getOrElse(p)
      case n => n
    })
  }

  lazy val mainFrame: MainFrame = new MainFrame {
    title = "SlamEngine Admin Console"

    contents = new GridBagPanel {
      import GridBagPanel._

      layout(new Label { icon = scale(loadImage("xl-onwhite-trans-sm.png"), 0.5) }) =
        new Constraints { gridx = 0; gridy = 0; gridheight = 3; insets = new Insets(0, 0, 10, 20) }

      layout(new Button(configAction)) =
        new Constraints { gridx = 2; gridy = 2; anchor = Anchor.East; insets = new Insets(2, 2, 2, 10) }

      layout(browser) =
        new Constraints { gridx = 0; gridy = 3; gridwidth = 3; weightx = 1; weighty = 1; fill = Fill.Both }
    }
    minimumSize = peer.getPreferredSize

    reactions += {
      case WindowOpened(_) => async(ConfigDialog.loadAndTestConfig(configPath))(_.fold(
        err => configAction.apply,
        config => { currentConfig = Some(config); syncFsTree }
      ))
    }
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

  def browser = new GridBagPanel {
    import SwingUtils._

    val MonoFont = new java.awt.Font("Monospaced", 0, 11)

    val resultLabel = new Label { text = "Elapsed: 0.2s" }

    lazy val queryArea: TextArea = new TextArea {
      text = "select *\n  from \"/zips\"\n  where pop > 1000"
      font = MonoFont

      this.bindEditActions

      import java.awt.event.KeyEvent._
      import javax.swing.KeyStroke.getKeyStroke
      val meta = java.awt.Toolkit.getDefaultToolkit().getMenuShortcutKeyMask()
      val shift = java.awt.Event.SHIFT_MASK
      val compileActionName = "compile-query"
      peer.getInputMap.put(getKeyStroke(VK_ENTER, shift + meta), compileActionName)
      peer.getActionMap.put(compileActionName, compileAction.peer)
      val runActionName = "run-query"
      peer.getInputMap.put(getKeyStroke(VK_ENTER, meta), runActionName)
      peer.getActionMap.put(runActionName, runAction.peer)
    }

    val workingDirLabel = new Label("Active Mount:") { visible = false }
    lazy val workingDir = new ComboBox[Path](Nil)    { visible = false }

    lazy val compileAction: Action = Action("Compile") {
      planQuery.map { case (phases, fs, task) =>
        phases.lastOption match {
          case Some(PhaseResult.Detail(_, value)) => {
            planArea.text = value
            cards.show(PlanCard)
          }
          case _ => {
            cards.show(BlankCard)
          }
        }
      }.getOrElse {
        cards.show(BlankCard)
      }
    }

    lazy val runAction: Action = Action("Run") {
      runAction.enabled = false

      planQuery.map { case (phases, fs, task) =>
        async(task) { rez =>
          runAction.enabled = true
          rez.fold(
            err => {
              statusArea.text = phases + "\n\n" + err.toString
              cards.show(BlankCard)
            },
            resultPath => {
              statusArea.text = "result: " + resultPath
              cleanupResult
              resultTable.model = new CollectionTableModel(fs, resultPath)
              cards.show(ResultsCard)
            })
        }
      }.getOrElse {
        runAction.enabled = true
        cards.show(BlankCard)
      }
    }

    lazy val runButton = new Button(runAction)

    def cleanupResult = resultTable.model match {
      case m: CollectionTableModel => async(m.cleanup)(_.leftMap(
          err => println("Error while deleting temp collection: " + err)))
      case _ => ()
    }

    trait QueryError
    case object NoFileSystem extends QueryError
    case class NoMount(path: Path) extends QueryError
    case class ExecutionFailed(cause: Throwable) extends QueryError

    def planQuery: QueryError \/ (Vector[PhaseResult], FileSystem, Task[ResultPath]) =
      fsTable.map { fs =>
        val contextPath = Option(workingDir.selection.item).map(_.asDir).getOrElse(Path.Root)
        fs.lookup(contextPath).map { case (backend, mountPath, relPath) =>
          \/.fromTryCatchNonFatal(backend.run(QueryRequest(slamdata.engine.sql.Query(queryArea.text), None, mountPath, mountPath))).bimap(
            ExecutionFailed(_),
            t => (t._1, backend.dataSource, t._2))
        }.getOrElse(-\/(NoMount(contextPath)))
      }.getOrElse(-\/(NoFileSystem))

    val validateQuery = new CoalescingAction(400, {
      val (log, queryColor, workingDirColor) = planQuery.fold(
        _ match {
          case NoFileSystem => ("No filesystem configured", Valid, Valid)
          case NoMount(path) => ("No mount for path: " + path, Valid, Invalid)
          case ExecutionFailed(cause) => (cause.toString, Invalid, Valid)
        },
        t => {
          val (phases: Vector[PhaseResult], _, _) = t
          phases.lastOption match {
            case Some(err @ PhaseResult.Error(_, _)) => (err.toString, Invalid, Valid)
            case Some(_) => ("Parsed query:\n" + queryArea.text + "\n\n" + phases.mkString("\n\n"), Valid, Valid)
            case None =>("no results", Invalid, Valid)
          }
        })

      statusArea.text = log
      onEDT { statusArea.peer.scrollRectToVisible(new java.awt.Rectangle(0, 0, 10, 10)) }
      queryArea.background = queryColor
      workingDir.background = workingDirColor
    })

    val BlankCard = "blank"
    val PlanCard = "plan"
    val ResultsCard = "results"
    lazy val cards = new CardPanel {
      layout(new FlowPanel {
        contents += new Label("Enter a query and click Compile or Run")
      }) = BlankCard

      layout(new GridBagPanel {
        import GridBagPanel._

        layout(new Label("Plan")) = new Constraints { gridx = 0; gridy = 0; anchor = Anchor.West; insets = new Insets(2, 2, 2, 2) }

        layout(new ScrollPane(planArea) {
          minimumSize = new Dimension(600, 200)
          preferredSize = new Dimension(600, 200)
        }) = new Constraints { gridx = 0; gridy = 1; gridwidth = 2; weightx = 1; weighty = 1; fill = Fill.Both }

        layout(new FlowPanel {
          contents += new Button(copyPlanAction)
          contents += new Button(savePlanAction)
        }) = new Constraints { gridx = 1; gridy = 2; anchor = Anchor.East; insets = new Insets(2, 2, 2, 2) }
      }) = PlanCard

      layout(new GridBagPanel {
        import GridBagPanel._

        layout(new Label("Results")) = new Constraints { gridx = 0; gridy = 0; anchor = Anchor.West; insets = new Insets(2, 2, 2, 2) }

        layout(new ScrollPane(resultTable) {
          minimumSize = new Dimension(600, 200)
          preferredSize = new Dimension(600, 200)
        }) = new Constraints { gridx = 0; gridy = 1; gridwidth = 2; weightx = 1; weighty = 1; fill = Fill.Both }

        layout(resultSummary) = new Constraints { gridx = 0; gridy = 2; anchor = Anchor.West; insets = new Insets(2, 2, 2, 2) }
        layout(new FlowPanel {
          contents += new Button(copyResultsAction)
          contents += new Button(saveResultsAction)
        }) = new Constraints { gridx = 1; gridy = 2; anchor = Anchor.East; insets = new Insets(2, 2, 2, 2) }
      }) = ResultsCard
    }

    lazy val planArea = new TextArea {
      editable = false
      font = MonoFont

      this.bindEditActions
    }

    lazy val resultTable = new Table {
      autoResizeMode = Table.AutoResizeMode.Off
    }
    lazy val resultSummary = new Label

    val copyPlanAction = Action("Copy") {
      copyToClipboard(planArea.text)
    }
    val savePlanAction = Action("Export...") {
      val dialog = new java.awt.FileDialog(mainFrame.peer, "Save MongoDB Plan", java.awt.FileDialog.SAVE)
      dialog.setFile("plan.js")
      dialog.setVisible(true)
      (Option(dialog.getDirectory) |@| Option(dialog.getFile)){ (dir, file) =>
        val w = fileWriter(dir + "/" + file)
        w.write(planArea.text)
        w.close()
      }
    }

    val copyResultsAction = Action("Copy") {
      val (count, p) = writeCsv(new java.io.StringWriter)(w => copyToClipboard(w.toString))
      (new ProgressDialog(mainFrame, "Copying results to the clipboard", count, p)).open
    }
    val saveResultsAction = Action("Export...") {
      val dialog = new java.awt.FileDialog(mainFrame.peer, "Save Results", java.awt.FileDialog.SAVE)
      dialog.setFile("results.csv")
      dialog.setVisible(true)
      (Option(dialog.getDirectory) |@| Option(dialog.getFile)){ (dir, file) =>
        val (count, p) = writeCsv(fileWriter(dir + "/" + file))(_ => println("Wrote CSV file: " + file))
        (new ProgressDialog(mainFrame, "Writing results to file: " + file, count, p)).open
      }
    }

    def fileWriter(path: String) = new java.io.OutputStreamWriter(new java.io.FileOutputStream(new java.io.File(path)), "UTF-8")

    def writeCsv[W <: java.io.Writer](w: W)(f: W => Unit): (Int, Process[Task, Unit]) = {
      import com.github.tototoshi.csv._
      val count = resultTable.model.getRowCount
      val rows = resultTable.model.asInstanceOf[CollectionTableModel].getAllValues
      val cw = CSVWriter.open(w)
      count -> (rows.map(row => cw.writeRow(row)) ++ Process.emit { cw.close; f(w) })
    }

    listenTo(queryArea)
    listenTo(workingDir)
    listenTo(fsTree)
    reactions += {
      case ValueChanged(`queryArea`) => validateQuery.trigger
      case ValueChanged(`workingDir`) => validateQuery.trigger

      case scalaswingcontrib.event.TreeNodesInserted(_, _, _, _) => {
        currentConfig.map { cfg =>
          val mounts = cfg.mountings.keys.toList.sorted
          comboBoxPeer(workingDir).setModel(comboBoxModel(mounts))
          workingDirLabel.visible = mounts.length > 1
          workingDir.visible = mounts.length > 1

          validateQuery.trigger
        }
      }

      // case evt => println("not handled:\n" + evt + "; " + evt.getClass)
    }

    lazy val statusArea = new TextArea {
      editable = false
      font = MonoFont

      this.bindEditActions
    }


    import GridBagPanel._

    layout(new Label { text = "File System" }) =
      new Constraints { gridx = 0; gridy = 0; weightx = 1; anchor = Anchor.West; insets = new Insets(5, 2, 5, 2) }

    layout(new ScrollPane(fsTree) {
      minimumSize = new Dimension(300, 500)
      preferredSize = new Dimension(300, 500)
    }) = new Constraints { gridx = 0; gridy = 1; gridheight = 3; weightx = 1; weighty = 1; fill = Fill.Both }

    layout(new Label { text = "Test Query" }) =
      new Constraints { gridx = 1; gridy = 0; anchor = Anchor.West; insets = new Insets(5, 2, 5, 2) }
    layout(new ScrollPane(queryArea) {
      minimumSize = new Dimension(600, 200)
      preferredSize = new Dimension(600, 200)
    }) = new Constraints { gridx = 1; gridy = 1; gridwidth = 3; weightx = 2; weighty = 1; fill = Fill.Both }
    layout(workingDirLabel)       = new Constraints { gridx = 1; gridy = 2; anchor = Anchor.West; insets = new Insets(2, 2, 2, 2) }
    layout(workingDir)            = new Constraints { gridx = 2; gridy = 2; anchor = Anchor.West; insets = new Insets(2, 2, 2, 2) }
    layout(new FlowPanel {
      contents += new Button(compileAction)
      contents += runButton
    }) = new Constraints { gridx = 3; gridy = 2; anchor = Anchor.East; insets = new Insets(2, 2, 2, 2) }

    layout(cards) = new Constraints { gridx = 1; gridy = 3; gridwidth = 3; weightx = 2; weighty = 2; fill = Fill.Both; insets = new Insets(15, 0, 0, 0) }

    layout(new ScrollPane(statusArea) {
      minimumSize = new Dimension(600, 100)
      preferredSize = new Dimension(600, 100)
    }) = new Constraints { gridx = 0; gridy = 4; gridwidth = 4; fill = Fill.Both }

    onEDT {
      mainFrame.defaultButton = runButton
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
