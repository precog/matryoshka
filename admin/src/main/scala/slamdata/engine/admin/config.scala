package slamdata.engine.admin

import scala.swing._
import scala.swing.event._
import Swing._

import scalaz._
import Scalaz._
import scalaz.concurrent._

import slamdata.engine.{Backend, Mounter}
import slamdata.engine.fs.Path
import slamdata.engine.config._

class ConfigDialog(parent: Window, configPath: String) extends Dialog(parent) {
  import SwingUtils._

  var config: Option[Config] = None

  private val startConfig = Config.fromFile(configPath).attemptRun.fold(
    err => {
      if (!err.isInstanceOf[java.nio.file.NoSuchFileException]) errorAlert(mountTable, err.toString)
      Config(SDServerConfig(Some(SDServerConfig.DefaultPort)), Map())
    },
    identity)

  private lazy val plusAction = Action("+") {
    val otherPaths = mountTM.validMounts.map(_._1.pathname)
    MountEditDialog.show(this, MongoDbConfig("", ""), Some("/"), otherPaths).map { case (cfg, path) =>
      mountTM.add(Path(path) -> cfg)
      syncColumns
    }
  }
  private lazy val minusAction = Action("-") {
    for (i <- mountTable.selection.rows.toList.sorted.reverse) mountTM.remove(i)
    syncColumns
  }
  minusAction.enabled = false
  private lazy val editAction = Action("...") {
    mountTable.selection.rows.toList match {
      case index :: Nil => {
        val (path, cfg) = mountTM(index)
        cfg match {
          case m @ MongoDbConfig(_, _) =>
            val otherPaths = mountTM.validMounts.filterNot(_._2 == cfg).map(_._1.pathname)
            MountEditDialog.show(this, m, Some(path.pathname), otherPaths).map { case (cfg, path) =>
              mountTM.update(index, Path(path) -> cfg)
            }
          case _ => errorAlert(mountTable, "Unrecognized backend: " + cfg)
        }
      }
      case _ => ()
    }
  }
  editAction.enabled = false

  private lazy val cancelAction = Action("Cancel") { dispose }
  private lazy val saveAction = Action("Save") {
    config = for {
      port <- \/.fromTryCatchNonFatal(portField.text.toInt).toOption
      mountings = mountTM.validMounts
    } yield Config(SDServerConfig(Some(port)), Map(mountings: _*))
    config.map(cfg => async(Config.toFile(cfg, configPath))(_.fold(
      err => errorAlert(mountTable, err.toString),
      _ => dispose
    )))
  }

  lazy val saveButton = new Button(saveAction)

  def startMounts = startConfig.mountings.toList

  case object MountsUpdated extends Event

  object mountTM extends javax.swing.table.AbstractTableModel with Publisher {
    private val columnNames = List("Host", "Database", "SlamData Path")

    private val mounts = scala.collection.mutable.ListBuffer[(Path, BackendConfig)]()

    def apply(index: Int) = mounts(index)

    def add(mount: (Path, BackendConfig)) = {
      mounts.append(mount)
      fireTableRowsInserted(mounts.length, mounts.length)
      publish(MountsUpdated)
    }

    def remove(index: Int) = {
      mounts.remove(index)
      fireTableRowsDeleted(index, index)
      publish(MountsUpdated)
    }

    def update(index: Int, value: (Path, BackendConfig)) = {
      mounts.remove(index)
      mounts.insert(index, value)
      fireTableRowsUpdated(index, index)
      publish(MountsUpdated)
    }

    def validMounts = mounts.toList

    def getRowCount: Int = mounts.length
    def getColumnCount: Int = if (mounts.length > 1) columnNames.length else columnNames.length-1
    override def getColumnName(col: Int): String = columnNames(col)
    def getValueAt(row: Int, col: Int) = {
      val (path, cfg) = mounts(row)
      (col, cfg) match {
        case (0, MongoDbConfig(_, uri)) => uri match { case MountEditDialog.UriPattern(_, _, host, _, _, _, _) => host; case _ => "?" }
        case (0, _) => "?"
        case (1, MongoDbConfig(db, _)) => db
        case (1, _) => "?"
        case (2, _) => path.pathname
        case _ => ""
      }
    }
  }

  lazy val mountTable: Table =
    new Table(mountTM.getRowCount, mountTM.getColumnCount) {
      model = mountTM

      // peer.setDefaultRenderer((new Object).getClass, (new Table.LabelRenderer[Any] {
      //   override def configure(table: Table, isSelected: Boolean, hasFocus: Boolean, a: Any, row: Int, column: Int) {
      //     super.configure(table, isSelected, hasFocus, a, row, column)
      //     if (???) component.foreground = java.awt.Color.RED
      //   }
      // }).peer)

    peer.addMouseListener(new java.awt.event.MouseAdapter {
      override def mouseClicked(evt: java.awt.event.MouseEvent) = {
        if (evt.getClickCount == 2) editAction.apply
      }
    })
  }

  def syncColumns = mountTable.peer.createDefaultColumnsFromModel

  val simpleIntFormat = {
    val f = java.text.NumberFormat.getIntegerInstance
    f.setGroupingUsed(false)
    f.setMaximumIntegerDigits(5)
    f
  }
  lazy val portField = new TextField {
    columns = 10
    text = startConfig.server.port.foldMap(_.toString)
    this.bindEditActions
  }

  listenTo(mountTable.selection)
  listenTo(mountTM)
  listenTo(portField)
  reactions += {
    case TableRowsSelected(_, _, false) => {
      minusAction.enabled = !mountTable.selection.rows.isEmpty
      editAction.enabled = mountTable.selection.rows.size == 1
    }

    case ValueChanged(`portField`) =>
      saveAction.enabled = !portField.matched(MountEditDialog.PortPattern).isEmpty

    // case e => println("not handled: " + e)
  }

  startMounts.map { case (p, c) =>
    mountTM.add(p -> c)
  }
  syncColumns


  modal = true

  title = "Edit SlamData Configuration"

  contents = new GridBagPanel {
    import GridBagPanel._

    layout(new Label("Mounts:")) =
      new Constraints { gridx = 0; gridy = 0; anchor = Anchor.West; insets = new Insets(2, 2, 2, 2) }
    layout(new Button(plusAction)) =
      new Constraints { gridx = 2; gridy = 0; insets = new Insets(2, 2, 2, 2) }
    layout(new Button(minusAction)) =
      new Constraints { gridx = 3; gridy = 0; insets = new Insets(2, 2, 2, 2) }
    layout(new Button(editAction)) =
      new Constraints { gridx = 4; gridy = 0; insets = new Insets(2, 2, 2, 2) }

    layout(new ScrollPane(mountTable) {
      preferredSize = new Dimension(400, 100)
    }) =
      new Constraints { gridx = 0; gridy = 1; gridwidth = 5; weightx = 1; weighty = 1; fill = Fill.Both; insets = new Insets(2, 2, 2, 2) }

    layout(new Label("SlamData server port:")) =
      new Constraints { gridx = 0; gridy = 3; anchor = Anchor.West; insets = new Insets(2, 2, 2, 2) }
    layout(portField) =
      new Constraints { gridx = 1; gridy = 3; weightx = 1; anchor = Anchor.West; insets = new Insets(2, 2, 2, 2) }

    layout(new FlowPanel {
      contents += new Button(cancelAction)
      contents += saveButton
    }) = new Constraints { gridx = 0; gridy = 5; gridwidth = 5; anchor = Anchor.East }

    border = EmptyBorder(10)
  }

  defaultButton = saveButton

  setLocationRelativeTo(parent)
}
object ConfigDialog {
  def loadAndTestConfig(path: String): Task[Config] = for {
    config <- Config.fromFile(path)
    tests  <- config.mountings.values.map(bc => Backend.test(bc)).toList.sequenceU
    rez    <- if (tests.isEmpty || tests.collect { case Backend.TestResult.Failure(_, _) => () }.nonEmpty) Task.fail(new RuntimeException("mounting(s) failed")) else Task.now(config)
  } yield rez
}
