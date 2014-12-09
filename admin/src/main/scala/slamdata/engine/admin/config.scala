package slamdata.engine.admin

import scala.swing._
import scala.swing.event._
import scalaswingcontrib.tree._

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
      Config(SDServerConfig(Some(8080)), Map()) 
    },
    identity)

  private lazy val plusAction = Action("+") {
    mountTM.add(MountInfo(if (mountTM.getRowCount == 0) Some("/") else None, None, None, None))
    mountTable.selection.rows.clear
    mountTable.selection.rows += mountTM.getRowCount - 1
  }
  private lazy val minusAction = Action("-") {
    for (i <- mountTable.selection.rows.toList.sorted.reverse) mountTM.remove(i)
  }
  minusAction.enabled = false

  private lazy val cancelAction = Action("Cancel") { dispose }
  private lazy val saveAction = Action("Save") {
    config = for {
      port <- Option(portField.peer.getValue).map(_.asInstanceOf[Number].intValue)
      mountings <- mountTM.validMounts
    } yield Config(SDServerConfig(Some(port)), mountings)
    config.map(cfg => async(Config.toFile(cfg, configPath))(_.fold(
      err => errorAlert(mountTable, err.toString),
      _ => dispose
    )))
  }
  saveAction.enabled = false

  lazy val saveButton = new Button(saveAction)

  case class MountInfo(path: Option[String], connectionUri: Option[String], database: Option[String], status: Option[Backend.TestResult]) {
    def toConfig: Option[MongoDbConfig] = (database |@| connectionUri)(MongoDbConfig(_, _))
  }

  def startMounts = startConfig.mountings.map {
    case (path, MongoDbConfig(database, uri)) => MountInfo(Some(path.toString), Some(uri), Some(database), None)
  }

  case object MountsUpdated extends Event

  object mountTM extends javax.swing.table.AbstractTableModel with Publisher {
    private val columnNames = List("SlamData Path", "MongoDB URI", "Database", "Status")

    private val mounts = scala.collection.mutable.ListBuffer[MountInfo]()

    def add(mount: MountInfo) = {
      mounts.append(mount)
      fireTableRowsInserted(mounts.length, mounts.length)
      publish(MountsUpdated)
    }

    def remove(index: Int) = {
      mounts.remove(index)
      fireTableRowsDeleted(index, index)
      publish(MountsUpdated)
    }

    def update(index: Int, value: MountInfo) = {
      mounts.remove(index)
      mounts.insert(index, value)
      fireTableRowsUpdated(index, index)
      publish(MountsUpdated)
    }

    def validMounts =
      mounts.foldLeft[Option[Map[Path, BackendConfig]]](Some(Map())) {
        case (None, _) => None
        case (Some(map), info @ MountInfo(Some(path), _, _, Some(Backend.TestResult.Success))) =>
          info.toConfig.map(cfg => map + (Path(path) -> cfg))
        case _ => None
      }

    def updateStatus(pred: MountInfo => Boolean, status: Option[Backend.TestResult]) = {
      val index = mounts.indexWhere(pred)
      if (index >= 0) update(index, mounts(index).copy(status = status))
    }

    def getRowCount: Int = mounts.length
    def getColumnCount: Int = columnNames.length
    override def getColumnName(col: Int): String = columnNames(col)
    def getValueAt(row: Int, col: Int) = {
      val m = mounts(row)
      col match {
        case 0 => m.path.getOrElse("")
        case 1 => m.connectionUri.getOrElse("")
        case 2 => m.database.getOrElse("")
        case 3 => m.status.getOrElse("")
      }
    }
    override def isCellEditable(row: Int, col: Int): Boolean = col != columnNames.length-1
    override def setValueAt(value: Any, row: Int, col: Int): Unit = {
      val valueOpt = value.toString.trim match { case "" => None; case s => Some(s) }
      val update: MountInfo => MountInfo = col match {
        case 0 => _.copy(path = valueOpt)
        case 1 => _.copy(connectionUri = valueOpt)
        case 2 => _.copy(database = valueOpt)
      }
      val updated = update(mounts.remove(row))
      mounts.insert(row, updated)
      if (col != 0) test(updated)
    }
  }

  lazy val mountTable = {
    new Table(mountTM.getRowCount, mountTM.getColumnCount) {
      model = mountTM
    }
  }

  val simpleIntFormat = {
    val f = java.text.NumberFormat.getIntegerInstance
    f.setGroupingUsed(false)
    f.setMaximumIntegerDigits(5)
    f
  }
  lazy val portField = new FormattedTextField(simpleIntFormat) {
      text = "99999"
      peer.setPreferredSize(peer.getPreferredSize)
      peer.setMinimumSize(peer.getPreferredSize)

      text = startConfig.server.port.fold("8080")(_.toString)
    }

  def test(mount: MountInfo) = {
    mountTM.updateStatus(_ == mount, None)
    mount.toConfig.map { cfg =>
      async(Backend.test(cfg))(_.fold(
        err => errorAlert(mountTable, err.toString),
        { case (rez, log) =>
            mountTM.updateStatus(_.toConfig == Some(cfg), Some(rez))
            if (rez != Backend.TestResult.Success) errorAlert(mountTable, log.toString)
        }))
    }
  }

  reactions += {
    case TableRowsSelected(_, _, false) => minusAction.enabled = !mountTable.selection.rows.isEmpty
    case `MountsUpdated`                => saveAction.peer.setEnabled(mountTM.validMounts.fold(false)(_ => true))
    // case e => println("not handled: " + e)
  }
  listenTo(mountTable.selection)
  listenTo(mountTM)

  if (startMounts.isEmpty) {
    mountTM.add(MountInfo(Some("/"), None, None, None))
  }
  else { 
    startMounts.map { m =>
      mountTM.add(m)
      test(m)
    }
  }
  
  modal = true

  contents = new GridBagPanel {
    import GridBagPanel._

    layout(new Label("SlamData server port:")) =
      new Constraints { gridx = 0; gridy = 1; anchor = Anchor.West; insets = new Insets(2, 2, 2, 2) }
    layout(portField) =
      new Constraints { gridx = 1; gridy = 1; weightx = 1; anchor = Anchor.West; insets = new Insets(2, 2, 2, 2) }

    layout(new Label("Mounts:")) =
      new Constraints { gridx = 0; gridy = 2; anchor = Anchor.West; insets = new Insets(2, 2, 2, 2) }
    layout(new Button(plusAction)) =
      new Constraints { gridx = 2; gridy = 2; insets = new Insets(2, 2, 2, 2) }
    layout(new Button(minusAction)) =
      new Constraints { gridx = 3; gridy = 2; insets = new Insets(2, 2, 2, 2) }

    layout(new ScrollPane(mountTable) {
      preferredSize = new Dimension(400, 100)
    }) =
      new Constraints { gridx = 0; gridy = 3; gridwidth = 4; weightx = 1; weighty = 1; fill = Fill.Both; insets = new Insets(2, 2, 2, 2) }

    layout(new Label {
      text = """<html>
      <b>MongoDB Connection URIs</b><br>
      <br>
      To connect to a local MongoDB instance on the default port, use <tt>mongodb://localhost:27017/</tt>.<br>
      <br>
      If authentication is required, include the user and password and also specify the database in the URI:<br>
      <tt>&nbsp;&nbsp;mongodb://<i>user</i>:<i>password</i>@<i>host</i>:<i>port</i>/<i>database</i></tt>
      """
    }) =
      new Constraints { gridx = 0; gridy = 4; gridwidth = 4; weightx = 1; weighty = 1; fill = Fill.Both; insets = new Insets(2, 2, 2, 2) }


    layout(new FlowPanel {
      contents += new Button(cancelAction)
      contents += saveButton
    }) = new Constraints { gridx = 0; gridy = 5; gridwidth = 4; anchor = Anchor.East }
  }

  defaultButton = saveButton
}
object ConfigDialog {
  def loadAndTestConfig(path: String): Task[Config] = for {
    config <- Config.fromFile(path)
    tests  <- config.mountings.values.map(bc => Backend.test(bc)).toList.sequenceU
    rez    <- if (tests.isEmpty || tests.exists(_._1 != Backend.TestResult.Success)) Task.fail(new RuntimeException("mounting(s) failed")) else Task.now(config)
  } yield rez
}