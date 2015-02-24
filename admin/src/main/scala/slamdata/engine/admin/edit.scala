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

import SwingUtils._

class MountEditDialog private (parent: Window, startConfig: MongoDbConfig, startPath: Option[String], otherPaths: List[String]) extends Dialog(parent) {
  import MountEditDialog._

  val primaryHost = new TextField {
    columns = 20
    this.bindEditActions
  }
  val primaryPort = new TextField {
    // text = "27017"
    columns = 5
    this.bindEditActions
  }

  val database = new TextField {
    columns = 20
    this.bindEditActions
  }

  val authentication = new CheckBox("Authentication")

  val userName = new TextField {
    columns = 10
    enabled = false
    this.bindEditActions
  }
  val password = new PasswordField {
    columns = 8
    enabled = false
    this.bindEditActions
  }

  val additionalHosts = new TextArea {
    this.bindEditActions
  }

  val options = new TextArea {
    this.bindEditActions
  }

  val uri = new TextArea {
    preferredSize = new Dimension(30, 30)
    editable = false
  }

  val pathLabel = new Label("SlamData Path:")
  val path = new TextField {
    columns = 10
    this.bindEditActions
  }

  val okAction: Action = Action("Save") {
    okAction.enabled = false
    okAction.title = "Testing"
    toUri.map(uri => async(Backend.test(MongoDbConfig(database.text, uri)))(handleTestResult(uri)))
  }
  okAction.enabled = false
  val okButton = new Button(okAction)
  val cancelAction = Action("Cancel") {
    dispose
  }

  val pasteAction = Action("Paste from Clipboard") {
    val clipboard = java.awt.Toolkit.getDefaultToolkit().getSystemClipboard()
    val str = clipboard.getData(java.awt.datatransfer.DataFlavor.stringFlavor).asInstanceOf[String]
    fromUri(str).leftMap(msg => errorAlert(contents(0), msg))
  }

  listenTo(primaryHost)
  listenTo(primaryPort)
  listenTo(database)
  listenTo(authentication)
  listenTo(userName)
  listenTo(password)
  listenTo(additionalHosts)
  listenTo(options)
  listenTo(uri)
  listenTo(path)
  reactions += {
    case ButtonClicked(`authentication`) => {
      userName.enabled = authentication.selected
      password.enabled = authentication.selected
      validate
    }

    case ValueChanged(`uri`) => ()
    case ValueChanged(_) => validate

    // case evt => println("not handled: " + evt)
  }

  def validate = {
    okAction.title = "Save"

    val (uriText, uriValid) = toUri.fold(
      err => {
        ("",  // TODO: describe the error?
          false)
      },
      (_, true))

    val pathValid = !(otherPaths contains path.text)
    path.background = if (pathValid) Valid else Invalid

    uri.text = uriText
    okAction.enabled = uriValid && pathValid
  }

  def handleTestResult(testUri: String)(rez: Throwable \/ Backend.TestResult): Unit = {
    def interpretError(err: Throwable): String = err match {
      case _: com.mongodb.MongoTimeoutException =>
        "Connection timed out; check host and port"

      case x: com.mongodb.CommandFailureException =>
        x.getCommandResult.getErrorMessage match {
          case "auth failed" => "Authentication failed: check username and password"
          case msg => "Connection succeeded but command failed: " + msg
        }

      case x: com.mongodb.MongoException =>
        if (x.getMessage.startsWith("not authorized")) "Authentication required"
        else "Connection failed: " + x.getMessage

      case x: java.lang.IllegalArgumentException =>
        "Invalid value for option: " + x.getMessage

      case x: slamdata.engine.Error =>
        x.message

      case _ => err.toString
    }

    toUri.flatMap { currentUri =>
      if (currentUri == testUri)
        rez.map(_ match {
          case Backend.TestResult.Failure(err, _) =>
            errorAlert(contents(0), interpretError(err))
            \/-(())
          case Backend.TestResult.Success(_) =>
            result = Some(MongoDbConfig(database.text, testUri) -> path.text)
            dispose
            \/-(())
        })
      else -\/("")
    }.fold(
      _ => println("Ignoring result for expired test"),
      identity)
  }

  def toUri: String \/ String = {
    (primaryHost.matched(HostPattern) |@|
      primaryPort.matched(PortPattern) |@|
      database.matched(DatabasePattern) |@|
      (if (authentication.selected) userName.matched(UserNamePattern) else \/-(None)) |@|
      (if (authentication.selected) password.matched(PasswordPattern) else \/-(None)) |@|
      additionalHosts.matched(AdditionalHostsPattern) |@|
      options.matched(OptionsPattern)) { (hostOpt, portOpt, databaseOpt, userNameOpt, passwordOpt, additionalHostsOpt, optionsOpt) =>
      hostOpt.foldMap(host => "mongodb://" +
        userNameOpt.foldMap(_ + ":") +
        passwordOpt.foldMap(_ + "@") +   // FIXME: presumably needs encoding
        host +
        portOpt.foldMap(":" + _) +
        additionalHostsOpt.foldMap("," + _.replace("\n", ",")) +
        "/" +
        databaseOpt.getOrElse("") +
        optionsOpt.foldMap("?" + _.replace("\n", "&")))
    }
  }

  def fromUri(uri: String): String \/ Unit = {
    def orNone(s: String) = Option(s).flatMap(s => if (s == "") None else Some(s))
    uri match {
      case UriPattern(u, pw, h, p, hs, _, os) => {
        orNone(h).map(primaryHost.text = _)
        orNone(p).map(primaryPort.text = _)
        orNone(u).map { u => authentication.selected = true; userName.enabled = true; password.enabled = true; userName.text = u }
        orNone(pw).map { p => authentication.selected = true; userName.enabled = true; password.enabled = true; password.peer.setText(p) }
        orNone(hs).map(hs => additionalHosts.text = hs.substring(1).replaceAll(",", "\n"))
        orNone(os).map(os => options.text = os.replaceAll("&", "\n"))
        \/-(())
      }
      case _ => -\/("Could not be parsed as a MogoDB URI: " + uri)
    }
  }

  var result: Option[(MongoDbConfig, String)] = None

  modal = true

  title = "Edit MongoDB Connection"

  contents = new GridBagPanel {
    import GridBagPanel._

    layout(new Label("Host:"))             = new Constraints { gridx = 0; gridy =  0; anchor = Anchor.West; insets = new Insets(2, 2, 2, 2) }
    layout(primaryHost)                    = new Constraints { gridx = 1; gridy =  0; gridwidth = 3; anchor = Anchor.West; insets = new Insets(2, 2, 2, 2) }
    layout(new Label("Port:"))             = new Constraints { gridx = 4; gridy =  0; anchor = Anchor.West; insets = new Insets(2, 2, 2, 2) }
    layout(primaryPort)                    = new Constraints { gridx = 5; gridy =  0; anchor = Anchor.West; insets = new Insets(2, 2, 2, 2) }

    layout(new Label("Database:"))         = new Constraints { gridx = 0; gridy =  1; anchor = Anchor.West; insets = new Insets(2, 2, 2, 2) }
    layout(database)                       = new Constraints { gridx = 1; gridy =  1; gridwidth = 5; anchor = Anchor.West; insets = new Insets(2, 2, 2, 2) }

    layout(new BorderPanel)                = new Constraints { gridx = 0; gridy =  9; ipady = 10 }

    layout(authentication)                 = new Constraints { gridx = 0; gridy = 10; gridwidth = 4; anchor = Anchor.West; insets = new Insets(2, 2, 2, 2) }

    layout(new Label("Username:"))         = new Constraints { gridx = 0; gridy = 11; anchor = Anchor.West; insets = new Insets(2, 17, 2, 2) }
    layout(userName)                       = new Constraints { gridx = 1; gridy = 11; anchor = Anchor.West; insets = new Insets(2, 2, 2, 2) }
    layout(new Label("Password:"))         = new Constraints { gridx = 2; gridy = 11; anchor = Anchor.West; insets = new Insets(2, 2, 2, 2) }
    layout(password)                       = new Constraints { gridx = 3; gridy = 11; anchor = Anchor.West; insets = new Insets(2, 2, 2, 2) }

    layout(new BorderPanel)                = new Constraints { gridx = 0; gridy = 19; ipady = 10 }

    layout(new Label("<html>Additional hosts (enter <tt><i>host</i></tt> or <tt><i>host</i>:<i>port</i></tt>, one per line):")) =
      new Constraints { gridx = 0; gridy = 20; gridwidth = 6; anchor = Anchor.West; insets = new Insets(2, 2, 2, 2) }
    layout(new ScrollPane(additionalHosts) {
      preferredSize = new Dimension(30, 50)
    }) = new Constraints { gridx = 0; gridy = 21; gridwidth = 6; weightx = 1; weighty = 1; fill = Fill.Both; insets = new Insets(2, 12, 2, 2) }

    layout(new BorderPanel)                = new Constraints { gridx = 0; gridy = 29; ipady = 10 }

    layout(new Label("<html>Options (enter <tt><i>name</i>=<i>value</i></tt>, one per line):")) =
      new Constraints { gridx = 0; gridy = 30; gridwidth = 6; anchor = Anchor.West; insets = new Insets(2, 2, 2, 2) }
    layout(new ScrollPane(options) {
      preferredSize = new Dimension(30, 50)
    })                        = new Constraints { gridx = 0; gridy = 31; gridwidth = 6; weightx = 1; weighty = 1; fill = Fill.Both; insets = new Insets(2, 12, 2, 2) }

    layout(new BorderPanel)                = new Constraints { gridx = 0; gridy = 39; ipady = 20 }

    layout(new Label("Connection URI:"))   = new Constraints { gridx = 0; gridy = 40; gridwidth = 6; anchor = Anchor.West; insets = new Insets(2, 2, 2, 2) }
    layout(uri)                            = new Constraints { gridx = 0; gridy = 41; gridwidth = 6; weightx = 1; fill = Fill.Both; insets = new Insets(2, 12, 2, 2) }
    layout(new Button(pasteAction)) =
      new Constraints { gridx = 0; gridy = 42; gridwidth = 6; anchor = Anchor.East; insets = new Insets(2, 2, 2, 2) }

    layout(new BorderPanel)                = new Constraints { gridx = 0; gridy = 49; ipady = 10 }

    layout(pathLabel)                      = new Constraints { gridx = 0; gridy = 50; anchor = Anchor.West; insets = new Insets(2, 2, 2, 2) }
    layout(path)                           = new Constraints { gridx = 1; gridy = 50; anchor = Anchor.West; insets = new Insets(2, 2, 2, 2) }

    layout(new FlowPanel {
      contents += new Button(cancelAction)
      contents += okButton
    }) = new Constraints { gridx = 0; gridy = 60; gridwidth = 6; anchor = Anchor.East }

    border = EmptyBorder(10, 10, 10, 10)
  }

  defaultButton = okButton

  minimumSize = peer.getPreferredSize

  {
    database.text = startConfig.database

    fromUri(startConfig.connectionUri)

    otherPaths match {
      case Nil =>
        pathLabel.visible = false
        path.visible = false
      case _ =>
        path.text = startPath.getOrElse("/")
    }

    validate
  }
}
object MountEditDialog {
  def show(parent: Window, startConfig: MongoDbConfig, startPath: Option[String], otherPaths: List[String]): Option[(MongoDbConfig, String)] = {
    val dialog = new MountEditDialog(parent, startConfig, startPath, otherPaths)
    dialog.open
    dialog.result
  }

  /** This pattern is as lenient as possible, so that we can parse out the parts of any possible URI. */
  val UriPattern = (
    "^mongodb://" +
    "(?:" +
      "([^:]+):([^@]+)" +  // username, password
    "@)?" +
    "([^:/@,]+)" +         // (primary) host [required]
    "(?::([0-9]+))?" +     // (primary) port
    "((?:,[^,/]+)*)" +     // additional hosts
    "(?:/" +
      "([^?]+)?" +         // database
      "(?:\\?(.+))?" +     // options
    ")?$").r


  // These patterns try to catch most possible errors:
  private val HostPatternStr = "[^/@:]+"
  private val PortPatternStr = "[0-9]{0,5}"
  val HostPattern     = HostPatternStr.r  // NB: this could be tighter--what's legal in a hostname?
  val PortPattern     = PortPatternStr.r
  val DatabasePattern = "[^ ?]+".r  // NB: this could be tighter--what does MongoDB allow?
  val UserNamePattern = "[^:@]+".r  // NB: this could be tighter--what does MongoDB allow?
  val PasswordPattern = ".+".r
  private val HostPortPatternStr = HostPatternStr + "(?::" + PortPatternStr + ")?"
  val AdditionalHostsPattern = ("(?s)(?:" + HostPortPatternStr + "(?:\n" + HostPortPatternStr + ")*)?").r
  val OptionsPattern = "(?s).*".r  // TODO
}


object DialogTest extends SimpleSwingApplication {
  LookAndFeel.init

  def top = new MainFrame {
    MountEditDialog.show(this, MongoDbConfig("test", "mongodb://localhost/test"), None, List("/"))
  }
}
