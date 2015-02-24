package slamdata.engine.admin

import scala.swing._
import scala.swing.event._
import Swing._

import scalaz._
import Scalaz._
import scalaz.concurrent._
import scalaz.stream._

class ProgressDialog(parent: Window, message: String, count: Int, process: Process[Task, Unit]) extends Dialog(parent) {
  import SwingUtils._

  val label = new Label(message) {
    border = EmptyBorder(5)
    xLayoutAlignment = 0.5
  }

  val progress = new ProgressBar {
    min = 0
    max = count
    value = 0
  }

  val done = new Button("Done") {
    visible = false

    border = EmptyBorder(5)
    xLayoutAlignment = 0.5
  }

  private def run = {
    val t = process.map { _ =>
      // NB: this is a lot of chatter to update the UI much more often than is
      // really needed. But it's asynchronous and entertaining, so it's worth it.
      onEDT { progress.value = progress.value + 1 }
    }.run

    async(t)(_.fold(
      err => {
        errorAlert(progress, err.toString)
        dispose
      },
      _ => {
        done.visible = true
        progress.visible = false
        pack
      }))
  }

  listenTo(this)
  listenTo(done)
  reactions += {
    case WindowActivated(_) => run

    case ButtonClicked(`done`) => dispose

    // case evt => println("not handled: " + evt)
  }

  modal = true

  title = "Progress"

  contents = new BoxPanel(Orientation.Vertical) {
    contents += label
    contents += progress
    contents += done

    border = EmptyBorder(5)
  }

  resizable = false

  setLocationRelativeTo(parent)
}

