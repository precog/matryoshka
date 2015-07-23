/*
 * Copyright 2014 - 2015 SlamData Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package slamdata.engine.admin

import slamdata.Predef._

import scala.swing._
import scala.swing.event._
import Swing._

import scalaz.stream._

import slamdata.engine.Backend._
import slamdata.engine.fp._

class ProgressDialog(parent: Window, message: String, count: Int, process: Process[PathTask, Unit]) extends Dialog(parent) {
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

    async(t.run)(_.fold(
      err => {
          errorAlert(progress, err.toString)
          dispose
        },
      x => ignore(x.fold(
        err => {
          errorAlert(progress, err.toString)
          dispose
        },
        κ {
          done.visible = true
          progress.visible = false
          pack
        }))))
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
