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
import java.awt.Color
import javax.swing.UIManager

import scalaz._

object LookAndFeel {
  def initMacOs = {
    // Neither of these seems to work anymore (since Oracle VMs? Or maybe an interaction with scala-swing startup?)
    java.lang.System.setProperty("apple.laf.useScreenMenuBar", "true")
    java.lang.System.setProperty("com.apple.macos.useScreenMenuBar", "true")

    // NB: "-Xdock:name=" does seem to work from the command-line
  }

  // Lifted from the web site:
  val SlamDataOrange = new Color(0xFF8500)
  val SlamDataGrey =   new Color(0x4D4D4D)
  val SlamDataBlue =   new Color(0x2598B7)
  val SlamDataGreen =  new Color(0x85BF00)

  // Related colors:
  val SubtleOrange =   new Color(0xFFC180)//(0xFFB566)//(0xFF9C30)
  val LighterGrey =    new Color(0x999999)
  // val EvenLighterGrey = new Color(0xCCCCCC)

  def init = initNimbus

  def initNimbus = {
    // Map(
    //   "nimbusBase" -> SlamDataGrey,  // buttons
    //   "nimbusBlueGrey" -> LighterGrey,  // other controls
    //   // "control" -> EvenLighterGrey,  // panels
    //   "control" -> SlamDataGrey,  // panels
    //   "nimbusFocus" -> SlamDataOrange,
    //   "nimbusSelection" -> SubtleOrange,
    //   "nimbusSelectedText" -> Color.BLACK,
    //   "nimbusSelectionBackground" -> SubtleOrange,
    //   "textHighlight" -> SubtleOrange,
    //   "textHighlightText" -> Color.BLACK,
    //   "textBackground" -> SubtleOrange
    // ).map((UIManager.put _).tupled)

    \/.fromTryCatchNonFatal {
        UIManager.getInstalledLookAndFeels()
          .filter(_.getName == "Nimbus")
          .map(info => UIManager.setLookAndFeel(info.getClassName()))
    }
  }
}
