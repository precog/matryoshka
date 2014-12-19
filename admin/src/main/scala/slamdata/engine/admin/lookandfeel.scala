package slamdata.engine.admin

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