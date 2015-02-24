package slamdata.engine.admin

import scala.swing._
import Swing._

import scalaz._
import scalaz.concurrent._

trait SwingUtils {
  /** Run on a worker thread, and handle the result on Swing's event thread. */
  def async[A](t: Task[A])(f: Throwable \/ A => Unit): Unit = Task.fork(t).runAsync(v => Swing.onEDT { f(v) } )

  def loadImage(relPath: String) = Icon(getClass.getResource(relPath))

  // NB: scale a @2x image by 0.5 for "poor man's Retina"
  def scale(icon: javax.swing.Icon, scale: Double) = new javax.swing.Icon {
    def getIconWidth  = (icon.getIconWidth*scale).toInt
    def getIconHeight = (icon.getIconHeight*scale).toInt
    def paintIcon(comp: java.awt.Component, g: java.awt.Graphics, x: Int, y: Int) = {
      val g2 = g.create(x, y, getIconWidth, getIconHeight).asInstanceOf[java.awt.Graphics2D]
      g2.scale(scale, scale)
      icon.paintIcon(comp, g2, x, y)
    }
  }

  def errorAlert(parent: Component, detail: String): Unit =
    Dialog.showMessage(parent, detail, javax.swing.UIManager.getString("OptionPane.messageDialogTitle"), Dialog.Message.Error)

  def copyToClipboard(text: String) = {
    val clipboard = java.awt.Toolkit.getDefaultToolkit().getSystemClipboard()
    clipboard.setContents(new java.awt.datatransfer.StringSelection(text), null)
  }

  // NB: when scala-swing was last updated, JComboBox did not have a type parameter:
  def comboBoxPeer[A](cb: ComboBox[A]) = cb.peer.asInstanceOf[javax.swing.JComboBox[A]]
  def comboBoxModel[A <: AnyRef](items: Seq[A]) = ComboBox.newConstantModel(items).asInstanceOf[javax.swing.ComboBoxModel[A]]

  /** Action that may be triggered many times, but fires no more than once, after a delay. */
  class CoalescingAction(delayMS: Int, body: => Unit) {
    var seq = 0

    def trigger = {
      seq += 1
      val cur = seq

      val timer = new javax.swing.Timer(delayMS, ActionListener { _ =>
        if (seq == cur) body
      })
      timer.setRepeats(false)
      timer.start
    }
  }

  val Valid   = new java.awt.Color(0xFFFFFF)
  val Invalid = new java.awt.Color(0xFFCCCC)

  implicit class TextComponentOps(comp: TextComponent) {
    def matched(pattern: scala.util.matching.Regex): String \/ Option[String] = {
      val t = comp match {
        case p: PasswordField => p.password.mkString.trim
        case _ => comp.text.trim
      }
      t match {
        case pattern() =>
          comp.peer.setBackground(Valid)
          \/-(if (t == "") None else Some(t))
        case _ =>
          comp.peer.setBackground(Invalid)
          -\/("not matched")
      }
    }

    def bindEditActions = {
      if (scala.util.Properties.isMac) {
        import java.awt.event.KeyEvent._
        import javax.swing.KeyStroke.getKeyStroke

        val cmd = java.awt.Toolkit.getDefaultToolkit().getMenuShortcutKeyMask()
        val opt = java.awt.event.InputEvent.ALT_DOWN_MASK
        val shift = java.awt.event.InputEvent.SHIFT_DOWN_MASK

        val extraKeys = Map(
          getKeyStroke(VK_X, cmd) -> "cut-to-clipboard",
          getKeyStroke(VK_C, cmd) -> "copy-to-clipboard",
          getKeyStroke(VK_V, cmd) -> "paste-from-clipboard",
          getKeyStroke(VK_A, cmd) -> "select-all",

          getKeyStroke(VK_LEFT,     opt) -> "caret-previous-word",
          getKeyStroke(VK_KP_LEFT,  opt) -> "caret-previous-word",
          getKeyStroke(VK_RIGHT,    opt) -> "caret-next-word",
          getKeyStroke(VK_KP_RIGHT, opt) -> "caret-next-word",

          getKeyStroke(VK_LEFT,     cmd) -> "caret-begin-line",
          getKeyStroke(VK_KP_LEFT,  cmd) -> "caret-begin-line",
          getKeyStroke(VK_RIGHT,    cmd) -> "caret-end-line",
          getKeyStroke(VK_KP_RIGHT, cmd) -> "caret-end-line",

          getKeyStroke(VK_LEFT,     opt + shift) -> "selection-previous-word",
          getKeyStroke(VK_KP_LEFT,  opt + shift) -> "selection-previous-word",
          getKeyStroke(VK_RIGHT,    opt + shift) -> "selection-next-word",
          getKeyStroke(VK_KP_RIGHT, opt + shift) -> "selection-next-word",

          getKeyStroke(VK_LEFT,     cmd + shift) -> "selection-begin-line",
          getKeyStroke(VK_KP_LEFT,  cmd + shift) -> "selection-begin-line",
          getKeyStroke(VK_RIGHT,    cmd + shift) -> "selection-end-line",
          getKeyStroke(VK_KP_RIGHT, cmd + shift) -> "selection-end-line"
        )

        val im = comp.peer.getInputMap
        extraKeys.map { case (k, a) => im.put(k, a) }
      }
    }
  }
}
object SwingUtils extends SwingUtils
