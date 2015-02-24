package slamdata.engine.admin

import scala.swing._
import java.awt.CardLayout

class CardPanel extends Panel with LayoutContainer {
  def layoutManager = peer.getLayout.asInstanceOf[CardLayout]
  override lazy val peer = new javax.swing.JPanel(new CardLayout) with SuperMixin

  def show(card: String) = layoutManager.show(peer, card)

  type Constraints = String

  def constraintsFor(c: Component) = ???

  protected def areValid(c: Constraints): (Boolean, String) = (true, "")
  protected def add(c: Component, l: Constraints) {
    peer.add(c.peer, l.toString)
  }
}
