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
import java.awt.CardLayout

class CardPanel extends Panel with LayoutContainer {
  def layoutManager = peer.getLayout.asInstanceOf[CardLayout]
  override lazy val peer = new javax.swing.JPanel(new CardLayout) with SuperMixin

  def show(card: String) = layoutManager.show(peer, card)

  type Constraints = String

  def constraintsFor(c: Component) = ???

  protected def areValid(c: Constraints): (Boolean, String) = (true, "")
  protected def add(c: Component, l: Constraints): Unit =
    peer.add(c.peer, l.toString)
}
