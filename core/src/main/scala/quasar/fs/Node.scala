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

package quasar
package fs

import quasar.Predef._
import quasar.fp._

import scala.math.Ordering

import argonaut._, Argonaut._
import monocle.Prism
import pathy.Path._
import scalaz._, Scalaz._

sealed trait Node {
  import Node._

  def fold[X](mnt: RDir => X, pln: RPath => X, viw: RFile => X): X =
    this match {
      case Case.Mount(d) => mnt(d)
      case Case.Plain(p) => pln(p)
      case Case.View(p)  => viw(p)
    }

  def dir: Option[RDir] =
    fold(some, p => refineType(p).swap.toOption, κ(none[RDir]))

  def file: Option[RFile] =
    fold(κ(none[RFile]), p => refineType(p).toOption, some)

  def path: RPath =
    fold(ι, ι, ι)
}

object Node {
  object Case {
    final case class Mount(d: RDir) extends Node
    final case class Plain(p: RPath) extends Node
    final case class View(p: RFile) extends Node
  }

  val Mount: RDir => Node =
    Case.Mount(_)

  val Plain: RPath => Node =
    Case.Plain(_)

  val View: RFile => Node =
    Case.View(_)

  val mount: Prism[Node, RDir] =
    Prism((_: Node).fold(_.some, κ(none), κ(none)))(Mount)

  val plain: Prism[Node, RPath] =
    Prism((_: Node).fold(κ(none), _.some, κ(none)))(Plain)

  val view: Prism[Node, RFile] =
    Prism((_: Node).fold(κ(none), κ(none), _.some))(View)

  def fromFirstSegmentOf(f: RFile): Option[Node] =
    flatten(none, none, none,
      n => Plain(dir(n)).some,
      n => Plain(file(n)).some,
      f).toIList.unite.headOption

  implicit val nodeEncodeJson: EncodeJson[Node] =
    EncodeJson(node => Json(
      ("name" := posixCodec.printPath(node.path)) ::
        ("type" := node.fold(κ("directory"), p => refineType(p).fold(κ("directory"), κ("file")), κ("file"))) ::
        node.fold(κ("mongodb".some), κ(none), κ("view".some)).map("mount" := _).toList: _*))

  implicit val nodeOrdering: Ordering[Node] =
    Ordering.by(n => posixCodec.printPath(n.path))

  implicit val nodeOrder: Order[Node] =
    Order.fromScalaOrdering
}
