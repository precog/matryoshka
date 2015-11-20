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

  def fold[X](mnt: RDir => X, pln: RPath => X): X =
    this match {
      case Case.Mount(d) => mnt(d)
      case Case.Plain(p) => pln(p)
    }

  def dir: Option[RDir] =
    fold(some, p => refineType(p).swap.toOption)

  def file: Option[RFile] =
    fold(κ(none[RFile]), p => refineType(p).toOption)

  def path: RPath =
    fold(ι, ι)
}

object Node {
  object Case {
    final case class Mount(d: RDir) extends Node
    final case class Plain(p: RPath) extends Node
  }

  val Mount: RDir => Node =
    Case.Mount(_)

  val Plain: RPath => Node =
    Case.Plain(_)

  val mount: Prism[Node, RDir] =
    Prism((_: Node).fold(_.some, κ(none)))(Mount)

  val plain: Prism[Node, RPath] =
    Prism((_: Node).fold(κ(none), _.some))(Plain)

  def fromFirstSegmentOf(f: RFile): Option[Node] =
    flatten(none, none, none,
      n => Plain(dir(n)).some,
      n => Plain(file(n)).some,
      f).toIList.unite.headOption

  implicit val nodeEncodeJson: EncodeJson[Node] =
    EncodeJson(node => Json(
      "name" := posixCodec.printPath(node.path),
      "type" := node.fold(κ("mount"), p => refineType(p).fold(κ("directory"), κ("file")))
    ))

  implicit val nodeOrdering: Ordering[Node] =
    Ordering.by(n => posixCodec.printPath(n.path))

  implicit val nodeOrder: Order[Node] =
    Order.fromScalaOrdering
}
