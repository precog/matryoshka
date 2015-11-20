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

  def fold[X](
    mnt: RDir => X,
    pln: RelPath[Sandboxed] => X
  ): X =
    this match {
      case Case.Mount(d) => mnt(d)
      case Case.Plain(p) => pln(p)
    }

  def dir: Option[RDir] =
    fold(some, _.swap.toOption)

  def file: Option[RFile] =
    fold(κ(none[RFile]), _.toOption)

  def path: RelPath[Sandboxed] =
    fold(\/.left, ι)
}

object Node {
  object Case {
    final case class Mount(d: RDir) extends Node
    final case class Plain(p: RelPath[Sandboxed]) extends Node
  }

  val Mount: RDir => Node =
    Case.Mount(_)

  val Plain: RelPath[Sandboxed] => Node =
    Case.Plain(_)

  val Dir: RDir => Node =
    Plain compose \/.left

  val File: RFile => Node =
    Plain compose \/.right

  val mount: Prism[Node, RDir] =
    Prism((_: Node).fold(_.some, κ(none)))(Mount)

  val plain: Prism[Node, RelPath[Sandboxed]] =
    Prism((_: Node).fold(κ(none), _.some))(Plain)

  def fromFirstSegmentOf(f: RFile): Option[Node] =
    flatten(none, none, none,
      n => Dir(dir(n)).some,
      n => File(file(n)).some,
      f).toIList.unite.headOption

  implicit val nodeEncodeJson: EncodeJson[Node] =
    EncodeJson(node => Json(
      "name" := node.path.fold(posixCodec.printPath, posixCodec.printPath),
      "type" := node.fold(κ("mount"), _.fold(κ("directory"), κ("file")))
    ))

  implicit val nodeOrdering: Ordering[Node] =
    Ordering.by(_.path.fold(posixCodec.printPath, posixCodec.printPath))

  implicit val nodeOrder: Order[Node] =
    Order.fromScalaOrdering
}
