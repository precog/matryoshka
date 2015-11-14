package quasar
package fs

import quasar.Predef._
import quasar.fp._

import scala.math.Ordering

import argonaut._, Argonaut._

import pathy.Path._

import scalaz._, Scalaz._

sealed trait Node {
  import Node._

  def fold[X](
    mnt: RelDir[Sandboxed] => X,
    pln: RelPath[Sandboxed] => X
  ): X =
    this match {
      case Mount0(d) => mnt(d)
      case Plain0(p) => pln(p)
    }

  def dir: Option[RelDir[Sandboxed]] =
    fold(some, _.swap.toOption)

  def file: Option[RelFile[Sandboxed]] =
    fold(κ(none[RelFile[Sandboxed]]), _.toOption)

  def path: RelPath[Sandboxed] =
    fold(\/.left, ι)
}

object Node {
  private final case class Mount0(d: RelDir[Sandboxed]) extends Node
  private final case class Plain0(p: RelPath[Sandboxed]) extends Node

  val Mount: RelDir[Sandboxed] => Node =
    Mount0(_)

  val Plain: RelPath[Sandboxed] => Node =
    Plain0(_)

  val Dir: RelDir[Sandboxed] => Node =
    Plain compose \/.left

  val File: RelFile[Sandboxed] => Node =
    Plain compose \/.right

  def fromFirstSegmentOf(f: RelFile[Sandboxed]): Option[Node] =
    flatten(none, none, none,
      n => Dir(dir(n)).some,
      n => File(file(n)).some,
      f).unite.headOption

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
