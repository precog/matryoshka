package slamdata.engine.fs

import scalaz._

import argonaut._, Argonaut._

case class Path(dir: List[DirNode], file: Option[FileNode] = None) {
  def pathname = ("/" + dir.map(_.value).mkString("/") + "/").replaceAll("/+", "/") + file.map(_.value).getOrElse("")

  override lazy val toString = pathname
}

object Path {
  implicit def Codec = CodecJson[Path](
    encoder = x => jString(x.toString),
    decoder = (j: HCursor) => DecodeJson.StringDecodeJson.decode(j).map(apply _)
  )

  val Root = Path(Nil, None)

  def apply(value: String): Path = {
    val segs = value.replaceAll("/+", "/").split("/").toList.filter(_ != "")

    if (segs.length == 0) Root
    else if (value.endsWith("/")) new Path(segs.map(DirNode.apply), None)
    else new Path(segs.init.map(DirNode.apply), Some(FileNode(segs.last)))
  }

  def dir(segs: List[String]): Path = new Path(segs.map(DirNode.apply))

  def file(dir0: List[String], file: String) = dir(dir0).copy(file = Some(FileNode(file)))
}

case class DirNode(value: String)
case class FileNode(value: String)