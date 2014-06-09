package slamdata.engine.fs

import scalaz._

import argonaut._, Argonaut._

final case class Path(dir: List[DirNode], file: Option[FileNode] = None) {
  def relative = dir.headOption.map(_.value == ".").getOrElse(false)

  def absolute = !relative

  lazy val pathname = dirname + filename

  lazy val dirname = if (relative) {
    ("./" + dir.drop(1).map(_.value).mkString("/") + "/").replaceAll("/+", "/")
  } else {
    ("/" + dir.map(_.value).mkString("/") + "/").replaceAll("/+", "/")
  }

  lazy val filename = file.map(_.value).getOrElse("")

  override lazy val toString = pathname
}

object Path {
  implicit def Codec = CodecJson[Path](
    encoder = x => jString(x.toString),
    decoder = (j: HCursor) => DecodeJson.StringDecodeJson.decode(j).map(apply _)
  )

  val Root = Path(Nil, None)

  val Current = Path(".")

  def apply(value: String): Path = {
    val segs = value.replaceAll("/+", "/").split("/").toList.filter(_ != "")

    if (segs.length == 0) Root    
    else if (value == ".") new Path(DirNode(".") :: Nil, None)
    else if (value.endsWith("/")) new Path(segs.map(DirNode.apply), None)
    else new Path(segs.init.map(DirNode.apply), Some(FileNode(segs.last)))
  }

  def dir(segs: List[String]): Path = new Path(segs.map(DirNode.apply))

  def file(dir0: List[String], file: String) = dir(dir0).copy(file = Some(FileNode(file)))

  def canonicalize(value: String): String = Path(value).pathname
}

final case class DirNode(value: String)
final case class FileNode(value: String)