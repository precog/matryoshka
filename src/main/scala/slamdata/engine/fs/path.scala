package slamdata.engine.fs

import scalaz._

import argonaut._, Argonaut._

// TODO: Should probably make this an ADT
final case class Path private (dir: List[DirNode], file: Option[FileNode] = None) {
  def contains(that: Path): Boolean = {
    dir.length <= that.dir.length && (that.dir.take(dir.length) == dir)
  }

  def pureFile = (dir.isEmpty || (dir.length == 1 && dir(0).value == ".")) && !file.isEmpty

  def pureDir = file.isEmpty

  def withFile(path: Path) = copy(file = path.file)

  def withDir(path: Path) = copy(dir = path.dir)

  def dirOf: Path = copy(file = None)

  def fileOf: Path = copy(dir = Nil)

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
    else if (value == ".") new Path(DirNode.Current :: Nil, None)
    else if (value.endsWith("/")) {
      val dir = segs.map(DirNode.apply)

      if (value.startsWith("/") || segs(0) == ".") new Path(dir, None)
      else new Path(DirNode.Current :: dir, None)
    } else {
      val dir  = segs.init.map(DirNode.apply)
      val file = Some(FileNode(segs.last))

      if (value.startsWith("/") || segs(0) == ".") new Path(dir, file)
      else new Path(DirNode.Current :: dir, file)
    }
  }

  def dir(segs: List[String]): Path = new Path(segs.map(DirNode.apply))

  def file(dir0: List[String], file: String) = dir(dir0).copy(file = Some(FileNode(file)))

  def fileAbs(file0: String) = file(Nil, file0)

  def fileRel(file0: String) = file("." :: Nil, file0)

  def canonicalize(value: String): String = Path(value).pathname
}

final case class DirNode(value: String)
object DirNode {
  def Current = DirNode(".")
}
final case class FileNode(value: String)