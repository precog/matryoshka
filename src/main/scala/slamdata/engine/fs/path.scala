package slamdata.engine.fs

import scalaz._
import Scalaz._

import argonaut._, Argonaut._

// TODO: Should probably make this an ADT
final case class Path private (dir: List[DirNode], file: Option[FileNode] = None) {
  def contains(that: Path): Boolean = {
    dir.length <= that.dir.length && (that.dir.take(dir.length) == dir)
  }

  def pureFile = (dir.isEmpty || (dir.length == 1 && dir(0).value == ".")) && !file.isEmpty

  def pureDir = file.isEmpty

  def ++(path: Path) = Path(dir ++ (if (path.relative) path.dir.tail else path.dir), path.file)

  def withFile(path: Path) = copy(file = path.file)

  def withDir(path: Path) = copy(dir = path.dir)

  def dirOf: Path = copy(file = None)

  def fileOf: Path = copy(dir = Nil)

  def head: Path = dir match {
    case DirNode.Current :: Nil => Path(DirNode.Current :: Nil, file)
    case DirNode.Current :: head :: _ => Path(DirNode.Current :: head :: Nil, None)
    case head :: _ => Path(head :: Nil, None)
    case _=> this
  }

  def asAbsolute: Path = dir match {
    case DirNode.Current :: ds => Path(ds, file)
    case _ => this
  }
  
  def asDir: Path = file match {
    case Some(fileNode) => Path(dir :+ DirNode(fileNode.value))
    case None => this
  }
  
  def relative = dir.headOption == Some(DirNode.Current)

  def absolute = !relative

  lazy val pathname = dirname + filename

  lazy val dirname = if (relative) {
    ("./" + dir.drop(1).map(_.value).mkString("/") + "/").replaceAll("/+", "/")
  } else {
    ("/" + dir.map(_.value).mkString("/") + "/").replaceAll("/+", "/")
  }

  lazy val filename = file.map(_.value).getOrElse("")

  def ancestors: List[Path] = dir.reverse.tails.map(ds => Path(ds.reverse, None)).toList
  
  def relativeTo(path: Path): Option[Path] = 
    if (path.pureDir && path.contains(this)) Some(Path(DirNode.Current :: dir.drop(path.dir.length), file))
    else None

  override lazy val toString = pathname
}

object Path {
  implicit def PathEncodeJson = EncodeJson[Path] { p =>
    val simplePathName = p.pathname.replaceFirst("^\\./", "").replaceFirst("/$", "")
    Json("name" := simplePathName, "type" := (if (p.file.isEmpty) "directory" else "file"))
  }

  implicit val PathOrder: scala.Ordering[Path] = scala.Ordering[(String, Boolean)].on(p => (p.pathname, p.pureDir))

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
  val Current = DirNode(".")
}
final case class FileNode(value: String)

case class PathError(hint: Option[String]) extends slamdata.engine.Error {
  def message = hint.getOrElse("invalid path")
}

case class FSTable[A](private val table0: Map[Path, A]) {
  val table = table0.mapKeys(_.asAbsolute.asDir)
  
  def isEmpty = table.isEmpty
  
  def lookup(path: Path): Option[(A, Path)] =
    path.ancestors.map(p => table.get(p).map(_ -> p)).flatten.headOption.map {
      case (a, p) => path.relativeTo(p).map(relPath => a -> relPath)
    }.flatten
    
  def children(path: Path): List[Path] = 
    table.keys.filter(path contains _).toList.map(_.relativeTo(path).map(_.head)).flatten
}
