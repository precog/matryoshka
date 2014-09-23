package slamdata.engine.fs

import scalaz._
import Scalaz._

import argonaut._, Argonaut._

// TODO: Should probably make this an ADT
final case class Path private (dir: List[DirNode], file: Option[FileNode] = None) {
  def contains(that: Path): Boolean = {
    file.isEmpty && dir.length <= that.dir.length && (that.dir.take(dir.length) == dir)
  }

  def pureFile = (dir.isEmpty || (dir.length == 1 && dir(0).value == ".")) && !file.isEmpty

  def pureDir = file.isEmpty

  def ++(path: Path) =
    Path(dir ++ (if (path.relative) path.dir.drop(1) else path.dir), path.file)

  def withFile(path: Path) = copy(file = path.file)

  def withDir(path: Path) = copy(dir = path.dir)

  def dirOf: Path = copy(file = None)

  def fileOf: Path = copy(dir = Nil)

  def parent: Path = if (pureDir) Path(dir.dropRight(1), None) else dirOf

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

  /** Pathname with no leading "./" or trailing "/", for UIs, mostly. */
  def simplePathname = pathname.replaceFirst("^\\./", "").replaceFirst("/$", "")

  lazy val dirname = if (relative) {
    ("./" + dir.drop(1).map(_.value).mkString("/") + "/").replaceAll("/+", "/")
  } else {
    ("/" + dir.map(_.value).mkString("/") + "/").replaceAll("/+", "/")
  }

  lazy val filename = file.map(_.value).getOrElse("")

  def ancestors: List[Path] = dir.reverse.tails.map(ds => Path(ds.reverse, None)).toList

  def rebase(referenceDir: Path): PathError \/ Path =
    if (referenceDir.pureDir && referenceDir.contains(this)) \/- (Path(DirNode.Current :: dir.drop(referenceDir.dir.length), file))
    else -\/ (PathError(Some("path not contained by referenceDir: " + this + "; " + referenceDir)))

  def from(workingDir: Path) : PathError \/ Path =
    if (!workingDir.pureDir) -\/ (PathError(Some("invalid workingDir: " + workingDir.pathname + " (not a directory path)")))
    else \/- (if (relative) (workingDir ++ this) else this)

  /**
   Interpret this path, which may be absolute or relative to a certain (absolute) working directory, so
   that it is definitely relative to a particular (absolute) reference directory. If either directory
   path is not absolute, or if this path is not contained by the reference directory, an error
   results.
   */
  def interpret(referenceDir: Path, workingDir: Path): PathError \/ Path = for {
    actual <- from(workingDir)
    rel    <- actual.rebase(referenceDir)
  } yield rel
    
  override lazy val toString = pathname
}

object Path {
  implicit def ShowPath = new Show[Path] {
    override def show(v: Path) = Cord(v.pathname)
  }

  implicit def PathEncodeJson = EncodeJson[Path] { p =>
    Json("name" := p.simplePathname, "type" := (if (p.file.isEmpty) "directory" else "file"))
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
      val file = segs.lastOption.map(FileNode(_))

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
  
  def lookup(path: Path): Option[(A, Path, Path)] =
    path.ancestors.map(p => table.get(p).map(_ -> p)).flatten.headOption.map {
      case (a, p) => path.rebase(p).toOption.map(relPath => (a, p, relPath))
    }.flatten
    
  def children(path: Path): List[Path] = 
    table.keys.filter(path contains _).toList.map(_.rebase(path).toOption.map(_.head)).flatten
}
