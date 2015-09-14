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

package quasar.fs

import quasar.Predef._

import scalaz._, Scalaz._

// TODO: Should probably make this an ADT
final case class Path(dir: List[DirNode], file: Option[FileNode]) {
  import Path._

  def pureFile = (dir.isEmpty || (dir.length == 1 && dir(0).value == ".")) && !file.isEmpty

  def pureDir = file.isEmpty

  def ++(path: Path) =
    Path(dir ++ (if (path.relative) path.dir.drop(1) else path.dir), path.file)

  def withFile(path: Path) = copy(file = path.file)

  def withDir(path: Path) = copy(dir = path.dir)

  def dirOf: Path = copy(file = None)

  def fileOf: Path = copy(dir = List(DirNode.Current))

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

  def asRelative: Path = Path.Current ++ this

  def asDir: Path = file match {
    case Some(fileNode) => Path(dir :+ DirNode(fileNode.value), None)
    case None => this
  }

  def relative = dir.headOption == Some(DirNode.Current)

  def absolute = !relative

  lazy val pathname = dirname + filename

  /** Pathname with no leading "./" or trailing "/", for UIs, mostly. */
  def simplePathname = pathname.replaceFirst("^\\.?/", "").replaceFirst("/$", "")

  lazy val dirname = if (relative) {
    ("./" + dir.drop(1).map(_.value).mkString("/") + "/").replaceAll("/+", "/")
  } else {
    ("/" + dir.map(_.value).mkString("/") + "/").replaceAll("/+", "/")
  }

  lazy val filename = file.map(_.value).getOrElse("")

  def ancestors: List[Path] = dir.reverse.tails.map(ds => Path(ds.reverse, None)).toList

  def rebase(referenceDir: Path): PathError \/ Path =
    if (!referenceDir.pureDir) -\/(PathTypeError(referenceDir, Some("not a directory")))
    else if (referenceDir.dir.length <= dir.length &&
             dir.take(referenceDir.dir.length) == referenceDir.dir)
      \/-(Path(dir.drop(referenceDir.dir.length), file).asRelative)
    else -\/(NonexistentPathError(this, Some("not contained by referenceDir (" + referenceDir + ")")))

  def from(workingDir: Path) : PathError \/ Path =
    if (!workingDir.pureDir) -\/(PathTypeError(workingDir, Some("invalid workingDir (not a directory)")))
    else \/- (if (relative) (workingDir ++ this) else this)

  override lazy val toString = pathname
}

object Path {
  implicit def ShowPath = new Show[Path] {
    override def show(v: Path) = Cord(v.pathname)
  }

  implicit val PathOrder: scala.Ordering[Path] =
    scala.Ordering[(String, Boolean)].on(p => (p.pathname, p.pureDir))

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
      val dir  = segs.dropRight(1).map(DirNode.apply)
      val file = segs.lastOption.map(FileNode(_))

      if (value.startsWith("/") || segs(0) == ".") new Path(dir, file)
      else new Path(DirNode.Current :: dir, file)
    }
  }

  def dir(segs: List[String]): Path = new Path(segs.map(DirNode.apply), None)

  def file(dir0: List[String], file: String) = dir(dir0).copy(file = Some(FileNode(file)))

  def fileAbs(file0: String) = file(Nil, file0)

  def fileRel(file0: String) = file("." :: Nil, file0)

  def canonicalize(value: String): String = Path(value).pathname

  sealed trait PathError {
    def message: String
  }
  object PathError {
    final case class ExistingPathError(path: Path, hint: Option[String])
        extends PathError {
      def message = path.pathname + ": " + hint.getOrElse("already exists")
    }

    final case class NonexistentPathError(path: Path, hint: Option[String])
        extends PathError {
      def message = path.pathname + ": " + hint.getOrElse("doesn't exist")
    }

    final case class PathTypeError(path: Path, hint: Option[String])
        extends PathError {
      def message = path.pathname + ": " + hint.getOrElse("not the correct path type")
    }

    final case class InvalidPathError(message: String) extends PathError

    /** Path errors that are the fault of our implementation. */
    final case class InternalPathError(message: String) extends PathError
  }

  implicit val PathErrorShow = Show.showFromToString[PathError]

  object ExistingPathError {
    def apply(path: Path, hint: Option[String]): PathError = PathError.ExistingPathError(path, hint)
    def unapply(obj: PathError): Option[(Path, Option[String])] = obj match {
      case PathError.ExistingPathError(path, hint) => Some((path, hint))
      case _                       => None
    }
  }
  object NonexistentPathError {
    def apply(path: Path, hint: Option[String]): PathError = PathError.NonexistentPathError(path, hint)
    def unapply(obj: PathError): Option[(Path, Option[String])] = obj match {
      case PathError.NonexistentPathError(path, hint) => Some((path, hint))
      case _                       => None
    }
  }
  object PathTypeError {
    def apply(path: Path, hint: Option[String]): PathError = PathError.PathTypeError(path, hint)
    def unapply(obj: PathError): Option[(Path, Option[String])] = obj match {
      case PathError.PathTypeError(path, hint) => Some((path, hint))
      case _                       => None
    }
  }
  object InvalidPathError {
    def apply(message: String): PathError = PathError.InvalidPathError(message)
    def unapply(obj: PathError): Option[String] = obj match {
      case PathError.InvalidPathError(message) => Some(message)
      case _                       => None
    }
  }
  object InternalPathError {
    def apply(message: String): PathError = PathError.InternalPathError(message)
    def unapply(obj: PathError): Option[String] = obj match {
      case PathError.InternalPathError(message) => Some(message)
      case _                       => None
    }
  }
}

final case class DirNode(value: String)
object DirNode {
  val Current = DirNode(".")
}
final case class FileNode(value: String)
