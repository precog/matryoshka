package quasar
package fs

import quasar.Predef._
import quasar.fp._

import monocle.{Prism, Lens}
import pathy.Path._
import scalaz._

// TODO: Rename to [[PathError]] once we've deprecated the other [[Path]] type.
sealed trait PathError2

object PathError2 {
  object Case {
    final case class PathExists(path: AbsPath[Sandboxed])
      extends PathError2
    final case class PathNotFound(path: AbsPath[Sandboxed])
      extends PathError2
    final case class InvalidPath(path: AbsPath[Sandboxed], reason: String)
      extends PathError2
  }

  val PathExists: AbsPath[Sandboxed] => PathError2 =
    Case.PathExists(_)

  val DirExists: AbsDir[Sandboxed] => PathError2 =
    PathExists compose \/.left

  val FileExists: AbsFile[Sandboxed] => PathError2 =
    PathExists compose \/.right

  val PathNotFound: AbsPath[Sandboxed] => PathError2 =
    Case.PathNotFound(_)

  val DirNotFound: AbsDir[Sandboxed] => PathError2 =
    PathNotFound compose \/.left

  val FileNotFound: AbsFile[Sandboxed] => PathError2 =
    PathNotFound compose \/.right

  val InvalidPath: (AbsPath[Sandboxed], String) => PathError2 =
    Case.InvalidPath(_, _)

  val pathExists: Prism[PathError2, AbsPath[Sandboxed]] =
    Prism[PathError2, AbsPath[Sandboxed]] {
      case Case.PathExists(p) => Some(p)
      case _ => None
    } (PathExists)

  val pathNotFound: Prism[PathError2, AbsPath[Sandboxed]] =
    Prism[PathError2, AbsPath[Sandboxed]] {
      case Case.PathNotFound(p) => Some(p)
      case _ => None
    } (PathNotFound)

  val invalidPath: Prism[PathError2, (AbsPath[Sandboxed], String)] =
    Prism[PathError2, (AbsPath[Sandboxed], String)] {
      case Case.InvalidPath(p, r) => Some((p, r))
      case _ => None
    } (InvalidPath.tupled)

  val errorPath: Lens[PathError2, AbsPath[Sandboxed]] =
    Lens[PathError2, AbsPath[Sandboxed]] {
      case Case.PathExists(p)     => p
      case Case.PathNotFound(p)   => p
      case Case.InvalidPath(p, _) => p
    } { p => {
      case Case.PathExists(_)     => PathExists(p)
      case Case.PathNotFound(_)   => PathNotFound(p)
      case Case.InvalidPath(_, r) => InvalidPath(p, r)
    }}

  implicit val pathErrorShow: Show[PathError2] = {
    val typeStr: AbsPath[Sandboxed] => String =
      _.fold(κ("Dir"), κ("File"))

    Show.shows {
      case Case.PathExists(p) =>
        s"${typeStr(p)} already exists: " +
          p.fold(posixCodec.printPath, posixCodec.printPath)

      case Case.PathNotFound(p) =>
        s"${typeStr(p)} not found: " +
          p.fold(posixCodec.printPath, posixCodec.printPath)

      case Case.InvalidPath(p, r) =>
        s"${typeStr(p)} " +
          p.fold(posixCodec.printPath, posixCodec.printPath) +
          s" is invalid: $r"
    }
  }
}

