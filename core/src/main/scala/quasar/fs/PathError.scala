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
    final case class PathExists(path: APath)
      extends PathError2
    final case class PathNotFound(path: APath)
      extends PathError2
    final case class InvalidPath(path: APath, reason: String)
      extends PathError2
  }

  val PathExists: APath => PathError2 =
    Case.PathExists(_)

  val PathNotFound: APath => PathError2 =
    Case.PathNotFound(_)

  val InvalidPath: (APath, String) => PathError2 =
    Case.InvalidPath(_, _)

  val pathExists: Prism[PathError2, APath] =
    Prism[PathError2, APath] {
      case Case.PathExists(p) => Some(p)
      case _ => None
    } (PathExists)

  val pathNotFound: Prism[PathError2, APath] =
    Prism[PathError2, APath] {
      case Case.PathNotFound(p) => Some(p)
      case _ => None
    } (PathNotFound)

  val invalidPath: Prism[PathError2, (APath, String)] =
    Prism[PathError2, (APath, String)] {
      case Case.InvalidPath(p, r) => Some((p, r))
      case _ => None
    } (InvalidPath.tupled)

  val errorPath: Lens[PathError2, APath] =
    Lens[PathError2, APath] {
      case Case.PathExists(p)     => p
      case Case.PathNotFound(p)   => p
      case Case.InvalidPath(p, _) => p
    } { p => {
      case Case.PathExists(_)     => PathExists(p)
      case Case.PathNotFound(_)   => PathNotFound(p)
      case Case.InvalidPath(_, r) => InvalidPath(p, r)
    }}

  implicit val pathErrorShow: Show[PathError2] = {
    val typeStr: APath => String =
      p => refineType(p).fold(κ("Dir"), κ("File"))

    Show.shows {
      case Case.PathExists(p) =>
        s"${typeStr(p)} already exists: ${posixCodec.printPath(p)}"

      case Case.PathNotFound(p) =>
        s"${typeStr(p)} not found: ${posixCodec.printPath(p)}"

      case Case.InvalidPath(p, r) =>
        s"${typeStr(p)} ${posixCodec.printPath(p)} is invalid: $r"
    }
  }
}

