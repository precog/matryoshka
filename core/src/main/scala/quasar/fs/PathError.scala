package quasar
package fs

import quasar.Predef._
import quasar.fp._

import monocle.{Prism, Lens}

import pathy.Path._

import scalaz._
import scalaz.syntax.std.option._
import scalaz.std.option._

// TODO: Rename to [[PathError]] once we've deprecated the other [[Path]] type.
sealed trait PathError2 {
  import PathError2._

  def fold[X](
    pathExists: AbsPath[Sandboxed] => X,
    pathNotFound: AbsPath[Sandboxed] => X,
    invalidPath: (AbsPath[Sandboxed], String) => X
  ): X =
    this match {
      case PathExists0(p)     => pathExists(p)
      case PathNotFound0(p)   => pathNotFound(p)
      case InvalidPath0(p, r) => invalidPath(p, r)
    }
}

object PathError2 {
  private final case class PathExists0(path: AbsPath[Sandboxed])
    extends PathError2
  private final case class PathNotFound0(path: AbsPath[Sandboxed])
    extends PathError2
  private final case class InvalidPath0(path: AbsPath[Sandboxed], reason: String)
    extends PathError2

  val PathExists: AbsPath[Sandboxed] => PathError2 =
    PathExists0(_)

  val DirExists: AbsDir[Sandboxed] => PathError2 =
    PathExists compose \/.left

  val FileExists: AbsFile[Sandboxed] => PathError2 =
    PathExists compose \/.right

  val PathNotFound: AbsPath[Sandboxed] => PathError2 =
    PathNotFound0(_)

  val DirNotFound: AbsDir[Sandboxed] => PathError2 =
    PathNotFound compose \/.left

  val FileNotFound: AbsFile[Sandboxed] => PathError2 =
    PathNotFound compose \/.right

  val InvalidPath: (AbsPath[Sandboxed], String) => PathError2 =
    InvalidPath0(_, _)

  val pathExists: Prism[PathError2, AbsPath[Sandboxed]] =
    Prism((_: PathError2).fold(_.some, κ(none), κ(none)))(PathExists)

  val pathNotFound: Prism[PathError2, AbsPath[Sandboxed]] =
    Prism((_: PathError2).fold(κ(none), _.some, κ(none)))(PathNotFound)

  val invalidPath: Prism[PathError2, (AbsPath[Sandboxed], String)] =
    Prism((_: PathError2).fold(κ(none), κ(none), (_, _).some))(InvalidPath.tupled)

  val errorPath: Lens[PathError2, AbsPath[Sandboxed]] =
    Lens((_: PathError2).fold(ι, ι, (p, _) => p))(p =>
      _.fold(κ(PathExists(p)), κ(PathNotFound(p)), (_, r) => InvalidPath(p, r)))

  implicit val pathErrorShow: Show[PathError2] = {
    val typeStr: AbsPath[Sandboxed] => String =
      _.fold(κ("Dir"), κ("File"))

    Show.shows(_.fold(
      p => s"${typeStr(p)} already exists: " +
           p.fold(posixCodec.printPath, posixCodec.printPath),

      p => s"${typeStr(p)} not found: " +
           p.fold(posixCodec.printPath, posixCodec.printPath),

      (p, r) => s"${typeStr(p)} " +
           p.fold(posixCodec.printPath, posixCodec.printPath) +
           s" is invalid: $r"))
  }
}

