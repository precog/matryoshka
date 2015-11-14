package quasar
package fs

import quasar.Predef._
import quasar.fp._
import quasar.recursionschemes._
import quasar.Planner.{PlannerError => PlannerErr}

import monocle.Prism

import scalaz._
import scalaz.std.option._
import scalaz.syntax.std.option._
import scalaz.syntax.show._

import ReadFile.ReadHandle
import WriteFile.WriteHandle

sealed trait FileSystemError {
  import FileSystemError._

  def fold[X](
    pathError: PathError2 => X,
    plannerError: (Fix[LogicalPlan], PlannerErr) => X,
    unkReadHandle: ReadHandle => X,
    unkWriteHandle: WriteHandle => X,
    partialWrite: Int => X,
    writeFailed: (Data, String) => X
  ): X =
    this match {
      case PathError0(e)          => pathError(e)
      case PlannerError0(lp, e)   => plannerError(lp, e)
      case UnknownReadHandle0(h)  => unkReadHandle(h)
      case UnknownWriteHandle0(h) => unkWriteHandle(h)
      case PartialWrite0(n)       => partialWrite(n)
      case WriteFailed0(d, s)     => writeFailed(d, s)
    }
}

object FileSystemError {
  private final case class PathError0(e: PathError2)
    extends FileSystemError
  private final case class PlannerError0(lp: Fix[LogicalPlan], err: PlannerErr)
    extends FileSystemError
  private final case class UnknownReadHandle0(h: ReadHandle)
    extends FileSystemError
  private final case class UnknownWriteHandle0(h: WriteHandle)
    extends FileSystemError
  private final case class PartialWrite0(numFailed: Int)
    extends FileSystemError
  private final case class WriteFailed0(data: Data, reason: String)
    extends FileSystemError

  val PathError: PathError2 => FileSystemError =
    PathError0(_)

  val PlannerError: (Fix[LogicalPlan], PlannerErr) => FileSystemError =
    PlannerError0(_, _)

  val UnknownReadHandle: ReadHandle => FileSystemError =
    UnknownReadHandle0(_)

  val UnknownWriteHandle: WriteHandle => FileSystemError =
    UnknownWriteHandle0(_)

  val PartialWrite: Int => FileSystemError =
    PartialWrite0(_)

  val WriteFailed: (Data, String) => FileSystemError =
    WriteFailed0(_, _)

  val pathError: Prism[FileSystemError, PathError2] =
    Prism((_: FileSystemError).fold(_.some, κ(none), κ(none), κ(none), κ(none), (_, _) => none))(PathError)

  val plannerError: Prism[FileSystemError, (Fix[LogicalPlan], PlannerErr)] =
    Prism((_: FileSystemError).fold(κ(none), (lp, e) => (lp, e).some, κ(none), κ(none), κ(none), κ(none)))(PlannerError.tupled)

  val unknownReadHandle: Prism[FileSystemError, ReadHandle] =
    Prism((_: FileSystemError).fold(κ(none), κ(none), _.some, κ(none), κ(none), (_, _) => none))(UnknownReadHandle)

  val unknownWriteHandle: Prism[FileSystemError, WriteHandle] =
    Prism((_: FileSystemError).fold(κ(none), κ(none), κ(none), _.some, κ(none), (_, _) => none))(UnknownWriteHandle)

  val partialWrite: Prism[FileSystemError, Int] =
    Prism((_: FileSystemError).fold(κ(none), κ(none), κ(none), κ(none), _.some, (_, _) => none))(PartialWrite)

  val writeFailed: Prism[FileSystemError, (Data, String)] =
    Prism((_: FileSystemError).fold(κ(none), κ(none), κ(none), κ(none), κ(none), (d, r) => (d, r).some))(WriteFailed.tupled)

  implicit def fileSystemErrorShow: Show[FileSystemError] =
    Show.shows(_.fold(
      e      => e.shows,
      (_, e) => e.shows,
      h      => s"Attempted to read from an unknown or closed handle: ${h.run}",
      h      => s"Attempted to write to an unknown or closed handle: ${h.run}",
      n      => s"Failed to write $n data.",
      (d, r) => s"Failed to write datum: reason='$r', datum=${d.shows}"))
}
