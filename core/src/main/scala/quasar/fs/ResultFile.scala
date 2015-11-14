package quasar

import quasar.fp._

import monocle.{Lens, Prism}

import pathy.Path._

import scalaz.std.option._
import scalaz.syntax.std.option._

sealed trait ResultFile {
  import ResultFile._

  def fold[X](
    user: AbsFile[Sandboxed] => X,
    temp: AbsFile[Sandboxed] => X
  ): X =
    this match {
      case User0(f) => user(f)
      case Temp0(f) => temp(f)
    }
}

object ResultFile {
  private final case class User0(file: AbsFile[Sandboxed]) extends ResultFile
  private final case class Temp0(file: AbsFile[Sandboxed]) extends ResultFile

  /** Path to a result which names an unaltered source file or the requested
    * destination.
    */
  val User: AbsFile[Sandboxed] => ResultFile = User0(_)

  /** Path to a result which names a new temporary file created during plan
    * execution.
    */
  val Temp: AbsFile[Sandboxed] => ResultFile = Temp0(_)

  val user: Prism[ResultFile, AbsFile[Sandboxed]] =
    Prism((_: ResultFile).fold(_.some, κ(none)))(User)

  val temp: Prism[ResultFile, AbsFile[Sandboxed]] =
    Prism((_: ResultFile).fold(κ(none), _.some))(Temp)

  val resultFile: Lens[ResultFile, AbsFile[Sandboxed]] =
    Lens((_: ResultFile).fold(ι, ι))(f => _.fold(κ(User(f)), κ(Temp(f))))
}
