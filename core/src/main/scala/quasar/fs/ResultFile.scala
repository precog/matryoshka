package quasar

import quasar.Predef._

import monocle.{Lens, Prism}
import pathy.Path._

sealed trait ResultFile

object ResultFile {
  object Case {
    final case class User(file: AbsFile[Sandboxed]) extends ResultFile
    final case class Temp(file: AbsFile[Sandboxed]) extends ResultFile
  }

  /** Path to a result which names an unaltered source file or the requested
    * destination.
    */
  val User: AbsFile[Sandboxed] => ResultFile =
    Case.User(_)

  /** Path to a result which names a new temporary file created during plan
    * execution.
    */
  val Temp: AbsFile[Sandboxed] => ResultFile =
    Case.Temp(_)

  val user: Prism[ResultFile, AbsFile[Sandboxed]] =
    Prism[ResultFile, AbsFile[Sandboxed]] {
      case Case.User(f) => Some(f)
      case _ => None
    } (User)

  val temp: Prism[ResultFile, AbsFile[Sandboxed]] =
    Prism[ResultFile, AbsFile[Sandboxed]] {
      case Case.Temp(f) => Some(f)
      case _ => None
    } (Temp)

  val resultFile: Lens[ResultFile, AbsFile[Sandboxed]] =
    Lens[ResultFile, AbsFile[Sandboxed]] {
      case Case.User(f) => f
      case Case.Temp(f) => f
    } { f => {
      case Case.User(_) => User(f)
      case Case.Temp(_) => Temp(f)
    }}
}
