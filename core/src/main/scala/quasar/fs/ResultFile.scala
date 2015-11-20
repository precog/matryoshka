package quasar

import quasar.Predef._
import quasar.fs._

import monocle.{Lens, Prism}

sealed trait ResultFile

object ResultFile {
  object Case {
    final case class User(file: AFile) extends ResultFile
    final case class Temp(file: AFile) extends ResultFile
  }

  /** Path to a result which names an unaltered source file or the requested
    * destination.
    */
  val User: AFile => ResultFile =
    Case.User(_)

  /** Path to a result which names a new temporary file created during plan
    * execution.
    */
  val Temp: AFile => ResultFile =
    Case.Temp(_)

  val user: Prism[ResultFile, AFile] =
    Prism[ResultFile, AFile] {
      case Case.User(f) => Some(f)
      case _ => None
    } (User)

  val temp: Prism[ResultFile, AFile] =
    Prism[ResultFile, AFile] {
      case Case.Temp(f) => Some(f)
      case _ => None
    } (Temp)

  val resultFile: Lens[ResultFile, AFile] =
    Lens[ResultFile, AFile] {
      case Case.User(f) => f
      case Case.Temp(f) => f
    } { f => {
      case Case.User(_) => User(f)
      case Case.Temp(_) => Temp(f)
    }}
}
