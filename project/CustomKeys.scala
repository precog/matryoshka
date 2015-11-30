import sbt._
import Keys._

object CustomKeys {
  val scalazVersion  = settingKey[String]("The scalaz version used for building.")
  val slcVersion     = settingKey[String]("The slc version used for building.")
  val monocleVersion = settingKey[String]("The monocle version used for building.")
  val pathyVersion   = settingKey[String]("The pathy version used for building.")
  val http4sVersion  = settingKey[String]("The http4s version used for building.")
}
