package quasar

import Predef._
import quasar.fs.PathError2

import argonaut._, Argonaut._

import monocle._

import scalaz._
import scalaz.syntax.show._

sealed trait EnvironmentError2

object EnvironmentError2 {
  final case class ConnectionFailed(message: String)
    extends EnvironmentError2
  final case class EnvPathError(error: PathError2)
    extends EnvironmentError2
  final case class InsufficientPermissions(message: String)
    extends EnvironmentError2
  final case class InvalidConfig(message: String)
    extends EnvironmentError2
  final case class InvalidCredentials(message: String)
    extends EnvironmentError2
  final case class UnsupportedVersion(backendName: String, version: List[Int])
    extends EnvironmentError2

  val connectionFailed: Prism[EnvironmentError2, String] =
    Prism[EnvironmentError2, String] {
      case ConnectionFailed2(msg) => Some(msg)
      case _ => None
    } (ConnectionFailed2(_))

  val envPathError: Prism[EnvironmentError2, PathError2] =
    Prism[EnvironmentError2, PathError2] {
      case EnvPathError2(err) => Some(err)
      case _ => None
    } (EnvPathError2(_))

  val insufficientPermissions: Prism[EnvironmentError2, String] =
    Prism[EnvironmentError2, String] {
      case InsufficientPermissions2(msg) => Some(msg)
      case _ => None
    } (InsufficientPermissions2(_))

  val invalidConfig: Prism[EnvironmentError2, String] =
    Prism[EnvironmentError2, String] {
      case InvalidConfig2(msg) => Some(msg)
      case _ => None
    } (InvalidConfig2(_))

  val invalidCredentials: Prism[EnvironmentError2, String] =
    Prism[EnvironmentError2, String] {
      case InvalidCredentials2(msg) => Some(msg)
      case _ => None
    } (InvalidCredentials2(_))

  val unsupportedVersion: Prism[EnvironmentError2, (String, List[Int])] =
    Prism[EnvironmentError2, (String, List[Int])] {
      case UnsupportedVersion2(name, version) => Some((name, version))
      case _ => None
    } ((UnsupportedVersion2(_, _)).tupled)

  implicit val environmentErrorShow: Show[EnvironmentError2] =
    Show.shows {
      case ConnectionFailed2(msg) =>
        s"Connection failed: $msg"
      case EnvPathError2(error) =>
        error.shows
      case InsufficientPermissions2(msg) =>
        s"Insufficient permissions: $msg"
      case InvalidConfig2(msg) =>
        s"Invalid configuration: $msg"
      case InvalidCredentials2(msg) =>
        s"Invalid credentials: $msg"
      case UnsupportedVersion2(name, version) =>
        s"Unsupported $name version: ${version.mkString(".")}"
    }

  implicit val environmentErrorEncodeJson: EncodeJson[EnvironmentError2] = {
    def format(message: String, detail: Option[String]) =
      Json(("error" := message) :: detail.toList.map("errorDetail" := _): _*)

    EncodeJson[EnvironmentError2] {
      case ConnectionFailed2(msg) =>
        format("Connection failed.", Some(msg))
      case InsufficientPermissions2(msg) =>
        format("Database user does not have permissions on database.", Some(msg))
      case InvalidCredentials2(msg) =>
        format("Invalid username and/or password specified.", Some(msg))
      case err =>
        format(err.shows, None)
    }
  }
}

object ConnectionFailed2 {
  def apply(message: String): EnvironmentError2 =
    EnvironmentError2.ConnectionFailed(message)
  def unapply(obj: EnvironmentError2): Option[String] =
    EnvironmentError2.connectionFailed.getOption(obj)
}

object EnvPathError2 {
  def apply(pathError: PathError2): EnvironmentError2 =
    EnvironmentError2.EnvPathError(pathError)
  def unapply(obj: EnvironmentError2): Option[PathError2] =
    EnvironmentError2.envPathError.getOption(obj)
}

object InsufficientPermissions2 {
  def apply(message: String): EnvironmentError2 =
    EnvironmentError2.InsufficientPermissions(message)
  def unapply(obj: EnvironmentError2): Option[String] =
    EnvironmentError2.insufficientPermissions.getOption(obj)
}

object InvalidConfig2 {
  def apply(message: String): EnvironmentError2 =
    EnvironmentError2.InvalidConfig(message)
  def unapply(obj: EnvironmentError2): Option[String] =
    EnvironmentError2.invalidConfig.getOption(obj)
}

object InvalidCredentials2 {
  def apply(message: String): EnvironmentError2 =
    EnvironmentError2.InvalidCredentials(message)
  def unapply(obj: EnvironmentError2): Option[String] =
    EnvironmentError2.invalidCredentials.getOption(obj)
}

object UnsupportedVersion2 {
  def apply(name: String, version: List[Int]): EnvironmentError2 =
    EnvironmentError2.UnsupportedVersion(name, version)
  def unapply(obj: EnvironmentError2): Option[(String, List[Int])] =
    EnvironmentError2.unsupportedVersion.getOption(obj)
}
