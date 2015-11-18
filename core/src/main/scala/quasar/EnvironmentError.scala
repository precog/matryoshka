package quasar

import Predef._
import quasar.fs.PathError2

import argonaut._, Argonaut._

import monocle._

import scalaz._
import scalaz.syntax.show._

sealed trait EnvironmentError2

object EnvironmentError2 {
  object Case {
    final case class ConnectionFailed(message: String)
      extends EnvironmentError2
    final case class PathError(error: PathError2)
      extends EnvironmentError2
    final case class InsufficientPermissions(message: String)
      extends EnvironmentError2
    final case class InvalidConfig(message: String)
      extends EnvironmentError2
    final case class InvalidCredentials(message: String)
      extends EnvironmentError2
    final case class UnsupportedVersion(backendName: String, version: List[Int])
      extends EnvironmentError2
  }

  val ConnectionFailed: String => EnvironmentError2 =
    Case.ConnectionFailed(_)

  val PathError: PathError2 => EnvironmentError2 =
    Case.PathError(_)

  val InsufficientPermissions: String => EnvironmentError2 =
    Case.InsufficientPermissions(_)

  val InvalidConfig: String => EnvironmentError2 =
    Case.InvalidConfig(_)

  val InvalidCredentials: String => EnvironmentError2 =
    Case.InvalidCredentials(_)

  val UnsupportedVersion: (String, List[Int]) => EnvironmentError2 =
    Case.UnsupportedVersion(_, _)

  val connectionFailed: Prism[EnvironmentError2, String] =
    Prism[EnvironmentError2, String] {
      case Case.ConnectionFailed(msg) => Some(msg)
      case _ => None
    } (ConnectionFailed)

  val pathError: Prism[EnvironmentError2, PathError2] =
    Prism[EnvironmentError2, PathError2] {
      case Case.PathError(err) => Some(err)
      case _ => None
    } (PathError)

  val insufficientPermissions: Prism[EnvironmentError2, String] =
    Prism[EnvironmentError2, String] {
      case Case.InsufficientPermissions(msg) => Some(msg)
      case _ => None
    } (InsufficientPermissions)

  val invalidConfig: Prism[EnvironmentError2, String] =
    Prism[EnvironmentError2, String] {
      case Case.InvalidConfig(msg) => Some(msg)
      case _ => None
    } (InvalidConfig)

  val invalidCredentials: Prism[EnvironmentError2, String] =
    Prism[EnvironmentError2, String] {
      case Case.InvalidCredentials(msg) => Some(msg)
      case _ => None
    } (InvalidCredentials)

  val unsupportedVersion: Prism[EnvironmentError2, (String, List[Int])] =
    Prism[EnvironmentError2, (String, List[Int])] {
      case Case.UnsupportedVersion(name, version) => Some((name, version))
      case _ => None
    } (UnsupportedVersion.tupled)

  implicit val environmentErrorShow: Show[EnvironmentError2] =
    Show.shows {
      case Case.ConnectionFailed(msg) =>
        s"Connection failed: $msg"
      case Case.PathError(error) =>
        error.shows
      case Case.InsufficientPermissions(msg) =>
        s"Insufficient permissions: $msg"
      case Case.InvalidConfig(msg) =>
        s"Invalid configuration: $msg"
      case Case.InvalidCredentials(msg) =>
        s"Invalid credentials: $msg"
      case Case.UnsupportedVersion(name, version) =>
        s"Unsupported $name version: ${version.mkString(".")}"
    }

  implicit val environmentErrorEncodeJson: EncodeJson[EnvironmentError2] = {
    def format(message: String, detail: Option[String]) =
      Json(("error" := message) :: detail.toList.map("errorDetail" := _): _*)

    EncodeJson[EnvironmentError2] {
      case Case.ConnectionFailed(msg) =>
        format("Connection failed.", Some(msg))
      case Case.InsufficientPermissions(msg) =>
        format("Database user does not have permissions on database.", Some(msg))
      case Case.InvalidCredentials(msg) =>
        format("Invalid username and/or password specified.", Some(msg))
      case err =>
        format(err.shows, None)
    }
  }
}
