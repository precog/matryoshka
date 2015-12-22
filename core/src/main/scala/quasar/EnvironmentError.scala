/*
 * Copyright 2014 - 2015 SlamData Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package quasar

import Predef._
import quasar.fs.PathError2

import argonaut._, Argonaut._
import monocle._
import scalaz._
import scalaz.syntax.show._

sealed trait EnvironmentError2

object EnvironmentError2 {
  final case class ConnectionFailed private[quasar] (message: String)
    extends EnvironmentError2
  final case class InsufficientPermissions private[quasar] (message: String)
    extends EnvironmentError2
  final case class InvalidCredentials private[quasar] (message: String)
    extends EnvironmentError2
  final case class UnsupportedVersion private[quasar] (backendName: String, version: List[Int])
    extends EnvironmentError2

  val connectionFailed: Prism[EnvironmentError2, String] =
    Prism[EnvironmentError2, String] {
      case ConnectionFailed(msg) => Some(msg)
      case _ => None
    } (ConnectionFailed(_))

  val insufficientPermissions: Prism[EnvironmentError2, String] =
    Prism[EnvironmentError2, String] {
      case InsufficientPermissions(msg) => Some(msg)
      case _ => None
    } (InsufficientPermissions(_))

  val invalidCredentials: Prism[EnvironmentError2, String] =
    Prism[EnvironmentError2, String] {
      case InvalidCredentials(msg) => Some(msg)
      case _ => None
    } (InvalidCredentials(_))

  val unsupportedVersion: Prism[EnvironmentError2, (String, List[Int])] =
    Prism[EnvironmentError2, (String, List[Int])] {
      case UnsupportedVersion(name, version) => Some((name, version))
      case _ => None
    } ((UnsupportedVersion(_, _)).tupled)

  implicit val environmentErrorShow: Show[EnvironmentError2] =
    Show.shows {
      case ConnectionFailed(msg) =>
        s"Connection failed: $msg"
      case InsufficientPermissions(msg) =>
        s"Insufficient permissions: $msg"
      case InvalidCredentials(msg) =>
        s"Invalid credentials: $msg"
      case UnsupportedVersion(name, version) =>
        s"Unsupported $name version: ${version.mkString(".")}"
    }

  implicit val environmentErrorEncodeJson: EncodeJson[EnvironmentError2] = {
    def format(message: String, detail: Option[String]) =
      Json(("error" := message) :: detail.toList.map("errorDetail" := _): _*)

    EncodeJson[EnvironmentError2] {
      case ConnectionFailed(msg) =>
        format("Connection failed.", Some(msg))
      case InsufficientPermissions(msg) =>
        format("Database user does not have permissions on database.", Some(msg))
      case InvalidCredentials(msg) =>
        format("Invalid username and/or password specified.", Some(msg))
      case err =>
        format(err.shows, None)
    }
  }
}
