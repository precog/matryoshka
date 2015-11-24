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

import quasar.Predef._
import quasar.Backend.ResultError
import quasar.config.FsPath.FsPathError
import quasar.fs._, Path._
import quasar.Errors._

import scalaz._
import scalaz.concurrent._
import scalaz.stream._

sealed trait ResultPath {
  def path: Path
}
object ResultPath {
  /** Path to a result which names an unaltered source resource or the requested destination. */
  final case class User(path: Path) extends ResultPath

  /** Path to a result which is a new temporary resource created during query execution. */
  final case class Temp(path: Path) extends ResultPath
}

trait Evaluator[PhysicalPlan] {
  import Evaluator._

  /**
   * Human readable name of the evaluator, used to identify in logs and error
   * messages.
   */
  def name: String

  /**
   * Executes the specified physical plan, storing the results in the
   * filesystem.
   *
   * Returns the location where the output results are located. In some
   * cases (e.g. SELECT * FROM FOO), this may not be equal to the specified
   * destination resource (because this would require copying all the data).
   */
  def executeTo(physical: PhysicalPlan, out: Path): ETask[EvaluationError, ResultPath]

  /**
   * Executes the specified physical plan and streams the results.
   *
   * Note that the evaluator may write temporary results to the filesystem
   * during evaluation, but must clean up any such temporary files after
   * the output is consumed.
   */
  def evaluate(physical: PhysicalPlan): Process[ETask[EvaluationError, ?], Data]

  /**
   * Compile the specified physical plan to a command
   * that can be run natively on the backend.
   */
  def compile(physical: PhysicalPlan): (String, Cord)
}
object Evaluator {

  implicit def evaluatorShow[A]: Show[Evaluator[A]] =
    Show.shows(_.name)

  sealed trait EnvironmentError {
    def message: String
  }
  object EnvironmentError {
    final case class MissingBackend(message: String) extends EnvironmentError
    final case class MissingFileSystem(path: Path, config: quasar.config.MountConfig) extends EnvironmentError {
      def message = "No data source could be mounted at the path " + path + " using the config " + config
    }
    final case class InvalidConfig(message: String) extends EnvironmentError
    final case class ConnectionFailed(message: String) extends EnvironmentError
    final case class InvalidCredentials(message: String) extends EnvironmentError
    final case class InsufficientPermissions(message: String) extends EnvironmentError
    final case class EnvPathError(error: PathError) extends EnvironmentError {
      def message = error.message
    }
    // TODO: use PathError from the Free Architecture work once merged
    final case class EnvFsPathError(error: FsPathError) extends EnvironmentError {
      def message = error.message
    }
    final case class EnvEvalError(error: EvaluationError) extends EnvironmentError {
      def message = error.message
    }
    final case class EnvWriteError(error: Backend.ProcessingError) extends EnvironmentError {
      def message = "write failed: " + error.message
    }
    final case class UnsupportedVersion(backendName: String, version: List[Int]) extends EnvironmentError {
      def message = s"Unsupported $backendName version: ${version.mkString(".")}"
    }

    import argonaut._, Argonaut._
    implicit val EnvironmentErrorEncodeJson: EncodeJson[EnvironmentError] = {
      def format(message: String, detail: Option[String]) =
        Json(("error" := message) :: detail.toList.map("errorDetail" := _): _*)

      EncodeJson[EnvironmentError] {
        case ConnectionFailed(msg)        => format("Connection failed.", Some(msg))
        case InvalidCredentials(msg)      => format("Invalid username and/or password specified.", Some(msg))
        case InsufficientPermissions(msg) => format("Database user does not have permissions on database.", Some(msg))
        case EnvWriteError(pe)            => format("Database user does not have necessary write permissions.", Some(pe.message))
        case e                            => format(e.message, None)
      }
    }
  }

  type EnvErrT[F[_], A] = EitherT[F, EnvironmentError, A]
  type EnvTask[A] = EitherT[Task, EnvironmentError, A]
  implicit val EnvironmentErrorShow: Show[EnvironmentError] =
    Show.showFromToString[EnvironmentError]

  object MissingBackend {
    def apply(message: String): EnvironmentError = EnvironmentError.MissingBackend(message)
    def unapply(obj: EnvironmentError): Option[String] = obj match {
      case EnvironmentError.MissingBackend(message) => Some(message)
      case _                       => None
    }
  }
  object MissingFileSystem {
    def apply(path: Path, config: quasar.config.MountConfig): EnvironmentError = EnvironmentError.MissingFileSystem(path, config)
    def unapply(obj: EnvironmentError): Option[(Path, quasar.config.MountConfig)] = obj match {
      case EnvironmentError.MissingFileSystem(path, config) => Some((path, config))
      case _                       => None
    }
  }
  object InvalidConfig {
    def apply(message: String): EnvironmentError = EnvironmentError.InvalidConfig(message)
    def unapply(obj: EnvironmentError): Option[String] = obj match {
      case EnvironmentError.InvalidConfig(message) => Some(message)
      case _                       => None
    }
  }
  object ConnectionFailed {
    def apply(message: String): EnvironmentError = EnvironmentError.ConnectionFailed(message)
    def unapply(obj: EnvironmentError): Option[String] = obj match {
      case EnvironmentError.ConnectionFailed(message) => Some(message)
      case _                       => None
    }
  }
  object InvalidCredentials {
    def apply(message: String): EnvironmentError = EnvironmentError.InvalidCredentials(message)
    def unapply(obj: EnvironmentError): Option[String] = obj match {
      case EnvironmentError.InvalidCredentials(message) => Some(message)
      case _                       => None
    }
  }
  object InsufficientPermissions {
    def apply(message: String): EnvironmentError = EnvironmentError.InsufficientPermissions(message)
    def unapply(obj: EnvironmentError): Option[String] = obj match {
      case EnvironmentError.InsufficientPermissions(message) => Some(message)
      case _                       => None
    }
  }
  object EnvPathError {
    def apply(error: PathError): EnvironmentError =
      EnvironmentError.EnvPathError(error)
    def unapply(obj: EnvironmentError): Option[PathError] = obj match {
      case EnvironmentError.EnvPathError(error) => Some(error)
      case _                       => None
    }
  }
  object EnvWriteError {
    def apply(error: Backend.ProcessingError): EnvironmentError =
      EnvironmentError.EnvWriteError(error)
    def unapply(obj: EnvironmentError): Option[Backend.ProcessingError] = obj match {
      case EnvironmentError.EnvWriteError(error) => Some(error)
      case _                       => None
    }
  }
  object EnvEvalError {
    def apply(error: EvaluationError): EnvironmentError = EnvironmentError.EnvEvalError(error)
    def unapply(obj: EnvironmentError): Option[EvaluationError] = obj match {
      case EnvironmentError.EnvEvalError(error) => Some(error)
      case _                       => None
    }
  }
  object UnsupportedVersion {
    def apply(backendName: String, version: List[Int]): EnvironmentError =
      EnvironmentError.UnsupportedVersion(backendName, version)

    def unapply(obj: EnvironmentError): Option[(String, List[Int])] = obj match {
      case EnvironmentError.UnsupportedVersion(name, version) => Some((name, version))
      case _                       => None
    }
  }

  sealed trait EvaluationError {
    def message: String
  }
  object EvaluationError {
    final case class EvalPathError(error: PathError) extends EvaluationError {
      def message = error.message
    }
    final case class EvalResultError(error: ResultError) extends EvaluationError {
      def message = error.message
    }
    final case object NoDatabase extends EvaluationError {
      def message = "no database found"
    }
    final case class UnableToStore(message: String) extends EvaluationError
    final case class InvalidTask(message: String) extends EvaluationError
    final case class CommandFailed(message: String) extends EvaluationError
  }

  type EvalErrT[F[_], A] = EitherT[F, EvaluationError, A]
  type EvaluationTask[A] = ETask[EvaluationError, A]

  object EvalPathError {
    def apply(error: PathError): EvaluationError =
      EvaluationError.EvalPathError(error)
    def unapply(obj: EvaluationError): Option[PathError] = obj match {
      case EvaluationError.EvalPathError(error) => Some(error)
      case _                       => None
    }
  }
  object EvalResultError {
    def apply(error: ResultError): EvaluationError =
      EvaluationError.EvalResultError(error)
    def unapply(obj: EvaluationError): Option[ResultError] = obj match {
      case EvaluationError.EvalResultError(error) => Some(error)
      case _                       => None
    }
  }
  object NoDatabase {
    def apply(): EvaluationError = EvaluationError.NoDatabase
    def unapply(obj: EvaluationError): Boolean = obj match {
      case EvaluationError.NoDatabase => true
      case _                => false
    }
  }
  object UnableToStore {
    def apply(message: String): EvaluationError = EvaluationError.UnableToStore(message)
    def unapply(obj: EvaluationError): Option[String] = obj match {
      case EvaluationError.UnableToStore(message) => Some(message)
      case _                       => None
    }
  }
  object InvalidTask {
    def apply(message: String): EvaluationError = EvaluationError.InvalidTask(message)
    def unapply(obj: EvaluationError): Option[String] = obj match {
      case EvaluationError.InvalidTask(message) => Some(message)
      case _                       => None
    }
  }
  object CommandFailed {
    def apply(message: String): EvaluationError = EvaluationError.CommandFailed(message)
    def unapply(obj: EvaluationError): Option[String] = obj match {
      case EvaluationError.CommandFailed(message) => Some(message)
      case _                       => None
    }
  }
}
