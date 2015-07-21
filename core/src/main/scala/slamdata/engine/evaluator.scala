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

package slamdata.engine

import scalaz._
import scalaz.concurrent._

import slamdata.engine.fs._
import slamdata.engine.Errors._

sealed trait EnvironmentError {
  def message: String
}
object EnvironmentError {
  type EnvTask[A] = EitherT[Task, EnvironmentError, A]
  implicit val EnvironmentErrorShow = Show.showFromToString[EnvironmentError]

  object Types {
    final case class MissingBackend(message: String) extends EnvironmentError
    final case class MissingFileSystem(path: Path, config: slamdata.engine.config.BackendConfig) extends EnvironmentError {
      def message = "No data source could be mounted at the path " + path + " using the config " + config
    }
    final case object MissingDatabase extends EnvironmentError {
      def message = "no database found"
    }
    final case class InvalidConfig(message: String) extends EnvironmentError
    final case class EnvPathError(error: slamdata.engine.fs.PathError)
        extends EnvironmentError {
      def message = error.message
    }
    final case class EnvEvalError(error: EvaluationError) extends EnvironmentError {
      def message = error.message
    }
    final case class UnsupportedVersion(backend: Evaluator[_], version: List[Int]) extends EnvironmentError {
      def message = "Unsupported " + backend + " version: " + version.mkString(".")
    }
  }

  object MissingBackend {
    def apply(message: String): EnvironmentError = Types.MissingBackend(message)
    def unapply(obj: EnvironmentError): Option[String] = obj match {
      case Types.MissingBackend(message) => Some(message)
      case _                       => None
    }
  }
  object MissingFileSystem {
    def apply(path: Path, config: slamdata.engine.config.BackendConfig): EnvironmentError = Types.MissingFileSystem(path, config)
    def unapply(obj: EnvironmentError): Option[(Path, slamdata.engine.config.BackendConfig)] = obj match {
      case Types.MissingFileSystem(path, config) => Some((path, config))
      case _                       => None
    }
  }
  object MissingDatabase {
    def apply(): EnvironmentError = Types.MissingDatabase
    def unapply(obj: EnvironmentError): Boolean = obj match {
      case Types.MissingDatabase => true
      case _                     => false
    }
  }
  object InvalidConfig {
    def apply(message: String): EnvironmentError = Types.InvalidConfig(message)
    def unapply(obj: EnvironmentError): Option[String] = obj match {
      case Types.InvalidConfig(message) => Some(message)
      case _                       => None
    }
  }
  object EnvPathError {
    def apply(error: slamdata.engine.fs.PathError): EnvironmentError = Types.EnvPathError(error)
    def unapply(obj: EnvironmentError): Option[slamdata.engine.fs.PathError] = obj match {
      case Types.EnvPathError(error) => Some(error)
      case _                       => None
    }
  }
  object EnvEvalError {
    def apply(error: EvaluationError): EnvironmentError = Types.EnvEvalError(error)
    def unapply(obj: EnvironmentError): Option[EvaluationError] = obj match {
      case Types.EnvEvalError(error) => Some(error)
      case _                       => None
    }
  }
  object UnsupportedVersion {
    def apply(backend: Evaluator[_], version: List[Int]): EnvironmentError = Types.UnsupportedVersion(backend, version)
    def unapply(obj: EnvironmentError): Option[(Evaluator[_], List[Int])] = obj match {
      case Types.UnsupportedVersion(backend, version) => Some((backend, version))
      case _                       => None
    }
  }
}

sealed trait EvaluationError {
  def message: String
}
object EvaluationError {
  type EvaluationTask[A] = ETask[EvaluationError, A]

  object Types {
    final case class EvalPathError(error: PathError) extends EvaluationError {
      def message = error.message
    }
    // NB: this is just a sigil that compilation failed before we got to evaluation
    final case object CompileFailed extends EvaluationError {
      def message = "compilation failed – check phase results"
    }
    final case class UnknownEvalError(cause: Throwable) extends EvaluationError {
      def message = "An error occurred during evaluation: " + cause.toString
    }
    final case object NoDatabase extends EvaluationError {
      def message = "no database found"
    }
    final case class UnableToStore(message: String) extends EvaluationError
    final case class InvalidTask(message: String) extends EvaluationError
  }

  object EvalPathError {
    def apply(error: PathError): EvaluationError = Types.EvalPathError(error)
    def unapply(obj: EvaluationError): Option[PathError] = obj match {
      case Types.EvalPathError(error) => Some(error)
      case _                       => None
    }
  }
  object CompileFailed {
    def apply(): EvaluationError = Types.CompileFailed
    def unapply(obj: EvaluationError): Boolean = obj match {
      case Types.CompileFailed => true
      case _                   => false
    }
  }
  object NoDatabase {
    def apply(): EvaluationError = Types.NoDatabase
    def unapply(obj: EvaluationError): Boolean = obj match {
      case Types.NoDatabase => true
      case _                => false
    }
  }
  object UnableToStore {
    def apply(message: String): EvaluationError = Types.UnableToStore(message)
    def unapply(obj: EvaluationError): Option[String] = obj match {
      case Types.UnableToStore(message) => Some(message)
      case _                       => None
    }
  }
  object InvalidTask {
    def apply(message: String): EvaluationError = Types.InvalidTask(message)
    def unapply(obj: EvaluationError): Option[String] = obj match {
      case Types.InvalidTask(message) => Some(message)
      case _                       => None
    }
  }
}

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
  /**
   * Executes the specified physical plan.
   *
   * Returns the location where the output results are located. In some
   * cases (e.g. SELECT * FROM FOO), this may not be equal to the specified
   * destination resource (because this would require copying all the data).
   */
  def execute(physical: PhysicalPlan): ETask[EvaluationError, ResultPath]

  /**
   * Compile the specified physical plan to a command
   * that can be run natively on the backend.
   */
  def compile(physical: PhysicalPlan): (String, Cord)

  /**
   * Fails if the backend implementation is not compatible with the connected
   * system (typically because it does not have not the correct version number).
   */
  def checkCompatibility: ETask[EnvironmentError, Unit]
}
