package slamdata.engine

import scalaz._

import slamdata.engine.fs._
import slamdata.engine.Errors._

sealed trait EvaluationError {
  def message: String
}
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
