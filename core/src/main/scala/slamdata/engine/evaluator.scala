package slamdata.engine

import scalaz._
import scalaz.concurrent.Task

import slamdata.engine.fs._

case class EvaluationError(cause: Throwable) extends Error {
  def message = "An error occurred during evaluation: " + cause.toString
}

sealed trait ResultPath {
  def path: Path
}
object ResultPath {
  /** Path to a result which names an unaltered source resource or the requested destination. */
  case class User(path: Path) extends ResultPath

  /** Path to a result which is a new temporary resource created during query execution. */
  case class Temp(path: Path) extends ResultPath
}

trait Evaluator[PhysicalPlan] {
  /**
   * Executes the specified physical plan.
   *
   * Returns the location where the output results are located. In some
   * cases (e.g. SELECT * FROM FOO), this may not be equal to the specified
   * destination resource (because this would require copying all the data).
   */
  def execute(physical: PhysicalPlan): Task[ResultPath]

  /**
   * Compile the specified physical plan to a command
   * that can be run natively on the backend.
   */
  def compile(physical: PhysicalPlan): (String, Cord)

  /**
   * Fails if the backend implementation is not compatible with the connected
   * system (typically because it does not have not the correct version number).
   */
  def checkCompatibility: Task[Error \/ Unit]
}
