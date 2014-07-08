package slamdata.engine

import scalaz._
import scalaz.concurrent.Task

import slamdata.engine.fs._

case class EvaluationError(cause: Throwable) extends Error {
  def message = "An error occurred during evaluation: " + cause.toString
}

trait Evaluator[PhysicalPlan] {
  /**
   * Executes the specified physical plan, attempting to place the results into
   * the specified destination resource.
   *
   * Returns the actual location where the output results are located. In some
   * cases (e.g. SELECT * FROM FOO), this may not be equal to the specified 
   * destination resource (because this would require copying all the data).
   */
  def execute(physical: PhysicalPlan, out: Path): Task[Path]
}