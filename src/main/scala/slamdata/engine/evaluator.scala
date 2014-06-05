package slamdata.engine

import scalaz.concurrent.Task

trait Evaluator[PhysicalPlan] {
  /**
   * Executes the specified physical plan, attempting to place the results into
   * the specified destination resource.
   *
   * Returns the actual location where the output results are located. In some
   * cases (e.g. SELECT * FROM FOO), this may not be equal to the specified 
   * destination resource (because this would require copying all the data).
   */
  def execute(physical: PhysicalPlan, out: String): Task[String]
}