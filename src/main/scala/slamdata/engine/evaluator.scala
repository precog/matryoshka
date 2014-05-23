package slamdata.engine

import scalaz.concurrent.Task

trait Evaluator[PhysicalPlan] {
  def execute(physical: PhysicalPlan, out: String): Unit
}