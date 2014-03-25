package slamdata.engine

import scalaz.{StreamT, \/}

import scalaz.concurrent.Task

trait Planner {
  type PhysicalPlan 

  def plan(logical: LogicalPlan, dest: String): PlannerError \/ PhysicalPlan

  def execute(physical: PhysicalPlan): StreamT[Task, Progress]

  case class Progress(message: String, percentComplete: Option[Double])
}