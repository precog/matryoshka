package slamdata.engine

import scalaz.{StreamT, \/}

import scalaz.task.Task

import slamdata.engine.analysis.fixplate._

trait Planner {
  type PhysicalPlan 

  def plan(logical: Term[LogicalPlan], dest: String): PlannerError \/ PhysicalPlan
}