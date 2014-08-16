package slamdata.engine

import scalaz.{StreamT, \/}

import slamdata.engine.analysis.fixplate._

trait Planner[PhysicalPlan] {
  def plan(logical: Term[LogicalPlan]): Error \/ PhysicalPlan
}