package slamdata.engine

import scalaz.{EphemeralStream, Order}

trait Planner {
  type PhysicalPlan 
  
  def plan(logical: LogicalPlan): PhysicalPlan
}