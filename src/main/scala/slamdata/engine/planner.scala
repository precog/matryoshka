package slamdata.engine

import scalaz.{EphemeralStream, Order}

trait Planner {
  type PhysicalPlan
  type Cost

  def plan(logical: LogicalPlan): PhysicalPlan

  def permutations(plan: PhysicalPlan): EphemeralStream[PhysicalPlan]

  def cost(plan: PhysicalPlan): Cost

  implicit def CostOrder: Order[Cost]
}