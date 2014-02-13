package slamdata.engine.physical.mongodb

import slamdata.engine.{LogicalPlan, Planner}

import scalaz.{EphemeralStream, Order}

trait MongoDbPlanner extends Planner {
  type PhysicalPlan = Workflow
  type Cost = Int

  def plan(logical: LogicalPlan): Workflow = {
    ???
  }

  def permute(physical: Workflow): EphemeralStream[Workflow] = EphemeralStream(physical)

  def cost(plan: PhysicalPlan): Cost = 1

  implicit def CostOrder = Order[Int]
}