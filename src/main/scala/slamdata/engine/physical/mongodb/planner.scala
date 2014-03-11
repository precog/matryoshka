package slamdata.engine.physical.mongodb

import slamdata.engine.{LogicalPlan, Planner}

import scalaz.{EphemeralStream, Order}

trait MongoDbPlanner extends Planner {
  type PhysicalPlan = Workflow
  
  def plan(logical: LogicalPlan): Workflow = ???
}