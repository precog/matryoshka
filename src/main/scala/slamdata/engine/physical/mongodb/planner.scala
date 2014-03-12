package slamdata.engine.physical.mongodb

import slamdata.engine.{LogicalPlan, Planner}

import scalaz.{StreamT, Order}
import scalaz.concurrent.Task

trait MongoDbPlanner extends Planner {
  type PhysicalPlan = Workflow
  
  def plan(logical: LogicalPlan): Workflow = ???

  def execute(workflow: Workflow): StreamT[Task, Progress] = ???
}