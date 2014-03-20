package slamdata.engine.physical.mongodb

import slamdata.engine.{LogicalPlan, Planner, PlannerError}

import scalaz.{EitherT, StreamT, Order, StateT, Free, \/, Functor}
import scalaz.concurrent.Task

trait MongoDbPlanner extends Planner {
  type PhysicalPlan = Workflow

  private type F[A] = EitherT[Free.Trampoline, PlannerError, A]
  private type State[A] = StateT[F, PlannerState, A]

  private case class PlannerState()

  private def plan0(logical: LogicalPlan): State[Workflow] = {
    ???
  }
  
  def plan(logical: LogicalPlan): PlannerError \/ Workflow = plan0(logical).eval(PlannerState()).run.run

  def execute(workflow: Workflow): StreamT[Task, Progress] = ???
}