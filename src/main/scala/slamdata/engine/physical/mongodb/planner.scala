package slamdata.engine.physical.mongodb

import slamdata.engine.{LogicalPlan, Planner, PlannerError}
import slamdata.engine.std.StdLib._

import scalaz.{EitherT, StreamT, Order, StateT, Free => FreeM, \/, Functor, Monad}
import scalaz.concurrent.Task

trait MongoDbPlanner extends Planner {
  type PhysicalPlan = Workflow

  private type M[A] = FreeM.Trampoline[A]
  private type F[A] = EitherT[M, PlannerError, A]
  private type State[A] = StateT[F, PlannerState, A]

  private case class PlannerState()

  import LogicalPlan._

  private def emit[A](v: A): State[A] = Monad[State].point(v)

  private def fail[A](e: PlannerError): State[A] = StateT[F, PlannerState, A]((s: PlannerState) => EitherT.left(Monad[M].point(e)))

  // private def BuiltInFunctions: (Invoke => Map[LogicalPlan, Workflow]

  private def plan0(logical: LogicalPlan): State[WorkflowTask] = logical match {
    case Read(resource) => ???

    case Constant(data) => emit(WorkflowTask.PureTask(???))

    case Filter(input, predicate) => ???

    case Join(left, right, joinType, joinRel, leftProj, rightProj) => ???

    case Cross(left, right) => ???

    case Invoke(func, values) => ???

    case Cond(pred, ifTrue, ifFalse) => ???

    case Free(name) => ???

    case Lambda(name, value) => ???

    case Sort(value, by) => ???

    case Group(value, by) => ???

    case Take(value, count) => ???

    case Drop(value, count) => ???
  }
  
  def plan(logical: LogicalPlan, dest: String): PlannerError \/ Workflow = {
    plan0(logical).eval(PlannerState()).run.run.map(task => Workflow(task, Collection(dest)))
  }

  def execute(workflow: Workflow): StreamT[Task, Progress] = ???
}