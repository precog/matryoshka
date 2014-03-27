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

  private type StackElem = ExprOp \/ PipelineOp \/ WorkflowTask

  private def exprOp(op: ExprOp): StackElem = \/ left (\/ left (op))
  private def pipelineOp(op: PipelineOp): StackElem = \/ left (\/ right (op))
  private def workflowTask(task: WorkflowTask): StackElem = \/ right (task)

  private def extractExprOp(elem: StackElem) = elem.fold(_.fold(Some.apply _, Function.const(None)), Function.const(None))
  private def extractPipelineOp(elem: StackElem) = elem.fold(_.fold(Function.const(None), Some.apply _), Function.const(None))
  private def extractWorkflowTask(elem: StackElem) = elem.fold(_.fold(Function.const(None), Function.const(None)), Some.apply _)

  // TODO: Refactor to be a single stack: ExprOp \/ PipelineOp \/ WorkflowTask \/ Selector
  private case class PlannerState(stack: List[StackElem] = Nil,
                                  table: Map[String, ExprOp] = Map.empty[String, ExprOp]) {
    def push(elem: StackElem): PlannerState = copy(stack = elem :: stack)

    def pop: PlannerState = if (stack.isEmpty) this else copy(stack = stack.tail)

    def peek: Option[StackElem] = stack.headOption

    def pushExpr(op: ExprOp): PlannerState = push(exprOp(op))

    def peekExpr: Option[ExprOp] = peek.flatMap(extractExprOp _)

    def pushTask(task: WorkflowTask): PlannerState = push(workflowTask(task))

    def peekTask: Option[WorkflowTask] = peek.flatMap(extractWorkflowTask _)

    def pushPipeline(op: PipelineOp): PlannerState = push(pipelineOp(op))

    def peekPipeline: Option[PipelineOp] = peek.flatMap(extractPipelineOp _)

    def addTable(name: String, op: ExprOp): PlannerState = copy(table = table + (name -> op))

    def getTable(name: String): Option[ExprOp] = table.get(name)
  }

  import LogicalPlan._

  private def emit[A](v: A): State[A] = Monad[State].point(v)

  private def fail[A](e: PlannerError): State[A] = StateT[F, PlannerState, A](s => EitherT.left(Monad[M].point(e)))

  private def next[A](f: PlannerState => (PlannerState, A)): State[A] = {
    StateT[F, PlannerState, A](s => EitherT.right(Monad[M].point(f(s))))
  }

  private def mod(f: PlannerState => PlannerState): State[Unit] = {
    next(s => (f(s), Unit))
  }

  private def read[A](f: PlannerState => A): State[A] = {
    StateT[F, PlannerState, A](s => EitherT.right(Monad[M].point((s, f(s)))))
  }

  private def readState: State[PlannerState] = read(identity)

  private def pushExpr(op: ExprOp): State[Unit] = mod(s => s.pushExpr(op))

  private def popExprOpt: State[Option[ExprOp]] = next(s => (s.pop, s.peekExpr))

  private def popExpr: State[ExprOp] = for {
    exprOpt <- popExprOpt
    expr    <- exprOpt.map(emit _).getOrElse(fail(PlannerError.InternalError("Expected to find expression but expression stack was empty!")))
  } yield expr

  private def pushPipeline(op: PipelineOp): State[Unit] = mod(s => s.pushPipeline(op))

  private def popPipelineOpt: State[Option[PipelineOp]] = next(s => (s.pop, s.peekPipeline))

  private def popPipeline: State[PipelineOp] = for {
    pipelineOpt <-  popPipelineOpt
    pipeline    <-  pipelineOpt.map(emit _).getOrElse(
                      fail(PlannerError.InternalError("Expected to find pipeline but pipeline stack was empty!"))
                    )
  } yield pipeline

  private def pushTask(task: WorkflowTask): State[Unit] = mod(s => s.pushTask(task))

  private def popTaskOpt: State[Option[WorkflowTask]] = next(s => (s.pop, s.peekTask))

  private def popTask: State[WorkflowTask] = for {
    taskOpt <- popTaskOpt
    task    <- taskOpt.map(emit _).getOrElse(fail(PlannerError.InternalError("Expected to find task but task stack was empty!")))
  } yield task

  private def addTable(table: String, op: ExprOp): State[Unit] = mod(s => s.addTable(table, op))

  private def getTableOpt(table: String): State[Option[ExprOp]] = read(_.getTable(table))

  private def getTable(table: String): State[ExprOp] = for {
    exprOpt <- getTableOpt(table)
    rez     <- exprOpt.map(emit _).getOrElse(
                 fail[ExprOp](PlannerError.InternalError("Expected to find table '" + table + "' in planner state"))
               )
  } yield rez

  private def getOrElse[A, B](value: A \/ B)(f: A => PlannerError): State[B] = {
    value.fold((fail[B] _) compose f, emit)
  }

  private def getOrElse[A](value: Option[A])(e: => PlannerError): State[A] = {
    value.map(emit _).getOrElse(fail(e))
  }  

  private def verifyStacksAreEmpty: State[Unit] = {    
    for {
      stack <- read(_.stack)
      rez   <- if (stack.isEmpty) emit[Unit](Unit) 
               else fail[Unit](PlannerError.InternalError("Expected stack to be empty but found: " + stack))
    } yield rez
  }

  private def BuiltInFunctions: Invoke => State[Unit] = (({
    case _ => emit(Unit)
  }: PartialFunction[Invoke, State[Unit]]) orElse {
    case invoke => fail(PlannerError.UnsupportedFunction(invoke.func))
  })

  private def plan0(logical: LogicalPlan): State[Unit] = logical match {
    case Read(resource) =>
      for {
        tableOp <- getTableOpt(resource)
        _       <- tableOp.map(pushExpr _).getOrElse(emit[Unit](Unit))
      } yield Unit

    case Constant(data) => 
      for {
        bson <- getOrElse(Bson.fromData(data))(error => PlannerError.NonRepresentableData(data))
        _    <- pushExpr(ExprOp.Literal(bson))
      } yield Unit

    case Filter(input, predicate) => 
      for {
        _     <- plan0(predicate)
        pred  <- popExpr
        _     <- pushPipeline(PipelineOp.Match(???))
      } yield Unit

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
    val task = for {
      _     <- plan0(logical)
      state <- readState
      task  <- popTask
      _     <- verifyStacksAreEmpty
    } yield task

    task.eval(PlannerState()).run.run.map { task =>
      Workflow(task, Collection(dest))
    }
  }

  def execute(workflow: Workflow): StreamT[Task, Progress] = ???
}