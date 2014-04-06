package slamdata.engine.physical.mongodb

import slamdata.engine._
import slamdata.engine.std.StdLib._

import scalaz.{Applicative, ApplicativePlus, PlusEmpty, Apply, EitherT, StreamT, Order, StateT, Free => FreeM, \/, -\/, \/-, Functor, Monad, NonEmptyList, Compose, Arrow}
import scalaz.task.Task

import scalaz.syntax.either._
import scalaz.syntax.compose._
import scalaz.syntax.applicativePlus._

import scalaz.std.option._

trait MongoDbPlanner extends Planner {
  import LogicalPlan._

  type PhysicalPlan = Workflow

  private type M[A] = FreeM.Trampoline[A]
  private type F[A] = EitherT[M, PlannerError, A]
  private type State[A] = StateT[F, PlannerState, A]

  private type Build = ExprOp \/ PipelineOp \/ WorkflowTask \/ Selector

  private type Mode = Invoke => State[Build]

  private def exprOp(op: ExprOp): Build = op.left.left.left
  private def pipelineOp(op: PipelineOp): Build = op.right.left.left
  private def workflowTask(task: WorkflowTask): Build = task.right.left
  private def selector(sel: Selector): Build = sel.right

  private def extractExprOp(elem: Build) = elem match {
    case -\/(-\/(-\/(x))) => Some(x)
    case _ => None
  }
  private def extractPipelineOp(elem: Build) = elem match {
    case -\/(-\/(\/-(x))) => Some(x)
    case _ => None
  }
  private def extractWorkflowTask(elem: Build) = elem match {
    case -\/(\/-(x)) => Some(x)
    case _ => None
  }
  private def extractSelector(elem: Build) = elem match {
    case \/-(x) => Some(x)
    case _ => None
  }

  private implicit def LiftedApplicativePlus[F[_], G[_]]
      (implicit F: Applicative[F], G: ApplicativePlus[G]) = new ApplicativePlus[({type f[A]=F[G[A]]})#f] {
    def empty[A] = F.point(G.empty)

    def point[A](a: => A): F[G[A]] = F.point(G.point(a))

    def plus[A](v1: F[G[A]], v2: => F[G[A]]): F[G[A]] = {
      F.apply2(v1, v2)(G.plus(_, _))
    }

    def ap[A, B](fa: => F[G[A]])(f: => F[G[A => B]]): F[G[B]] = {
      F.ap(fa)(F.map(f)(f => x => G.apply2(f, x)((f, x) => f(x))))
    }
  }

  // TODO: Refactor to be a single stack: ExprOp \/ PipelineOp \/ WorkflowTask \/ Selector
  private case class PlannerState(table: Map[String, ExprOp] = Map.empty[String, ExprOp],
                                  mode: NonEmptyList[Mode] = NonEmptyList.nels(DefaultMode)) {
    def addTable(name: String, op: ExprOp): PlannerState = copy(table = table + (name -> op))

    def getTable(name: String): Option[ExprOp] = table.get(name)

    def enterMode(mode0: Mode): PlannerState = copy(mode = NonEmptyList(mode0, mode.list: _*))

    def exitMode: (PlannerState, Option[Mode]) = (mode.tail.headOption.map { newHead =>
      (copy(mode = NonEmptyList(newHead, mode.tail.tail: _*)), Some(mode.head))
    }).getOrElse((this, None))
  }

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

  private def as[A](name: String, f: Build => Option[A]) = (s: Build) => ((f(s).map(emit _).getOrElse {
    fail[A](PlannerError.InternalError("Expected to find " + name))
  }): State[A])



  private val asExpr      = as("expression",  extractExprOp _)
  private val asPipeline  = as("pipeline",    extractPipelineOp _)
  private val asTask      = as("task",        extractWorkflowTask _)
  private val asSelector  = as("selector",    extractSelector _)

  private def emitExpr(v: ExprOp): State[Build] = emit(exprOp(v))

  private def emitPipeline(v: PipelineOp): State[Build] = emit(pipelineOp(v))

  private def emitTask(v: WorkflowTask): State[Build] = emit(workflowTask(v))

  private def emitSelector(v: Selector): State[Build] = emit(selector(v))

  private def collect[A, B](s: State[A])(f: PartialFunction[A, B]): State[Option[B]] = s.map(f.lift)

  private def addTable(table: String, op: ExprOp): State[Unit] = mod(s => s.addTable(table, op))

  private def getTableOpt(table: String): State[Option[ExprOp]] = read(_.getTable(table))

  private def getTable(table: String): State[ExprOp] = for {
    exprOpt <- getTableOpt(table)
    rez     <- exprOpt.map(emit _).getOrElse(
                 fail[ExprOp](PlannerError.InternalError("Expected to find table '" + table + "' in planner state"))
               )
  } yield rez

  private def currentMode: State[Mode] = read(_.mode.head)

  private def enterMode(newMode: Mode) = mod(_.enterMode(newMode))

  private def exitMode: State[Boolean] = next(_.exitMode).map(!_.isEmpty)

  private def inMode[A](mode: Mode)(f: => State[A]): State[A] = for {
    _ <- enterMode(mode)
    a <- f
    _ <- exitMode
  } yield a

  private def getOrError[A, B](value: A \/ B)(f: A => PlannerError): State[B] = {
    value.fold((fail[B] _) compose f, emit)
  }

  private def getOrError[A](value: Option[A])(e: => PlannerError): State[A] = {
    value.map(emit _).getOrElse(fail(e))
  }  

  private def getOrError[A](value: State[Option[A]])(e: => PlannerError): State[A] = value.flatMap {
    case Some(a) => emit(a)
    case None    => fail(e)
  }  

  import structural._

  private def internalError[A](message: String) = fail[A](PlannerError.InternalError(message))

  private def DefaultMode: Mode = (({
    case Invoke(`MakeObject`, Constant(Data.Str(name)) :: value :: Nil) => 
      implicit val Plus = LiftedApplicativePlus[State, Option]

      for {
        value <-  compile(value)
        value <-  (extractExprOp(value).map[ExprOp \/ PipelineOp.Reshape](\/ left) orElse
                  (extractPipelineOp(value).flatMap[ExprOp \/ PipelineOp.Reshape] {
                      case PipelineOp.Project(x) => Some(\/ right x)
                      case _ => None
                    }
                  )).map(emit _).getOrElse(fail(PlannerError.InternalError("Expected to find expression or pipeline reshape operation")))
      } yield pipelineOp(PipelineOp.Project(PipelineOp.Reshape(Map(name -> value))))

    case Invoke(`ObjectConcat`, v1 :: v2 :: Nil) =>
      ???

    case Invoke(`ObjectProject`, Read(table) :: Constant(Data.Str(name)) :: Nil) =>
      for {
        tableOpt <- (getTableOpt(table): State[Option[ExprOp]])
        rez      <- tableOpt match {
                      case Some(ExprOp.DocField(field)) => emit(exprOp(ExprOp.DocField(BsonField.Name(name) :+ field)))
                      case Some(x) => internalError[Build]("Expected object or array dereference but found: " + x)
                      case None => emit(exprOp(ExprOp.DocField(BsonField.Name(name))))
                    }
      } yield rez

    case Invoke(`ObjectProject`, obj :: Constant(Data.Str(name)) :: Nil) =>
      for {
        compiled <- compile(obj).flatMap(asExpr)
        field    <- compiled match { 
                      case ExprOp.DocField(f) => emit[BsonField](f)

                      // TODO: Can handle this case by introducing another pipeline
                      case _ => internalError[BsonField]("Expected to find object or array dereference but found: ") 
                    }
      } yield exprOp(ExprOp.DocField(BsonField.Name(name) :+ field))


  }: PartialFunction[Invoke, State[Build]]) orElse {
    case invoke => fail(PlannerError.UnsupportedFunction(invoke.func))
  })

  private def FilterMode: Mode = (({
    case _ => ???
  }: PartialFunction[Invoke, State[Build]]) orElse {
    case invoke => 
      fail(
        PlannerError.UnsupportedFunction(invoke.func, "The function '" + invoke.func.name + "' is not currently supported in a WHERE clause")
      )
  })

  

  private def GroupMode: Mode = (({
    case _ => ???
  }: PartialFunction[Invoke, State[Build]]) orElse {
    case invoke => 
      fail(
        PlannerError.UnsupportedFunction(invoke.func, "The function '" + invoke.func.name + "' is not currently supported when using a GROUP BY clause")
      )
  })

  private def GroupByMode: Mode = (({
    case _ => ???
  }: PartialFunction[Invoke, State[Build]]) orElse {
    case invoke => 
      fail(
        PlannerError.UnsupportedFunction(invoke.func, "The function '" + invoke.func.name + "' is not currently supported in a GROUP BY clause")
      )
  })

  private def SortMode: Mode = (({
    case _ => ???
  }: PartialFunction[Invoke, State[Build]]) orElse {
    case invoke => 
      fail(
        PlannerError.UnsupportedFunction(invoke.func, "The function '" + invoke.func.name + "' is not currently supported in a SORT BY clause")
      )
  })

  private def unsupported[A](plan: LogicalPlan): State[A] = fail(PlannerError.UnsupportedPlan(plan))

  private def compile(logical: LogicalPlan): State[Build] = logical match {
    case Read(resource) =>
      ???

    case Constant(data) => 
      for {
        bson <- getOrError(Bson.fromData(data))(_ => PlannerError.NonRepresentableData(data))
      } yield exprOp(ExprOp.Literal(bson))

    case Filter(input, predicate) => 
      for {
        input <- compile(input)
        pred  <- inMode(FilterMode)(compile(predicate)).flatMap(asSelector)
      } yield pipelineOp(PipelineOp.Match(pred))

    case Join(left, right, joinType, joinRel, leftProj, rightProj) => ???

    case Cross(left, right) => unsupported(logical)

    case invoke @ Invoke(_, _) =>
      for {
        mode <- currentMode
        rez  <- mode(invoke)
      } yield rez

    case Free(name) => ???

    case Lambda(name, value) => ???

    case Sort(value, by) => 
      for {
        by    <- inMode(SortMode)(compile(by))
        value <- compile(value)
      } yield pipelineOp(PipelineOp.Sort(???))

    case Group(value, by) => 
      for {
        value <- compile(value)
        by    <- inMode(GroupByMode)(compile(by))
      } yield pipelineOp(PipelineOp.Group(???))

    case Take(value, count) => 
      for {
        value <- compile(value)
      } yield pipelineOp(PipelineOp.Limit(count))

    case Drop(value, count) => 
      for {
        value <- compile(value)
      } yield pipelineOp(PipelineOp.Skip(count))
  }
  
  def plan(logical: LogicalPlan, dest: String): PlannerError \/ Workflow = {
    val task = for {
      compiled <- compile(logical)
      task     <- asTask(compiled)
    } yield task

    task.eval(PlannerState()).run.run.map { task =>
      Workflow(task, Collection(dest))
    }
  }

  def execute(workflow: Workflow): StreamT[Task, Progress] = ???
}

trait MongoDbPlanner2 {
  import LogicalPlan2._

  import slamdata.engine.analysis.fixplate._

  type FieldPhaseAttr = Option[ExprOp.DocField]

  import set._
  import relations._
  import structural._

  /**
   * This phase works bottom-up to assemble sequences of object dereferences into
   * the format required by MongoDB -- e.g. "foo.bar.baz".
   *
   * This is necessary because MongoDB does not treat object dereference as a 
   * first-class binary operator, and the resulting irregular structure cannot
   * be easily generated without computing this intermediate.
   *
   * This annotation can also be used to help detect two spans of dereferences
   * separated by non-dereference operations. Such "broken" dereferences cannot
   * be translated into a single pipeline operation and require 3 pipeline 
   * operations: [dereference, middle op, dereference].
   */
  def FieldPhase[A]: LPPhase[A, FieldPhaseAttr] = { (attr: LPAttr[A]) =>
    synthPara(forget(attr)) { (tuple: LogicalPlan2[(LPTerm, FieldPhaseAttr)]) =>
      tuple.fold[FieldPhaseAttr](
        read      = Function.const(None), 
        constant  = Function.const(None),
        free      = Function.const(None), 
        join      = (left, right, tpe, rel, lproj, rproj) => None,
        invoke    = (func, args) => 
                    if (func == ObjectProject) {
                      val obj :: (Term(LogicalPlan2.Constant(Data.Str(fieldName))), None) :: Nil = args

                      Some(obj match {
                        case (objTerm, Some(objAttr)) =>
                          ExprOp.DocField(objAttr.field :+ BsonField.Name(fieldName))

                        case (objTerm, None) =>
                          ExprOp.DocField(BsonField.Name(fieldName))
                      })
                    } else {
                      None
                    },
        fmap      = (value, lambda) => None,
        group     = (value, by) => None
      )
    }
  }

  def ExprPhase[A]: LPPhase[A, ExprOp] = { (attr: LPAttr[A]) =>
    ???
  }

  def SelectorPhase[A]: LPPhase[A, Selector] = { (attr: LPAttr[A]) =>
    ???
  }

  def plan(logical: LPTerm, dest: String): PlannerError \/ Workflow = {
    ???
  }
}