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

  import set._
  import relations._
  import structural._
  import math._

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
  def FieldPhase[A]: Phase[LogicalPlan2, A, Option[BsonField]] = {
    type FieldPhaseAttr = Option[BsonField]
    
    Phase { (attr: LPAttr[A]) =>
      synthPara2(forget(attr)) { (node: LogicalPlan2[(LPTerm, FieldPhaseAttr)]) =>
        node.fold[FieldPhaseAttr](
          read      = Function.const(None), 
          constant  = Function.const(None),
          free      = Function.const(None), 
          join      = (left, right, tpe, rel, lproj, rproj) => None,
          invoke    = (func, args) => 
                      if (func == ObjectProject) {
                        val (objTerm, objAttrOpt) :: (Term(LogicalPlan2.Constant(Data.Str(fieldName))), None) :: Nil = args

                        Some(objAttrOpt match {
                          case Some(objAttr) =>
                            objAttr :+ BsonField.Name(fieldName)

                          case None =>
                            BsonField.Name(fieldName)
                        })
                      } else {
                        None
                      },
          fmap      = (value, lambda) => None,
          group     = (value, by) => None
        )
      }
    }
  }

  /**
   * This phase builds up expression operations from field attributes.
   *
   * As it works its way up the tree, at some point, it will reach a place where
   * the value cannot be computed as an expression operation. The phase will produce
   * None at these points. Further up the tree from such a position, it may again
   * be possible to build expression operations, so this process will naturally
   * result in spans of expressions alternating with spans of nothing (i.e. None).
   *
   * The "holes" represent positions where a pipeline operation or even a workflow
   * task is required to compute the given expression.
   */
  def ExprPhase: PhaseE[LogicalPlan2, PlannerError, Option[BsonField], Option[ExprOp]] = {
    type ExprPhaseAttr = PlannerError \/ Option[ExprOp]

    toPhaseE(Phase { (attr: LPAttr[Option[BsonField]]) =>
      scanCata(attr) { (fieldAttr: Option[BsonField], node: LogicalPlan2[ExprPhaseAttr]) =>
        def emit(expr: ExprOp): ExprPhaseAttr = \/- (Some(expr))

        def promoteBsonField = \/- (fieldAttr.map(ExprOp.DocField.apply _))

        def nothing = \/- (None)

        def invoke(func: Func, args: List[ExprPhaseAttr]): ExprPhaseAttr = {
          def invoke1(f: ExprOp => ExprOp) = {
            val x :: Nil = args

            x.map(_.map(f))
          }
          def invoke2(f: (ExprOp, ExprOp) => ExprOp) = {
            val x :: y :: Nil = args

            (x |@| y)(f)
          }

          func match {
            case `Add`      => invoke2(ExprOp.Add.apply _)
            case `Multiply` => invoke2(ExprOp.Multiply.apply _)
            case `Subtract` => invoke2(ExprOp.Subtract.apply _)
            case `Divide`   => invoke2(ExprOp.Divide.apply _)

            case `Eq`       => invoke2(ExprOp.Eq.apply _)
            case `Neq`      => invoke2(ExprOp.Neq.apply _)
            case `Lt`       => invoke2(ExprOp.Lt.apply _)
            case `Lte`      => invoke2(ExprOp.Lte.apply _)
            case `Gt`       => invoke2(ExprOp.Gt.apply _)
            case `Gte`      => invoke2(ExprOp.Gte.apply _)

            case `ObjectProject`  => promoteBsonField
            case `ArrayProject`   => promoteBsonField

            case _ => nothing
          }
        }

        node.fold[ExprPhaseAttr](
          read      = _ => promoteBsonField, // FIXME: Need to descend into appropriate join
          constant  = data => Bson.fromData(data).bimap[PlannerError, Option[ExprOp]](
                        _ => PlannerError.NonRepresentableData(data), 
                        d => Some(ExprOp.Literal(d))
                      ),
          free      = _ => nothing,
          join      = (_, _, _, _, _, _) => nothing,
          invoke    = invoke(_, _),
          fmap      = (_, _) => nothing,
          group     = (_, _) => nothing
        )
      }
    })
  }

  /**
   * The selector phase tries to turn expressions into MongoDB selectors -- i.e. 
   * Mongo query expressions. Selectors are only used for the filtering pipeline op,
   * so it's quite possible we build more stuff than is needed (but it doesn't matter, 
   * unneeded annotations will be ignored by the pipeline phase).
   *
   * Like the expression op phase, this one requires bson field annotations.
   *
   * Most expressions cannot be turned into selector expressions without using the
   * "Where" operator, which allows embedding JavaScript code. Unfortunately, using
   * this operator turns filtering into a full table scan. We should do a pass over
   * the tree to identify partial boolean expressions which can be turned into selectors,
   * factoring out the leftovers for conversion using Where.
   *
   */
  def SelectorPhase: PhaseE[LogicalPlan2, PlannerError, Option[BsonField], Option[Selector]] = {
    type SelectorAnn = Option[Selector]

    liftPhaseE(Phase { (attr: LPAttr[Option[BsonField]]) =>
      scanPara2(attr) { (fieldAttr: Option[BsonField], node: LogicalPlan2[(Term[LogicalPlan2], Option[BsonField], SelectorAnn)]) =>
        def emit(sel: Selector): SelectorAnn = Some(sel)

        def promoteBsonField = fieldAttr.map(???)

        def nothing = None

        def invoke(func: Func, args: List[(Term[LogicalPlan2], Option[BsonField], SelectorAnn)]): SelectorAnn = {
          /**
           * Attempts to extract a BsonField annotation and a selector from
           * an argument list of length two.
           */
          def extractFieldAndSelector: Option[(BsonField, Selector)] = {
            val (_, f1, s1) :: (_, f2, s2) :: Nil = args

            f1.map((_, s2)).orElse(f2.map((_, s1))).flatMap {
              case (field, optSel) => 
                optSel.map((field, _))
            }
          }

          /**
           * All the relational operators require a field as one parameter, and 
           * BSON literal value as the other parameter. So we have to try to
           * extract out both a field annotation and a selector and then verify
           * the selector is actually a BSON literal value before we can 
           * construct the relational operator selector. If this fails for any
           * reason, it just means the given expression cannot be represented
           * using MongoDB's query operators, and must instead be written as
           * Javascript using the "$where" operator. Currently that's not supported.
           */
          def relop(f: Bson => Selector) = {
            extractFieldAndSelector.flatMap[Selector] {
              case (field, selector) => 
                selector match {
                  case Selector.Literal(bson) => Some(Selector.Doc(Map(field -> f(bson))))
                  case _ => None
                }
            }
          }
          def invoke1(f: Selector => Selector) = {
            val x :: Nil = args.map(_._3)

            x.map(f)
          }
          def invoke2Nel(f: NonEmptyList[Selector] => Selector) = {
            val x :: y :: Nil = args.map(_._3)

            (x |@| y)((a, b) => f(NonEmptyList(a, b)))
          }

          func match {
            case `Eq`       => relop(Selector.Eq.apply _)
            case `Neq`      => relop(Selector.Neq.apply _)
            case `Lt`       => relop(Selector.Lt.apply _)
            case `Lte`      => relop(Selector.Lte.apply _)
            case `Gt`       => relop(Selector.Gt.apply _)
            case `Gte`      => relop(Selector.Gte.apply _)

            case `And`      => invoke2Nel(Selector.And.apply _)
            case `Or`       => invoke2Nel(Selector.Or.apply _)
            case `Not`      => invoke1(Selector.Not.apply _)

            case `ObjectProject`  => promoteBsonField
            case `ArrayProject`   => promoteBsonField

            case _ => nothing
          }
        }

        node.fold[SelectorAnn](
          read      = _ => promoteBsonField,
          constant  = data => Bson.fromData(data).fold[SelectorAnn](
                        _ => None, 
                        d => Some(Selector.Literal(d))
                      ),
          free      = _ => nothing,
          join      = (_, _, _, _, _, _) => nothing,
          invoke    = invoke(_, _),
          fmap      = (_, _) => nothing,
          group     = (_, _) => nothing
        )
      }
    })
  }

  def plan(logical: LPTerm, dest: String): PlannerError \/ Workflow = {
    ???
  }
}