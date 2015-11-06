package quasar
package physical
package mongodb

import quasar.Predef._
import quasar.fp._
import quasar.javascript._
import quasar.recursionschemes.Fix

import scalaz._, Scalaz._

object executeplan {
  import Planner.{PlannerError => PPlannerError}
  import Workflow.Crystallized
  import ExecutionError._

  type MongoExecute[A] = WorkflowExecErrT[MongoDb, A]

  def interpret(
    execMongo: WorkflowExecutor[MongoDb],
    execJs: WorkflowExecutor[JavaScriptLog]
  ): ExecutePlan ~> MongoExecute =
    new (ExecutePlan ~> MongoExecute) {
      def apply[A](ep: ExecutePlan[A]) = {
        val resultFile = for {
          wf     <- convertP(ep.lp)(MongoDbPlanner.plan(ep.lp))
          dst    <- EitherT(Collection.fromFile(ep.out)
                              .leftMap(PathError(ep.lp, _))
                              .point[F])
          salt   <- liftG(MongoDb.liftTask(NameGenerator.salt)
                            .liftM[WorkflowExecErrT])
          prefix =  s"tmp.gen_${salt}"
          _      <- writeJsLog(wf, dst, execJs) run prefix
          coll   <- execWorkflow(wf, dst, execMongo) run prefix
        } yield ResultFile.User(coll.asFile)

        resultFile.run.run map ep.f
      }
    }

  ////

  private type P[A] = EitherT[(PhaseResults, ?), PPlannerError, A]
  private type F[A] = PhaseResultT[MongoExecute, A]
  private type G[A] = ExecErrT[F, A]

  private type W[A, B]  = WriterT[MongoExecute, A, B]
  private type GE[A, B] = EitherT[F, A, B]

  private val liftG: MongoExecute ~> G =
    liftMT[F, ExecErrT] compose liftMT[MongoExecute, PhaseResultT]

  private def convertP(lp: Fix[LogicalPlan]): P ~> G =
    new (P ~> G) {
      def apply[A](pa: P[A]) = {
        val r = pa.leftMap(PlannerError(lp, _)).run
        val f: F[ExecutionError \/ A] = WriterT(r.point[MongoExecute])
        EitherT(f)
      }
    }

  private def writeJsLog(
    wf: Crystallized,
    dst: Collection,
    execJs: WorkflowExecutor[JavaScriptLog]
  ) = ReaderT[G, String, Unit] { tmpPrefix =>
    val (stmts, r) =
      execJs.execute(wf, dst).run.run(tmpPrefix).eval(0).run

    def phaseR: PhaseResult =
      PhaseResult.Detail("MongoDB", Js.Stmts(stmts.toList).pprint(0))

    r.fold(
      err => liftG(err.raiseError[EitherT[MongoDb, ?, ?], Unit]),
      _   => (MonadTell[W, PhaseResults].tell(Vector(phaseR)): F[Unit])
               .liftM[ExecErrT])
  }

  private def execWorkflow(
    wf: Crystallized,
    dst: Collection,
    execMongo: WorkflowExecutor[MongoDb]
  ) = ReaderT[G, String, Collection] { tmpPrefix =>
    liftG(EitherT(execMongo.execute(wf, dst).run.run(tmpPrefix).eval(0)))
  }
}
