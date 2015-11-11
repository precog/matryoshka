package quasar
package physical
package mongodb

import quasar.Predef._
import quasar.fp._
import quasar.fs.{Path => QPath, _}
import quasar.javascript._
import quasar.recursionschemes.{Fix, Recursive}

import com.mongodb.async.client.MongoClient

import pathy.Path._

import scalaz._, Scalaz._
import scalaz.concurrent.Task

object executeplan {
  import Planner.{PlannerError => PPlannerError}
  import Workflow.Crystallized
  import ExecutionError._, PathError2._
  import LogicalPlan.ReadF
  import Recursive.ops._

  type MongoExecute[A] = WorkflowExecErrT[MongoDb, A]
  type TaskExecute[A]  = WorkflowExecErrT[Task, A]

  def interpret(
    execMongo: WorkflowExecutor[MongoDb],
    execJs: WorkflowExecutor[JavaScriptLog]
  ): ExecutePlan ~> MongoExecute =
    new (ExecutePlan ~> MongoExecute) {
      def apply[A](ep: ExecutePlan[A]) = {
        val resultFile = for {
          _      <- checkPathsExist(ep.lp)
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

  def run(client: MongoClient): EnvErr2T[Task, ExecutePlan ~> TaskExecute] = {
    val f = Hoist[EnvErr2T].hoist(MongoDb.runNT(client))

    f(WorkflowExecutor.mongoDb map { mongoExec =>
      val g = interpret(mongoExec, WorkflowExecutor.javaScript)
      val h = Hoist[WorkflowExecErrT].hoist(MongoDb.runNT(client))
      h compose g
    })
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

  private def checkPathsExist(lp: Fix[LogicalPlan]): G[Unit] = {
    def checkPathExists(p: QPath): ExecErrT[MongoDb, Unit] = for {
      coll <- EitherT(Collection.fromPath(p)
                .leftMap(e => PathError(lp, InvalidPath(qPathToPathy(p), e.message)))
                .point[MongoDb])
      _    <- EitherT(MongoDb.collectionExists(coll)
                .map(_ either (()) or PathError(lp, PathNotFound(qPathToPathy(p)))))
    } yield ()

    EitherT[F, ExecutionError, Unit](
      paths(lp).traverse_[ExecErrT[MongoDb, ?]](checkPathExists)
        .run.liftM[WorkflowExecErrT].liftM[PhaseResultT])
  }

  private def paths(lp: Fix[LogicalPlan]): Set[QPath] =
    lp.foldMap(_.cata[Set[QPath]] {
      case ReadF(p) => Set(p)
      case other    => other.fold
    })

  // TODO: This is a hack, but is only used to create a Pathy.Path for error
  //       messages and will go away once LogicalPlan is converted to Pathy.
  private def qPathToPathy(p: QPath): AbsPath[Sandboxed] = {
    val abs = p.asAbsolute
    val absDir = abs.dir.foldLeft(rootDir[Sandboxed])((d, n) => d </> dir(n.value))
    abs.file.map(n => absDir </> file(n.value)) \/> absDir
  }
}
