package quasar
package physical
package mongodb
package fs

import quasar.Predef._
import quasar.fp._
import quasar.fs.{Path => QPath, _}
import quasar.javascript._
import quasar.recursionschemes.{Fix, Recursive}

import com.mongodb.async.client.MongoClient
import pathy.Path._
import scalaz.{Node => _, _}, Scalaz._
import scalaz.stream._
import scalaz.concurrent.Task

object queryfile {
  import QueryFile._
  import Planner.{PlannerError => PPlannerError}
  import Workflow.Crystallized
  import FileSystemError._, PathError2._, fsops._
  import LogicalPlan.ReadF
  import Recursive.ops._

  type MongoQuery[A] = WorkflowExecErrT[MongoDbIO, A]

  val interpret: EnvErr2T[MongoDbIO, QueryFile ~> MongoQuery] =
    WorkflowExecutor.mongoDb map { execMongo =>
      val execJs = WorkflowExecutor.javaScript

      new (QueryFile ~> MongoQuery) {
        def apply[A](qf: QueryFile[A]) = qf match {
          case ExecutePlan(lp, out) =>
            (for {
              _      <- checkPathsExist(lp)
              wf     <- convertP(lp)(MongoDbPlanner.plan(lp))
              dst    <- EitherT(Collection.fromPathy(out)
                                  .leftMap(PathError)
                                  .point[F])
              salt   <- liftG(MongoDbIO.liftTask(NameGenerator.salt)
                                .liftM[WorkflowExecErrT])
              prefix =  s"tmp.gen_${salt}"
              _      <- writeJsLog(wf, dst, execJs) run prefix
              coll   <- execWorkflow(wf, dst, execMongo) run prefix
            } yield ResultFile.User(coll.asFile)).run.run

          case ListContents(dir) =>
            (dirName(dir) match {
              case Some(_) =>
                collectionsInDir(dir)
                  .map(_ foldMap (collectionToNode(dir) andThen (_.toSet)))
                  .run

              case None if depth(dir) == 0 =>
                MongoDbIO.collections
                  .map(collectionToNode(dir))
                  .pipe(process1.stripNone)
                  .runLog
                  .map(_.toSet.right[FileSystemError])

              case None =>
                nonExistentParent[Set[Node]](dir).run
            }).liftM[WorkflowExecErrT]
        }
      }
    }

  def run(client: MongoClient): EnvErr2T[MongoDbIO, QueryFile ~> MongoQuery] => EnvErr2T[Task, QueryFile ~> WFTask] = {
    val f = Hoist[EnvErr2T].hoist(MongoDbIO.runNT(client))
    val g = Hoist[WorkflowExecErrT].hoist(MongoDbIO.runNT(client))

    interp => f(interp map (g compose _))
  }

  ////

  private type P[A] = EitherT[(PhaseResults, ?), PPlannerError, A]
  private type F[A] = PhaseResultT[MongoQuery, A]
  private type G[A] = FileSystemErrT[F, A]

  private type W[A, B]  = WriterT[MongoQuery, A, B]
  private type GE[A, B] = EitherT[F, A, B]

  private val liftG: MongoQuery ~> G =
    liftMT[F, FileSystemErrT] compose liftMT[MongoQuery, PhaseResultT]

  private def convertP(lp: Fix[LogicalPlan]): P ~> G =
    new (P ~> G) {
      def apply[A](pa: P[A]) = {
        val r = pa.leftMap(PlannerError(lp, _)).run
        val f: F[FileSystemError \/ A] = WriterT(r.point[MongoQuery])
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
      err => liftG(err.raiseError[MongoE, Unit]),
      _   => (MonadTell[W, PhaseResults].tell(Vector(phaseR)): F[Unit])
               .liftM[FileSystemErrT])
  }

  private def execWorkflow(
    wf: Crystallized,
    dst: Collection,
    execMongo: WorkflowExecutor[MongoDbIO]
  ) = ReaderT[G, String, Collection] { tmpPrefix =>
    liftG(EitherT(execMongo.execute(wf, dst).run.run(tmpPrefix).eval(0)))
  }

  private def checkPathsExist(lp: Fix[LogicalPlan]): G[Unit] = {
    def checkPathExists(p: QPath): MongoFsM[Unit] = for {
      coll <- EitherT(Collection.fromPath(p)
                .leftMap(e => PathError(InvalidPath(qPathToPathy(p), e.message)))
                .point[MongoDbIO])
      _    <- EitherT(MongoDbIO.collectionExists(coll)
                .map(_ either (()) or PathError(PathNotFound(qPathToPathy(p)))))
    } yield ()

    EitherT[F, FileSystemError, Unit](
      paths(lp).traverse_(checkPathExists)
        .run.liftM[WorkflowExecErrT].liftM[PhaseResultT])
  }

  private def paths(lp: Fix[LogicalPlan]): Set[QPath] =
    lp.foldMap(_.cata[Set[QPath]] {
      case ReadF(p) => Set(p)
      case other    => other.fold
    })

  // TODO: This is a hack, but is only used to create a Pathy.Path for error
  //       messages and will go away once LogicalPlan is converted to Pathy.
  private def qPathToPathy(p: QPath): APath = {
    val abs = p.asAbsolute
    val absDir = abs.dir.foldLeft(rootDir[Sandboxed])((d, n) => d </> dir(n.value))
    abs.file.map(n => absDir </> file(n.value)) getOrElse absDir
  }
}
