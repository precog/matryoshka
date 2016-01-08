/*
 * Copyright 2014 - 2015 SlamData Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package quasar.physical.mongodb.fs

import quasar.Predef._
import quasar._
import quasar.fp._
import quasar.fs.{Path => QPath, _}
import quasar.javascript._
import quasar.physical.mongodb._, WorkflowExecutor.WorkflowCursor
import quasar.recursionschemes.{Fix, Recursive}

import com.mongodb.async.client.MongoClient
import pathy.Path._
import scalaz.{Node => _, _}, Scalaz._
import scalaz.stream._
import scalaz.concurrent.Task

object queryfile {
  import QueryFile.ResultHandle

  type ResultCursor[C]     = List[Bson] \/ WorkflowCursor[C]
  type ResultMap[C]        = Map[ResultHandle, ResultCursor[C]]
  type EvalState[C]        = (Long, ResultMap[C])
  type QueryRT[F[_], C, A] = ReaderT[F, (Option[DefaultDb], TaskRef[EvalState[C]]), A]
  type QueryR[C, A]        = QueryRT[MongoDbIO, C, A]
  type MongoQuery[C, A]    = WorkflowExecErrT[QueryR[C, ?], A]

  def interpret[C](execMongo: WorkflowExecutor[MongoDbIO, C])
                  (implicit C: DataCursor[MongoDbIO, C])
                  : QueryFile ~> MongoQuery[C, ?] = {

    new QueryFileInterpreter(execMongo)
  }

  def run[C](
    client: MongoClient,
    defDb: Option[DefaultDb]
  ): Task[MongoQuery[C, ?] ~> WFTask] = {
    type QR[A] = QueryR[C, A]
    type MQ[A] = MongoQuery[C, A]

    def runQR(ref: TaskRef[EvalState[C]]): QR ~> MongoDbIO =
      new (QR ~> MongoDbIO) {
        def apply[A](qr: QR[A]) = qr.run((defDb, ref))
      }

    def runMQ(ref: TaskRef[EvalState[C]]): MQ ~> WFTask =
      Hoist[WorkflowExecErrT].hoist(MongoDbIO.runNT(client) compose runQR(ref))

    TaskRef((0L, Map.empty: ResultMap[C])) map runMQ
  }
}

private final class QueryFileInterpreter[C](
  execMongo: WorkflowExecutor[MongoDbIO, C])(
  implicit C: DataCursor[MongoDbIO, C]
) extends (QueryFile ~> queryfile.MongoQuery[C, ?]) {

  import QueryFile._
  import Planner.{PlannerError => PPlannerError}
  import Workflow._
  import FileSystemError._, PathError2._, fsops._
  import LogicalPlan.ReadF
  import Recursive.ops._
  import queryfile._

  type QRT[F[_], A] = QueryRT[F, C, A]
  type QR[A]        = QRT[MongoDbIO, A]
  type MQ[A]        = WorkflowExecErrT[QR, A]

  private val execJs = WorkflowExecutor.javaScript

  def apply[A](qf: QueryFile[A]) = qf match {
    case ExecutePlan(lp, out) =>
      EitherT[QR, WorkflowExecutionError, (PhaseResults, FileSystemError \/ AFile)](
        (for {
          _      <- checkPathsExist(lp)
          dst    <- EitherT(Collection.fromPathy(out)
                              .leftMap(PathError)
                              .point[MongoLogWF])
          wf     <- convertPlanR(lp)(MongoDbPlanner plan lp)
          prefix <- liftMQ(genPrefix)
          _      <- writeJsLog(execJs.execute(wf, dst), prefix)
          coll   <- liftMQ(execWorkflow(wf, dst, prefix))
        } yield coll.asFile).run.run.run)

    case EvaluatePlan(lp) =>
      EitherT[QR, WorkflowExecutionError, (PhaseResults, FileSystemError \/ ResultHandle)](
        (for {
          _       <- checkPathsExist(lp)
          wf      <- convertPlanR(lp)(MongoDbPlanner plan lp)
          prefix  <- liftMQ(genPrefix)
          dbName  <- liftMQ(defaultDbName)
          _       <- writeJsLog(execJs.evaluate(wf, dbName), prefix)
          rcursor <- liftMQ(evalWorkflow(wf, dbName, prefix))
          handle  <- liftMQ(recordCursor(rcursor))
        } yield handle).run.run.run)

    case More(h) =>
      moreResults(h)
        .toRight(UnknownResultHandle(h))
        .run

    case Close(h) =>
      OptionT[MQ, ResultCursor[C]](MongoQuery(resultsL(h) <:= none))
        .flatMapF(_.fold(κ(().point[MQ]), wc =>
          DataCursor[MongoDbIO, WorkflowCursor[C]]
            .close(wc)
            .liftM[QRT]
            .liftM[WorkflowExecErrT]))
        .run.void

    case Explain(lp) =>
      EitherT[QR, WorkflowExecutionError, (PhaseResults, FileSystemError \/ ExecutionPlan)](
        (for {
          wf  <- convertPlanR(lp)(MongoDbPlanner plan lp)
          db  <- liftMQ(defaultDbName)
          res =  execJs.evaluate(wf, db).run.run("tmp.gen").eval(0).run
          (stmts, r) = res
          out =  Js.Stmts(stmts.toList).pprint(0)
          ep  <- liftMQ(EitherT.fromDisjunction(r as ExecutionPlan(MongoDBFsType, out)))
          _   <- logProgram(stmts)
        } yield ep).run.run.run)

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
      }).liftM[QRT].liftM[WorkflowExecErrT]

    case FileExists(file) =>
      collFromPathM(file).flatMap(MongoDbIO.collectionExists(_).liftM[FileSystemErrT]).run.liftM[QRT].liftM[WorkflowExecErrT]
  }

  ////

  private type PlanR[A]       = EitherT[(PhaseResults, ?), PPlannerError, A]
  private type MongoLogWF[A]  = PhaseResultT[MQ, A]
  private type MongoLogWFR[A] = FileSystemErrT[MongoLogWF, A]

  private type MQE[A, B] = EitherT[QR, A, B]
  private type W[A, B]   = WriterT[MQ, A, B]
  private type R[A, B]   = ReaderT[MongoDbIO, A, B]

  private val queryR =
    MonadReader[R, (Option[DefaultDb], TaskRef[EvalState[C]])]

  private def MongoQuery[A](f: TaskRef[EvalState[C]] => Task[A]): MQ[A] =
    queryR.ask
      .flatMapK { case (_, ref) => MongoDbIO.liftTask(f(ref)) }
      .liftM[WorkflowExecErrT]

  private def MongoQuery[A](s: State[EvalState[C], A]): MQ[A] =
    MongoQuery(_ modifyS s.run)

  private val seqL: EvalState[C] @> Long =
    Lens.firstLens

  private val resultMapL: EvalState[C] @> ResultMap[C] =
    Lens.secondLens

  private def resultsL(h: ResultHandle): EvalState[C] @> Option[ResultCursor[C]] =
    Lens.mapVLens(h) <=< resultMapL

  private def freshHandle: MQ[ResultHandle] =
    MongoQuery(seqL <%= (_ + 1)) map (ResultHandle(_))

  private def recordCursor(c: ResultCursor[C]): MQ[ResultHandle] =
    freshHandle flatMap (h => MongoQuery(resultsL(h) := some(c)) as h)

  private def lookupCursor(h: ResultHandle): OptionT[MQ, ResultCursor[C]] =
    OptionT(MongoQuery(resultsL(h).st))

  private def defaultDbName: MQ[Option[String]] =
    queryR.asks(_._1.map(_.run)).liftM[WorkflowExecErrT]

  private def genPrefix: MQ[String] =
    (MongoDbIO.liftTask(NameGenerator.salt)
      .liftM[QRT]: QR[String])
      .liftM[WorkflowExecErrT]
      .map(salt => s"tmp.gen_${salt}")

  private val liftMQ: MQ ~> MongoLogWFR =
    liftMT[MongoLogWF, FileSystemErrT] compose liftMT[MQ, PhaseResultT]

  private def convertPlanR(lp: Fix[LogicalPlan]): PlanR ~> MongoLogWFR =
    new (PlanR ~> MongoLogWFR) {
      def apply[A](pa: PlanR[A]) = {
        val r = pa.leftMap(PlannerError(lp, _)).run
        val f: MongoLogWF[FileSystemError \/ A] = WriterT(r.point[MQ])
        EitherT(f)
      }
    }

  private def execWorkflow(
    wf: Crystallized,
    dst: Collection,
    tmpPrefix: String
  ): MQ[Collection] =
    EitherT[QR, WorkflowExecutionError, Collection](
      execMongo.execute(wf, dst).run.run(tmpPrefix).eval(0).liftM[QRT])

  private def evalWorkflow(
    wf: Crystallized,
    defDb: Option[String],
    tmpPrefix: String
  ): MQ[ResultCursor[C]] =
    EitherT[QR, WorkflowExecutionError, ResultCursor[C]](
      execMongo.evaluate(wf, defDb).run.run(tmpPrefix).eval(0).liftM[QRT])

  private type JsR[A] =
    WorkflowExecErrT[ReaderT[SeqNameGeneratorT[JavaScriptLog,?],String,?], A]

  private def writeJsLog(jsr: JsR[_], tmpPrefix: String): MongoLogWFR[Unit] = {
    val (stmts, r) = jsr.run.run(tmpPrefix).eval(0).run
    r.fold(err => liftMQ(err.raiseError[MQE, Unit]), κ(logProgram(stmts)))
  }

  private def logProgram(prog: JavaScriptPrg): MongoLogWFR[Unit] = {
    val phaseR: PhaseResult =
      PhaseResult.Detail("MongoDB", Js.Stmts(prog.toList).pprint(0))

    (MonadTell[W, PhaseResults].tell(Vector(phaseR)): MongoLogWF[Unit])
      .liftM[FileSystemErrT]
  }

  private def checkPathsExist(lp: Fix[LogicalPlan]): MongoLogWFR[Unit] = {
    def checkPathExists(p: QPath): MongoFsM[Unit] = for {
      coll <- EitherT.fromDisjunction[MongoDbIO](Collection.fromPath(p))
                .leftMap(e => PathError(InvalidPath(qPathToPathy(p), e.message)))
      _    <- EitherT(MongoDbIO.collectionExists(coll)
                .map(_ either (()) or PathError(PathNotFound(qPathToPathy(p)))))
    } yield ()

    EitherT[MongoLogWF, FileSystemError, Unit](
      (paths(lp).traverse_(checkPathExists).run
        .liftM[QRT]
        .liftM[WorkflowExecErrT]: MQ[FileSystemError \/ Unit])
        .liftM[PhaseResultT])
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

  private def moreResults(h: ResultHandle): OptionT[MQ, Vector[Data]] = {
    def pureNextChunk(bsons: List[Bson]) =
      if (bsons.isEmpty)
        Vector.empty[Data].point[MQ]
      else
        MongoQuery(resultsL(h) := some(List().left))
          .as(bsons.map(BsonCodec.toData).toVector)

    lookupCursor(h) flatMapF (_.fold(pureNextChunk, wc =>
      DataCursor[MongoDbIO, WorkflowCursor[C]]
        .nextChunk(wc)
        .liftM[QRT]
        .liftM[WorkflowExecErrT]))
  }
}
