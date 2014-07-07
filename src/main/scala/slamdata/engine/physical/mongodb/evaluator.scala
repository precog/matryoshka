package slamdata.engine.physical.mongodb

import slamdata.engine._
import slamdata.engine.fp._
import slamdata.engine.fs._
import slamdata.engine.std.StdLib._

import scala.util.Try
import com.mongodb._

import scalaz.{Free => FreeM, Node => _, _}

import scalaz.syntax.either._
import scalaz.syntax.foldable._
import scalaz.syntax.compose._
import scalaz.syntax.applicativePlus._

import scalaz.std.AllInstances._

import scalaz.concurrent.Task

trait MongoDbEvaluator extends Evaluator[Workflow] {
  type StateTEvalState[F[_], A] = StateT[F, EvalState, A]

  type M[A] = StateTEvalState[Task, A]

  case class EvalState(tmp: String, counter: Int) {
    def inc: EvalState = copy(counter = counter + 1)
  }

  def readState: M[EvalState] = StateT[Task, EvalState, EvalState](state => ((state, state)).point[Task])

  def liftTask[A](v: Task[A]): M[A] = StateT[Task, EvalState, A](state => v.map((state, _)))

  def liftS(f: EvalState => EvalState): M[Unit] = liftS2(s => ((f(s), Unit: Unit)))

  def liftS2[A](f: EvalState => (EvalState, A)): M[A] = StateT[Task, EvalState, A](state => f(state).point[Task])

  def db: DB

  def ret[A](v: A): M[A] = StateT[Task, EvalState, A](state => ((state, v)).point[Task])

  def col(c: Col): Task[DBCollection] = Task.delay(db.getCollection(c.collection.name))

  def colS(c: Col): M[DBCollection] = liftTask(col(c))

  def failure[A](e: Throwable): M[A] = liftTask(Task.fail(e))

  def generateTempName: M[Col] = liftS2[Col] { state =>
    (state.inc, Col.Tmp(Collection(state.tmp + state.counter.toString)))
  }

  def execPipeline(source: Col, pipeline: Pipeline): M[EvaluationError \/ Unit] =
    colS(source).map(c => wrapMongoException(Try(c.aggregate(pipeline.repr))))

  def execMapReduce(source: Col, dst: Col, mr: MapReduce): M[EvaluationError \/ Unit] =
    colS(source).map { src =>
      wrapMongoException(Try(
        src.mapReduce(new MapReduceCommand(
          src,
          mr.map.render(0),
          mr.reduce.render(0),
          dst.collection.name,
          (mr.out.map(_.action) match {
            case Some(Action.Merge)  => MapReduceCommand.OutputType.MERGE
            case Some(Action.Reduce) => MapReduceCommand.OutputType.REDUCE
            case _                   => MapReduceCommand.OutputType.REPLACE
          }),
          // mr.selection.map(_.repr).getOrElse((new QueryBuilder).get)
          (new QueryBuilder).get))))
    }

  private def wrapMongoException(t: Try[Unit]): EvaluationError \/ Unit = t match {
    case scala.util.Success(_)     =>  \/- (())
    case scala.util.Failure(cause) => -\/ (EvaluationError(cause))
  }

  def emitProgress(p: Progress): M[Unit] = (Unit: Unit).point[M]

  sealed trait Col {
    def collection: Collection
  }
  object Col {
    case class Tmp(collection: Collection) extends Col
    case class User(collection: Collection) extends Col
  }
  
  private def execute0(requestedCol: Col, task0: WorkflowTask): M[EvaluationError \/ Col] = {
    import WorkflowTask._

    task0 match {
      case PureTask(value: Bson.Doc) =>
        for {
          tmpCol  <- colS(requestedCol)
          _       <- liftTask(Task.delay(tmpCol.insert(value.repr)))
          _       <- emitProgress(Progress("Finished inserting constant value into collection " + requestedCol, None))
        } yield \/- (requestedCol)

      case PureTask(Bson.Arr(value)) =>
        for {
          tmpCol <- colS(requestedCol)
          dst    <- value.toList.foldLeftM[M, EvaluationError \/ Col](\/- (requestedCol)) { (v, doc) =>
            v.fold(e => ret(-\/(e)), col => execute0(col, PureTask(doc)))
          }
        } yield dst

      case PureTask(v) => 
        ret(-\/ (EvaluationError(new RuntimeException("MongoDB cannot store anything except documents inside collections: " + v))))
      
      case ReadTask(value) => 
        for {
          _ <- emitProgress(Progress("Reading from data source " + value, None))
        } yield \/- (Col.User(value))
      
      case QueryTask(source, query, skip, limit) => 
        // TODO: This is an approximation since we're ignoring all fields of "Query" except the selector.
        execute0(
          requestedCol,
          PipelineTask(
            source,
            Pipeline(
              PipelineOp.Match(query.query) ::
                skip.map(PipelineOp.Skip(_) :: Nil).getOrElse(Nil) :::
                limit.map(PipelineOp.Limit(_) :: Nil).getOrElse(Nil))))

      case PipelineTask(source, pipeline) => 
        for {
          tmp <- generateTempName
          src <- execute0(tmp, source)
          rez <- src.fold(e => ret(-\/(e)), col => execPipeline(col, Pipeline(pipeline.ops :+ PipelineOp.Out(requestedCol.collection))))
          _   <- emitProgress(Progress("Finished executing pipeline aggregation", None))
        } yield rez.map(_ => requestedCol)

      case MapReduceTask(source, mapReduce) => for {
        tmp <- generateTempName
        src <- execute0(tmp, source)
        rez <- src.fold(e => ret(-\/(e)), col => execMapReduce(col, requestedCol, mapReduce))  // TODO
      } yield rez.map(_ => requestedCol)

      case FoldLeftTask(steps) =>
        // FIXME: This is pretty fragile. A ReadTask will cause any later steps
        //        to merge into the read collection, a PipelineTask will
        //        overwrite any previous steps, etc. This is mostly useful if
        //        you have a series of MapReduceTasks, optionally preceded by a
        //        PipelineTask.
        steps.foldLeftM[M, EvaluationError \/ Col](\/- (requestedCol)) {
          (v, task) => v.fold(e => ret(-\/(e)), col => execute0(col, task))
        }

      case JoinTask(steps) =>
        ???
    }
  }

  def execute(physical: Workflow, out: Path): Task[EvaluationError \/ Path] = {
    for {
      prefix  <-  Task.delay("tmp" + scala.util.Random.nextInt() + "_") // TODO: Make this deterministic or controllable?
      dst     <-  execute0(Col.Tmp(Collection(out.filename)), physical.task).eval(EvalState(prefix, 0))
    } yield dst.map(v => Path.fileAbs(v.collection.name))
  }

  case class Progress(message: String, percentComplete: Option[Double])
}

object MongoDbEvaluator {
  def apply(db0: DB): Evaluator[Workflow] = new MongoDbEvaluator {
    val db: DB = db0
  }
}
