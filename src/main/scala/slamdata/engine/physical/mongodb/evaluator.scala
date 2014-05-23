package slamdata.engine.physical.mongodb

import slamdata.engine._
import slamdata.engine.fp._
import slamdata.engine.std.StdLib._

import com.mongodb._

import scalaz.{Free => FreeM, Node => _, _}

import scalaz.syntax.either._
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

  def liftP[A](v: Task[A]): M[A] = StateT[Task, EvalState, A](state => v.map((state, _)))

  def liftS(f: EvalState => EvalState): M[Unit] = liftS2(s => ((f(s), Unit: Unit)))

  def liftS2[A](f: EvalState => (EvalState, A)): M[A] = StateT[Task, EvalState, A](state => f(state).point[Task])

  def db: Task[DB]

  def ret[A](v: A): M[A] = StateT[Task, EvalState, A](state => ((state, v)).point[Task])

  def col(c: Collection): Task[DBCollection] = for {
    db <- db
  } yield db.getCollection(c.name)

  def colS(c: Collection): M[DBCollection] = liftP(col(c))

  def failure[A](e: Throwable): M[A] = liftP(Task.fail(e))

  def generateTempName: M[Collection] = liftS2[Collection] { state =>
    (state.inc, Collection(state.tmp + state.counter.toString))
  }

  def execPipeline(source: Collection, pipeline: Pipeline): M[Unit] = colS(source).map(_.aggregate(pipeline.repr))

  def emitProgress(p: Progress): M[Unit] = (Unit: Unit).point[M]
  
  private def execute0(task0: WorkflowTask): M[Collection] = {
    import WorkflowTask._

    task0 match {
      case PureTask(value: Bson.Doc) =>
        for {
          tmp     <- generateTempName
          tmpCol  <- colS(tmp)
          _       <- liftP(Task.delay(tmpCol.insert(value.repr)))
          _       <- emitProgress(Progress("Finished inserting constant value into collection " + tmp, None))
        } yield tmp

      case PureTask(v) => failure(new RuntimeException("MongoDB cannot store anything except documents inside collections: " + v))
      
      case ReadTask(value) => 
        for {
          _ <- emitProgress(Progress("Reading from data source " + value, None))
        } yield value
      
      case QueryTask(source, query, skip, limit) => 
        // TODO: This is an approximation since we're ignoring all fields of "Query" except the selector.
        for {
          dst <-  execute0(PipelineTask(
                    source, 
                    Pipeline(
                      PipelineOp.Match(query.query) ::
                      skip.map(PipelineOp.Skip(_) :: Nil).getOrElse(Nil) :::
                      limit.map(PipelineOp.Limit(_) :: Nil).getOrElse(Nil)
                    )
                  ))
        } yield dst

      case PipelineTask(source, pipeline) => 
        for {
          src <- execute0(source)
          dst <- generateTempName
          _   <- execPipeline(src, Pipeline(pipeline.ops :+ PipelineOp.Out(dst)))
          _   <- emitProgress(Progress("Finished executing pipeline aggregation", None))
        } yield dst

      case MapReduceTask(source, mapReduce) => 
        ???

      case JoinTask(steps) => 
        ???
    }
  }

  def execute(physical: Workflow, out: String): Unit = {
    for {
      prefix  <- Task.delay("tmp" + scala.util.Random.nextInt() + "_") // TODO: Make this deterministic or controllable?
      dst     <- execute0(physical.task).eval(EvalState(prefix, 0))
      dstCol  <- col(dst)
      _       <- Task.delay(dstCol.rename(out))
    } yield (Unit: Unit)
  }

  case class Progress(message: String, percentComplete: Option[Double])
}

object MongoDbEvaluator {
  def apply(db0: Task[DB]): Evaluator[Workflow] = new MongoDbEvaluator {
    val db: Task[DB] = db0
  }
}