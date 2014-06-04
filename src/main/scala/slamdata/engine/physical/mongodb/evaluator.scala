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

  def execPipeline(source: Col, pipeline: Pipeline): M[Unit] = colS(source).map(_.aggregate({println(pipeline.repr); pipeline.repr}))

  def emitProgress(p: Progress): M[Unit] = (Unit: Unit).point[M]

  sealed trait Col {
    def collection: Collection
  }
  object Col {
    case class Tmp(collection: Collection) extends Col
    case class User(collection: Collection) extends Col
  }
  
  private def execute0(task0: WorkflowTask): M[Col] = {
    import WorkflowTask._

    task0 match {
      case PureTask(value: Bson.Doc) =>
        for {
          tmp     <- generateTempName
          tmpCol  <- colS(tmp)
          _       <- liftTask(Task.delay(tmpCol.insert(value.repr)))
          _       <- emitProgress(Progress("Finished inserting constant value into collection " + tmp, None))
        } yield tmp

      case PureTask(v) => 
        // TODO: Handle set or array of documents???
        failure(new RuntimeException("MongoDB cannot store anything except documents inside collections: " + v))
      
      case ReadTask(value) => 
        for {
          _ <- emitProgress(Progress("Reading from data source " + value, None))
        } yield Col.User(value)
      
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
          _   <- execPipeline(src, Pipeline(pipeline.ops :+ PipelineOp.Out(dst.collection)))
          _   <- emitProgress(Progress("Finished executing pipeline aggregation", None))
        } yield dst

      case MapReduceTask(source, mapReduce) => 
        ???

      case JoinTask(steps) => 
        ???
    }
  }

  def execute(physical: Workflow, out: String): Task[String] = {
    for {
      prefix  <-  Task.delay("tmp" + scala.util.Random.nextInt() + "_") // TODO: Make this deterministic or controllable?
      dst     <-  execute0(physical.task).eval(EvalState(prefix, 0))
      out     <-  dst match {
                    case c @ Col.Tmp(_) =>
                      // It's a temp collection, rename it to the specified output:
                      for {
                        dstCol  <- col(dst)
                        _       <- Task.delay(dstCol.rename(out))
                      } yield out

                    case Col.User(c) =>
                      // It's a user collection and we don't want to copy it, so
                      // just reject the request to place the results in the specified
                      // location.
                      Task.now(c.name)
                  }
    } yield out
  }

  case class Progress(message: String, percentComplete: Option[Double])
}

object MongoDbEvaluator {
  def apply(db0: DB): Evaluator[Workflow] = new MongoDbEvaluator {
    val db: DB = db0
  }
}