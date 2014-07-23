package slamdata.engine.physical.mongodb

import slamdata.engine._
import slamdata.engine.fp._
import slamdata.engine.fs._
import slamdata.engine.std.StdLib._

import com.mongodb._

import scalaz.{Free => FreeM, Node => _, _}
import Scalaz._
import scalaz.\/.{fromTryCatchNonFatal}
import scalaz.concurrent.Task

trait Executor[F[_]] {
  def generateTempName: F[Collection]
  
  def insert(dst: Collection, value: Bson.Doc): F[Unit]
  def aggregate(source: Collection, pipeline: Pipeline): F[Unit]
  def mapReduce(source: Collection, dst: Collection, mr: MapReduce): F[Unit]
  
  def fail[A](e: EvaluationError): F[A]
}

class MongoDbEvaluator(impl: MongoDbEvaluatorImpl[({type λ[α] = StateT[Task, SequenceNameGenerator.EvalState,α]})#λ]) extends Evaluator[Workflow] {
  def execute(physical: Workflow, out: Path): Task[Path] = {
    impl.execute(physical, out).eval(SequenceNameGenerator.startUnique)
  }
}

object MongoDbEvaluator {
  type ST[A] = StateT[Task, SequenceNameGenerator.EvalState, A]

  def apply(db0: DB)(implicit m0: Monad[ST]): Evaluator[Workflow] = {
    val executor0: Executor[ST] = new MongoDbExecutor(db0, SequenceNameGenerator.Gen)
    new MongoDbEvaluator(new MongoDbEvaluatorImpl[ST] {
      val executor = executor0
    })
  }

  def toJS(physical: Workflow): EvaluationError \/ String = {
    type EitherState[A] = EitherT[SequenceNameGenerator.SequenceState, EvaluationError, A]
    type WriterEitherState[A] = WriterT[EitherState, Vector[String], A]
    
    val executor0: Executor[WriterEitherState] = new JSExecutor(SequenceNameGenerator.Gen)
    val impl = new MongoDbEvaluatorImpl[WriterEitherState] {
      val executor = executor0
    }
    impl.execute(physical, Path("result")).run.run.eval(SequenceNameGenerator.startSimple).map {
      case (log, path) => (log :+ ("db." + path.filename + ".find()")).mkString("\n")
    }
  }
}

trait MongoDbEvaluatorImpl[F[_]] {
  protected def executor: Executor[F]

  sealed trait Col {
    def collection: Collection
  }
  object Col {
    case class Tmp(collection: Collection) extends Col
    case class User(collection: Collection) extends Col
  }

  def execute(physical: Workflow, out: Path)(implicit MF: Monad[F]): F[Path] = {
    def execute0(requestedCol: Col, task0: WorkflowTask): F[Col] = {
      import WorkflowTask._

      task0 match {
        case PureTask(value: Bson.Doc) =>
          for {
            _ <- executor.insert(requestedCol.collection, value)
          } yield requestedCol

        case PureTask(Bson.Arr(value)) =>
          for {
            dst <- value.toList.foldLeftM(requestedCol) { (col, doc) => 
              execute0(col, PureTask(doc)) 
            }
          } yield dst

        case PureTask(v) =>
          executor.fail(EvaluationError(new RuntimeException("MongoDB cannot store anything except documents inside collections: " + v)))
      
        case ReadTask(value) =>
          (Col.User(value): Col).point[F]
        
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

        case PipelineTask(source, pipeline) => for {
          tmp <- executor.generateTempName
          src <- execute0(Col.Tmp(tmp), source)
          _   <- executor.aggregate(src.collection, Pipeline(pipeline.ops :+ PipelineOp.Out(requestedCol.collection)))
        } yield requestedCol
        
        case MapReduceTask(source, mapReduce) => for {
          tmp <- executor.generateTempName
          src <- execute0(Col.Tmp(tmp), source)
          _   <- executor.mapReduce(src.collection, requestedCol.collection, mapReduce)
        } yield requestedCol

        case FoldLeftTask(steps) =>
          // FIXME: This is pretty fragile. A ReadTask will cause any later steps
          //        to merge into the read collection, a PipelineTask will
          //        overwrite any previous steps, etc. This is mostly useful if
          //        you have a series of MapReduceTasks, optionally preceded by a
          //        PipelineTask.
          steps.foldLeftM(requestedCol)(execute0)

        case JoinTask(steps) =>
          ???
      }
    }

    for {
      dst <- execute0(Col.Tmp(Collection(out.filename)), physical.task)
    } yield Path.fileAbs(dst.collection.name)
  }
}

trait NameGenerator[F[_]] {
  def generateTempName: F[Collection]
}

object SequenceNameGenerator {
  case class EvalState(tmp: String, counter: Int) {
    def inc: EvalState = copy(counter = counter + 1)
  }

  type SequenceState[A] = State[EvalState, A]

  def startUnique: EvalState = EvalState("tmp" + scala.util.Random.nextInt() + "_", 0)
  def startSimple: EvalState = EvalState("tmp_", 0)
  
  case object Gen extends NameGenerator[SequenceState] {
    def generateTempName: SequenceState[Collection] = for {
      st <- get
      _  <- put(st.inc)
    } yield Collection(st.tmp + st.counter.toString)
  }
}

class MongoDbExecutor[S](db: DB, nameGen: NameGenerator[({type λ[α] = State[S, α]})#λ])
    extends Executor[({type λ[α] = StateT[Task, S, α]})#λ] 
{
  type M[A] = StateT[Task, S, A]

  def generateTempName: M[Collection] = 
    StateT(s => Task.delay(nameGen.generateTempName(s)))

  def insert(dst: Collection, value: Bson.Doc): M[Unit] =
    liftMongoException(mongoCol(dst).insert(value.repr))

  def aggregate(source: Collection, pipeline: Pipeline): M[Unit] =
    liftMongoException(mongoCol(source).aggregate(pipeline.repr))

  def mapReduce(source: Collection, dst: Collection, mr: MapReduce): M[Unit] = {
    val mongoSrc = mongoCol(source)
    liftMongoException(mongoSrc.mapReduce(new MapReduceCommand(
          mongoSrc,
          mr.map.render(0),
          mr.reduce.render(0),
          dst.name,
          mr.out.map(_.outputTypeEnum).getOrElse(MapReduceCommand.OutputType.REPLACE),
          (new QueryBuilder).get)))
  }

  def fail[A](e: EvaluationError): M[A] = 
    StateT(s => (Task.fail(e): Task[(S, A)]))

  private def mongoCol(col: Collection) = db.getCollection(col.name)

  private def liftMongoException(a: => Unit): M[Unit] =
    StateT(s => fromTryCatchNonFatal(a).fold(e => Task.fail(EvaluationError(e)),
                                             _ => Task.delay((s, Unit))))
}

// Convenient partially-applied type: LoggerT[X]#Rec
private[mongodb] trait LoggerT[F[_]] {
  type EitherF[X] = EitherT[F, EvaluationError, X]
  type Rec[A] = WriterT[EitherF, Vector[String], A]
}

class JSExecutor[F[_]](nameGen: NameGenerator[F])(implicit mf: Monad[F]) extends Executor[LoggerT[F]#Rec] {
  def generateTempName() =
    ret(nameGen.generateTempName)

  def insert(dst: Collection, value: Bson.Doc) =
    write("db." + dst.name + ".insert(" + value.repr + ")")
  
  def aggregate(source: Collection, pipeline: Pipeline) =
    write("db." + source.name + ".aggregate([\n  " + pipeline.ops.map(_.bson.repr).mkString(",\n  ") + "\n])")

  def mapReduce(source: Collection, dst: Collection, mr: MapReduce) = {
    write("db." + source.name + ".mapReduce(\n" + 
               "  " + mr.map.render(0) + ",\n" +
               "  " + mr.reduce.render(0) + ",\n" + 
               "  " + Bson.Doc(Map(
                 (
                   mr.out.map(o => 
                     "out" -> Bson.Doc(Map(
                               o.outputType -> Bson.Text(dst.name)
                             ))) ::
                   Nil
                 ).flatten: _*)).repr + "\n" +
               ")")
  }

  def fail[A](e: EvaluationError) =
    WriterT[LoggerT[F]#EitherF, Vector[String], A](
      EitherT.left(e.point[F]))

  private def write(s: String): LoggerT[F]#Rec[Unit] = succeed(Some(s), ().point[F])

  private def ret[A](a: F[A]): LoggerT[F]#Rec[A] = succeed(None, a)

  private def succeed[A](msg: Option[String], a: F[A]): LoggerT[F]#Rec[A] = {
    val log = msg.map(Vector.empty :+ _).getOrElse(Vector.empty)
    WriterT[LoggerT[F]#EitherF, Vector[String], A](
      EitherT.right(a.map(a => (log -> a))))
  }
}
