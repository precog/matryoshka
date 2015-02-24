package slamdata.engine.physical.mongodb

import slamdata.engine._
import slamdata.engine.fp._
import slamdata.engine.fs._
import slamdata.engine.std.StdLib._
import Workflow._
import slamdata.engine.javascript._

import com.mongodb._

import collection.immutable.ListMap
import collection.JavaConverters._

import scalaz.{Free => FreeM, Node => _, _}
import Scalaz._
import scalaz.concurrent.Task

trait Executor[F[_]] {
  def generateTempName: F[Collection]

  def version: F[List[Int]]

  def eval(func: Js.Expr, args: List[Bson], nolock: Boolean): F[Unit]
  def insert(dst: Collection, value: Bson.Doc): F[Unit]
  def aggregate(source: Collection, pipeline: WorkflowTask.Pipeline): F[Unit]
  def mapReduce(source: Collection, dst: Collection, mr: MapReduce): F[Unit]
  def drop(coll: Collection): F[Unit]
  def rename(src: Collection, dst: Collection): F[Unit]

  def fail[A](e: EvaluationError): F[A]
}

case class UnsupportedMongoVersion(version: List[Int]) extends slamdata.engine.Error {
  def message = "Unsupported MongoDB version: " + version.mkString(".")
}

class MongoDbEvaluator(impl: MongoDbEvaluatorImpl[({type λ[α] = StateT[Task, SequenceNameGenerator.EvalState,α]})#λ]) extends Evaluator[Workflow] {
  def execute(physical: Workflow): Task[ResultPath] = for {
    nameSt <- SequenceNameGenerator.startUnique
    rez    <- impl.execute(physical).eval(nameSt)
  } yield rez

  def compile(workflow: Workflow) =
    "Mongo" -> MongoDbEvaluator.toJS(workflow).fold(e => "error: " + e.getMessage, s => Cord(s))

  private val MinVersion = List(2, 6, 0)

  def checkCompatibility: Task[Error \/ Unit] = for {
    nameSt  <- SequenceNameGenerator.startUnique
    version <- impl.executor.version.eval(nameSt)
  } yield if (version >= MinVersion) \/-(()) else -\/(UnsupportedMongoVersion(version))
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
    type WriterEitherState[A] = WriterT[EitherState, Vector[Js.Stmt], A]

    val executor0: Executor[WriterEitherState] = new JSExecutor(SequenceNameGenerator.Gen)
    val impl = new MongoDbEvaluatorImpl[WriterEitherState] {
      val executor = executor0
    }
    impl.execute(physical).run.run.eval(SequenceNameGenerator.startSimple).flatMap {
      case (log, path) => for {
        col <- Collection.fromPath(path.path).leftMap(e => EvaluationError(e))
      } yield Js.Stmts((log :+ Js.Call(Js.Select(JSExecutor.toJsRef(col), "find"), Nil)).toList).render(0)
    }
  }
}

trait MongoDbEvaluatorImpl[F[_]] {
  protected[mongodb] def executor: Executor[F]

  sealed trait Col {
    def collection: Collection
  }
  object Col {
    case class Tmp(collection: Collection) extends Col
    case class User(collection: Collection) extends Col
  }

  def execute(physical: Workflow)(implicit MF: Monad[F]): F[ResultPath] = {
    type W[A] = WriterT[F, Vector[Col.Tmp], A]

    def execute0(task0: WorkflowTask): W[Col] = {
      import WorkflowTask._

      def emit[A](a: F[A]): W[A] = WriterT(a.map(Vector.empty -> _))

      def fail[A](message: String): F[A] = executor.fail(EvaluationError(new RuntimeException(message)))

      def tempCol: W[Col.Tmp] = {
        val tmp = executor.generateTempName.map(Col.Tmp(_))
        WriterT(tmp.map(col => Vector(col) -> col))
      }

      task0 match {
        case PureTask(value @ Bson.Doc(_)) =>
          for {
            tmp <- tempCol
            _   <- emit(executor.insert(tmp.collection, value))
          } yield tmp

        case PureTask(Bson.Arr(value)) =>
          for {
            tmp <- tempCol
            _   <- emit(value.toList.traverse[F, Unit] {
                          case value @ Bson.Doc(_) => executor.insert(tmp.collection, value)
                          case v => fail("MongoDB cannot store anything except documents inside collections: " + v)
                        })
          } yield tmp

        case PureTask(v) =>
          emit(fail("MongoDB cannot store anything except documents inside collections: " + v))

        case ReadTask(value) =>
          emit((Col.User(value): Col).point[F])

        case QueryTask(source, query, skip, limit) =>
          // TODO: This is an approximation since we're ignoring all fields of "Query" except the selector.
          execute0(
            PipelineTask(
              source,
              $Match((), query.query) ::
                skip.map($Skip((), _) :: Nil).getOrElse(Nil) :::
                limit.map($Limit((), _) :: Nil).getOrElse(Nil)))

        case PipelineTask(source, pipeline) => for {
          src <- execute0(source)
          tmp <- tempCol
          _   <- emit(executor.aggregate(src.collection, pipeline :+ $Out[Unit]((), tmp.collection)))
        } yield tmp

        case MapReduceTask(source, mapReduce) => for {
          src <- execute0(source)
          tmp <- tempCol
          _   <- emit(executor.mapReduce(src.collection, tmp.collection, mapReduce))
        } yield tmp

        case FoldLeftTask(head, tail) =>
          for {
            head <- execute0(head)
            _    <- emit(head match { case Col.User(_) => fail("FoldLeft from simple read: " + head); case _ => ().point[F] })
            _    <- tail.map { case MapReduceTask(source, mapReduce) => for {
                                  src <- execute0(source)
                                  _   <- emit(executor.mapReduce(src.collection, head.collection, mapReduce))
                                } yield ()
                              }.sequenceU
          } yield head

        case JoinTask(steps) =>
          ???
      }
    }

    for {
      dst <- execute0(Workflow.task(physical)).run
      (temps, dstCol) = dst
      _   <- temps.collect {
                case tmp @ Col.Tmp(coll) => if (tmp != dstCol) executor.drop(coll)
                                            else ().point[F]
      }.sequenceU
    } yield dstCol match { case Col.User(coll) => ResultPath.User(coll.asPath); case Col.Tmp(coll) => ResultPath.Temp(coll.asPath) }
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

  val startUnique: Task[EvalState] = Task.delay(EvalState("tmp.gen_" + scala.util.Random.nextInt().toHexString + "_", 0))
  val startSimple: EvalState = EvalState("tmp.gen_", 0)

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

  def eval(func: Js.Expr, args: List[Bson], nolock: Boolean):
      M[Unit] =
    // TODO: Use db.runCommand({ eval : …}) so we can use nolock
    liftMongoException(
      db.eval(func.render(0), args.map(_.repr): _*))

  def insert(dst: Collection, value: Bson.Doc): M[Unit] =
    liftMongoException(mongoCol(dst).insert(value.repr))

  def aggregate(source: Collection, pipeline: WorkflowTask.Pipeline): M[Unit] = {
		runMongoCommand(Bson.Doc(ListMap(
      "aggregate" -> Bson.Text(source.name),
      "pipeline" -> Bson.Arr(pipeline.map(_.bson)),
      "allowDiskUse" -> Bson.Bool(true))))
	}

  def mapReduce(source: Collection, dst: Collection, mr: MapReduce): M[Unit] = {
    liftMongoException(mr.out match {
      // NB: The Java driver supports neither `sharded` nor `nonAtomic` as
      //     output options, so if these are set, we need to pass a “raw”
      //     command. (https://jira.mongodb.org/browse/JAVA-1350)
      case Some(out @ MapReduce.WithAction(_, db0, sharded, nonAtomic))
          if sharded.isDefined || nonAtomic.isDefined =>
        val obj = new BasicDBObject()
        obj.put("mapReduce", source.name)
        obj.put("map", new org.bson.types.Code(mr.map.render(0)))
        obj.put("reduce", new org.bson.types.Code(mr.reduce.render(0)))
        if (!mr.scope.isEmpty)
          obj.put("scope", mr.scope.mapValues[java.lang.Object](_.repr).asJava)
        val outObj = new BasicDBObject()
        outObj.put(out.outputType, dst.name)
        db0.map(outObj.put("db", _))
        sharded.map(outObj.put("sharded", _))
        nonAtomic.map(outObj.put("nonAtomic", _))
        obj.put("out", outObj)
        db.command(obj)

      case _ =>
        val mongoSrc = mongoCol(source)
        val command = new MapReduceCommand(
          mongoSrc,
          mr.map.render(0),
          mr.reduce.render(0),
          dst.name,
          mr.out.map(_.outputTypeEnum).getOrElse(MapReduceCommand.OutputType.REPLACE),
          mr.selection match {
            case None => (new QueryBuilder).get
            case Some(sel) => sel.bson.repr
          })
        mr.limit.map(x => command.setLimit(x.toInt))
        mr.finalizer.map(x => command.setFinalize(x.render(0)))
        if (!mr.scope.isEmpty)
          command.setScope(mr.scope.mapValues[java.lang.Object](_.repr).asJava)
        mr.verbose.map(x => command.setVerbose(Boolean.box(x)))
        mongoSrc.mapReduce(command)
    })
  }

  def drop(coll: Collection) =  {
    val mongoSrc = mongoCol(coll)
    liftMongoException(mongoSrc.drop())
  }
  def rename(src: Collection, dst: Collection) = {
    val mongoSrc = mongoCol(src)
    liftMongoException(mongoSrc.rename(dst.name, true))
  }

  def fail[A](e: EvaluationError): M[A] =
    StateT(s => (Task.fail(e): Task[(S, A)]))

  def version = StateT(s => Task.delay {
    val raw = db.command("buildinfo").getString("version")
    s -> Option(raw).fold(List[Int]())(str => str.split('.').toList.map(_.toInt))
  })

  private def mongoCol(col: Collection) = db.getCollection(col.name)

  private def liftMongoException(a: => Unit): M[Unit] =
    StateT(s => \/.fromTryCatchNonFatal(a).fold(
      e => Task.fail(EvaluationError(e)),
      κ(Task.delay((s, Unit)))))

  private def runMongoCommand(cmd: Bson.Doc): M[Unit] =
    StateT(s => {
      \/.fromTryCatchNonFatal(db.command(cmd.repr)).fold(
        e => Task.fail(EvaluationError(e)),
        rez => {
          val exc = rez.getException
          if (exc != null) Task.fail(EvaluationError(exc))
          else Task.now((s, Unit))
        }
      )
    })
}

// Convenient partially-applied type: LoggerT[X]#Rec
private[mongodb] trait LoggerT[F[_]] {
  type EitherF[X] = EitherT[F, EvaluationError, X]
  type Rec[A] = WriterT[EitherF, Vector[Js.Stmt], A]
}

class JSExecutor[F[_]](nameGen: NameGenerator[F])(implicit mf: Monad[F]) extends Executor[LoggerT[F]#Rec] {
  import Js._
  import JSExecutor._

  def generateTempName() = ret(nameGen.generateTempName)

  def eval(func: Js.Expr, args: List[Bson], nolock: Boolean) =
    write(Call(Select(Ident("db"), "eval"), func :: args.map(_.toJs)))

  def insert(dst: Collection, value: Bson.Doc) =
    write(Call(Select(toJsRef(dst), "insert"), List(value.toJs)))

  def aggregate(source: Collection, pipeline: WorkflowTask.Pipeline) =
    write(Call(Select(toJsRef(source), "aggregate"), List(
      AnonElem(pipeline.map(_.bson.toJs)),
      AnonObjDecl(List("allowDiskUse" -> Bool(true))))))

  def mapReduce(source: Collection, dst: Collection, mr: MapReduce) =
    write(Call(Select(toJsRef(source), "mapReduce"),
      List(mr.map, mr.reduce, mr.bson(dst).toJs)))

  def drop(coll: Collection) =
    write(Call(Select(toJsRef(coll), "drop"), Nil))

  def rename(src: Collection, dst: Collection) =
    write(Call(Select(toJsRef(src), "renameCollection"),
      List(Str(dst.name), Bool(true))))

  def fail[A](e: EvaluationError) =
    WriterT[LoggerT[F]#EitherF, Vector[Js.Stmt], A](
      EitherT.left(e.point[F]))

  def version = succeed(None, List[Int]().point[F])

  private def write(s: Js.Stmt): LoggerT[F]#Rec[Unit] = succeed(Some(s), ().point[F])

  private def ret[A](a: F[A]): LoggerT[F]#Rec[A] = succeed(None, a)

  private def succeed[A](msg: Option[Js.Stmt], a: F[A]): LoggerT[F]#Rec[A] = {
    val log = msg.map(Vector.empty :+ _).getOrElse(Vector.empty)
    WriterT[LoggerT[F]#EitherF, Vector[Js.Stmt], A](
      EitherT.right(a.map(a => (log -> a))))
  }
}
object JSExecutor {
  // Note: this pattern differs slightly from the similar pattern in Js, which allows leading '_'s.
  val SimpleCollectionNamePattern = "[a-zA-Z][_a-zA-Z0-9]*(?:\\.[a-zA-Z][_a-zA-Z0-9]*)*".r

  def toJsRef(col: Collection) = col.name match {
    case SimpleCollectionNamePattern() => Js.Select(Js.Ident("db"), col.name)
    case _                             =>
      Js.Call(Js.Select(Js.Ident("db"), "getCollection"), List(Js.Str(col.name)))
  }
}

