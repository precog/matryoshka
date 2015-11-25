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

package quasar.physical.mongodb

import quasar.Predef._
import quasar.fp._
import quasar.recursionschemes._, Recursive.ops._
import quasar._, Errors._, Evaluator._
import quasar.fs.Path, Path.PathError.NonexistentPathError
import quasar.javascript._
import Workflow._

import com.mongodb._
import scalaz.{Free => FreeM, _}, Scalaz._
import scalaz.concurrent._
import scalaz.stream._

trait Executor[F[_], C] {
  import MapReduce._

  def generateTempName(dbName: String): F[Collection]

  def version: F[List[Int]]

  /** Used as a default for creating temp collections when a query does not
    * involve a database.
    */
  def defaultWritableDB: Option[String]

  def insert(dst: Collection, value: Bson.Doc): F[Unit]
  def aggregate(source: Collection, pipeline: workflowtask.Pipeline): F[Unit]
  def mapReduce(source: Collection, dst: OutputCollection, mr: MapReduce): F[Unit]
  def drop(coll: Collection): F[Unit]
  def exists(coll: Collection): F[Unit]

  def pureCursor(values: List[Bson]): F[C]
  def readCursor(source: Col): F[C]
  def aggregateCursor(source: Col, pipeline: workflowtask.Pipeline): F[C]
  def mapReduceCursor(source: Col, mr: MapReduce): F[C]

  def fail[A](e: EvaluationError): F[A]
}

sealed trait Col {
  def collection: Collection
}
object Col {
  final case class Tmp(collection: Collection) extends Col
  final case class User(collection: Collection) extends Col
}

class MongoDbEvaluator(impl: MongoDbEvaluatorImpl[
    StateT[EvaluationTask, SequenceNameGenerator.EvalState, ?],
    Process[EvaluationTask, Data]])
    extends Evaluator[Crystallized] {

  implicit val MF: MonadState[StateT[EvaluationTask, ?, ?], SequenceNameGenerator.EvalState] =
    StateT.stateTMonadState[SequenceNameGenerator.EvalState, EvaluationTask]

  val name = "MongoDB"

  def executeTo(physical: Crystallized, out: Path): EvaluationTask[ResultPath] =
    Collection.fromPath(out).fold(
      err => EitherT.left(Task.now(EvalPathError(err))),
      out => for {
        nameSt <- EitherT.right(SequenceNameGenerator.startUnique)
        rez    <- impl.execute(physical, out).eval(nameSt)
      } yield rez)

  def evaluate(physical: Crystallized): Process[EvaluationTask, Data] = {
    val v = (for {
      nameSt <- EitherT.right(SequenceNameGenerator.startUnique)
      rez    <- impl.evaluate(physical).eval(nameSt)
    } yield rez).valueOr(
      err => Process.eval[EvaluationTask, Data](EitherT.left[Task, EvaluationError, Data](Task.now(err))))
    Process.eval[EvaluationTask, Process[EvaluationTask, Data]](liftE(v)).join
  }

  def compile(workflow: Crystallized) =
    "Mongo" -> MongoDbEvaluator.toJS(workflow).fold(e => "error: " + e.message, s => Cord(s))
}

object MongoDbEvaluator {

  type EvalState = SequenceNameGenerator.EvalState
  type ST[A] = StateT[EvaluationTask, EvalState, A]

  private val MinVersion = List(2, 6, 0)

  def apply(client0: MongoClient): EnvTask[Evaluator[Crystallized]] = {
    val nameGen = SequenceNameGenerator.Gen
    val testDoc = Bson.Doc(ListMap("a" -> Bson.Int32(1))).repr

    /** Returns the name of the database if a write succeeds. */
    def canWriteToCol(col: Collection): OptionT[Task, String] = {
      val mongoCol = Task.delay(
        client0.getDatabase(col.databaseName)
          getCollection(col.collectionName))

      val inserted = for {
        c <- mongoCol
        _ <- Task.delay(c.insertOne(testDoc))
        _ <- Task.delay(c.drop())
      } yield col.databaseName

      EitherT(inserted.attempt).toOption
    }

    def findWritableDb(dbNames: Set[String]): State[EvalState, OptionT[Task, String]] =
      dbNames.toList.traverseS(nameGen.generateTempName).map(cols =>
        cols.traverseU(col => canWriteToCol(col).toLeft(())).swap.toOption)

    def validateVersion(exec: MongoDbExecutor[EvalState]): StateT[EnvTask, EvalState, Unit] =
      exec.version.mapK[EnvTask, Unit, EvalState] { r =>
        r.leftMap(MongoDbEvaluator.mapError).flatMapF { case (s, v) =>
          Task.now((v >= MinVersion) either ((s, ())) or UnsupportedVersion("MongoDB", v))
        }
      }

    for {
      dbNames     <- MongoWrapper.liftTask(MongoWrapper.databaseNames(client0))
                       .leftMap(MongoDbEvaluator.mapError)
      genSeed     <- SequenceNameGenerator.startUnique.liftM[EnvErrT]
      (nxtS, wdb) =  findWritableDb(dbNames).run(genSeed)
      exec        <- wdb.run.map(new MongoDbExecutor(client0, nameGen, _)).liftM[EnvErrT]
      _           <- validateVersion(exec).eval(nxtS)
    } yield new MongoDbEvaluator(MongoDbEvaluatorImpl[ST, Process[EvaluationTask, Data]](exec))
  }

  def toJS(physical: Crystallized): EvaluationError \/ String = {
    type EitherState[A] = EitherT[SequenceNameGenerator.SequenceState, EvaluationError, A]
    type WriterEitherState[A] = WriterT[EitherState, Vector[Js.Stmt], A]

    val executor0: Executor[WriterEitherState, Unit] = new JSExecutor(SequenceNameGenerator.Gen)
    val impl = new MongoDbEvaluatorImpl[WriterEitherState, Unit] {
      val F = implicitly[Monad[WriterEitherState]]
      val executor = executor0
    }
    // NB: this _always_ renders as if the final result is to be streamed.
    impl.evaluate(physical).run.run.eval(SequenceNameGenerator.startSimple).map {
      case (log, _) => Js.Stmts(log.toList).pprint(0)
    }
  }

  def mapError(err: EvaluationError): EnvironmentError = err match {
    case CommandFailed(msg) if (msg contains "Command failed with error 18: 'auth failed'") =>
      InvalidCredentials(msg)
    case CommandFailed(msg) if (msg contains "com.mongodb.MongoSocketException") =>
      ConnectionFailed(msg)
    case CommandFailed(msg) if (msg contains "com.mongodb.MongoSocketOpenException") =>
      ConnectionFailed(msg)
    case err =>
      EnvEvalError(err)
  }
}

trait MongoDbEvaluatorImpl[F[_], C] {
  import MapReduce._

  implicit def F: Monad[F]

  protected[mongodb] def executor: Executor[F, C]

  type WT[M[_], A] = WriterT[M, Vector[Col.Tmp], A]
  type W[       A] = WT[F, A]

  def emit[A](a: F[A]): W[A] = WriterT(a.map(Vector.empty -> _))

  def fail[A](error: EvaluationError): F[A] = executor.fail(error)

  def tempCol(physical: Crystallized): W[Col.Tmp] = {
    val dbName = physical.op.foldMap {
      case Fix($Read(Collection(dbName, _))) => List(dbName)
      case _ => Nil
    }.headOption.orElse(executor.defaultWritableDB)

    dbName.fold[W[Col.Tmp]](emit(fail(NoDatabase()))) { dbName =>
      val tmp = executor.generateTempName(dbName).map(Col.Tmp(_))
      WriterT(tmp.map(col => (Vector(col), col)))
    }
  }

  def execute0(task0: workflowtask.WorkflowTask, out: Col, tempCol: W[Col.Tmp]): W[Col] = {
    import quasar.physical.mongodb.workflowtask._

    val execSource: workflowtask.WorkflowTask => W[Col] = {
      case ReadTask(coll) => emit(executor.exists(coll).as(Col.User(coll)))
      case t              => tempCol.flatMap(tmp => execute0(t, tmp, tempCol))
    }

    task0 match {
      case PureTask(value @ Bson.Doc(_)) => for {
        _ <- emit(executor.insert(out.collection, value))
      } yield out

      case PureTask(Bson.Arr(value)) =>
        for {
          _   <- emit(value.toList.traverse[F, Unit] {
                        case value @ Bson.Doc(_) => executor.insert(out.collection, value)
                        case v => fail(UnableToStore("MongoDB cannot store anything except documents inside collections: " + v))
                      })
        } yield out

      case PureTask(v) =>
        emit(fail(UnableToStore("MongoDB cannot store anything except documents inside collections: " + v)))

      case ReadTask(value) =>
        emit(executor.exists(value).as(Col.User(value): Col))

      case QueryTask(source, query, skip, limit) =>
        // TODO: This is an approximation since we're ignoring all fields of "Query" except the selector.
        execute0(
          PipelineTask(
            source,
            $Match((), query.query) ::
              skip.map($Skip((), _) :: Nil).getOrElse(Nil) :::
              limit.map($Limit((), _) :: Nil).getOrElse(Nil)), out, tempCol)

      case PipelineTask(source, pipeline) => for {
        src <- execSource(source)
        _   <- emit(executor.aggregate(src.collection, pipeline :+ $Out[Unit]((), out.collection)))
      } yield out

      case MapReduceTask(source, mapReduce, oact) => for {
        src <- execSource(source)
        act  = oact getOrElse Action.Replace
        _   <- emit(executor.mapReduce(
                 src.collection,
                 outputCollection(out.collection, act),
                 mapReduce))
      } yield out

      case FoldLeftTask(head, tail) =>
        for {
          head <- execute0(head, out, tempCol)
          _    <- emit(head match {
            case Col.User(_) => fail(InvalidTask("FoldLeft from simple read: " + head))
            case _           => ().point[F]
          })
          _    <- tail.traverse[W, Unit] {
            case MapReduceTask(source, mapReduce, Some(act)) => for {
              src <- execSource(source)
              _   <- emit(executor.mapReduce(
                       src.collection,
                       outputCollection(out.collection, act),
                       mapReduce))
            } yield ()
            case t => emit(fail(InvalidTask("un-mergable FoldLeft input: " + t)))
          }
        } yield out
    }
  }

  def execute(physical: Crystallized, out: Collection): F[ResultPath] =
    for {
      dst <- execute0(Workflow.task(physical), Col.User(out), tempCol(physical)).run
      (temps, dstCol) = dst
      _   <- temps.collect {
                case tmp @ Col.Tmp(coll) => if (tmp != dstCol) executor.drop(coll)
                                            else ().point[F]
      }.sequenceU
    } yield dstCol match {
      case Col.User(coll) => ResultPath.User(coll.asPath)
      case Col.Tmp(coll) => ResultPath.Temp(coll.asPath)
    }

  def evaluate(physical: Crystallized): F[C] = {
    import quasar.physical.mongodb.workflowtask._

    val tempGen = tempCol(physical)

    def cleanup(temps: Vector[Col.Tmp], dst: Col): F[Unit] =
      temps.traverse(c =>
        if (c == dst) ().point[F]
        else executor.drop(c.collection)).map(κ(()))

    Workflow.task(physical) match {
      case PureTask(Bson.Arr(bson)) =>
        executor.pureCursor(bson)
      case PureTask(bson) =>
        executor.pureCursor(bson :: Nil)

      case ReadTask(src) =>
        executor.exists(src) *> executor.readCursor(Col.User(src))

      case PipelineTask(ReadTask(src), pipeline) =>
        executor.exists(src) *> executor.aggregateCursor(Col.User(src), pipeline)

      case MapReduceTask(ReadTask(src), mapReduce, _) =>
        executor.exists(src) *> executor.mapReduceCursor(Col.User(src), mapReduce)

      case PipelineTask(src, pipeline) => for {
        t   <- tempGen.flatMap(tmp => execute0(src, tmp, tempGen)).run
        (temps, dstCol) = t
        _   <- cleanup(temps, dstCol)
        cur <- executor.aggregateCursor(dstCol, pipeline)
      } yield cur

      case MapReduceTask(src, mapReduce, _) => for {
        t <- tempGen.flatMap(tmp => execute0(src, tmp, tempGen)).run
        (temps, dstCol) = t
        _   <- cleanup(temps, dstCol)
        cur <- executor.mapReduceCursor(dstCol, mapReduce)
      } yield cur

      // NB: the last command in this case merges the output into a result
      // collection, so it simply can't be streamed
      case task @ FoldLeftTask(_, _) => for {
        t <- tempGen.flatMap(tmp => execute0(task, tmp, tempGen)).run
        (temps, dstCol) = t
        _   <- cleanup(temps, dstCol)
        cur <- executor.readCursor(dstCol)
      } yield cur
    }
  }

  ////

  private def outputCollection(c: Collection, a: Action) =
    OutputCollection(
      c.collectionName,
      Some(ActionedOutput(a, Some(c.databaseName), None)))
}

object MongoDbEvaluatorImpl {
  def apply[F[_], C](exec: Executor[F, C])(implicit F0: Monad[F]): MongoDbEvaluatorImpl[F, C] =
    new MongoDbEvaluatorImpl[F, C] {
      val F = F0
      val executor = exec
    }
}

trait NameGenerator[F[_]] {
  def generateTempName(dbName: String): F[Collection]
}

object SequenceNameGenerator {
  final case class EvalState(tmp: String, counter: Int) {
    def inc: EvalState = copy(counter = counter + 1)
  }

  type SequenceState[A] = State[EvalState, A]

  def startUnique: Task[EvalState] = Task.delay(EvalState("tmp.gen_" + scala.util.Random.nextInt().toHexString + "_", 0))
  def startSimple: EvalState = EvalState("tmp.gen_", 0)

  final case object Gen extends NameGenerator[SequenceState] {
    def generateTempName(dbName: String): SequenceState[Collection] = for {
      st <- get
      _  <- put(st.inc)
    } yield Collection(dbName, st.tmp + st.counter.toString)
  }
}

class MongoDbExecutor[S](client: MongoClient,
                         nameGen: NameGenerator[State[S, ?]],
                         val defaultWritableDB: Option[String])
    extends Executor[StateT[EvaluationTask, S, ?], Process[EvaluationTask, Data]] {

  import MapReduce._

  type MT[F[_], A] = StateT[F, S, A]
  type M[A]        = MT[EvaluationTask, A]

  def generateTempName(dbName: String): M[Collection] =
    nameGen.generateTempName(dbName).mapK(_.point[EvaluationTask])

  def insert(dst: Collection, value: Bson.Doc): M[Unit] =
    liftMongo(mongoCol(dst).insertOne(value.repr))

  def aggregate(source: Collection, pipeline: workflowtask.Pipeline): M[Unit] =
    runMongoCommand(
      source.databaseName,
      Bson.Doc(ListMap(
        "aggregate" -> Bson.Text(source.collectionName),
        "pipeline" -> Bson.Arr(pipeline.map(_.bson)),
        "allowDiskUse" -> Bson.Bool(true))))

  def mapReduce(source: Collection, dst: OutputCollection, mr: MapReduce): M[Unit] =
    // FIXME: check same db
    runMongoCommand(
      source.databaseName,
      Bson.Doc(ListMap(
        "mapReduce" -> Bson.Text(source.collectionName),
        "map"       -> Bson.JavaScript(mr.map),
        "reduce"    -> Bson.JavaScript(mr.reduce)
      ) ++ mr.toCollBson(dst).value)
    ) mapK (_ leftMap {
      case CommandFailed(s) if s.contains("ns doesn't exist") =>
        EvalPathError(NonexistentPathError(source.asPath.asAbsolute, None))
      case other => other
    }: EvaluationTask[(S, Unit)])

  def drop(coll: Collection) = liftMongo(mongoCol(coll).drop())

  def pureCursor(values: List[Bson]) =
    liftTask(Task.now(Process.emitAll(values.map(BsonCodec.toData))))

  def readCursor(source: Col) =
    fromCursor(Task.delay { mongoCol(source.collection).find() }, source)

  def aggregateCursor(source: Col, pipeline: workflowtask.Pipeline) = {
    import scala.collection.JavaConverters._
    import scala.Predef.boolean2Boolean

    fromCursor(Task.delay {
      mongoCol(source.collection)
        .aggregate(pipeline.map(_.bson.repr).asJava)
        .allowDiskUse(true)
    }, source)
  }

  def mapReduceCursor(source: Col, mr: MapReduce) = {
    implicit class CursorOp[A](a: A) {
      def app[B](b: Option[B])(f: (A, B) => A) = b.fold(a)(f(a, _))
    }

    fromCursor(Task.delay {
      mongoCol(source.collection)
        .mapReduce(mr.map.pprint(0), mr.reduce.pprint(0))
        .app(mr.selection)((c, q) => c.filter(q.bson.repr))
        .app(mr.limit)((c, l) => c.limit(l.toInt))
        .app(mr.finalizer)((c, f) => c.finalizeFunction(f.pprint(0)))
        .scope(Bson.Doc(mr.scope).repr)
        .app(mr.verbose)((c, v) => c.verbose(v))
    }, source)
  }

  private def fromCursor(cursor: Task[com.mongodb.client.MongoIterable[org.bson.Document]], source: Col): M[Process[EvaluationTask, Data]] = {
    val t = new (ETask[Backend.ResultError, ?] ~> EvaluationTask) {
      def apply[A](t: ETask[Backend.ResultError, A]) =
        t.leftMap(EvalResultError(_))
    }
    val cleanup: EvaluationTask[Unit] = source match {
      case Col.Tmp(coll) => MongoWrapper.liftTask(Task.delay { mongoCol(coll).drop() })
      case Col.User(_) => liftE(Task.now(()))
    }
    liftMongo(
      MongoWrapper(client).readCursor(cursor)
          .translate[EvaluationTask](t) ++
        Process.eval_(cleanup))
  }

  def fail[A](e: EvaluationError): M[A] =
    MonadError[ETask, EvaluationError].raiseError[A](e).liftM[MT]

  @SuppressWarnings(Array("org.brianmckenna.wartremover.warts.NoNeedForMonad"))
  def version: M[List[Int]] = {
    def lookupVersion(dbName: String): EvaluationTask[List[Int]] = {
      val cmd = Bson.Doc(ListMap("buildinfo" -> Bson.Int32(1))).repr
      MongoWrapper.delay(client.getDatabase(dbName).runCommand(cmd)) flatMapF { r =>
        Option(r getString "version")
          .toRightDisjunction(CommandFailed("buildInfo: response missing 'version' field"))
          .map(_.split('.').toList.map(_.toInt))
          .point[Task]
      }
    }

    def attemptVersion(dbNames: Set[String]): EvaluationTask[List[Int]] =
      EitherT(dbNames.toList.toNel.toRightDisjunction(NoDatabase()).point[Task])
        .flatMap(_.traverseU(lookupVersion(_).swap).map(_.head).swap)

    MongoWrapper.liftTask(MongoWrapper.databaseNames(client).map(_ ⊹ defaultWritableDB.toSet))
      .flatMap(attemptVersion)
      .liftM[MT]
  }

  override def exists(coll: Collection): M[Unit] = {
    liftTask(MongoWrapper(client).exists(coll)).flatMap {
      case true => liftTask(Task.now(()))
      case false => fail(EvalPathError(NonexistentPathError(coll.asPath.asAbsolute, None)))
    }
  }

  private def mongoCol(col: Collection) =
    client.getDatabase(col.databaseName).getCollection(col.collectionName)

  private def liftTask[A](a: Task[A]): M[A] =
    MongoWrapper.liftTask(a).liftM[MT]

  private def liftMongo[A](a: => A): M[A] =
    MongoWrapper.delay(a).liftM[MT]

  private def runMongoCommand(db: String, cmd: Bson.Doc): M[Unit] =
    liftMongo(client.getDatabase(db).runCommand(cmd.repr)).void
}

// Convenient partially-applied type: LoggerT[X]#Rec
private[mongodb] trait LoggerT[F[_]] {
  type EitherF[X] = EitherT[F, EvaluationError, X]
  type Rec[A] = WriterT[EitherF, Vector[Js.Stmt], A]
}

class JSExecutor[F[_]: Monad](nameGen: NameGenerator[F])
    extends Executor[LoggerT[F]#Rec, Unit] {
  import Js._
  import JSExecutor._
  import MapReduce._

  type M0[A] = EvalErrT[F, A]
  type M[A]  = WriterT[M0, Vector[Js.Stmt], A]

  def generateTempName(dbName: String) = ret(nameGen.generateTempName(dbName))

  def insert(dst: Collection, value: Bson.Doc) =
    write(Call(Select(toJsRef(dst), "insert"), List(value.toJs)))

  def aggregate(source: Collection, pipeline: workflowtask.Pipeline) =
    write(Call(Select(toJsRef(source), "aggregate"), List(
      AnonElem(pipeline.map(_.bson.toJs)),
      AnonObjDecl(List("allowDiskUse" -> Bool(true))))))

  def mapReduce(source: Collection, dst: OutputCollection, mr: MapReduce) =
    write(Call(Select(toJsRef(source), "mapReduce"),
      List(mr.map, mr.reduce, mr.toCollBson(dst).toJs)))

  def drop(coll: Collection) =
    write(Call(Select(toJsRef(coll), "drop"), Nil))

  def pureCursor(values: List[Bson]) =
    write(Bson.Arr(values).toJs)
  def readCursor(source: Col) =
    write(Call(Select(toJsRef(source.collection), "find"), Nil))
  def aggregateCursor(source: Col, pipeline: workflowtask.Pipeline) =
    aggregate(source.collection, pipeline)
  def mapReduceCursor(source: Col, mr: MapReduce) =
    write(Call(Select(toJsRef(source.collection), "mapReduce"),
      List(mr.map, mr.reduce, mr.inlineBson.toJs)))

  def fail[A](e: EvaluationError) =
    WriterT[LoggerT[F]#EitherF, Vector[Js.Stmt], A](
      EitherT.left(e.point[F]))

  val version = succeed(None, List[Int]().point[F])

  val defaultWritableDB = Some("default")

  // TODO (SD-1060): Consider modifying implementation to align with MongoDBExecutor
  def exists(coll: Collection): LoggerT[F]#Rec[Unit] = succeed(None, ().point[F])

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

  def toJsRef(col: Collection) = col.collectionName match {
    case SimpleCollectionNamePattern() => Js.Select(Js.Ident("db"), col.collectionName)
    case _                             =>
      Js.Call(Js.Select(Js.Ident("db"), "getCollection"), List(Js.Str(col.collectionName)))
  }
}
