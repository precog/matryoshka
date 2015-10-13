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
import quasar.recursionschemes._, Recursive.ops._
import quasar._, Errors._, Evaluator._
import quasar.javascript._
import Workflow._

import com.mongodb._
import com.mongodb.client.model._
import scalaz.{Free => FreeM, _}, Scalaz._
import scalaz.concurrent._

trait Executor[F[_]] {
  def generateTempName(dbName: String): F[Collection]

  def version: F[List[Int]]

  /** Used as a default for creating temp collections when a query does not
    * involve a database.
    */
  def defaultWritableDB: Option[String]

  def insert(dst: Collection, value: Bson.Doc): F[Unit]
  def aggregate(source: Collection, pipeline: WorkflowTask.Pipeline): F[Unit]
  def mapReduce(source: Collection, dst: Collection, mr: MapReduce): F[Unit]
  def drop(coll: Collection): F[Unit]
  def rename(src: Collection, dst: Collection): F[Unit]
  def exists(coll: Collection): F[Unit]

  def fail[A](e: EvaluationError): F[A]
}

class MongoDbEvaluator(impl: MongoDbEvaluatorImpl[StateT[ETask[EvaluationError, ?], SequenceNameGenerator.EvalState, ?]]) extends Evaluator[Crystallized] {

  implicit val MF = StateT.stateTMonadState[SequenceNameGenerator.EvalState, ETask[EvaluationError, ?]]

  val name = "MongoDB"

  def execute(physical: Crystallized): ETask[EvaluationError, ResultPath] = for {
    nameSt <- EitherT.right(SequenceNameGenerator.startUnique)
    rez    <- impl.execute(physical).eval(nameSt)
  } yield rez

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
    } yield new MongoDbEvaluator(MongoDbEvaluatorImpl[ST](exec))
  }

  def toJS(physical: Crystallized): EvaluationError \/ String = {
    type EitherState[A] = EitherT[SequenceNameGenerator.SequenceState, EvaluationError, A]
    type WriterEitherState[A] = WriterT[EitherState, Vector[Js.Stmt], A]

    val executor0: Executor[WriterEitherState] = new JSExecutor(SequenceNameGenerator.Gen)
    val impl = new MongoDbEvaluatorImpl[WriterEitherState] {
      val executor = executor0
    }
    impl.execute(physical).run.run.eval(SequenceNameGenerator.startSimple).flatMap {
      case (log, path) => for {
        col <- Collection.fromPath(path.path).leftMap(EvalPathError(_))
      } yield Js.Stmts((log :+ Js.Call(Js.Select(JSExecutor.toJsRef(col), "find"), Nil)).toList).pprint(0)
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

trait MongoDbEvaluatorImpl[F[_]] {

  protected[mongodb] def executor: Executor[F]

  sealed trait Col {
    def collection: Collection
  }
  object Col {
    case class Tmp(collection: Collection) extends Col
    case class User(collection: Collection) extends Col
  }

  def execute(physical: Crystallized)(implicit MF: Monad[F]): F[ResultPath] = {
    type WT[M[_], A] = WriterT[M, Vector[Col.Tmp], A]
    type W[       A] = WT[F, A]

    def execute0(task0: WorkflowTask): W[Col] = {
      import WorkflowTask._

      def emit[A](a: F[A]): W[A] = WriterT(a.map(Vector.empty -> _))

      def fail[A](error: EvaluationError): F[A] = executor.fail(error)

      def tempCol: W[Col.Tmp] = {
        val dbName = physical.op.foldMap {
          case Fix($Read(Collection(dbName, _))) => List(dbName)
          case _ => Nil
        }.headOption.orElse(executor.defaultWritableDB)

        dbName.fold[W[Col.Tmp]](emit(fail(NoDatabase()))) { dbName =>
          val tmp = executor.generateTempName(dbName).map(Col.Tmp(_))
          WriterT(tmp.map(col => (Vector(col), col)))
        }
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
                          case v => fail(UnableToStore("MongoDB cannot store anything except documents inside collections: " + v))
                        })
          } yield tmp

        case PureTask(v) =>
          emit(fail(UnableToStore("MongoDB cannot store anything except documents inside collections: " + v)))

        case ReadTask(value) =>
          emit(executor.exists(value).map(_ => Col.User(value): Col))

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
            _    <- emit(head match {
              case Col.User(_) => fail(InvalidTask("FoldLeft from simple read: " + head))
              case _           => ().point[F]
            })
            _    <- tail.map { case MapReduceTask(source, mapReduce) => for {
                                  src <- execute0(source)
                                  _   <- emit(executor.mapReduce(src.collection, head.collection, mapReduce))
                                } yield ()
                              }.sequenceU
          } yield head
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

object MongoDbEvaluatorImpl {
  def apply[F[_]](exec: Executor[F]): MongoDbEvaluatorImpl[F] =
    new MongoDbEvaluatorImpl[F] { val executor = exec }
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
    extends Executor[StateT[EvaluationTask, S, ?]] {

  type MT[F[_], A] = StateT[F, S, A]
  type M[A]        = MT[EvaluationTask, A]

  def generateTempName(dbName: String): M[Collection] =
    nameGen.generateTempName(dbName).mapK(_.point[EvaluationTask])

  def insert(dst: Collection, value: Bson.Doc): M[Unit] =
    liftMongo(mongoCol(dst).insertOne(value.repr))

  def aggregate(source: Collection, pipeline: WorkflowTask.Pipeline): M[Unit] =
    runMongoCommand(
      source.databaseName,
      Bson.Doc(ListMap(
        "aggregate" -> Bson.Text(source.collectionName),
        "pipeline" -> Bson.Arr(pipeline.map(_.bson)),
        "allowDiskUse" -> Bson.Bool(true))))

  def mapReduce(source: Collection, dst: Collection, mr: MapReduce): M[Unit] =
    // FIXME: check same db
    runMongoCommand(
      source.databaseName,
      Bson.Doc(
        ListMap(
          "mapReduce" -> Bson.Text(source.collectionName),
          "map"       -> Bson.JavaScript(mr.map),
          "reduce"    -> Bson.JavaScript(mr.reduce))
        ++ mr.bson(dst).value))

  def drop(coll: Collection) = liftMongo(mongoCol(coll).drop())

  def rename(src: Collection, dst: Collection) =
    liftMongo(
      mongoCol(src).renameCollection(
        new MongoNamespace(dst.databaseName, dst.collectionName),
        new RenameCollectionOptions().dropTarget(true)))

  def fail[A](e: EvaluationError): M[A] =
    MonadError[ETask, EvaluationError].raiseError[A](e).liftM[MT]

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
        .flatMap(_.traverseU(dbName => lookupVersion(dbName).swap).map(_.head).swap)

    MongoWrapper.liftTask(MongoWrapper.databaseNames(client))
      .map(_ ++ defaultWritableDB)
      .flatMap(attemptVersion)
      .liftM[MT]
  }

  override def exists(coll: Collection): M[Unit] = {
    import quasar.fs.Path.PathError.NonexistentPathError
    import quasar.fs.Path
    MongoWrapper.liftTask(MongoWrapper(client).exists(coll)).liftM[MT].flatMap {
      case true => MongoWrapper.liftTask(Task.now(())).liftM[MT]
      case false => fail(EvalPathError(NonexistentPathError(coll.asPath.asAbsolute, None)))
    }
  }

  private def mongoCol(col: Collection) =
    client.getDatabase(col.databaseName).getCollection(col.collectionName)

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

class JSExecutor[F[_]](nameGen: NameGenerator[F])(implicit mf: Monad[F])
    extends Executor[LoggerT[F]#Rec] {
  import Js._
  import JSExecutor._

  type M0[A] = EvalErrT[F, A]
  type M[A]  = WriterT[M0, Vector[Js.Stmt], A]

  def generateTempName(dbName: String) = ret(nameGen.generateTempName(dbName))

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
      List(Str(dst.collectionName), Bool(true))))

  def fail[A](e: EvaluationError) =
    WriterT[LoggerT[F]#EitherF, Vector[Js.Stmt], A](
      EitherT.left(e.point[F]))

  val version = succeed(None, List[Int]().point[F])

  val defaultWritableDB = Some("default")

  // Not sure what should be done here...
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
