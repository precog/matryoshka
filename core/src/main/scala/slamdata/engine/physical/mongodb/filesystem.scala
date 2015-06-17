package slamdata.engine.physical.mongodb

import slamdata.engine._; import Backend._; import Errors._
import slamdata.engine.fs._
import slamdata.engine.fp._

import scalaz._
import Scalaz._
import scalaz.stream._
import scalaz.stream.io._
import scalaz.concurrent._

trait MongoDbFileSystem {
  protected def db: MongoWrapper

  val ChunkSize = 1000

  def scan0(path: Path, offset: Long, limit: Option[Long]):
      Process[PathTask, Data] =
    Collection.fromPath(path).fold(
      e => Process.eval[PathTask, Data](EitherT.left(Task.now(e))),
      col => {
        val skipper = (it: com.mongodb.client.FindIterable[org.bson.Document]) => it.skip(offset.toInt)
        val limiter = (it: com.mongodb.client.FindIterable[org.bson.Document]) => limit.map(v => it.limit(-v.toInt)).getOrElse(it)

        val skipperAndLimiter = skipper andThen limiter

        resource(db.get(col).map(c => skipperAndLimiter(c.find()).iterator))(
          cursor => Task.delay(cursor.close()))(
          cursor => Task.delay {
            if (cursor.hasNext) {
              val obj = cursor.next
              ignore(obj.remove("_id"))
              BsonCodec.toData(Bson.fromRepr(obj))
            }
            else throw Cause.End.asThrowable
          }).translate(liftP)
      })

  def count(path: Path): PathTask[Long] =
    Collection.fromPath(path).fold(
      e => EitherT(Task.now(\/.left(e))),
      x => liftP(db.get(x).map(_.count)))

  def save(path: Path, values: Process[Task, Data]): ETask[EvaluationError, Unit] =
    Collection.fromPath(path).fold(
      e => EitherT.left(Task.now(EvalPathError(e): EvaluationError)),
      col => for {
        tmp <- liftP(db.genTempName(col)).leftMap[EvaluationError](EvalPathError)
        _   <- append(tmp.asPath, values).runLog.leftMap[EvaluationError](EvalPathError).flatMap(_.headOption.fold[ETask[EvaluationError, Unit]](
          ().point[ETask[EvaluationError, ?]])(
          e => delete(tmp.asPath).leftMap[EvaluationError](EvalPathError).ignoreAndThen(EitherT.left(Task.now(e)))))
        _   <- delete(path).leftMap[EvaluationError](EvalPathError)
        _   <- liftP(db.rename(tmp, col)).onFailure(delete(tmp.asPath)).leftMap[EvaluationError](EvalPathError)
      } yield ())

  def append(path: Path, values: Process[Task, Data]):
      Process[PathTask, WriteError] =
    Collection.fromPath(path).fold(
      e => Process.eval[PathTask, WriteError](EitherT.left(Task.now(e))),
      col => {
        import process1._

        val chunks: Process[Task, Vector[(Data, String \/ org.bson.Document)]] = {
          def unwrap(obj: Bson) = obj match {
            case doc @ Bson.Doc(_) => \/-(doc.repr)
            case value => -\/("Cannot store value in MongoDB: " + value)
          }
          values.map(json => json -> BsonCodec.fromData(json).fold(err => -\/(err.toString), unwrap)) pipe chunk(ChunkSize)
        }

        chunks.flatMap { vs =>
          Process.eval(Task.delay {
                val parseErrors = vs.collect { case (json, -\/ (err)) => WriteError(json, Some(err)) }
                val objs        = vs.collect { case (json,  \/-(obj)) => json -> obj }

                val insertErrors = db.insert(col, objs.map(_._2)).attemptRun.fold(
                  e => objs.map { case (json, _) => WriteError(json, Some(e.getMessage)) },
                  _ => Nil)

                parseErrors ++ insertErrors
              }).flatMap(errs => Process.emitAll(errs))
        }
      }.translate(liftP))

  def move(src: Path, dst: Path): PathTask[Unit] = {
    def target(col: Collection): Option[Collection] =
      (for {
        rel <- col.asPath rebase src
        p = dst.asDir ++ rel
        c   <- Collection.fromPath(p)
      } yield c).toOption

    Collection.fromPath(dst).fold(
      e => EitherT.left(Task.now(e)),
      dstCol =>
        if (src.pureDir)
          liftP(for {
            cols    <- db.list
            renames <- cols.map { s => target(s).map(db.rename(s, _)) }.flatten.sequenceU
          } yield ())
        else
          Collection.fromPath(src).fold(
            e => EitherT.left(Task.now(e)),
            srcCol => liftP(db.rename(srcCol, dstCol))))
    }

  def delete(path: Path): PathTask[Unit] = liftP(for {
    all     <- db.list
    cols = all.filter(col => { val p = col.asPath; path == p || (path contains p) } )
    deletes <- cols.map(db.drop).sequenceU
  } yield ())

  // Note: a mongo db can contain a collection named "foo" as well as "foo.bar" and "foo.baz",
  // in which case "foo" acts as both a directory and a file, as far as slamengine is concerned.
  def ls(dir: Path): PathTask[Set[FilesystemNode]] = liftP(for {
    cols <- db.list
    allPaths = cols.map(_.asPath)
  } yield allPaths.map(_.rebase(dir.asDir).toOption.map(p => FilesystemNode(p.head, Plain))).flatten.toSet)

  def defaultPath = db.defaultDB.map(Path(_).asDir).getOrElse(Path.Current)
}

sealed trait MongoWrapper {
  import com.mongodb._
  import com.mongodb.client._
  import com.mongodb.client.model._
  import org.bson._
  import scala.collection.JavaConverters._

  protected def client: MongoClient
  def defaultDB: Option[String]

  def genTempName(col: Collection): Task[Collection] = for {
    start <- SequenceNameGenerator.startUnique
  } yield SequenceNameGenerator.Gen.generateTempName(col.databaseName).eval(start)

  private val db = Memo.mutableHashMapMemo[String, MongoDatabase] { (name: String) =>
    client.getDatabase(name)
  }

  // Note: this exposes the Java obj, so should be made private at some point
  def get(col: Collection): Task[MongoCollection[Document]] =
    Task.delay(db(col.databaseName).getCollection(col.collectionName))

  def rename(src: Collection, dst: Collection): Task[Unit] =
    if (src.equals(dst)) Task.now(())
    else
      for {
        s <- get(src)
        _ <- Task.delay(s.renameCollection(new MongoNamespace(dst.databaseName, dst.collectionName)))
      } yield ()

  def drop(col: Collection): Task[Unit] = for {
    c <- get(col)
    _ = c.drop
  } yield ()

  def insert(col: Collection, data: Vector[Document]): Task[Unit] = for {
    c <- get(col)
    _ = c.bulkWrite(data.map(new InsertOneModel(_)).asJava)
  } yield ()

  val list: Task[List[Collection]] = Task.delay(for {
    dbName  <- try {
      client.listDatabaseNames.asScala.toList
    } catch {
      case _: MongoCommandException => defaultDB.toList
    }
    colName <- db(dbName).listCollectionNames.asScala.toList
  } yield Collection(dbName, colName))
}
object MongoWrapper {
  def apply(client0: com.mongodb.MongoClient, defaultDb0: Option[String]) = new MongoWrapper {
    def client = client0
    def defaultDB = defaultDb0
  }
}
