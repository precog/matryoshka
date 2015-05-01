package slamdata.engine.physical.mongodb

import slamdata.engine._
import slamdata.engine.fs._
import slamdata.engine.fp._

import scala.collection.JavaConversions._

import scalaz._
import Scalaz._
import scalaz.stream._
import scalaz.stream.io._
import scalaz.concurrent._

sealed trait MongoDbFileSystem extends FileSystem {
  protected def db: MongoWrapper

  val ChunkSize = 1000

  def scan(path: Path, offset: Option[Long], limit: Option[Long]): Process[Task, Data] =
    Collection.fromPath(path).fold(
      e => Process.eval(Task.fail(e)),
      col => {
        val skipper = (it: com.mongodb.client.FindIterable[org.bson.Document]) => offset.map(v => it.skip(v.toInt)).getOrElse(it)
        val limiter = (it: com.mongodb.client.FindIterable[org.bson.Document]) => limit.map(v => it.limit(v.toInt)).getOrElse(it)

        val skipperAndLimiter = skipper andThen limiter

        resource(db.get(col).map(c => skipperAndLimiter(c.find()).iterator))(
          cursor => Task.delay(cursor.close()))(
          cursor => Task.delay {
            if (cursor.hasNext) {
              val obj = cursor.next
              obj.remove("_id")
              BsonCodec.toData(Bson.fromRepr(obj))
            }
            else throw Cause.End.asThrowable
          })
      })

  def count(path: Path): Task[Long] =
    Collection.fromPath(path).fold(Task.fail, db.get(_).map(_.count))

  def save(path: Path, values: Process[Task, Data]) =
    Collection.fromPath(path).fold(
      e => Task.fail(e),
      col => {
        for {
          tmp <- db.genTempName(col)
          _   <- append(tmp.asPath, values).runLog.flatMap(_.toList match {
            case e :: _ => delete(tmp.asPath) ignoreAndThen Task.fail(e)
            case _      => Task.now(())
          })
          _   <- db.rename(tmp, col) onFailure delete(tmp.asPath)
        } yield ()
      })

  def append(path: Path, values: Process[Task, Data]) =
    Collection.fromPath(path).fold(
      e => Process.fail(e),
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
                  _ => Nil
                )

                parseErrors ++ insertErrors
              }
          ).flatMap(errs => Process.emitAll(errs))
        }
      }
    )

  def move(src: Path, dst: Path): Task[Unit] = {
    def target(col: Collection): Option[Collection] =
      if (col.asPath == src) Collection.fromPath(dst).toOption
      else (for {
        rel <- col.asPath rebase src
        p = dst ++ rel
        c   <- Collection.fromPath(p)
      } yield c).toOption

    Collection.fromPath(dst).fold(
      e => Task.fail(e),
      dstCol => for {
        cols    <- db.list
        renames <- cols.map { s => target(s).map(db.rename(s, _)) }.flatten.sequenceU
        _       <- if (renames.isEmpty) Task.fail(FileSystem.FileNotFoundError(src)) else Task.now(())
      } yield ())
    }

  def delete(path: Path): Task[Unit] = for {
    all     <- db.list
    cols = all.filter(col => { val p = col.asPath; path == p || (path contains p) } )
    deletes <- cols.map(db.drop).sequenceU
  } yield ()

  // Note: a mongo db can contain a collection named "foo" as well as "foo.bar" and "foo.baz",
  // in which case "foo" acts as both a directory and a file, as far as slamengine is concerned.
  def ls(dir: Path): Task[List[Path]] = for {
    cols <- db.list
    allPaths = cols.map(_.asPath)
  } yield allPaths.map(p => p.rebase(dir).toOption.map(_.head)).flatten.sorted.distinct

  def defaultPath = db.defaultDB.map(Path(_).asDir).getOrElse(Path("."))
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

object MongoDbFileSystem {
  def apply(client0: com.mongodb.MongoClient, db0: Option[String]): MongoDbFileSystem =
    new MongoDbFileSystem {
      protected def db = new MongoWrapper {
        protected def client = client0
        def defaultDB = db0
      }
    }
}
