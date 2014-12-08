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

  def scan(path: Path, offset: Option[Long], limit: Option[Long]): Process[Task, RenderedJson] = {
    import scala.collection.mutable.ArrayBuffer

    Collection.fromPath(path).fold(
      e => Process.eval(Task.fail(e)),
      col => {
        val skipper = (cursor: com.mongodb.DBCursor) => offset.map(v => cursor.skip(v.toInt)).getOrElse(cursor)
        val limiter = (cursor: com.mongodb.DBCursor) => limit.map(v => cursor.limit(v.toInt)).getOrElse(cursor)

        val skipperAndLimiter = skipper andThen limiter

        resource(db.get(col).map(c => skipperAndLimiter(c.find())))(
          cursor => Task.delay(cursor.close()))(
          cursor => Task.delay {
            if (cursor.hasNext) {
              val obj = cursor.next
              obj.removeField("_id")
              RenderedJson(com.mongodb.util.JSON.serialize(obj))
            }
            else throw Cause.End.asThrowable
          }
        )
      }
    )
  }

  def save(path: Path, values: Process[Task, RenderedJson]) =
    Collection.fromPath(path).fold(
      e => Task.fail(e),
      col => {
        for {
          tmp <- db.genTempName
          _   <- append(tmp.asPath, values).runLog.flatMap(_.toList match {
            case e :: _ => delete(tmp.asPath) ignoreAndThen Task.fail(e)
            case _      => Task.now(())
          })
          _   <- db.rename(tmp, col) onFailure delete(tmp.asPath)
        } yield ()
      })

  def append(path: Path, values: Process[Task, RenderedJson]) =
    Collection.fromPath(path).fold(
      e => Process.fail(e),
      col => {
        import process1._

        val chunks: Process[Task, Vector[(RenderedJson, Option[com.mongodb.DBObject])]] = values.map(json => json -> db.parse(json)) pipe chunk(ChunkSize)

        chunks.flatMap { vs => 
          Process.eval(Task.delay {
                val parseErrors = vs.collect { case (json, None)      => JsonWriteError(json, Some("parse error")) }
                val objs        = vs.collect { case (json, Some(obj)) => json -> obj }

                val insertErrors = db.insert(col, objs.map(_._2)).attemptRun.fold(
                  e => objs.map { case (json, _) => JsonWriteError(json, Some(e.getMessage)) },
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
}

sealed trait MongoWrapper {
  import com.mongodb._
  import scala.collection.JavaConverters._
  import scala.collection.JavaConversions._

  protected def db: DB

  val genTempName: Task[Collection] = for {
    start <- SequenceNameGenerator.startUnique
  } yield SequenceNameGenerator.Gen.generateTempName.eval(start)

  def parse(json: RenderedJson): Option[com.mongodb.DBObject] = com.mongodb.util.JSON.parse(json.value) match {
    case obj: DBObject => Some(obj)
    case x => None
  }

  // Note: this exposes the Java obj, so should be made private at some point
  def get(col: Collection): Task[DBCollection] = Task.delay { db.getCollection(col.name) }

  def rename(src: Collection, dst: Collection): Task[Unit] = for {
    s <- get(src)
    _ = s.rename(dst.name, true)
  } yield ()

  def drop(col: Collection): Task[Unit] = for {
    c <- get(col)
    _ = c.drop
  } yield ()

  def insert(col: Collection, data: Vector[DBObject]): Task[Unit] = for {
    c <- get(col)
    _ = c.insert(data)
  } yield ()
  
  val list: Task[List[Collection]] = Task.delay { db.getCollectionNames().asScala.map(Collection.apply).toList }
}


object MongoDbFileSystem {
  def apply(db0: com.mongodb.DB): MongoDbFileSystem = new MongoDbFileSystem {
    protected def db = new MongoWrapper {
      protected def db = db0 
    }
  }
}
