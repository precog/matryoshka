package slamdata.engine.physical.mongodb

import slamdata.engine._
import slamdata.engine.fs._
import slamdata.engine.fp._

import scalaz._
import scalaz.stream._
import scalaz.stream.io._
import scalaz.concurrent._

import com.mongodb._

import scala.collection.JavaConverters._

sealed trait MongoDbFileSystem extends FileSystem {
  protected def db: DB

  val ChunkSize = 100

  def scan(path: Path, offset: Option[Long], limit: Option[Long]): Process[Task, RenderedJson] = {
    import scala.collection.mutable.ArrayBuffer

    Collection.fromPath(path).fold(
      e => Process.eval(Task.fail(e)),
      col => {
        val skipper = (cursor: DBCursor) => offset.map(v => cursor.skip(v.toInt)).getOrElse(cursor)
        val limiter = (cursor: DBCursor) => limit.map(v => cursor.limit(v.toInt)).getOrElse(cursor)

        val skipperAndLimiter = skipper andThen limiter

        resource(Task.delay(skipperAndLimiter(db.getCollection(col.name).find())))(
          cursor => Task.delay(cursor.close()))(
          cursor => Task.delay {
            if (cursor.hasNext) RenderedJson(com.mongodb.util.JSON.serialize(cursor.next))
            else throw Process.End
          }
        )
      }
    )
  }

  def save(path: Path, values: Process[Task, RenderedJson]) = Collection.fromPath(path).fold(
      e => Task.fail(e),
      col => {
        val genTempName: Task[Collection] = for {
          start <- SequenceNameGenerator.startUnique
        } yield SequenceNameGenerator.Gen.generateTempName.eval(start)

        for {
          tmp <- genTempName
          _   <- append(tmp.asPath, values).runLog.flatMap(_.toList match {
                    case e :: _ => delete(tmp.asPath) ignoreAndThen Task.fail(e)
                    case _      => Task.now(())
                  })
          _   <- move(tmp.asPath, col.asPath) onFailure delete(tmp.asPath)
        } yield ()
      })

  def append(path: Path, values: Process[Task, RenderedJson]) = Collection.fromPath(path).fold(
      e => Process.fail(e),
      col => {
        import process1._
        import scala.collection.JavaConversions._

        def parse(json: RenderedJson): Option[DBObject] = com.mongodb.util.JSON.parse(json.value) match {
          case obj: DBObject => Some(obj)
          case x => None
        }

        val chunks: Process[Task, Vector[(RenderedJson, Option[DBObject])]] = values.map(json => json -> parse(json)) pipe chunk(ChunkSize)

        chunks.flatMap { vs => 
          Process.eval(Task.delay {
            \/.fromTryCatchNonFatal(db.getCollection(col.name)).map(
              mongoCol => {
                val parseErrors = vs.collect { case (json, None)      => JsonWriteError(json, Some("parse error")) }
                val objs        = vs.collect { case (json, Some(obj)) => json -> obj }

                val insertErrors = \/.fromTryCatchNonFatal(mongoCol.insert(objs.map(_._2))).fold(
                  e => objs.map { case (json, _) => JsonWriteError(json, Some(e.getMessage)) },
                  _ => Nil
                )
              
                parseErrors ++ insertErrors
              }
            )
          }).flatMap(_.fold(Process.fail(_), errs => Process.emitAll(errs)))
        }
      })

    def move(src: Path, dst: Path): Task[Unit] = (for {
      srcCol <- Collection.fromPath(src)
      dstCol <- Collection.fromPath(dst)
    } yield (srcCol, dstCol)).fold(
      e => Task.fail(e),
      { case (srcCol, dstCol) => Task.delay {
        db.getCollection(srcCol.name).rename(dstCol.name, true)
        ()
      }}
    )

    def delete(path: Path): Task[Unit] = Collection.fromPath(path).fold(
      e => Task.fail(e),
      col => Task.delay(db.getCollection(col.name).drop)
    )

  // Note: a mongo db can contain a collection named "foo" as well as "foo.bar" and "foo.baz", 
  // in which case "foo" acts as both a directory and a file, as far as slamengine is concerned.
  def ls(dir: Path): Task[List[Path]] = Task.delay {
    val colls = db.getCollectionNames().asScala.toList.map(Collection(_))
    val allPaths = colls.map(_.asPath)
    val children = allPaths.map { p => 
      for {
        rel <- p.relativeTo(dir)
      } yield rel.head
    }.flatten.sorted.distinct
    children
  }
}

object MongoDbFileSystem {
  def apply(db0: DB): MongoDbFileSystem = new MongoDbFileSystem {
    protected def db = db0
  }
}