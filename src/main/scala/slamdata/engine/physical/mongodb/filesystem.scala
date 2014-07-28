package slamdata.engine.physical.mongodb

import slamdata.engine._
import slamdata.engine.fs._

import scalaz.stream._
import scalaz.stream.io._
import scalaz.concurrent._

import com.mongodb._

import scala.collection.JavaConverters._

sealed trait MongoDbFileSystem extends FileSystem {
  protected def db: DB

  def scan(path: Path, offset: Option[Long], limit: Option[Long]): Process[Task, RenderedJson] = {
    import scala.collection.mutable.ArrayBuffer
    import Process._

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
            else throw End
          }
        )
      }
    )
  }

  def delete(path: Path): Task[Unit] = Collection.fromPath(path).fold(
    e => Task.fail(e),
    col => Task.delay(db.getCollection(col.name).drop())
  )

  // Note: a mongo db can contain a collection named "foo" as well as "foo.bar" and "foo.baz", 
  // in which case "foo" acts as both a directory and a file, as far as slamengine is concerned.
  def ls(dir: Path): Task[Option[List[Path]]] = {
    val colls = db.getCollectionNames().asScala.toList.map(Collection(_))
    val allPaths = colls.map(_.asPath)
    val children = allPaths.map { p => 
      for {
        rel <- p.relativeTo(dir)
      } yield rel.head
    }.flatten.sorted.distinct
    Task.delay(if (children.isEmpty) None else Some(children))
  }
}

object MongoDbFileSystem {
  def apply(db0: DB): MongoDbFileSystem = new MongoDbFileSystem {
    protected def db = db0
  }
}