package slamdata.engine.physical.mongodb

import slamdata.engine._

import scalaz.stream._
import scalaz.stream.io._
import scalaz.concurrent._

import com.mongodb._

import scala.collection.JavaConverters._

sealed trait MongoDbDataSource extends DataSource {
  protected def db: DB

  def scan(table: String): Process[Task, RenderedJson] = {
    import scala.collection.mutable.ArrayBuffer
    import Process._

    val cursor = db.getCollection(table).find()

    resource(Task.delay(db.getCollection(table).find()))(
      cursor => Task.delay(cursor.close()))(
      cursor => Task.delay {
        if (cursor.hasNext) RenderedJson(com.mongodb.util.JSON.serialize(cursor.next))
        else throw End
      }
    )
  }

  def delete(table: String): Task[Unit] = Task.delay(db.getCollection(table).drop())

  def ls: Task[List[String]] = Task.delay(db.getCollectionNames().asScala.toList)
}

object MongoDbDataSource {
  def apply(db0: DB): MongoDbDataSource = new MongoDbDataSource {
    protected def db = db0
  }
}