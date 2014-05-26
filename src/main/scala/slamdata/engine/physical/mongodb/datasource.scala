package slamdata.engine.physical.mongodb

import slamdata.engine._

import scalaz.stream._
import scalaz.concurrent._

import com.mongodb._

sealed trait MongoDbDataSource extends DataSource {
  protected def db: DB

  def scan(table: String): Process[Task, RenderedJson] = {
    import scala.collection.mutable.ArrayBuffer
    import Process._

    val cursor = db.getCollection(table).find()

    repeatEval(Task.delay {
      if (!cursor.hasNext) throw End
      else RenderedJson(com.mongodb.util.JSON.serialize(cursor.next))
    })
  }

  def delete(table: String): Task[Unit] = Task.delay(db.getCollection(table).drop())
}

object MongoDbDataSource {
  def apply(db0: DB): MongoDbDataSource = new MongoDbDataSource {
    protected def db = db0
  }
}