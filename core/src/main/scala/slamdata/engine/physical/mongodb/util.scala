package slamdata.engine.physical.mongodb

import slamdata.engine.config._

import scalaz.concurrent.Task
import scalaz.{Memo, Need}

import com.mongodb._

object util {
  private val mongoClient = Memo.mutableHashMapMemo[String, MongoClient] { (connectionUri: String) =>
    new MongoClient(new MongoClientURI(connectionUri))
  }

  def createMongoDB(config: MongoDbConfig): Task[DB] = Task.delay {
    val client = mongoClient(config.connectionUri)

    client.getDB(config.database)
  }
}
