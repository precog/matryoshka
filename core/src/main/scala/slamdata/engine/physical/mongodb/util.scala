package slamdata.engine.physical.mongodb

import collection.JavaConversions._
import slamdata.engine.config._

import scalaz.concurrent.Task
import scalaz.{Memo, Need}

import com.mongodb._
import com.mongodb.client._

object util {
  private val mongoClient = Memo.mutableHashMapMemo[String, MongoClient] { (connectionUri: String) =>
    new MongoClient(new MongoClientURI(connectionUri))
  }

  def createMongoDB(config: MongoDbConfig): Task[MongoDatabase] = {
    // Disable Mongo’s logger … by disabling all logging
    val globalLogger = java.util.logging.Logger.getGlobal
    globalLogger.getHandlers.map(globalLogger.removeHandler)

    Task.delay(mongoClient(config.connectionUri).getDatabase(config.database))
  }
}
