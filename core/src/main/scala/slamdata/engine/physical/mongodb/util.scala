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

  def createMongoClient(config: MongoDbConfig): Task[MongoClient] = {
    disableMongoLogging

    Task.delay(mongoClient(config.connectionUri))
  }

  private def disableMongoLogging = {
    import java.util.logging._

    Logger.getLogger("org.mongodb").setLevel(Level.WARNING)
  }
}
