package slamdata.engine.physical.mongodb

import collection.JavaConversions._
import slamdata.engine.config._

import scalaz.concurrent.Task
import scalaz.{Memo, Need}

import com.mongodb._
import com.mongodb.client._

object util {
  private val mongoClient: String => Task[MongoClient] = {
    val memo = Memo.mutableHashMapMemo[String, MongoClient] { (connectionUri: String) =>
      new MongoClient(new MongoClientURI(connectionUri))
    }

    uri => Task.delay { memo(uri) }
  }

  def createMongoClient(config: MongoDbConfig): Task[MongoClient] = {
    for {
      _ <- disableMongoLogging
      c <- mongoClient(config.connectionUri)
    } yield c
  }

  private def disableMongoLogging: Task[Unit] = {
    import java.util.logging._

    Task.delay { Logger.getLogger("org.mongodb").setLevel(Level.WARNING) }
  }
}
