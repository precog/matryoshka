package slamdata.engine

import slamdata.engine.fp._
import slamdata.engine.config._
import slamdata.engine.fs._

import scalaz.Foldable
import scalaz.std.list._

object BackendDefinitions {
  val MongoDB: BackendDefinition = BackendDefinition({
    case config : MongoDbConfig =>
      import slamdata.engine.physical.mongodb._
      import Reshape._
      import Workflow._
      import com.mongodb.{util => _, _}

      val tclient = util.createMongoClient(config) // FIXME: This will leak because Task will be re-run every time. Cache the DB for a given config.

      val defaultDb = config.connectionUri match {
        case MongoDbConfig.UriPattern(_, _, _, _, _, authDb, _) => Some(authDb)
        case _ => None
      }

      for {
        client <- tclient
      } yield Backend(MongoDbPlanner, MongoDbEvaluator(client, defaultDb), MongoDbFileSystem(client, defaultDb))
  })

  val All = Foldable[List].foldMap(MongoDB :: Nil)(identity)
}
