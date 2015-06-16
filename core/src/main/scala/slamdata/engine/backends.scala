package slamdata.engine

import slamdata.engine.config._

import scalaz.Foldable
import scalaz.std.list._

object BackendDefinitions {
  val MongoDB: BackendDefinition = BackendDefinition({
    case config : MongoDbConfig =>
      import slamdata.engine.physical.mongodb._
      import Workflow._

      val tclient = util.createMongoClient(config) // FIXME: This will leak because Task will be re-run every time. Cache the DB for a given config.

      val defaultDb = config.connectionUri match {
        case MongoDbConfig.ParsedUri(_, _, _, _, _, authDb, _) => authDb
        case _ => None
      }

      for {
        client <- tclient
      } yield new PlannerBackend[Workflow] with MongoDbFileSystem {
        val planner = MongoDbPlanner
        val evaluator = MongoDbEvaluator(client, defaultDb)
        val RP = implicitly[RenderTree[Workflow]]
        protected def db = MongoWrapper(client, defaultDb)
      }
  })

  val All = Foldable[List].foldMap(MongoDB :: Nil)(identity)
}
