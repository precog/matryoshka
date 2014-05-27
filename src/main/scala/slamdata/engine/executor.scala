package slamdata.engine

import slamdata.engine.config._

import scalaz._
import Scalaz._

import scalaz.concurrent._

object Executor {
  sealed trait ExecutorError extends Error
  case class MissingDataSource(path: String, config: BackendConfig) extends ExecutorError {
    def message = "No data source could be mounted at the path " + path + " using the config " + config
  }

  def createE(config: Config): ExecutorError \/ Task[Map[String, Backend]] = {
    type MapString[X] = Map[String, X]
    type EitherError[X] = ExecutorError \/ X

    val map: ExecutorError \/ Map[String, Task[Backend]] = Traverse[MapString].sequence[EitherError, Task[Backend]](config.mountings.transform {
      case (path, config) => BackendDefinitions.All(config).map(backend => \/-(backend)).getOrElse(-\/(MissingDataSource(path, config): ExecutorError))
    })

    map.map { map =>
      Traverse[MapString].sequence[Task, Backend](map)
    }
  }

  def create(config: Config): Task[Map[String, Backend]] = create(config).fold(Task.fail _, identity)
}