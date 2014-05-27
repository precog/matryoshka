package slamdata.engine

import slamdata.engine.config._

import scalaz._
import Scalaz._

import scalaz.concurrent._

object Mounter {
  sealed trait MounterError extends Error
  case class MissingDataSource(path: String, config: BackendConfig) extends MounterError {
    def message = "No data source could be mounted at the path " + path + " using the config " + config
  }

  def mountE(config: Config): MounterError \/ Task[Map[String, Backend]] = {
    type MapString[X] = Map[String, X]
    type EitherError[X] = MounterError \/ X

    val map: MounterError \/ Map[String, Task[Backend]] = Traverse[MapString].sequence[EitherError, Task[Backend]](config.mountings.transform {
      case (path, config) => BackendDefinitions.All(config).map(backend => \/-(backend)).getOrElse(-\/(MissingDataSource(path, config): MounterError))
    })

    map.map { map =>
      Traverse[MapString].sequence[Task, Backend](map)
    }
  }

  def mount(config: Config): Task[Map[String, Backend]] = mountE(config).fold(Task.fail _, identity)
}