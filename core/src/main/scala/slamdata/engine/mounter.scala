package slamdata.engine

import slamdata.engine.Errors._
import slamdata.engine.config._
import slamdata.engine.fs._

import scalaz._
import Scalaz._

import scalaz.concurrent._

object Mounter {
  final case class MissingFileSystem(path: Path, config: BackendConfig) extends EnvironmentError {
    def message = "No data source could be mounted at the path " + path + " using the config " + config
  }

  def mount(config: Config): ETask[EnvironmentError, Backend] = {
    val map: EnvironmentError \/ Map[Path, Task[Backend]] =
      Traverse[Map[Path, ?]].sequence[EnvironmentError \/ ?, Task[Backend]](
        config.mountings.transform {
          case (path, config) => BackendDefinitions.All(config).map(backend => \/-(backend)).getOrElse(-\/(MissingFileSystem(path, config): EnvironmentError))
        })

    EitherT(map.map { map =>
      Traverse[Map[Path, ?]].sequence[Task, Backend](map).map(NestedBackend)
    }.sequenceU)
  }
}
