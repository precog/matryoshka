package slamdata.engine

import slamdata.engine.config._
import slamdata.engine.fs._

import scalaz._
import Scalaz._

import scalaz.concurrent._

object Mounter {
  sealed trait MountError extends Error
  final case class MissingFileSystem(path: Path, config: BackendConfig) extends MountError {
    def message = "No data source could be mounted at the path " + path + " using the config " + config
  }

  def mountE(config: Config): MountError \/ Task[Backend] = {
    val map: MountError \/ Map[Path, Task[Backend]] =
      Traverse[Map[Path, ?]].sequence[MountError \/ ?, Task[Backend]](
        config.mountings.transform {
          case (path, config) => BackendDefinitions.All(config).map(backend => \/-(backend)).getOrElse(-\/(MissingFileSystem(path, config): MountError))
       })

    map.map { map =>
      Traverse[Map[Path, ?]].sequence[Task, Backend](map).map(NestedBackend)
    }
  }

  def mount(config: Config): Task[Backend] = mountE(config).fold(Task.fail _, identity)
}
