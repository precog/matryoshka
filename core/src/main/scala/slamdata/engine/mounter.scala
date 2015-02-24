package slamdata.engine

import slamdata.engine.config._
import slamdata.engine.fs._

import scalaz._
import Scalaz._

import scalaz.concurrent._

object Mounter {
  sealed trait MountError extends Error
  case class MissingFileSystem(path: Path, config: BackendConfig) extends MountError {
    def message = "No data source could be mounted at the path " + path + " using the config " + config
  }

  def mountE(config: Config): MountError \/ Task[FSTable[Backend]] = {
    type MapPath[X] = Map[Path, X]
    type EitherError[X] = MountError \/ X

    val map: MountError \/ Map[Path, Task[Backend]] = Traverse[MapPath].sequence[EitherError, Task[Backend]](config.mountings.transform {
      case (path, config) => BackendDefinitions.All(config).map(backend => \/-(backend)).getOrElse(-\/(MissingFileSystem(path, config): MountError))
    })

    map.map { map =>
      Traverse[MapPath].sequence[Task, Backend](map).map(FSTable(_))
    }
  }

  def mount(config: Config): Task[FSTable[Backend]] = mountE(config).fold(Task.fail _, identity)
}
