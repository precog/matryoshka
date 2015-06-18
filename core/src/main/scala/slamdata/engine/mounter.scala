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
  final case class InvalidConfig(message: String) extends MountError

  def mountE(config: Config): EitherT[Task, MountError, Backend] = {
    def rec(backend: Backend, path: List[DirNode], conf: BackendConfig): EitherT[Task, MountError, Backend] =
      backend match {
        case NestedBackend(base) =>
          path match {
            case Nil => BackendDefinitions.All(conf).fold[EitherT[Task, MountError, Backend]](
              EitherT.left(Task.now(MissingFileSystem(Path(path, None), conf))))(
              EitherT.right)
            case dir :: dirs =>
              rec(base.get(dir).getOrElse(NestedBackend(Map())), dirs, conf).map(rez => NestedBackend(base + (dir -> rez)))
          }
        case _ => EitherT.left(Task.now(InvalidConfig("attempting to mount a backend within an existing backend.")))
      }

    config.mountings.foldLeft[EitherT[Task, MountError, Backend]](
      EitherT.right(Task.now(NestedBackend(Map())))) {
      case (root, (path, config)) =>
        root.flatMap(rec(_, path.asAbsolute.asDir.dir, config))
    }
  }

  def mount(config: Config): Task[Backend] =
    mountE(config).fold(Task.fail, Task.now).join
}
