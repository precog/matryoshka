package quasar.mount

import quasar.Predef._
import quasar.EnvironmentError2
import quasar.fs._

import monocle.Prism

sealed trait MountingError

object MountingError {
  final case class PathError private[mount] (err: PathError2)
    extends MountingError

  final case class EnvironmentError private[mount] (err: EnvironmentError2)
    extends MountingError

  final case class InvalidConfig private[mount] (config: MountConfig2, reason: String)
    extends MountingError

  val pathError: Prism[MountingError, PathError2] =
    Prism[MountingError, PathError2] {
      case PathError(err) => Some(err)
      case _ => None
    } (PathError(_))

  val environmentError: Prism[MountingError, EnvironmentError2] =
    Prism[MountingError, EnvironmentError2] {
      case EnvironmentError(err) => Some(err)
      case _ => None
    } (EnvironmentError(_))

  val invalidConfig: Prism[MountingError, (MountConfig2, String)] =
    Prism[MountingError, (MountConfig2, String)] {
      case InvalidConfig(cfg, reason) => Some((cfg, reason))
      case _ => None
    } ((InvalidConfig(_, _)).tupled)
}
