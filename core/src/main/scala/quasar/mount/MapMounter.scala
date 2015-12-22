/*
 * Copyright 2014 - 2015 SlamData Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package quasar.mount

import quasar.Predef._
import quasar.{EnvironmentError2, Variables}
import quasar.fs.{Path => _, _}
import quasar.fp._
import quasar.sql.Expr

import pathy.Path._
import scalaz._, Scalaz._

object MapMounter {
  type VCfg                 = (Expr, Variables)
  type FsCfg                = (FileSystemType, ConnectionUri)
  type ViewMounts           = Map[AFile, VCfg]
  type FsMounts             = Map[ADir, FsCfg]
  type MapMounterT[F[_], A] = StateT[F, Mounts, A]
  type MountR               = Either3[String, EnvironmentError2, Unit]

  final case class Mounts(view: ViewMounts, fs: FsMounts)

  object Mounts {
    val empty: Mounts = Mounts(Map.empty, Map.empty)
  }

  /** `Mounting` interpreter using in-memory maps to maintain state.
    *
    * @param mount Handles the actual mounting of the mount described by the
    *              config. Returns `Left3` if the config is invalid, `Middle3`
    *              if there was a failure during mounting and `Right3` if mounting
    *              succeded.
    */
  def apply[F[_]: Monad](
    mount: MountConfig2 => F[MountR]
  ): Mounting ~> MapMounterT[F, ?] =
    new MapMounter(mount)
}

private final class MapMounter[F[_]: Monad](
  mount0: MountConfig2 => F[MapMounter.MountR]
) extends (Mounting ~> MapMounter.MapMounterT[F, ?]) {

  import MapMounter._, Mounting._, MountConfig2._

  type Mnt[A]  = MapMounterT[F, A]
  type MntE[A] = MntErrT[Mnt, A]

  def apply[A](m: Mounting[A]) = m match {
    case Lookup(path) =>
      refineType(path)
        .fold(fsMntL, viewMntL)
        .st.lift[F]

    case MountView(loc, query, vars) =>
      OptionT[Mnt, VCfg](viewL(loc).st.lift[F])
        .as(pathExists(loc))
        .toLeft(())
        .flatMap(κ(mountView(loc, query, vars)))
        .run

    case MountFileSystem(loc, typ, uri) =>
      OptionT[Mnt, FsCfg](fsL(loc).st.lift[F])
        .as(pathExists(loc))
        .toLeft(())
        .flatMap(κ(mountFileSystem(loc, typ, uri)))
        .run

    case Unmount(path) =>
      refineType(path)
        .fold(fsMntL, viewMntL)
        .assigno(None)
        .map(_.void \/> pathNotFound(path))
        .lift[F]
  }

  val pathNotFound = MountingError.pathError composePrism PathError2.pathNotFound
  val pathExists = MountingError.pathError composePrism PathError2.pathExists
  val invalidPath = MountingError.pathError composePrism PathError2.invalidPath

  val viewsL: Mounts @> ViewMounts =
    Lens.lensu((m, vs) => m.copy(view = vs), _.view)

  def viewL(f: AFile): Mounts @> Option[VCfg] =
    Lens.mapVLens(f) <=< viewsL

  def viewMntL(f: AFile): Mounts @> Option[MountConfig2] =
    viewL(f).xmapB(_ map (viewConfig(_)))(_ flatMap viewConfig.getOption)

  val fssL: Mounts @> FsMounts =
    Lens.lensu((m, f) => m.copy(fs = f), _.fs)

  def fsL(d: ADir): Mounts @> Option[FsCfg] =
    Lens.mapVLens(d) <=< fssL

  def fsMntL(d: ADir): Mounts @> Option[MountConfig2] =
    fsL(d).xmapB(_ map (fileSystemConfig(_)))(_ flatMap fileSystemConfig.getOption)

  def mountView(loc: AFile, query: Expr, vars: Variables): MntE[Unit] =
    mount(viewConfig(query, vars)) *>
      liftPure(viewL(loc).assign((query, vars).some).void)

  def mountFileSystem(loc: ADir, typ: FileSystemType, uri: ConnectionUri): MntE[Unit] = {
    val checkOverlaps: MntE[Unit] =
      EitherT(ensureNoOverlaps(loc).lift[F]: Mnt[MountingError \/ Unit])

    checkOverlaps                         *>
    mount(fileSystemConfig(typ, uri))     <*
    liftPure(fsL(loc) := (typ, uri).some)
  }

  def mount(cfg: MountConfig2): MntE[Unit] =
    EitherT[Mnt, MountingError, Unit](
      mount0(cfg).map(_.fold(
        s => MountingError.invalidConfig(cfg, s).left,
        e => MountingError.environmentError(e).left,
        _ => ().right)).liftM[MapMounterT])

  def liftPure[A](a: State[Mounts, A]): MntE[A] =
    (a.lift[F]: Mnt[A]).liftM[MntErrT]

  def ensureNoOverlaps(d0: ADir): State[Mounts, MountingError \/ Unit] =
    fssL.st map { fss =>
      fss.keys.toList.traverseU_(d1 =>
        d1.relativeTo(d0)
          .as("existing mount below: " + posixCodec.printPath(d1))
          .toLeftDisjunction(()) *>
        d0.relativeTo(d1)
          .as("existing mount above: " + posixCodec.printPath(d1))
          .toLeftDisjunction(())
      ) leftMap (invalidPath(d0, _))
    }
}
