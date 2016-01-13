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

package quasar.fs.mount

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
  type FsMounts             = Mounts[FsCfg]
  type MapMounterT[F[_], A] = StateT[F, Mountings, A]
  type MountR               = Either3[String, EnvironmentError2, Unit]

  final case class Mountings(view: ViewMounts, fs: FsMounts)

  object Mountings {
    val empty: Mountings = Mountings(Map.empty, Mounts.empty)
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
      refineType(path).fold(lookupFsMnt, f => viewMntL(f).st.lift[F])

    case MountView(loc, query, vars) =>
      OptionT[Mnt, VCfg](viewL(loc).st.lift[F])
        .as(pathExists(loc))
        .toLeft(())
        .flatMap(κ(mountView(loc, query, vars)))
        .run

    case MountFileSystem(loc, typ, uri) =>
      OptionT[Mnt, FsCfg](lookupFsCfg(loc))
        .as(pathExists(loc))
        .toLeft(())
        .flatMap(κ(mountFileSystem(loc, typ, uri)))
        .run

    case Unmount(path) =>
      refineType(path).fold(
        d => OptionT(lookupFsMnt(d))
               .toRight(pathNotFound(path))
               .flatMap(κ(liftPure(fssL.mods_(_ - d))))
               .run,
        f => viewMntL(f)
               .assigno(None)
               .map(_.void \/> pathNotFound(path))
               .lift[F])
  }

  val pathNotFound = MountingError.pathError composePrism PathError2.pathNotFound
  val pathExists = MountingError.pathError composePrism PathError2.pathExists
  val invalidPath = MountingError.pathError composePrism PathError2.invalidPath

  val viewsL: Mountings @> ViewMounts =
    Lens.lensu((m, vs) => m.copy(view = vs), _.view)

  def viewL(f: AFile): Mountings @> Option[VCfg] =
    Lens.mapVLens(f) <=< viewsL

  def viewMntL(f: AFile): Mountings @> Option[MountConfig2] =
    viewL(f).xmapB(_ map (viewConfig(_)))(_ flatMap viewConfig.getOption)

  val fssL: Mountings @> FsMounts =
    Lens.lensu((m, f) => m.copy(fs = f), _.fs)

  def lookupFsCfg(d: ADir): Mnt[Option[FsCfg]] =
    fssL.st.map(_ lookup d).lift[F]

  def lookupFsMnt(d: ADir): Mnt[Option[MountConfig2]] =
    lookupFsCfg(d) map (_ map (fileSystemConfig(_)))

  def mountView(loc: AFile, query: Expr, vars: Variables): MntE[Unit] =
    mount(viewConfig(query, vars)) *>
      liftPure(viewL(loc).assign((query, vars).some).void)

  def mountFileSystem(loc: ADir, typ: FileSystemType, uri: ConnectionUri): MntE[Unit] = {
    def addMountTo(mnts: FsMounts): MntE[FsMounts] =
      EitherT.fromDisjunction[Mnt](
        mnts.add(loc, (typ, uri)).leftMap(invalidPath(loc, _)))

    for {
      prevMnts <- liftPure(fssL.st)
      newMnts  <- addMountTo(prevMnts)
      _        <- mount(fileSystemConfig(typ, uri))
      _        <- liftPure(fssL := newMnts)
    } yield ()
  }

  def mount(cfg: MountConfig2): MntE[Unit] =
    EitherT[Mnt, MountingError, Unit](
      mount0(cfg).map(_.fold(
        s => MountingError.invalidConfig(cfg, s).left,
        e => MountingError.environmentError(e).left,
        _ => ().right)).liftM[MapMounterT])

  def liftPure[A](a: State[Mountings, A]): MntE[A] =
    (a.lift[F]: Mnt[A]).liftM[MntErrT]
}
