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
import quasar.Variables
import quasar.effect.LiftedOps
import quasar.fp._
import quasar.fs.{Path => _, _}
import quasar.sql._

import monocle.Prism
import monocle.std.{disjunction => D}
import pathy._, Path._
import scalaz._
import scalaz.syntax.std.option._
import scalaz.syntax.apply._
import scalaz.syntax.monadError._

sealed trait Mounting[A]

object Mounting {
  final case class Lookup(path: APath)
    extends Mounting[Option[MountConfig2]]

  final case class MountView(loc: AFile, query: Expr, vars: Variables)
    extends Mounting[MountingError \/ Unit]

  final case class MountFileSystem(loc: ADir, typ: FileSystemType, uri: ConnectionUri)
    extends Mounting[MountingError \/ Unit]

  final case class Unmount(path: APath)
    extends Mounting[MountingError \/ Unit]

  @SuppressWarnings(Array("org.brianmckenna.wartremover.warts.NonUnitStatements"))
  final class Ops[S[_]](implicit S0: Functor[S], S1: MountingF :<: S)
    extends LiftedOps[Mounting, S] {

    import MountConfig2._

    type M[A] = EitherT[F, MountingError, A]

    /** Returns the mount configuration for the given mount path or nothing
      * if the path does not refer to a mount.
      */
    def lookup(path: APath): OptionT[F, MountConfig2] =
      OptionT(lift(Lookup(path)))

    /** Create a view mount at the given location. */
    def mountView(loc: AFile, query: Expr, vars: Variables): M[Unit] =
      EitherT(lift(MountView(loc, query, vars)))

    /** Create a filesystem mount at the given location. */
    def mountFileSystem(loc: ADir, typ: FileSystemType, uri: ConnectionUri): M[Unit] =
      EitherT(lift(MountFileSystem(loc, typ, uri)))

    /** Attempt to create a mount described by the given configuration at the
      * given location.
      */
    def mount(loc: APath, config: MountConfig2): M[Unit] =
      config match {
        case ViewConfig(query, vars) =>
          D.right.getOption(refineType(loc)) cata (file =>
              mountView(file, query, vars),
              invalidPath(loc, "view mount location must be a file")
                .raiseError[E, Unit])

        case FileSystemConfig(typ, uri) =>
          D.left.getOption(refineType(loc)) cata (dir =>
            mountFileSystem(dir, typ, uri),
            invalidPath(loc, "filesytem mount location must be a directory")
              .raiseError[E, Unit])
      }

    /** Remount `src` at `dst`, results in an error if there is no mount at
      * `src`.
      */
    def remount[T](src: Path[Abs,T,Sandboxed], dst: Path[Abs,T,Sandboxed]): M[Unit] =
      modify(src, dst, ι)

    /** Replace the mount at the given path with one described by the
      * provided config.
      */
    def replace(loc: APath, config: MountConfig2): M[Unit] =
      modify(loc, loc, κ(config))

    /** Remove the mount at the given path. */
    def unmount(path: APath): M[Unit] =
      EitherT(lift(Unmount(path)))

    ////

    private type E[A, B] = EitherT[F, A, B]

    private val mErr: MonadError[E, MountingError] =
      MonadError[E, MountingError]

    private val notFound: Prism[MountingError, APath] =
      MountingError.pathError composePrism PathError2.pathNotFound

    private val invalidPath: Prism[MountingError, (APath, String)] =
      MountingError.pathError composePrism PathError2.invalidPath

    private def modify[T](
      src: Path[Abs,T,Sandboxed],
      dst: Path[Abs,T,Sandboxed],
      f: MountConfig2 => MountConfig2
    ): M[Unit] = {
      import mErr._

      for {
        cfg <- lookup(src) toRight notFound(src)
        _   <- unmount(src)
        _   <- handleError(mount(dst, f(cfg))) { err =>
                 mount(src, cfg) *> raiseError(err)
               }
      } yield ()
    }
  }

  object Ops {
    implicit def apply[S[_]](implicit S0: Functor[S], S1: MountingF :<: S): Ops[S] =
      new Ops[S]
  }
}
