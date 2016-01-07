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

package quasar
package fs

import quasar.Predef._
import quasar.effect.LiftedOps
import quasar.fp._

import scala.Ordering

import monocle.Prism
import pathy.{Path => PPath}, PPath._
import scalaz._, Scalaz._

sealed trait ManageFile[A]

object ManageFile {
  sealed trait MoveSemantics

  /** NB: Certain write operations' consistency is affected by faithful support
    *     of these semantics, thus their consistency/atomicity is as good as the
    *     support of these semantics by the interpreter.
    *
    *     Currently, this allows us to implement all the write scenarios in terms
    *     of append and move, however if this proves too difficult to support by
    *     backends, we may want to relax the move semantics and instead add
    *     additional primitive operations for the conditional write operations.
    */
  object MoveSemantics {
    object Case {
      case object Overwrite extends MoveSemantics
      case object FailIfExists extends MoveSemantics
      case object FailIfMissing extends MoveSemantics
    }

    /** Indicates the move operation should overwrite anything at the
      * destination, creating it if it doesn't exist.
      */
    val Overwrite: MoveSemantics = Case.Overwrite

    /** Indicates the move should (atomically, if possible) fail if the
      * destination exists.
      */
    val FailIfExists: MoveSemantics = Case.FailIfExists

    /** Indicates the move should (atomically, if possible) fail unless
      * the destination exists, overwriting it otherwise.
      */
    val FailIfMissing: MoveSemantics = Case.FailIfMissing

    val overwrite: Prism[MoveSemantics, Unit] =
      Prism[MoveSemantics, Unit] {
        case Case.Overwrite => Some(())
        case _ => None
      } (κ(Overwrite))

    val failIfExists: Prism[MoveSemantics, Unit] =
      Prism[MoveSemantics, Unit] {
        case Case.FailIfExists => Some(())
        case _ => None
      } (κ(FailIfExists))

    val failIfMissing: Prism[MoveSemantics, Unit] =
      Prism[MoveSemantics, Unit] {
        case Case.FailIfMissing => Some(())
        case _ => None
      } (κ(FailIfMissing))
  }

  sealed trait MoveScenario {
    import MoveScenario._

    def fold[X](
      d2d: (ADir, ADir) => X,
      f2f: (AFile, AFile) => X
    ): X =
      this match {
        case Case.DirToDir(sd, dd)   => d2d(sd, dd)
        case Case.FileToFile(sf, df) => f2f(sf, df)
      }

    def src: APath

    def dst: APath
  }

  object MoveScenario {
    object Case {
      final case class DirToDir(src: ADir, dst: ADir)
        extends MoveScenario
      final case class FileToFile(src: AFile, dst: AFile)
        extends MoveScenario
    }

    val DirToDir: (ADir, ADir) => MoveScenario =
      Case.DirToDir(_, _)

    val FileToFile: (AFile, AFile) => MoveScenario =
      Case.FileToFile(_, _)

    val dirToDir: Prism[MoveScenario, (ADir, ADir)] =
      Prism((_: MoveScenario).fold((s, d) => (s, d).some, κ(none)))(DirToDir.tupled)

    val fileToFile: Prism[MoveScenario, (AFile, AFile)] =
      Prism((_: MoveScenario).fold(κ(none), (s, d) => (s, d).some))(FileToFile.tupled)
  }

  final case class Move(scenario: MoveScenario, semantics: MoveSemantics)
    extends ManageFile[FileSystemError \/ Unit]

  final case class Delete(path: APath)
    extends ManageFile[FileSystemError \/ Unit]

  final case class TempFile(nearTo: Option[AFile])
    extends ManageFile[AFile]

  // TODO{scalaz}: Refactor, dropping Coyoneda and Functor constraint once we
  //               update to scalaz-7.2
  @SuppressWarnings(Array("org.brianmckenna.wartremover.warts.NonUnitStatements"))
  final class Ops[S[_]](implicit S0: Functor[S], S1: ManageFileF :<: S)
    extends LiftedOps[ManageFile, S] {

    type M[A] = FileSystemErrT[F, A]

    /** Request the given move scenario be applied to the file system, using the
      * given semantics.
      */
    def move(scenario: MoveScenario, semantics: MoveSemantics): M[Unit] =
      EitherT(lift(Move(scenario, semantics)))

    /** Move the `src` dir to `dst` dir, requesting the semantics described by `sem`. */
    def moveDir(src: ADir, dst: ADir, sem: MoveSemantics): M[Unit] =
      move(MoveScenario.DirToDir(src, dst), sem)

    /** Move the `src` file to `dst` file, requesting the semantics described by `sem`. */
    def moveFile(src: AFile, dst: AFile, sem: MoveSemantics): M[Unit] =
      move(MoveScenario.FileToFile(src, dst), sem)

    /** Rename the `src` file in the same directory. */
    def renameFile(src: AFile, name: String): M[AFile] = {
      val dst = PPath.renameFile(src, κ(FileName(name)))
      moveFile(src, dst, MoveSemantics.Overwrite).as(dst)
    }

    /** Delete the given file system path, fails if the path does not exist. */
    def delete(path: APath): M[Unit] =
      EitherT(lift(Delete(path)))

    /** Returns the path to a new temporary file. When `nearTo` is specified,
      * an attempt is made to return a tmp path that is as physically close to
      * the given file as possible.
      */
    def tempFile(nearTo: Option[AFile]): F[AFile] =
      lift(TempFile(nearTo))

    /** Returns the path to a new temporary file. */
    def anyTempFile: F[AFile] =
      tempFile(None)

    /** Returns the path to a new temporary file as physically close to the
      * specified file as possible.
      */
    def tempFileNear(file: AFile): F[AFile] =
      tempFile(Some(file))
  }

  object Ops {
    implicit def apply[S[_]](implicit S0: Functor[S], S1: ManageFileF :<: S): Ops[S] =
      new Ops[S]
  }
}
