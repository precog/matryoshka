package quasar
package fs

import quasar.Predef._
import quasar.fp._

import scala.Ordering

import pathy.{Path => PPath}, PPath._

import scalaz._, Scalaz._

sealed trait ManageFile[A]

object ManageFile {
  sealed trait MoveSemantics {
    import MoveSemantics._

    def fold[X](overwrite: => X, failIfExists: => X, failIfMissing: => X): X =
      this match {
        case Overwrite0     => overwrite
        case FailIfExists0  => failIfExists
        case FailIfMissing0 => failIfMissing
      }
  }

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
    private case object Overwrite0 extends MoveSemantics
    private case object FailIfExists0 extends MoveSemantics
    private case object FailIfMissing0 extends MoveSemantics

    /** Indicates the move operation should overwrite anything at the
      * destination, creating it if it doesn't exist.
      */
    val Overwrite: MoveSemantics = Overwrite0

    /** Indicates the move should (atomically, if possible) fail if the
      * destination exists.
      */
    val FailIfExists: MoveSemantics = FailIfExists0

    /** Indicates the move should (atomically, if possible) fail unless
      * the destination exists, overwriting it otherwise.
      */
    val FailIfMissing: MoveSemantics = FailIfMissing0
  }

  sealed trait MoveScenario {
    import MoveScenario._

    def fold[X](
      d2d: (AbsDir[Sandboxed], AbsDir[Sandboxed]) => X,
      f2f: (AbsFile[Sandboxed], AbsFile[Sandboxed]) => X
    ): X =
      this match {
        case DirToDir0(sd, dd)   => d2d(sd, dd)
        case FileToFile0(sf, df) => f2f(sf, df)
      }
  }

  object MoveScenario {
    private final case class DirToDir0(src: AbsDir[Sandboxed], dst: AbsDir[Sandboxed])
      extends MoveScenario
    private final case class FileToFile0(src: AbsFile[Sandboxed], dst: AbsFile[Sandboxed])
      extends MoveScenario

    val DirToDir: (AbsDir[Sandboxed], AbsDir[Sandboxed]) => MoveScenario =
      DirToDir0(_, _)

    val FileToFile: (AbsFile[Sandboxed], AbsFile[Sandboxed]) => MoveScenario =
      FileToFile0(_, _)
  }

  final case class Move(scenario: MoveScenario, semantics: MoveSemantics)
    extends ManageFile[FileSystemError \/ Unit]

  final case class Delete(path: AbsPath[Sandboxed])
    extends ManageFile[FileSystemError \/ Unit]

  final case class TempFile(nearTo: Option[AbsFile[Sandboxed]])
    extends ManageFile[AbsFile[Sandboxed]]

  final class Ops[S[_]](implicit S0: Functor[S], S1: ManageFileF :<: S) {
    type F[A] = Free[S, A]
    type M[A] = FileSystemErrT[F, A]

    /** Request the given move scenario be applied to the file system, using the
      * given semantics.
      */
    def move(scenario: MoveScenario, semantics: MoveSemantics): M[Unit] =
      EitherT(lift(Move(scenario, semantics)))

    /** Move the `src` dir to `dst` dir, requesting the semantics described by `sem`. */
    def moveDir(src: AbsDir[Sandboxed], dst: AbsDir[Sandboxed], sem: MoveSemantics): M[Unit] =
      move(MoveScenario.DirToDir(src, dst), sem)

    /** Move the `src` file to `dst` file, requesting the semantics described by `sem`. */
    def moveFile(src: AbsFile[Sandboxed], dst: AbsFile[Sandboxed], sem: MoveSemantics): M[Unit] =
      move(MoveScenario.FileToFile(src, dst), sem)

    /** Rename the `src` file in the same directory. */
    def renameFile(src: AbsFile[Sandboxed], name: String): M[AbsFile[Sandboxed]] = {
      val dst = PPath.renameFile(src, κ(FileName(name)))
      moveFile(src, dst, MoveSemantics.Overwrite).as(dst)
    }

    /** Delete the given file system path, fails if the path does not exist. */
    def delete(path: AbsPath[Sandboxed]): M[Unit] =
      EitherT(lift(Delete(path)))

    /** Delete the given directory, fails if the directory does not exist. */
    def deleteDir(dir: AbsDir[Sandboxed]): M[Unit] =
      delete(dir.left)

    /** Delete the given file, fails if the file does not exist. */
    def deleteFile(file: AbsFile[Sandboxed]): M[Unit] =
      delete(file.right)

    /** Returns the path to a new temporary file. When `nearTo` is specified,
      * an attempt is made to return a tmp path that is as physically close to
      * the given file as possible.
      */
    def tempFile(nearTo: Option[AbsFile[Sandboxed]]): F[AbsFile[Sandboxed]] =
      lift(TempFile(nearTo))

    /** Returns the path to a new temporary file. */
    def anyTempFile: F[AbsFile[Sandboxed]] =
      tempFile(None)

    /** Returns the path to a new temporary file as physically close to the
      * specified file as possible.
      */
    def tempFileNear(file: AbsFile[Sandboxed]): F[AbsFile[Sandboxed]] =
      tempFile(Some(file))

    ////

    private def lift[A](fs: ManageFile[A]): F[A] =
      Free.liftF(S1.inj(Coyoneda.lift(fs)))
  }

  object Ops {
    implicit def apply[S[_]](implicit S0: Functor[S], S1: ManageFileF :<: S): Ops[S] =
      new Ops[S]
  }
}
