package quasar
package fs

import quasar.Predef._

import scalaz._
import scalaz.std.anyVal._
import scalaz.syntax.monad._
import scalaz.stream._

sealed trait ReadFile[A]

object ReadFile {
  final case class ReadHandle(run: Long) extends scala.AnyVal

  object ReadHandle {
    implicit val readHandleShow: Show[ReadHandle] =
      Show.showFromToString

    implicit val readHandleOrder: Order[ReadHandle] =
      Order.orderBy(_.run)
  }

  final case class Open(file: AFile, offset: Natural, limit: Option[Positive])
    extends ReadFile[FileSystemError \/ ReadHandle]

  final case class Read(h: ReadHandle)
    extends ReadFile[FileSystemError \/ Vector[Data]]

  final case class Close(h: ReadHandle)
    extends ReadFile[Unit]

  final class Ops[S[_]](implicit S: Functor[S], val unsafe: Unsafe[S]) {
    type F[A] = unsafe.F[A]
    type M[A] = unsafe.M[A]

    /** Returns a process which produces data from the given file, beginning
      * at the specified offset. An optional limit may be supplied to restrict
      * the maximum amount of data read.
      */
    def scan(file: AFile, offset: Natural, limit: Option[Positive]): Process[M, Data] = {
      def readUntilEmpty(h: ReadHandle): Process[M, Data] =
        Process.await(unsafe.read(h)) { data =>
          if (data.isEmpty)
            Process.halt
          else
            Process.emitAll(data) ++ readUntilEmpty(h)
        }

      Process.await(unsafe.open(file, offset, limit))(h =>
        readUntilEmpty(h) onComplete Process.eval_[M, Unit](unsafe.close(h).liftM[FileSystemErrT]))
    }

    /** Returns a process that produces all the data contained in the
      * given file.
      */
    def scanAll(file: AFile): Process[M, Data] =
      scan(file, Natural._0, None)

    /** Returns a process that produces at most `limit` items from the beginning
      * of the given file.
      */
    def scanTo(file: AFile, limit: Positive): Process[M, Data] =
      scan(file, Natural._0, Some(limit))

    /** Returns a process that produces data from the given file, beginning
      * at the specified offset.
      */
    def scanFrom(file: AFile, offset: Natural): Process[M, Data] =
      scan(file, offset, None)
  }

  object Ops {
    implicit def apply[S[_]](implicit S: Functor[S], U: Unsafe[S]): Ops[S] =
      new Ops[S]
  }

  /** Low-level, unsafe operations. Clients are responsible for resource-safety
    * when using these.
    */
  final class Unsafe[S[_]](implicit S0: Functor[S], S1: ReadFileF :<: S) {
    type F[A] = Free[S, A]
    type M[A] = FileSystemErrT[F, A]

    /** Returns a read handle for the given file, positioned at the given
      * zero-indexed offset, that may be used to read chunks of data from the
      * file. An optional limit may be supplied to restrict the total amount of
      * data able to be read using the handle.
      *
      * Care must be taken to `close` the returned handle in order to avoid
      * potential resource leaks.
      */
    def open(file: AFile, offset: Natural, limit: Option[Positive]): M[ReadHandle] =
      EitherT(lift(Open(file, offset, limit)))

    /** Read a chunk of data from the file represented by the given handle.
      *
      * An empty `Vector` signals that all data has been read.
      */
    def read(rh: ReadHandle): M[Vector[Data]] =
      EitherT(lift(Read(rh)))

    /** Closes the given read handle, freeing any resources it was using. */
    def close(rh: ReadHandle): F[Unit] =
      lift(Close(rh))

    ////

    private def lift[A](rf: ReadFile[A]): F[A] =
      Free.liftF(S1.inj(Coyoneda.lift(rf)))
  }

  object Unsafe {
    implicit def apply[S[_]](implicit S0: Functor[S], S1: ReadFileF :<: S): Unsafe[S] =
      new Unsafe[S]
  }
}
