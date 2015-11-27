package quasar
package fs

import quasar.Predef._
import quasar.fp._

import scalaz._, Scalaz._
import scalaz.stream._

sealed trait WriteFile[A]

object WriteFile {
  final case class WriteHandle(run: Long) extends scala.AnyVal

  object WriteHandle {
    implicit val writeHandleShow: Show[WriteHandle] =
      Show.showFromToString

    implicit val writeHandleOrder: Order[WriteHandle] =
      Order.orderBy(_.run)
  }

  final case class Open(file: AFile)
    extends WriteFile[FileSystemError \/ WriteHandle]

  final case class Write(h: WriteHandle, chunk: Vector[Data])
    extends WriteFile[Vector[FileSystemError]]

  final case class Close(h: WriteHandle)
    extends WriteFile[Unit]

  final class Ops[S[_]](implicit S: Functor[S], val unsafe: Unsafe[S]) {
    import FileSystemError._, PathError2._
    import ManageFile.MoveSemantics

    type F[A]    = unsafe.F[A]
    type M[A]    = unsafe.M[A]
    type G[E, A] = EitherT[F, E, A]

    /** Returns a channel that appends chunks of data to the given file, creating
      * it if it doesn't exist. Any errors encountered while writing are emitted,
      * all attempts are made to continue consuming input until the source is
      * exhausted.
      */
    def appendChannel(dst: AFile): Channel[M, Vector[Data], Vector[FileSystemError]] = {
      def writeChunk(h: WriteHandle): Vector[Data] => M[Vector[FileSystemError]] =
        xs => unsafe.write(h, xs).liftM[FileSystemErrT]

      Process.await(unsafe.open(dst))(h =>
        channel.lift(writeChunk(h))
          .onComplete(Process.eval_[M, Unit](unsafe.close(h).liftM[FileSystemErrT])))
    }

    /** Same as `append` but accepts chunked [[Data]]. */
    def appendChunked(dst: AFile, src: Process[F, Vector[Data]]): Process[M, FileSystemError] = {
      val accumPartialWrites =
        process1.id[FileSystemError]
          .map(partialWrite.getOption)
          .reduceSemigroup
          .pipe(process1.stripNone)
          .map(PartialWrite)

      val dropPartialWrites =
        process1.filter[FileSystemError](e => !partialWrite.isMatching(e))

      // NB: We don't use `through` as we want to ensure the `Open` from
      //     `appendChannel` happens even if the src process never emits.
      appendChannel(dst)
        .zipWith(src.translate[M](liftMT[F, FileSystemErrT]))((f, o) => f(o))
        .eval
        .flatMap(Process.emitAll)
        .flatMap(e => Process.emitW(e) ++ Process.emitO(e))
        .pipe(process1.multiplex(dropPartialWrites, accumPartialWrites))
    }

    /** Appends data to the given file, creating it if it doesn't exist. May not
      * write all values from `src` in the presence of errors, will emit a
      * [[FileSystemError]] for each input value that failed to write.
      */
    def append(dst: AFile, src: Process[F, Data]): Process[M, FileSystemError] =
      appendChunked(dst, src map (Vector(_)))

    /** Same as `save` but accepts chunked [[Data]]. */
    def saveChunked(dst: AFile, src: Process[F, Vector[Data]])
                   (implicit MF: ManageFile.Ops[S])
                   : Process[M, FileSystemError] = {

      saveChunked0(dst, src, MoveSemantics.Overwrite)
    }

    /** Write the data stream to the given path, replacing any existing contents,
      * atomically. Any errors during writing will abort the entire operation
      * leaving any existing values unaffected.
      */
    def save(dst: AFile, src: Process[F, Data])
            (implicit MF: ManageFile.Ops[S])
            : Process[M, FileSystemError] = {

      saveChunked(dst, src map (Vector(_)))
    }

    /** Same as `create` but accepts chunked [[Data]]. */
    def createChunked(dst: AFile, src: Process[F, Vector[Data]])
                     (implicit QF: QueryFile.Ops[S], MF: ManageFile.Ops[S])
                     : Process[M, FileSystemError] = {

      def shouldNotExist: M[FileSystemError] =
        MonadError[G, FileSystemError].raiseError(PathError(PathExists(dst)))

      fileExistsM(dst).liftM[Process].ifM(
        shouldNotExist.liftM[Process],
        saveChunked0(dst, src, MoveSemantics.FailIfExists))
    }

    /** Create the given file with the contents of `src`. Fails if already exists. */
    def create(dst: AFile, src: Process[F, Data])
              (implicit QF: QueryFile.Ops[S], MF: ManageFile.Ops[S])
              : Process[M, FileSystemError] = {

      createChunked(dst, src map (Vector(_)))
    }

    /** Same as `replace` but accepts chunked [[Data]]. */
    def replaceChunked(dst: AFile, src: Process[F, Vector[Data]])
                      (implicit QF: QueryFile.Ops[S], MF: ManageFile.Ops[S])
                      : Process[M, FileSystemError] = {

      def shouldExist: M[FileSystemError] =
        MonadError[G, FileSystemError].raiseError(PathError(PathNotFound(dst)))

      fileExistsM(dst).liftM[Process].ifM(
        saveChunked0(dst, src, MoveSemantics.FailIfMissing),
        shouldExist.liftM[Process])
    }

    /** Replace the contents of the given file with `src`. Fails if the file
      * doesn't exist.
      */
    def replace(dst: AFile, src: Process[F, Data])
               (implicit QF: QueryFile.Ops[S], MF: ManageFile.Ops[S])
               : Process[M, FileSystemError] = {

      replaceChunked(dst, src map (Vector(_)))
    }

    ////

    private def fileExistsM(file: AFile)
                           (implicit QF: QueryFile.Ops[S])
                           : M[Boolean] = {

      QF.fileExists(file).liftM[FileSystemErrT]
    }

    private def saveChunked0(dst: AFile, src: Process[F, Vector[Data]], sem: MoveSemantics)
                            (implicit MF: ManageFile.Ops[S])
                            : Process[M, FileSystemError] = {

      val fsPathNotFound = pathError composePrism pathNotFound

      def cleanupTmp(tmp: AFile)(t: Throwable): Process[M, Nothing] =
        Process.eval_[M, Unit](MF.delete(tmp))
          .causedBy(Cause.Error(t))

      def attemptMove(tmp: AFile): M[Unit] =
        MonadError[G, FileSystemError].handleError(MF.moveFile(tmp, dst, sem))(e =>
          if (fsPathNotFound.getOption(e) exists (_ == tmp)) ().point[M]
          else MonadError[G, FileSystemError].raiseError(e))

      MF.tempFileNear(dst).liftM[FileSystemErrT].liftM[Process] flatMap { tmp =>
        appendChunked(tmp, src).terminated.take(1)
          .flatMap(_.cata(
            werr => MF.delete(tmp).as(werr).liftM[Process],
            Process.eval_[M, Unit](attemptMove(tmp))))
          .onFailure(cleanupTmp(tmp))
      }
    }
  }

  object Ops {
    implicit def apply[S[_]](implicit S: Functor[S], U: Unsafe[S]): Ops[S] =
      new Ops[S]
  }

  /** Low-level, unsafe operations. Clients are responsible for resource-safety
    * when using these.
    */
  final class Unsafe[S[_]](implicit S0: Functor[S], S1: WriteFileF :<: S) {
    type F[A] = Free[S, A]
    type M[A] = FileSystemErrT[F, A]

    /** Returns a write handle for the specified file which may be used to
      * append data to the file it represents, creating it if necessary.
      *
      * Care must be taken to `close` the handle when it is no longer needed
      * to avoid potential resource leaks.
      */
    def open(file: AFile): M[WriteHandle] =
      EitherT(lift(Open(file)))

    /** Write a chunk of data to the file represented by the write handle.
      *
      * Attempts to write as much of the chunk as possible, even if parts of
      * it fail, any such failures will be returned in the output `Vector`
      * An empty `Vector` means the entire chunk was written successfully.
      */
    def write(h: WriteHandle, chunk: Vector[Data]): F[Vector[FileSystemError]] =
      lift(Write(h, chunk))

    /** Close the write handle, freeing any resources it was using. */
    def close(h: WriteHandle): F[Unit] =
      lift(Close(h))

    ////

    private def lift[A](wf: WriteFile[A]): F[A] =
      Free.liftF(S1.inj(Coyoneda.lift(wf)))
  }

  object Unsafe {
    implicit def apply[S[_]](implicit S0: Functor[S], S1: WriteFileF :<: S): Unsafe[S] =
      new Unsafe[S]
  }
}
