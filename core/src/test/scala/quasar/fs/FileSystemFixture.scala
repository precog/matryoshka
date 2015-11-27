package quasar
package fs

import quasar.Predef._
import quasar.fp._
import scala.collection.IndexedSeq

import scalaz._
import scalaz.std.list._
import scalaz.std.vector._
import scalaz.syntax.monad._
import scalaz.syntax.std.option._
import scalaz.stream._
import scalaz.concurrent.Task

trait FileSystemFixture {
  import FileSystemFixture._, InMemory._

  type F[A]              = Free[FileSystem, A]
  type InMemFix[A]       = ReadWriteT[InMemoryFs, A]
  type InMemIO[A]        = StateT[Task, InMemState, A]
  type InMemResult[A]    = FileSystemErrT[InMemIO, A]
  type InMemFixIO[A]     = ReadWriteT[InMemIO, A]
  type InMemFixResult[A] = FileSystemErrT[InMemFixIO, A]

  val query  = QueryFile.Ops[FileSystem]
  val read   = ReadFile.Ops[FileSystem]
  val write  = WriteFile.Ops[FileSystem]
  val manage = ManageFile.Ops[FileSystem]

  val emptyMem = InMemState.empty

  val hoistInMem: InMemoryFs ~> InMemIO =
    Hoist[StateT[?[_], InMemState, ?]].hoist(pointNT[Task])

  val hoistFix: InMemFix ~> InMemFixIO =
    Hoist[StateT[?[_], ReadWrites, ?]].hoist(hoistInMem)

  val interpretInMem: FileSystem ~> InMemoryFs =
    interpretFileSystem(queryFile, readFile, writeFile, manageFile)

  val run: F ~> InMemIO =
    hoistInMem compose[F] hoistFree(interpretInMem)

  val interpretInMemFix: FileSystem ~> InMemFix =
    interpretFileSystem[InMemFix](
      liftMT[InMemoryFs, ReadWriteT] compose queryFile,
      interceptReads(readFile),
      amendWrites(writeFile),
      liftMT[InMemoryFs, ReadWriteT] compose manageFile)

  val runFixIO: F ~> InMemFixIO =
    hoistFix compose[F] hoistFree(interpretInMemFix)

  val runResult: FileSystemErrT[F, ?] ~> InMemResult =
    Hoist[FileSystemErrT].hoist(run)

  val runFixResult: FileSystemErrT[F, ?] ~> InMemFixResult =
    Hoist[FileSystemErrT].hoist(runFixIO)

  def runLog[A](p: Process[FileSystemErrT[F, ?], A]): InMemResult[IndexedSeq[A]] =
    p.translate[InMemResult](runResult).runLog

  def evalLogZero[A](p: Process[FileSystemErrT[F, ?], A]): Task[FileSystemError \/ IndexedSeq[A]] =
    runLog(p).run.eval(emptyMem)

  def runLogFix[A](p: Process[FileSystemErrT[F, ?], A]): InMemFixResult[IndexedSeq[A]] =
    p.translate[InMemFixResult](runFixResult).runLog

  def runLogWithRW[A](rs: Reads, ws: Writes, p: Process[FileSystemErrT[F, ?], A]): InMemResult[IndexedSeq[A]] =
    EitherT(runLogFix(p).run.eval((rs, ws)))

  def runLogWithReads[A](rs: Reads, p: Process[FileSystemErrT[F, ?], A]): InMemResult[IndexedSeq[A]] =
    runLogWithRW(rs, List(), p)

  def runLogWithWrites[A](ws: Writes, p: Process[FileSystemErrT[F, ?], A]): InMemResult[IndexedSeq[A]] =
    runLogWithRW(List(), ws, p)
}

object FileSystemFixture {
  import ReadFile._, WriteFile._

  type Reads      = List[FileSystemError \/ Vector[Data]]
  type Writes     = List[Vector[FileSystemError]]
  type ReadWrites = (Reads, Writes)

  type ReadWriteT[F[_], A] = StateT[F, ReadWrites, A]

  /** Transforms a [[ReadFile]] interpreter, intercepting responses to `Read`s
    * until the provided state is empty, falling back to the base interperter
    * thereafter. All other operations use the base interpreter.
    *
    * For some base interpreter, f,
    *   interceptReads(f).eval((Nil, Nil)) == f
    */
  def interceptReads[F[_]: Monad](f: ReadFile ~> F): ReadFile ~> ReadWriteT[F, ?] =
    new (ReadFile ~> ReadWriteT[F, ?]) {
      def apply[A](rf: ReadFile[A]) = rf match {
        case Read(_) => for {
          nr <- nextReadL.st.lift[F]
          rs <- restReadsL.st.lift[F]
          _  <- (readsL := rs.orZero).lift[F]
          rd <- nr.cata(_.point[ReadWriteT[F, ?]], f(rf).liftM[ReadWriteT])
        } yield rd

        case _ => f(rf).liftM[ReadWriteT]
      }
    }

  /** Transforms a [[WriteFile]] interpreter, amending errors to `Write`s
    * until the provided state is empty, falling back to the base interpreter
    * thereafter. All other operations use the base interpreter.
    *
    * For some base interpreter, f,
    *   amendWrites(f).eval((Nil, Nil)) == f
    */
  def amendWrites[F[_]: Monad](f: WriteFile ~> F): WriteFile ~> ReadWriteT[F, ?] =
    new (WriteFile ~> ReadWriteT[F, ?]) {
      def apply[A](wf: WriteFile[A]) = wf match {
        case Write(h, d) => for {
          nw <- nextWriteL.st.lift[F]
          ws <- restWritesL.st.lift[F]
          _  <- (writesL := ws.orZero).lift[F]
          es <- f(Write(h, d)).liftM[ReadWriteT]
        } yield es ++ nw.orZero

        case _ => f(wf).liftM[ReadWriteT]
      }
    }

  ////

  private val readsL: ReadWrites @> Reads =
    Lens.firstLens

  private val nextReadL: ReadWrites @?> (FileSystemError \/ Vector[Data]) =
    PLens.listHeadPLens <=< ~readsL

  private val restReadsL: ReadWrites @?> (List[FileSystemError \/ Vector[Data]]) =
    PLens.listTailPLens <=< ~readsL

  private val writesL: ReadWrites @> Writes =
    Lens.secondLens

  private val nextWriteL: ReadWrites @?> Vector[FileSystemError] =
    PLens.listHeadPLens <=< ~writesL

  private val restWritesL: ReadWrites @?> List[Vector[FileSystemError]] =
    PLens.listTailPLens <=< ~writesL
}
