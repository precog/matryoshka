package quasar
package fs

import org.scalacheck.{Gen, Arbitrary}
import pathy.Path._
import pathy.scalacheck._
import pathy.scalacheck.PathOf._
import quasar.Predef._
import quasar.fp._
import quasar.fp.free.{Interpreter, SpecializedInterpreter}
import scala.collection.IndexedSeq

import scalaz._, Scalaz._
import scalaz.scalacheck.ScalaCheckBinding._
import scalaz.scalacheck.ScalazArbitrary._
import quasar.DataGen._
import scalaz.stream._
import scalaz.concurrent.Task

case class AlphaCharacters(value: String)

object AlphaCharacters {
  implicit val arb: Arbitrary[AlphaCharacters] =
    Arbitrary(Gen.nonEmptyListOf(Gen.alphaChar).map(chars => AlphaCharacters(chars.mkString)))
  implicit val show: Show[AlphaCharacters] = Show.shows(_.value)
}

trait FileSystemFixture {
  import FileSystemFixture._, InMemory._

  val query  = QueryFile.Ops[FileSystem]
  val read   = ReadFile.Ops[FileSystem]
  val write  = WriteFile.Ops[FileSystem]
  val manage = ManageFile.Ops[FileSystem]

  val emptyMem = InMemState.empty

  import posixCodec.printPath

  case class SingleFileMemState(fileOfCharacters: AbsFileOf[AlphaCharacters], contents: Vector[Data]) {
    def file = fileOfCharacters.path
    def path = printPath(file)
    def state = InMemState fromFiles Map(file -> contents)
    def parent = fileParent(file)
    def filename = fileName(file)
  }

  def segAt[B,T,S](index: Int, path: pathy.Path[B,T,S]): Option[FileName \/ DirName] = {
    scala.Predef.require(index >= 0)
    val list =
      pathy.Path.flatten(none, none, none, DirName(_).right.some,FileName(_).left.some,path).toIList.unite
    list.drop(index).headOption
  }

  case class NonEmptyDir(
                          dirOfCharacters: AbsDirOf[AlphaCharacters],
                          filesInDir: NonEmptyList[(RelFileOf[AlphaCharacters], Vector[Data])]
                        ) {
    def dir = dirOfCharacters.path
    def state = {
      val fileMapping = filesInDir.map{ case (relFile,data) => (dir </> relFile.path, data)}
      InMemState fromFiles fileMapping.toList.toMap
    }
    def relFiles = filesInDir.unzip._1.map(_.path)
    def ls = relFiles.map(segAt(0,_)).list.flatten.distinct
      .sortBy((seg: FileName \/ DirName) => printPath(seg.fold(file1, dir1)))
  }

  implicit val arbSingleFileMemState: Arbitrary[SingleFileMemState] = Arbitrary(
    (Arbitrary.arbitrary[AbsFileOf[AlphaCharacters]] |@|
      Arbitrary.arbitrary[Vector[Data]])(SingleFileMemState.apply))

  implicit val arbNonEmptyDir: Arbitrary[NonEmptyDir] = Arbitrary(
    (Arbitrary.arbitrary[AbsDirOf[AlphaCharacters]] |@|
      Arbitrary.arbitrary[NonEmptyList[(RelFileOf[AlphaCharacters], Vector[Data])]])(NonEmptyDir.apply))

  type F[A]            = Free[FileSystem, A]
  type InMemFix[A]     = ReadWriteT[InMemoryFs, A]
  type MemStateTask[A] = StateT[Task, InMemState,A]
  type MemStateFix[A]  = ReadWriteT[MemStateTask,A]

  object Mem extends Interpreter[FileSystem,InMemoryFs](
    interpretTerm = fileSystem
  ) {
    def interpret[E,A](term: EitherT[F,E,A]): InMemoryFs[E \/ A] =
      interpretT[EitherT[?[_],E,?]].apply(term).run
    def interpret[L:Monoid,E,A](term: EitherT[WriterT[F,L,?],E,A]): InMemoryFs[(L,E \/ A)] = {
      type T1[M[_],A] = EitherT[M,E,A]
      type T2[M[_],A] = WriterT[M,L,A]
      interpretT2[T1,T2].apply(term).run.run
    }

  }

  val hoistTask: InMemoryFs ~> MemStateTask =
    Hoist[StateT[?[_], InMemState, ?]].hoist(pointNT[Task])

  object MemTask extends SpecializedInterpreter[FileSystem, MemStateTask](
    interpretTerm = hoistTask compose Mem.interpretTerm
  ) {
    def runLogEmpty[A](p: Process[FileSystemErrT[F,?],A]): Task[FileSystemError \/ IndexedSeq[A]] =
      runLog(p).run.eval(emptyMem)
  }

  val hoistFix: ReadWriteT[InMemoryFs,?] ~> MemStateFix =
    Hoist[StateT[?[_], ReadWrites, ?]].hoist(hoistTask)

  val readWrite: FileSystem ~> ReadWriteT[InMemoryFs,?] = interpretFileSystem[InMemFix](
    liftMT[InMemoryFs, ReadWriteT] compose queryFile,
    interceptReads(readFile),
    amendWrites(writeFile),
    liftMT[InMemoryFs, ReadWriteT] compose manageFile)

  object MemFixTask extends SpecializedInterpreter[FileSystem, MemStateFix](
    interpretTerm = hoistFix compose readWrite
  ) {
    def runLogWithRW[E,A](rs: Reads, ws: Writes, p: Process[EitherT[F,E, ?], A]): EitherT[MemStateTask,E,IndexedSeq[A]] =
      EitherT(runLog(p).run.eval((rs, ws)))

    def runLogWithReads[E,A](rs: Reads, p: Process[EitherT[F,E, ?], A]): EitherT[MemStateTask,E,IndexedSeq[A]] =
      runLogWithRW(rs, List(), p)

    def runLogWithWrites[E,A](ws: Writes, p: Process[EitherT[F,E, ?], A]): EitherT[MemStateTask,E,IndexedSeq[A]] =
      runLogWithRW(List(), ws, p)
  }
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
