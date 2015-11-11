package quasar
package fs

import quasar.Predef._
import quasar.fp._

import java.lang.RuntimeException

import monocle.std.{disjunction => D}

import pathy.Path._

import scala.annotation.tailrec

import scalaz.{EphemeralStream => EStream, _}, Scalaz._
import scalaz.concurrent.Task
import scalaz.stream._

class ReadFilesSpec extends FileSystemTest[FileSystem](FileSystemTest.allFsUT) {
  import ReadFilesSpec._, FileSystemError._, PathError2._
  import ReadFile._

  val read   = ReadFile.Ops[FileSystem]
  val write  = WriteFile.Ops[FileSystem]
  val manage = ManageFile.Ops[FileSystem]

  def loadForReading(run: Run): FsTask[Unit] = {
    type P[A] = Process[write.M, A]

    def loadDatum(td: TestDatum) = {
      val src = chunkStream(td.data, 1000)
        .foldRight(Process.halt: Process0[Vector[Data]])(xs => p => Process.emit(xs) ++ p)
      write.appendChunked(td.file, src)
    }

    List(emptyFile, smallFile, largeFile, veryLongFile)
      .foldMap(loadDatum)(PlusEmpty[P].monoid)
      .flatMap(err => Process.fail(new RuntimeException(err.shows)))
      .translate[FsTask](runT(run))
      .run
  }

  def deleteForReading(run: Run): FsTask[Unit] =
    runT(run)(manage.deleteDir(readsPrefix))

  fileSystemShould { _ => implicit run =>
    "Reading Files" should {
      // Load read-only data
      step((deleteForReading(run).run.void *> loadForReading(run).run.void).run)

      "open returns FileNotFound when file DNE" >>* {
        val dne = rootDir </> dir("doesnt") </> file("exist")
        read.open(dne, Natural._0, None).run map { r =>
          r.toEither must beLeft(PathError(FileNotFound(dne)))
        }
      }

      "read unopened file handle returns UnknownReadHandle" >>* {
        val h = ReadHandle(42)
        read.read(h).run map { r =>
          r.toEither must beLeft(UnknownReadHandle(h))
        }
      }

      "read closed file handle returns UnknownReadHandle" >>* {
        val r = for {
          h  <- read.open(smallFile.file, Natural._0, None)
          _  <- read.close(h).liftM[FileSystemErrT]
          xs <- read.read(h)
        } yield xs

        r.run map { x =>
          (D.left composePrism unknownReadHandle).isMatching(x) must beTrue
        }
      }

      "scan an empty file succeeds, yielding no data" >> {
        val r = runLogT(run, read.scanAll(emptyFile.file))
        r.runEither must beRight((xs: scala.collection.IndexedSeq[Data]) => xs must beEmpty)
      }

      "scan with offset zero and no limit reads entire file" >> {
        val r = runLogT(run, read.scan(smallFile.file, Natural._0, None))
        r.runEither must beRight(smallFile.data.toIndexedSeq)
      }

      "scan with offset k > 0 and no limit skips first k data" >> {
        val k = Natural._9 * Natural._2
        val r = runLogT(run, read.scan(smallFile.file, k, None))
        val d = smallFile.data.zip(EStream.iterate(0)(_ + 1))
                  .dropWhile(_._2 < k.run.toInt).map(_._1)

        r.runEither must beRight(d.toIndexedSeq)
      }

      "scan with offset zero and limit j stops after j data" >> {
        val j = Positive._5
        val r = runLogT(run, read.scan(smallFile.file, Natural._0, Some(j)))

        r.runEither must beRight(smallFile.data.take(j.run.toInt).toIndexedSeq)
      }

      "scan with offset k and limit j takes j data, starting from k" >> {
        val j = Positive._5 * Positive._5 * Positive._5
        val r = runLogT(run, read.scan(largeFile.file, Natural.fromPositive(j), Some(j)))
        val d = largeFile.data.zip(EStream.iterate(0)(_ + 1))
                  .dropWhile(_._2 < j.run.toInt).map(_._1)
                  .take(j.run.toInt)

        r.runEither must beRight(d.toIndexedSeq)
      }

      "scan with offset zero and limit j, where j > |file|, stops at end of file" >> {
        val j = Positive._5 * Positive._5 * Positive._5
        val r = runLogT(run, read.scan(smallFile.file, Natural._0, Some(j)))

        (j.run.toInt must beGreaterThan(smallFile.data.length)) and
        (r.runEither must beRight(smallFile.data.toIndexedSeq))
      }

      // TODO: What is the expected behavior here?
      "scan with offset k, where k > |file|, and no limit ???" >> todo

      "scan very long file is stack-safe" >> {
        runLogT(run, read.scanAll(veryLongFile.file).foldMap(_ => 1))
          .runEither must beRight(List(veryLongFile.data.length).toIndexedSeq)
      }

      // TODO: This was copied from existing tests, but what is being tested?
      "scan very long file twice" >> {
        val r = runLogT(run, read.scanAll(veryLongFile.file).foldMap(_ => 1))
        val l = List(veryLongFile.data.length).toIndexedSeq

        (r.runEither must beRight(l)) and (r.runEither must beRight(l))
      }

      step(deleteForReading(run).runVoid)
    }; ()
  }
}

object ReadFilesSpec {
  import FileSystemTest._

  final case class TestDatum(file: AbsFile[Sandboxed], data: EStream[Data])

  val readsPrefix: AbsDir[Sandboxed] = rootDir </> dir("forreading")

  val emptyFile = TestDatum(
    readsPrefix </> file("empty"),
    EStream())

  val smallFile = TestDatum(
    readsPrefix </> file("small"),
    manyDocs(100))

  val largeFile = {
    val sizeInMb = 10.0
    val bytesPerDoc = 750
    val numDocs = (sizeInMb * 1024 * 1024 / bytesPerDoc).toInt

    def jsonTree(depth: Int): Data =
      if (depth == 0)
        Data.Arr(Data.Str("abc") :: Data.Int(123) :: Data.Str("do, re, mi") :: Nil)
      else
        Data.Obj(ListMap("left" -> jsonTree(depth-1), "right" -> jsonTree(depth-1)))

    def json(i: Int) =
      Data.Obj(ListMap("seq" -> Data.Int(i), "filler" -> jsonTree(3)))

    TestDatum(
      readsPrefix </> file("large"),
      EStream.range(1, numDocs) map (json))
  }

  val veryLongFile = TestDatum(
    readsPrefix </> dir("length") </> file("very.long"),
    manyDocs(100000))

  ////

  private def chunkStream[A](s: EStream[A], size: Int): EStream[Vector[A]] = {
    @tailrec
    def chunk0(xs: EStream[A], v: Vector[A], i: Int): (EStream[A], Vector[A]) =
      if (i == 0)
        (xs, v)
      else
        (xs.headOption, xs.tailOption) match {
          case (Some(a), Some(xss)) =>
            chunk0(xss, v :+ a, i - 1)
          case (Some(a), None) =>
            (EStream(), v :+ a)
          case _ =>
            (EStream(), v)
        }

    EStream.unfold(chunk0(s, Vector(), size)) { case (ys, v) =>
      if (v.isEmpty) None else Some((v, chunk0(ys, Vector(), size)))
    }
  }
}
