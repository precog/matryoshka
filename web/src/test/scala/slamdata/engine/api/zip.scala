package slamdata.engine.api

import org.specs2.mutable._

import scalaz.concurrent._
import scalaz.stream.{Process}
import scodec.bits._

import slamdata.engine.fs.{Path}

class ZipSpecs extends Specification {
  args.report(showtimes=true)

  "zipFiles" should {
    import Zip._

    def rand: Byte = (java.lang.Math.random*256).toByte

    val f1: Process[Task, ByteVector] = Process.emit(ByteVector(Array[Byte](0)))
    val f2: Process[Task, ByteVector] = Process.emit(ByteVector(Array.fill[Byte](1000)(0)))
    def f3: Process[Task, ByteVector] = Process.emit(ByteVector(Array.fill[Byte](1000)(rand)))
    def f4: Process[Task, ByteVector] = Process.emitAll(Vector.fill(1000)(ByteVector(Array.fill[Byte](1000)(rand))))

    // For testing, capture all the bytes from a process, parse them with a
    // ZipInputStream, and capture just the size of the contents of each file.
    def list(p: Process[Task, ByteVector]): Task[List[(Path, Long)]] = {
      def count(is: java.io.InputStream): Long = {
        def loop(n: Long): Long = if (is.read == -1) n else loop(n+1)
        loop(0)
      }

      Task.delay {
        val bytes = p.runLog.run.reduce(_ ++ _)  // FIXME: this means we can't use this to test anything big
        val is = new java.io.ByteArrayInputStream(bytes.toArray)
        val zis = new java.util.zip.ZipInputStream(is)
        Stream.continually(zis.getNextEntry).takeWhile(_ != null).map { entry =>
          Path(entry.getName) -> count(zis)
        }.toList
      }
    }

    "zip one file" in {
      val z = zipFiles(List(Path("foo") -> f1))
      list(z).run must_== List(Path("foo") -> 1)
    }

    "zip two files" in {
      val z = zipFiles(List(
        Path("foo") -> f1,
        Path("bar") -> f1))
      list(z).run must_== List(Path("foo") -> 1, Path("bar") -> 1)
    }

    "zip one larger file" in {
      val z = zipFiles(List(
        Path("foo") -> f2))
      list(z).run must_== List(Path("foo") -> 1000)
    }

    "zip one file of random bytes" in {
      val z = zipFiles(List(
        Path("foo") -> f3))
      list(z).run must_== List(Path("foo") -> 1000)
    }

    "zip one large file of random bytes" in {
      val z = zipFiles(List(
        Path("foo") -> f4))
      list(z).run must_== List(Path("foo") -> 1000*1000)
    }

    "zip many large files of random bytes" in {
      // NB: this is mainly a performance check. Right now it's about 7 seconds for 100 MB for me.
      // That seems reasonable, but the main point is that it doesn't explode the heap.
      val Files = 100
      val MinExpectedSize = Files*1000*1000
      val MaxExpectedSize = (MinExpectedSize*1.001).toInt

      val paths = (0 until Files).toList.map(i => Path("foo" + i))
      val z = zipFiles(paths.map(_ -> f4))

      // NB: can't use my naive `list` function on a large file
      z.map(_.size).sum.runLog.run(0) must beBetween(MinExpectedSize, MaxExpectedSize)
    }

    "read twice without conflict" in {
      val z = zipFiles(List(
        Path("foo") -> f2,
        Path("bar") -> f2))

      val t = z.runLog

      t.run.reduce(_ ++ _) must_== t.run.reduce(_ ++ _)
    }
  }
}
