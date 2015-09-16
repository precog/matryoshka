package quasar.api

import quasar.Predef._

import org.specs2.mutable._

import scalaz._, Scalaz._
import scalaz.concurrent._
import scalaz.stream.{Process}

import scodec.bits._
import scodec.interop.scalaz._

import quasar.fs.{Path}

class ZipSpecs extends Specification {
  args.report(showtimes=true)

  "zipFiles" should {
    import Zip._

    def rand = new java.util.Random
    def randBlock = Array.fill[Byte](1000)(rand.nextInt.toByte)

    val f1: Process[Task, ByteVector] = Process.emit(ByteVector.view(Array[Byte](0)))
    val f2: Process[Task, ByteVector] = Process.emit(ByteVector.view(Array.fill[Byte](1000)(0)))
    def f3: Process[Task, ByteVector] = Process.emit(ByteVector.view(randBlock))
    def f4: Process[Task, ByteVector] = {
      val block = ByteVector.view(randBlock)
      Process.emitAll(Vector.fill(1000)(block))
    }

    def unzip[A](f: java.io.InputStream => A)(p: Process[Task, ByteVector]): Task[List[(Path, A)]] =
      Task.delay {
        val bytes = p.runLog.run.toList.concatenate  // FIXME: this means we can't use this to test anything big
        val is = new java.io.ByteArrayInputStream(bytes.toArray)
        val zis = new java.util.zip.ZipInputStream(is)
        Stream.continually(zis.getNextEntry).takeWhile(_ != null).map { entry =>
          Path(entry.getName) -> f(zis)
        }.toList
      }

    // For testing, capture all the bytes from a process, parse them with a
    // ZipInputStream, and capture just the size of the contents of each file.
    def counts(p: Process[Task, ByteVector]): Task[List[(Path, Long)]] = {
      def count(is: java.io.InputStream): Long = {
        def loop(n: Long): Long = if (is.read ≟ -1) n else loop(n+1)
        loop(0)
      }
      unzip(count)(p)
    }

    def bytes(p: Process[Task, ByteVector]): Task[List[(Path, ByteVector)]] = {
      def read(is: java.io.InputStream): ByteVector = {
        def loop(acc: ByteVector): ByteVector = {
          val buffer = new Array[Byte](16*1024)
          val n = is.read(buffer)
          if (n <= 0) acc else loop(acc ++ ByteVector.view(buffer).take(n))
        }
        loop(ByteVector.empty)
      }
      unzip(read)(p)
    }

    "zip one file" in {
      val z = zipFiles(List(Path("foo") -> f1))
      counts(z).run must_== List(Path("foo") -> 1)
    }

    "zip two files" in {
      val z = zipFiles(List(
        Path("foo") -> f1,
        Path("bar") -> f1))
      counts(z).run must_== List(Path("foo") -> 1, Path("bar") -> 1)
    }

    "zip one larger file" in {
      val z = zipFiles(List(
        Path("foo") -> f2))
      counts(z).run must_== List(Path("foo") -> 1000)
    }

    "zip one file of random bytes" in {
      val z = zipFiles(List(
        Path("foo") -> f3))
      counts(z).run must_== List(Path("foo") -> 1000)
    }

    "zip one large file of random bytes" in {
      val z = zipFiles(List(
        Path("foo") -> f4))
      counts(z).run must_== List(Path("foo") -> 1000*1000)
    }

    "zip many large files of random bytes (100 MB)" in {
      // NB: this is mainly a performance check. Right now it's about 2 seconds for 100 MB for me.
      val Files = 100
      val RawSize = Files*1000*1000L
      val MinExpectedSize = (RawSize*0.005).toInt
      val MaxExpectedSize = (RawSize*0.010).toInt

      val paths = (0 until Files).toList.map(i => Path("foo" + i))
      val z = zipFiles(paths.map(_ -> f4))

      // NB: can't use my naive `list` function on a large file
      z.map(_.size).sum.runLog.run(0) must beBetween(MinExpectedSize, MaxExpectedSize)
    }

    "zip many large files of random bytes (10 GB)" in {
      // NB: comment this to verify the heap is not consumed
      skipped("too slow to run every time (~2 minutes)")

      val Files = 10*1000
      val RawSize = Files*1000*1000L
      val MinExpectedSize = (RawSize*0.005).toInt
      val MaxExpectedSize = (RawSize*0.010).toInt

      val paths = (0 until Files).toList.map(i => Path("foo" + i))
      val z = zipFiles(paths.map(_ -> f4))

      // NB: can't use my naive `list` function on a large file
      z.map(_.size).sum.runLog.run(0) must beBetween(MinExpectedSize, MaxExpectedSize)
    }

    "read twice without conflict" in {
      val z = zipFiles(List(
        Path("foo") -> f2,
        Path("bar") -> f2))

      bytes(z).run must_== bytes(z).run
    }
  }
}
