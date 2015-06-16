package slamdata.engine.api

import org.specs2.mutable._

import scalaz.concurrent._
import scalaz.stream.{Process}
import scodec.bits._

import slamdata.engine.fs.{Path}

class ZipSpecs extends Specification {
  "zipFiles" should {
    import Zip._

    val f1: Process[Task, ByteVector] = Process.emit(ByteVector(Array[Byte](0)))

    // For testing, capture all the bytes from a process, parse them with a
    // ZipInputStream, and capture just the size of the contents of each file.
    def list(p: Process[Task, ByteVector]): Task[List[(Path, Long)]] = {
      def count(is: java.io.InputStream): Long = {
        def loop(n: Long): Long = if (is.read == -1) n else loop(n+1)
        loop(0)
      }

      Task.delay {
        val bytes = p.runLog.run.reduce(_ ++ _)
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
  }
}
