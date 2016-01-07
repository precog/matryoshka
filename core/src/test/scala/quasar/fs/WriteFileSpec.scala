package quasar
package fs

import quasar.Predef._
import quasar.fp._

import org.specs2.mutable.Specification
import org.specs2.ScalaCheck
import pathy.scalacheck.PathyArbitrary._
import scalaz._
import scalaz.std.vector._
import scalaz.syntax.monad._
import scalaz.stream._

class WriteFileSpec extends Specification with ScalaCheck with FileSystemFixture {
  import DataGen._, FileSystemError._, PathError2._

  "WriteFile" should {

    "append should consume input and close write handle when finished" ! prop {
      (f: AFile, xs: Vector[Data]) =>

      val p = write.append(f, xs.toProcess).drain ++ read.scanAll(f)

      type Result[A] = FileSystemErrT[MemStateTask,A]

      p.translate[Result](MemTask.interpretT).runLog.run
        .leftMap(_.wm)
        .run(emptyMem)
        .run must_== ((Map.empty, \/.right(xs)))
    }

    "append should aggregate all `PartialWrite` errors and emit the sum" ! prop {
      (f: AFile, xs: Vector[Data]) => (xs.length > 1) ==> {
        val wf = WriteFailed(Data.Str("foo"), "b/c reasons")
        val ws = Vector(wf) +: xs.tail.as(Vector(PartialWrite(1)))

        MemFixTask.runLogWithWrites(ws.toList, write.append(f, xs.toProcess))
          .run.eval(emptyMem)
          .run.toEither must beRight(Vector(wf, PartialWrite(xs.length - 1)))
      }
    }

    "save should replace existing file" ! prop {
      (f: AFile, xs: Vector[Data], ys: Vector[Data]) =>

      val p = (write.append(f, xs.toProcess) ++ write.save(f, ys.toProcess)).drain ++ read.scanAll(f)

      MemTask.runLogEmpty(p).run must_== \/-(ys)
    }

    "save with empty input should create an empty file" ! prop { f: AFile =>
      val p = write.save(f, Process.empty) ++
              (query.fileExists(f)).liftM[Process]

      MemTask.runLogEmpty(p).run must_== \/-(Vector(true))
    }

    "save should leave existing file untouched on failure" ! prop {
      (f: AFile, xs: Vector[Data], ys: Vector[Data]) => (xs.nonEmpty && ys.nonEmpty) ==> {
        val err = WriteFailed(Data.Str("bar"), "")
        val ws = (xs ++ ys.init).as(Vector()) :+ Vector(err)
        val p = (write.append(f, xs.toProcess) ++ write.save(f, ys.toProcess)).drain ++ read.scanAll(f)

        MemFixTask.runLogWithWrites(ws.toList, p).run
          .leftMap(_.contents.keySet)
          .run(emptyMem)
          .run must_== ((Set(f), \/.right(xs)))
      }
    }

    "create should fail if file exists" ! prop {
      (f: AFile, xs: Vector[Data], ys: Vector[Data]) =>

      val p = write.append(f, xs.toProcess) ++ write.create(f, ys.toProcess)

      MemTask.runLogEmpty(p).run must_== -\/(PathError(PathExists(f)))
    }

    "create should consume all input into a new file" ! prop {
      (f: AFile, xs: Vector[Data]) =>

      val p = write.create(f, xs.toProcess) ++ read.scanAll(f)

      MemTask.runLogEmpty(p).run must_== \/-(xs)
    }

    "replace should fail if the file does not exist" ! prop {
      (f: AFile, xs: Vector[Data]) =>

        val p = write.replace(f, xs.toProcess)

        MemTask.runLogEmpty(p).run must_== -\/(PathError(PathNotFound(f)))
    }

    "replace should leave the existing file untouched on failure" ! prop {
      (f: AFile, xs: Vector[Data], ys: Vector[Data]) => (xs.nonEmpty && ys.nonEmpty) ==> {
        val err = WriteFailed(Data.Int(42), "")
        val ws = (xs ++ ys.init).as(Vector()) :+ Vector(err)
        val p = (write.append(f, xs.toProcess) ++ write.replace(f, ys.toProcess)).drain ++ read.scanAll(f)

        MemFixTask.runLogWithWrites(ws.toList, p).run
          .leftMap(_.contents.keySet)
          .run(emptyMem)
          .run must_== ((Set(f), \/.right(xs)))
      }
    }

    "replace should overwrite the existing file with new data" ! prop {
      (f: AFile, xs: Vector[Data], ys: Vector[Data]) =>

      val p = write.save(f, xs.toProcess) ++ write.replace(f, ys.toProcess) ++ read.scanAll(f)

      MemTask.runLogEmpty(p).run must_== \/-(ys)
    }
  }
}
