package quasar
package fs

import quasar.Predef._
import quasar.fp._

import monocle.std.{disjunction => D}
import pathy.Path._
import scalaz._, Scalaz._
import scalaz.concurrent.Task
import scalaz.stream._

class WriteFilesSpec extends FileSystemTest[FileSystem](FileSystemTest.allFsUT) {
  import FileSystemTest._, FileSystemError._
  import WriteFile._

  val query  = QueryFile.Ops[FileSystem]
  val read   = ReadFile.Ops[FileSystem]
  val write  = WriteFile.Ops[FileSystem]
  val manage = ManageFile.Ops[FileSystem]

  val writesPrefix: ADir = rootDir </> dir("forwriting")

  def deleteForWriting(run: Run): FsTask[Unit] =
    runT(run)(manage.delete(writesPrefix))

  fileSystemShould { _ => implicit run =>
    "Writing Files" should {
      step(deleteForWriting(run).runVoid)

      "opening a file should create it" >>* {
        val f = writesPrefix </> dir("opencreates") </> file("f1")

        val r = for {
          h <- write.unsafe.open(f)
          _ <- write.unsafe.close(h).liftM[FileSystemErrT]
          p <- query.fileExists(f)
        } yield p

        r.run map (_.toEither must beRight(true))
      }

      "write to unknown handle returns UnknownWriteHandle" >>* {
        val h = WriteHandle(rootDir </> file("f1"), 42)
        write.unsafe.write(h, Vector()) map { r =>
          r must_== Vector(UnknownWriteHandle(h))
        }
      }

      "write to closed handle returns UnknownWriteHandle" >>* {
        val f = writesPrefix </> dir("d1") </> file("f1")
        val r = for {
          h    <- write.unsafe.open(f)
          _    <- write.unsafe.close(h).liftM[FileSystemErrT]
          errs <- write.unsafe.write(h, Vector()).liftM[FileSystemErrT]
        } yield errs

        r.run.map { xs =>
          D.right
            .composeOptional(vectorFirst[FileSystemError])
            .composePrism(unknownWriteHandle) isMatching (xs) must beTrue
        }
      }

      "append should write data to file" >> {
        val f = writesPrefix </> file("saveone")
        val p = write.append(f, oneDoc.toProcess).drain ++ read.scanAll(f)

        runLogT(run, p).runEither must beRight(oneDoc)
      }

      "append empty input should result in a new file" >> {
        val f = writesPrefix </> file("emptyfile")
        val p = write.append(f, Process.empty).drain ++
                (query.fileExists(f)).liftM[Process]

        runLogT(run, p).run.run must_== \/.right(Vector(true))
      }

      "append two files, one in subdir of the other's parent, should succeed" >> {
        val d = writesPrefix </> dir("subdir1")
        val descendant1 = file[Sandboxed]("subdirfile1")
        val f1 = d </> descendant1
        val descendant2 = dir[Sandboxed]("subdir2") </> file[Sandboxed]("subdirfile2")
        val f2 = d </> descendant2
        val p = write.append(f1, oneDoc.toProcess).drain ++
                write.append(f2, oneDoc.toProcess).drain ++
                query.descendantFiles(d).liftM[Process]

        runLogT(run, p).map(_.flatMap(_.toVector))
          .runEither must beRight(containTheSameElementsAs(List(descendant1, descendant2)))
      }

      step(deleteForWriting(run).runVoid)
    }; ()
  }
}
