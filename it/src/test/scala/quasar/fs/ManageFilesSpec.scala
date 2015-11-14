package quasar
package fs

import quasar.Predef._

import pathy.Path._

import scalaz._, Scalaz._
import scalaz.concurrent.Task
import scalaz.stream._

class ManageFilesSpec extends FileSystemTest[FileSystem](FileSystemTest.allFsUT) {
  import FileSystemTest._, FileSystemError._, PathError2._
  import ManageFile._

  val query  = QueryFile.Ops[FileSystem]
  val read   = ReadFile.Ops[FileSystem]
  val write  = WriteFile.Ops[FileSystem]
  val manage = ManageFile.Ops[FileSystem]

  val managePrefix: AbsDir[Sandboxed] = rootDir </> dir("formanage")

  def deleteForManage(run: Run): FsTask[Unit] =
    runT(run)(manage.deleteDir(managePrefix))

  fileSystemShould { _ => implicit run =>
    "Managing Files" should {
      step(deleteForManage(run).runVoid)

      "moving a file should make it available at the new path and not found at the old" >> {
        val f1 = managePrefix </> dir("d1") </> file("f1")
        val f2 = managePrefix </> dir("d2") </> file("f2")
        val p = write.saveF(f1, oneDoc).drain ++
                manage.moveFile(f1, f2, MoveSemantics.FailIfExists)
                  .liftM[Process].drain ++
                read.scanAll(f2).map(_.left[Boolean]) ++
                (query.fileExists(f1).liftM[FileSystemErrT]: query.M[Boolean])
                  .liftM[Process]
                  .map(_.right[Data])

        runLogT(run, p).map(_.toVector.separate)
          .runEither must beRight((oneDoc, Vector(false)))
      }

      "moving a file to an existing path using FailIfExists semantics should fail with PathExists" >> {
        val f1 = managePrefix </> dir("failifexists") </> file("f1")
        val f2 = managePrefix </> dir("failifexists") </> file("f2")
        val expectedFiles = List(Node.File(file("f1")), Node.File(file("f2")))
        val ls = query.ls(managePrefix </> dir("failifexists"))
        val p = write.saveF(f1, oneDoc).drain ++
                write.saveF(f2, oneDoc).drain ++
                manage.moveFile(f1, f2, MoveSemantics.FailIfExists).liftM[Process]

        (execT(run, p).runOption must beSome(PathError(FileExists(f2)))) and
        (runT(run)(ls).runEither must beRight(containTheSameElementsAs(expectedFiles)))
      }

      "moving a file to an existing path with Overwrite semantics should make contents available at new path" >> {
        val f1 = managePrefix </> dir("overwrite") </> file("f1")
        val f2 = managePrefix </> dir("overwrite") </> file("f2")
        val p = write.saveF(f1, oneDoc).drain ++
                write.saveF(f2, anotherDoc).drain ++
                manage.moveFile(f1, f2, MoveSemantics.Overwrite)
                  .liftM[Process].drain ++
                read.scanAll(f2).map(_.left[Boolean]) ++
                (query.fileExists(f1).liftM[FileSystemErrT]: query.M[Boolean])
                  .liftM[Process]
                  .map(_.right[Data])

        runLogT(run, p).map(_.toVector.separate)
          .runEither must beRight((oneDoc, Vector(false)))
      }

      "moving a file that doesn't exist to a file that does should fail with src NotFound" >> {
        val d = managePrefix </> dir("dnetoexists")
        val f1 = d </> file("f1")
        val f2 = d </> file("f2")
        val expectedFiles = List(Node.File(file("f2")))
        val ls = query.ls(d)
        val p = write.saveF(f2, oneDoc).drain ++
                manage.moveFile(f1, f2, MoveSemantics.Overwrite)
                  .liftM[Process]

        (execT(run, p).runOption must beSome(PathError(FileNotFound(f1)))) and
        (runT(run)(ls).runEither must beRight(containTheSameElementsAs(expectedFiles)))
      }

      "moving a file that doesn't exist to a file that also doesn't exist should fail with src NotFound" >> {
        val d = managePrefix </> dir("dnetodne")
        val f1 = d </> file("f1")
        val f2 = d </> file("f2")

        runT(run)(manage.moveFile(f1, f2, MoveSemantics.Overwrite))
          .runOption must beSome(PathError(FileNotFound(f1)))
      }

      "moving a file to itself with FailIfExists semantics should fail with PathExists" >> {
        val f1 = managePrefix </> dir("selftoself") </> file("f1")
        val p  = write.saveF(f1, oneDoc).drain ++
                 manage.moveFile(f1, f1, MoveSemantics.FailIfExists).liftM[Process]

        execT(run, p).runOption must beSome(PathError(FileExists(f1)))
      }

      "moving a file to a nonexistent path when using FailIfMissing sematics should fail with dst NotFound" >> {
        val d = managePrefix </> dir("existstodne")
        val f1 = d </> file("f1")
        val f2 = d </> file("f2")
        val expectedFiles = List(Node.File(file("f1")))
        val p  = write.saveF(f1, oneDoc).drain ++
                 manage.moveFile(f1, f2, MoveSemantics.FailIfMissing).liftM[Process]

        (execT(run, p).runOption must beSome(PathError(FileNotFound(f2)))) and
        (runT(run)(query.ls(d)).runEither must beRight(containTheSameElementsAs(expectedFiles)))
      }

      "moving a directory should move all files therein to dst path" >> {
        val d = managePrefix </> dir("movedir")
        val d1 = d </> dir("d1")
        val d2 = d </> dir("d2")
        val f1 = d1 </> file("f1")
        val f2 = d1 </> file("f2")

        val expectedFiles = List(Node.File(file("f1")), Node.File(file("f2")))

        val p = write.saveF(f1, oneDoc).drain ++
                write.saveF(f2, anotherDoc).drain ++
                manage.moveDir(d1, d2, MoveSemantics.FailIfExists).liftM[Process]

        (execT(run, p).runOption must beNone) and
        (runT(run)(query.ls(d2)).runEither must beRight(containTheSameElementsAs(expectedFiles))) and
        (runT(run)(query.ls(d1)).runEither must beLeft(PathError(DirNotFound(d1))))
      }

      "moving a nonexistent dir to another nonexistent dir fails with src NotFound" >> {
        val d1 = managePrefix </> dir("dirdnetodirdne") </> dir("d1")
        val d2 = managePrefix </> dir("dirdnetodirdne") </> dir("d2")

        runT(run)(manage.moveDir(d1, d2, MoveSemantics.FailIfExists))
          .runOption must beSome(PathError(DirNotFound(d1)))
      }

      "deleting a nonexistent file returns FileNotFound" >> {
        val f = managePrefix </> file("delfilenotfound")
        runT(run)(manage.deleteFile(f)).runEither must beLeft(PathError(FileNotFound(f)))
      }

      "deleting a file makes it no longer accessible" >> {
        val f1 = managePrefix </> dir("deleteone") </> file("f1")
        val p  = write.saveF(f1, oneDoc).drain ++ manage.deleteFile(f1).liftM[Process]

        (execT(run, p).runOption must beNone) and
        (runLogT(run, read.scanAll(f1)).runEither must beLeft(PathError(FileNotFound(f1))))
      }

      "deleting a file with siblings in directory leaves siblings untouched" >> {
        val d = managePrefix </> dir("withsiblings")
        val f1 = d </> file("f1")
        val f2 = d </> file("f2")

        val p = write.saveF(f1, oneDoc).drain ++
                write.saveF(f2, anotherDoc).drain ++
                manage.deleteFile(f1).liftM[Process]

        (execT(run, p).runOption must beNone) and
        (runLogT(run, read.scanAll(f1)).runEither must beLeft(PathError(FileNotFound(f1)))) and
        (runLogT(run, read.scanAll(f2)).runEither must beRight(anotherDoc))
      }

      "deleting a directory deletes all files therein" >> {
        val d = managePrefix </> dir("deldir")
        val f1 = d </> file("f1")
        val f2 = d </> file("f2")

        val p = write.saveF(f1, oneDoc).drain ++
                write.saveF(f2, anotherDoc).drain ++
                manage.deleteDir(d).liftM[Process]

        (execT(run, p).runOption must beNone) and
        (runLogT(run, read.scanAll(f1)).runEither must beLeft(PathError(FileNotFound(f1)))) and
        (runLogT(run, read.scanAll(f2)).runEither must beLeft(PathError(FileNotFound(f2)))) and
        (runT(run)(query.ls(d)).runEither must beLeft(PathError(DirNotFound(d))))
      }

      "deleting a nonexistent directory returns DirNotFound" >> {
        val d = managePrefix </> dir("deldirnotfound")
        runT(run)(manage.deleteDir(d)).runEither must beLeft(PathError(DirNotFound(d)))
      }

      "write/read from temp dir near existing" >> {
        val d = managePrefix </> dir("tmpnear1")
        val f = d </> file("somefile")

        val p = write.saveF(f, oneDoc).drain ++
                (manage.tempFileNear(f).liftM[FileSystemErrT]: manage.M[AbsFile[Sandboxed]])
                  .liftM[Process] flatMap { tf =>
                    write.saveF(tf, anotherDoc).drain ++
                    read.scanAll(tf) ++
                    manage.deleteFile(tf).liftM[Process].drain
                  }

        runLogT(run, p).runEither must beRight(anotherDoc)
      }

      "write/read from temp dir near non existing" >> {
        val d = managePrefix </> dir("tmpnear2")
        val f = d </> file("somefile")
        val p = (manage.tempFileNear(f).liftM[FileSystemErrT]: manage.M[AbsFile[Sandboxed]])
                  .liftM[Process] flatMap { tf =>
                    write.saveF(tf, anotherDoc).drain ++
                    read.scanAll(tf) ++
                    manage.deleteFile(tf).liftM[Process].drain
                  }

        runLogT(run, p).runEither must beRight(anotherDoc)
      }

      "write/read from arbitrary temp dir" >> {
        val p = (manage.anyTempFile.liftM[FileSystemErrT]: manage.M[AbsFile[Sandboxed]])
                  .liftM[Process] flatMap { tf =>
                    write.saveF(tf, oneDoc).drain ++
                    read.scanAll(tf) ++
                    manage.deleteFile(tf).liftM[Process].drain
                  }

        runLogT(run, p).runEither must beRight(oneDoc)
      }

      step(deleteForManage(run).runVoid)
    }; ()
  }
}
