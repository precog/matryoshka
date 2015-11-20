package quasar
package fs

import quasar.Predef._
import quasar.fp._

import pathy.Path._
import scalaz._, Scalaz._
import scalaz.concurrent.Task
import scalaz.stream._

class QueryFilesSpec extends FileSystemTest[FileSystem](FileSystemTest.allFsUT) {
  import FileSystemTest._, FileSystemError._, PathError2._

  val query  = QueryFile.Ops[FileSystem]
  val write  = WriteFile.Ops[FileSystem]
  val manage = ManageFile.Ops[FileSystem]

  val queryPrefix: ADir = rootDir </> dir("forquery")

  def deleteForQuery(run: Run): FsTask[Unit] =
    runT(run)(manage.delete(queryPrefix))

  fileSystemShould { _ => implicit run =>
    "Querying Files" should {
      step(deleteForQuery(run).runVoid)

      "listing directory returns immediate child nodes" >> {
        val d = queryPrefix </> dir("lschildren")
        val d1 = d </> dir("d1")
        val f1 = d1 </> file("f1")
        val f2 = d1 </> dir("d2") </> file("f1")
        val expectedNodes = List(Node.Plain(dir("d2")), Node.Plain(file("f1")))

        val p = write.save(f1, oneDoc.toProcess).drain ++
                write.save(f2, anotherDoc.toProcess).drain ++
                query.ls(d1).liftM[Process]
                  .flatMap(ns => Process.emitAll(ns.toVector))

        runLogT(run, p)
          .runEither must beRight(containTheSameElementsAs(expectedNodes))
      }

      "listing nonexistent directory returns dir NotFound" >> {
        val d = queryPrefix </> dir("lsdne")
        runT(run)(query.ls(d)).runEither must beLeft(PathError(PathNotFound(d)))
      }

      "listing results should not contain deleted files" >> {
        val d = queryPrefix </> dir("lsdeleted")
        val f1 = d </> file("f1")
        val f2 = d </> file("f2")
        val p  = write.save(f1, oneDoc.toProcess).drain ++
                 write.save(f2, anotherDoc.toProcess).drain ++
                 query.ls(d).liftM[Process]
                   .flatMap(ns => Process.emitAll(ns.toVector))

        val preDelete = List(Node.Plain(file("f1")), Node.Plain(file("f2")))

        (runLogT(run, p)
          .runEither must beRight(containTheSameElementsAs(preDelete))) and
        (runT(run)(manage.delete(f1) *> query.ls(d))
          .runEither must beRight(containTheSameElementsAs(preDelete.tail)))
      }

      step(deleteForQuery(run).runVoid)
    }; ()
  }
}
