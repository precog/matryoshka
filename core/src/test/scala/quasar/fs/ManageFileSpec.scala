package quasar
package fs

import quasar.Predef._
import quasar.fp._

import org.specs2.mutable.Specification
import org.specs2.ScalaCheck
import scalaz._, Scalaz._
import scalaz.stream._
import pathy.Path._

class ManageFileSpec extends Specification with ScalaCheck with FileSystemFixture {
  import inmemory._, DataGen._, PathyGen._, FileSystemError._, PathError2._, ManageFile.Node

  "ManageFile" should {
    "renameFile" >> {
      "moves the existing file to a new name in the same directory" ! prop {
        (f: AbsFile[Sandboxed], xs: Vector[Data], name: String) => (xs.nonEmpty) ==> {
          // TODO: switch to fileParent once Pathy is updated
          val parent = parentDir(f).get

          val rename =
            manage.renameFile(f, name).liftM[Process]
          val existsP: Process[manage.M, Boolean] =
            manage.fileExists(f).liftM[FileSystemErrT].liftM[Process]
          val existsAndData: Process[manage.M, (Boolean, Data)] =
            existsP tuple read.scanAll(parent </> file(name))

          runLog(rename.drain ++ existsAndData)
            .map(_.unzip.leftMap(_ exists ι))
            .run.eval(InMemState fromFiles Map(f -> xs))
            .run.toEither must beRight((false, xs))
        }
      }
    }

    "lsAll" >> {
      "returns all descendants of the given directory" ! prop {
        (dp: AbsDir[Sandboxed], dc1: RelDir[Sandboxed], dc2: RelDir[Sandboxed], od: AbsDir[Sandboxed], fns: List[String]) => ((dp != od) && depth(dp) > 0 && depth(od) > 0 && fns.nonEmpty) ==> {
          val body = Vector(Data.Str("foo"))
          val fs  = fns take 5 map file
          val f1s = fs map (f => (dp </> dc1 </> f, body))
          val f2s = fs map (f => (dp </> dc2 </> f, body))
          val fds = fs map (f => (od </> f, body))

          val mem = InMemState fromFiles (f1s ::: f2s ::: fds).toMap
          val expectedNodes = (fs.map(dc1 </> _) ::: fs.map(dc2 </> _)).map(Node.File).distinct

          runT(manage.lsAll(dp)).run.eval(mem)
            .run.toEither must beRight(containTheSameElementsAs(expectedNodes))
        }
      }

      "returns not found when dir does not exist" ! prop { d: AbsDir[Sandboxed] =>
        runT(manage.lsAll(d)).run.eval(emptyMem)
          .run.toEither must beLeft(PathError(DirNotFound(d)))
      }
    }

    "fileExists" >> {
      "return true when file exists" ! prop { f: AbsFile[Sandboxed] =>
        run(manage.fileExists(f))
          .eval(InMemState fromFiles Map(f -> Vector(Data.Int(1))))
          .run must beTrue
      }

      "return false when file doesn't exist" ! prop { (f1: AbsFile[Sandboxed], f2: AbsFile[Sandboxed]) =>
        run(manage.fileExists(f1))
          .eval(InMemState fromFiles Map(f2 -> Vector(Data.Int(1))))
          .run must beFalse
      }

      "return false when dir exists with same name as file" ! prop { f: AbsFile[Sandboxed] =>
        val n = fileName(f)
        val fd = parentDir(f).get </> dir(n.value) </> file("different.txt")

        run(manage.fileExists(f))
          .eval(InMemState fromFiles Map(fd -> Vector(Data.Str("a"))))
          .run must beFalse
      }
    }
  }
}
