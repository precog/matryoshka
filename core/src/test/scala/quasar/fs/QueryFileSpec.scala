package quasar
package fs

import quasar.Predef._

import org.specs2.mutable.Specification
import org.specs2.ScalaCheck
import pathy.Path._
import pathy.scalacheck.PathyArbitrary._

class QueryFileSpec extends Specification with ScalaCheck with FileSystemFixture {
  import InMemory._, FileSystemError._, PathError2._

  "QueryFile" should {
    "lsAll" >> {
      "returns all descendants of the given directory" ! prop {
        (dp: ADir, dc1: RDir, dc2: RDir, od: ADir, fns: List[String]) => ((dp != od) && depth(dp) > 0 && depth(od) > 0 && fns.nonEmpty) ==> {
          val body = Vector(Data.Str("foo"))
          val fs  = fns take 5 map file
          val f1s = fs map (f => (dp </> dc1 </> f, body))
          val f2s = fs map (f => (dp </> dc2 </> f, body))
          val fds = fs map (f => (od </> f, body))

          val mem = InMemState fromFiles (f1s ::: f2s ::: fds).toMap
          val expectedNodes = (fs.map(dc1 </> _) ::: fs.map(dc2 </> _)).map(Node.Plain).distinct

          runResult(query.lsAll(dp)).run.eval(mem)
            .run.toEither must beRight(containTheSameElementsAs(expectedNodes))
        }
      }

      "returns not found when dir does not exist" ! prop { d: ADir =>
        runResult(query.lsAll(d)).run.eval(emptyMem)
          .run.toEither must beLeft(PathError(PathNotFound(d)))
      }
    }

    "fileExists" >> {
      "return true when file exists" ! prop { f: AFile =>
        run(query.fileExists(f))
          .eval(InMemState fromFiles Map(f -> Vector(Data.Int(1))))
          .run must beTrue
      }

      "return false when file doesn't exist" ! prop { (f1: AFile, f2: AFile) =>
        run(query.fileExists(f1))
          .eval(InMemState fromFiles Map(f2 -> Vector(Data.Int(1))))
          .run must beFalse
      }

      "return false when dir exists with same name as file" ! prop { f: AFile =>
        val n = fileName(f)
        val fd = parentDir(f).get </> dir(n.value) </> file("different.txt")

        run(query.fileExists(f))
          .eval(InMemState fromFiles Map(fd -> Vector(Data.Str("a"))))
          .run must beFalse
      }
    }
  }
}
