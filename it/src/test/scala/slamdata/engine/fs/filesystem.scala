package slamdata.engine.physical.mongodb

import org.specs2.execute.{Result}
import org.specs2.mutable._

import scala.collection.immutable.ListMap

import scalaz._
import Scalaz._
import scalaz.concurrent._
import scalaz.stream._

import slamdata.engine.{Data, BackendTest, TestConfig}
import slamdata.engine.fp._

class FileSystemSpecs extends BackendTest with slamdata.engine.DisjunctionMatchers {
  import slamdata.engine.fs._

  val TestDir = TestRootDir ++ genTempDir.run

  def oneDoc: Process[Task, Data] = Process.emit(Data.Obj(ListMap("a" -> Data.Int(1))))

  tests {  case (backendName, backend) =>
    val fs = backend.dataSource

    backendName should {

      "FileSystem" should {
        // Run the task to create a single FileSystem instance for each run (I guess)

        "have zips" in {
          // This is the collection we use for all of our examples, so might as well make sure it's there.
          fs.ls(Path(".")).run must contain(Path("./zips"))
          fs.count(Path("/zips")).run must_== 29353
        }

        "save one" in {
          (for {
            tmp    <- genTempFile
            before <- fs.ls(TestDir)
            rez    <- fs.save(TestDir ++ tmp, oneDoc)
            after  <- fs.ls(TestDir)
          } yield {
            before must not(contain(tmp))
            after must contain(tmp)
          }).run
        }

        "save one (subdir)" in {
          (for {
            tmpDir <- genTempDir
            tmp = Path("file1")
            before <- fs.ls(TestDir ++ tmpDir)
            rez    <- fs.save(TestDir ++ tmpDir ++ tmp, oneDoc)
            after  <- fs.ls(TestDir ++ tmpDir)
          } yield {
            before must not(contain(tmp))
            after must contain(tmp)
          }).run
        }

        "save one with error" in {
          val badJson = Data.Int(1)
          val data: Process[Task, Data] = Process.emit(badJson)
          (for {
            tmpDir <- genTempDir
            file = tmpDir ++ Path("file1")

            before <- fs.ls(TestDir ++ tmpDir)
            rez    <- fs.save(TestDir ++ file, data).attempt
            after  <- fs.ls(TestDir ++ tmpDir)
          } yield {
            rez must beAnyLeftDisj
            after must_== before
          }).run
        }

        "save many (approx. 10 MB in 1K docs)" in {
          val sizeInMB = 10.0

          // About 0.5K each of data, and 0.25K of index, etc.:
          def jsonTree(depth: Int): Data =
            if (depth == 0) Data.Arr(Data.Str("abc") :: Data.Int(123) :: Data.Str("do, re, mi") :: Nil)
            else            Data.Obj(ListMap("left" -> jsonTree(depth-1), "right" -> jsonTree(depth-1)))
          def json(i: Int) = Data.Obj(ListMap("seq" -> Data.Int(i), "filler" -> jsonTree(3)))

          // This is _very_ approximate:
          val bytesPerDoc = 750
          val count = (sizeInMB*1024*1024/bytesPerDoc).toInt

          val data: Process[Task, Data] = Process.emitRange(0, count).map(json(_))

          (for {
            tmp  <- genTempFile
            _     <- fs.save(TestDir ++ tmp, data)
            after <- fs.ls(TestDir)
            _     <- fs.delete(TestDir ++ tmp)  // clean up this one eagerly, since it's a large file
          } yield {
            after must contain(tmp)
          }).run
        }

        "append one" in {
          val json = Data.Obj(ListMap("a" ->Data.Int(1)))
          val data: Process[Task, Data] = Process.emit(json)
          (for {
            tmp   <- genTempFile
            rez   <- fs.append(TestDir ++ tmp, data).runLog
            saved <- fs.scan(TestDir ++ tmp, None, None).runLog
          } yield {
            rez.size must_== 0
            saved.size must_== 1
          }).run
        }

        "append with one ok and one error" in {
          val json1 = Data.Obj(ListMap("a" ->Data.Int(1)))
          val json2 = Data.Int(1)
          val data: Process[Task, Data] = Process.emitAll(json1 :: json2 :: Nil)
          (for {
            tmp   <- genTempFile
            rez   <- fs.append(TestDir ++ tmp, data).runLog
            saved <- fs.scan(TestDir ++ tmp, None, None).runLog
          } yield {
            rez.size must_== 1
            saved.size must_== 1
          }).run
        }

        "move file" in {
          (for {
            tmp1  <- genTempFile
            tmp2  <- genTempFile
            _     <- fs.save(TestDir ++ tmp1, oneDoc)
            _     <- fs.move(TestDir ++ tmp1, TestDir ++ tmp2)
            after <- fs.ls(TestDir)
          } yield {
            after must not(contain(tmp1))
            after must contain(tmp2)
          }).run
        }

        "move dir" in {
          (for {
            tmpDir1  <- genTempDir
            tmp1 = tmpDir1 ++ Path("file1")
            tmp2 = tmpDir1 ++ Path("file2")
            _       <- fs.save(TestDir ++ tmp1, oneDoc)
            _       <- fs.save(TestDir ++ tmp2, oneDoc)
            tmpDir2 <- genTempDir
            _       <- fs.move(TestDir ++ tmpDir1, TestDir ++ tmpDir2)
            after   <- fs.ls(TestDir)
          } yield {
            after must not(contain(tmpDir1))
            after must contain(tmpDir2)
          }).run
        }

        "delete file" in {
          (for {
            tmp   <- genTempFile
            _     <- fs.save(TestDir ++ tmp, oneDoc)
            _     <- fs.delete(TestDir ++ tmp)
            after <- fs.ls(TestDir)
          } yield {
            after must not(contain(tmp))
          }).run
        }

        "delete file but not sibling" in {
          val tmp1 = Path("file1")
          val tmp2 = Path("file2")
          (for {
            tmpDir <- genTempDir
            _      <- fs.save(TestDir ++ tmpDir ++ tmp1, oneDoc)
            _      <- fs.save(TestDir ++ tmpDir ++ tmp2, oneDoc)
            before <- fs.ls(TestDir ++ tmpDir)
            _      <- fs.delete(TestDir ++ tmpDir ++ tmp1)
            after  <- fs.ls(TestDir ++ tmpDir)
          } yield {
            after must not(contain(tmp1))
            after must contain(tmp2)
          }).run
        }

        "delete dir" in {
          (for {
            tmpDir <- genTempDir
            tmp1 = tmpDir ++ Path("file1")
            tmp2 = tmpDir ++ Path("file2")
            _      <- fs.save(TestDir ++ tmp1, oneDoc)
            _      <- fs.save(TestDir ++ tmp2, oneDoc)
            _      <- fs.delete(TestDir ++ tmpDir)
            after  <- fs.ls(TestDir)
          } yield {
            after must not(contain(tmpDir))
          }).run
        }

        "delete missing file (not an error)" in {
          (for {
            tmp <- genTempFile
            rez <- fs.delete(TestDir ++ tmp).attempt
          } yield {
            rez must beAnyRightDisj
          }).run
        }

      }
    }

    step {
      deleteTempFiles(fs, TestDir)
    }
  }
}
