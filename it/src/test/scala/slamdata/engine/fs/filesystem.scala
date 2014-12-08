package slamdata.engine.physical.mongodb

import org.specs2.execute.{Result}
import org.specs2.mutable._

import scalaz._
import Scalaz._
import scalaz.concurrent._
import scalaz.stream._

import slamdata.engine.{BackendTest, TestConfig}
import slamdata.engine.fp._

// FIXME: not available unless the classpaths can be straightened out in build.sbt (see http://www.blog.project13.pl/index.php/coding/1434/scala-sbt-and-test-dependencies/)
//import slamdata.engine.{DisjunctionMatchers}

class FileSystemSpecs extends BackendTest {
  import slamdata.engine.fs._

  val testDir = testRootDir ++ genTempDir.run

  def oneDoc: Process[Task, RenderedJson] = Process.emit(RenderedJson("""{"a": 1}"""))

  tests {  case (config, backend) =>
    val fs = backend.dataSource
    
    config.toString should {
  
      "FileSystem" should {
        // Run the task to create a single FileSystem instance for each run (I guess)

        "have zips" in {
          // Here's how to skip a test on a particular backend:
          // if (config == TestConfig.mongolocal) skipped
          
          // This is the collection we use for all of our examples, so might as well make sure it's there.
          fs.ls(Path(".")).run must contain(Path("./zips"))
        }

        "save one" in {
          (for {
            tmp    <- genTempFile
            before <- fs.ls(testDir)
            rez    <- fs.save(testDir ++ tmp, oneDoc)
            after  <- fs.ls(testDir)
          } yield {
            before must not(contain(tmp))
            after must contain(tmp)
          }).run
        }

        "save one (subdir)" in {
          (for {
            tmpDir <- genTempDir
            tmp = Path("file1")
            before <- fs.ls(testDir ++ tmpDir)
            rez    <- fs.save(testDir ++ tmpDir ++ tmp, oneDoc)
            after  <- fs.ls(testDir ++ tmpDir)
          } yield {
            before must not(contain(tmp))
            after must contain(tmp)
          }).run
        }

        "save one with error" in {
          val badJson = RenderedJson("{")
          val data: Process[Task, RenderedJson] = Process.emit(badJson)
          (for {
            tmpDir <- genTempDir
            file = tmpDir ++ Path("file1")

            before <- fs.ls(testDir ++ tmpDir)
            rez    <- fs.save(testDir ++ file, data).attempt
            after  <- fs.ls(testDir ++ tmpDir)
          } yield {
            rez.toOption must beNone
            after must_== before
          }).run
        }

        "save many (approx. 10 MB in 1K docs)" in {
          val sizeInMB = 10.0

          // About 0.5K each of data, and 0.25K of index, etc.:
          def jsonTree(depth: Int): Cord = if (depth == 0) Cord("[ \"abc\", 123, \"do, re, mi\" ]") else Cord("{ \"left\": ") ++ jsonTree(depth-1) ++ ", \"right\": " ++ jsonTree(depth-1) ++ "}"
          def json(i: Int) = RenderedJson("{\"seq\": " + i + ", \"filler\": " + jsonTree(3) + "}")

          // This is _very_ approximate:
          val bytesPerDoc = 750
          val count = (sizeInMB*1024*1024/bytesPerDoc).toInt

          val data: Process[Task, RenderedJson] = Process.emitRange(0, count).map(json(_))

          (for {
            tmp  <- genTempFile
            _     <- fs.save(testDir ++ tmp, data)
            after <- fs.ls(testDir)
            _     <- fs.delete(testDir ++ tmp)  // clean up this one eagerly, since it's a large file
          } yield {
            after must contain(tmp)
          }).run
        }

        "append one" in {
          val json = RenderedJson("{\"a\": 1}")
          val data: Process[Task, RenderedJson] = Process.emit(json)
          (for {
            tmp   <- genTempFile
            rez   <- fs.append(testDir ++ tmp, data).runLog
            saved <- fs.scan(testDir ++ tmp, None, None).runLog
          } yield {
            rez.size must_== 0
            saved.size must_== 1
          }).run
        }

        "append with one ok and one error" in {
          val json1 = RenderedJson("{\"a\": 1}")
          val json2 = RenderedJson("1")
          val data: Process[Task, RenderedJson] = Process.emitAll(json1 :: json2 :: Nil)
          (for {
            tmp   <- genTempFile
            rez   <- fs.append(testDir ++ tmp, data).runLog
            saved <- fs.scan(testDir ++ tmp, None, None).runLog
          } yield {
            rez.size must_== 1
            saved.size must_== 1
          }).run
        }

        "move file" in {
          (for {
            tmp1  <- genTempFile
            tmp2  <- genTempFile
            _     <- fs.save(testDir ++ tmp1, oneDoc)
            _     <- fs.move(testDir ++ tmp1, testDir ++ tmp2)
            after <- fs.ls(testDir)
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
            _       <- fs.save(testDir ++ tmp1, oneDoc)
            _       <- fs.save(testDir ++ tmp2, oneDoc)
            tmpDir2 <- genTempDir
            _       <- fs.move(testDir ++ tmpDir1, testDir ++ tmpDir2)
            after   <- fs.ls(testDir)
          } yield {
            after must not(contain(tmpDir1))
            after must contain(tmpDir2)
          }).run
        }

        "delete file" in {
          (for {
            tmp   <- genTempFile
            _     <- fs.save(testDir ++ tmp, oneDoc)
            _     <- fs.delete(testDir ++ tmp)
            after <- fs.ls(testDir)
          } yield {
            after must not(contain(tmp))
          }).run
        }

        "delete file but not sibling" in {
          val tmp1 = Path("file1")
          val tmp2 = Path("file2")
          (for {
            tmpDir <- genTempDir
            _      <- fs.save(testDir ++ tmpDir ++ tmp1, oneDoc)
            _      <- fs.save(testDir ++ tmpDir ++ tmp2, oneDoc)
            before <- fs.ls(testDir ++ tmpDir)
            _      <- fs.delete(testDir ++ tmpDir ++ tmp1)
            after  <- fs.ls(testDir ++ tmpDir)
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
            _      <- fs.save(testDir ++ tmp1, oneDoc)
            _      <- fs.save(testDir ++ tmp2, oneDoc)
            _      <- fs.delete(testDir ++ tmpDir)
            after  <- fs.ls(testDir)
          } yield {
            after must not(contain(tmpDir))
          }).run
        }

        "delete missing file (not an error)" in {
          (for {
            tmp <- genTempFile
            rez <- fs.delete(testDir ++ tmp).attempt
          } yield {
            rez.toOption must beSome
          }).run
        }
    
      }
    }

    step {
      deleteTempFiles(fs, testDir)
    }
  }
}