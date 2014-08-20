package slamdata.engine.physical.mongodb

import org.specs2.execute.{Result}
import org.specs2.mutable._

import scalaz._
import Scalaz._
import scalaz.concurrent._
import scalaz.stream._

import slamdata.engine.fp._

// FIXME: not available unless the classpaths can be straightened out in build.sbt (see http://www.blog.project13.pl/index.php/coding/1434/scala-sbt-and-test-dependencies/)
//import slamdata.engine.{DisjunctionMatchers}

class FileSystemSpecs extends Specification {
  import slamdata.engine.fs._

  sequential  // makes it easier to clean up
  // isolated // each test needs to clean up its own files
  args.report(showtimes=true)

  /**
   MongoDB-specific setup. The rest of this test is meant to be backend-agnostic,
   so we can eventually run it against every supported backend.
   */
  private def mongoFs: Task[FileSystem] = {
    import slamdata.engine.config._

    // val config = MongoDbConfig("slamengine-test-01", "mongodb://slamengine:slamengine@ds045089.mongolab.com:45089/slamengine-test-01")
    val config = MongoDbConfig("test", "mongodb://localhost:27017")

    for {
      db <- util.createMongoDB(config) // FIXME: This will leak because Task will be re-run every time. Cache the DB for a given config.
    } yield MongoDbFileSystem(db)
  }

  val genTempName: Task[Path] = {
    val prefix = "gen_" + scala.util.Random.nextInt().toHexString + "_"
    Process.supply(0).map(i => Path(prefix + i.toString)).toTask
  }

  val testDir = Path("./test/")

  val fs = mongoFs.run

  "FileSystem" should {
    // Run the task to create a single FileSystem instance for each run (I guess)

    "have zips" in {
      // This is the collection we use for all of our examples, so might as well make sure it's there.
      fs.ls(Path(".")).run must contain(Path("./zips"))
    }

    "save one" in {
      val json = RenderedJson("{\"a\": 1}")
      val data: Process[Task, RenderedJson] = Process.emit(json)
      (for {
        tmp    <- genTempName
        before <- fs.ls(testDir)
        rez    <- fs.save(testDir ++ tmp, data)
        after  <- fs.ls(testDir)
      } yield {
        before must not(contain(tmp))
        after must contain(tmp)
      }).run
    }

    "save one with error" in {
      val badJson = RenderedJson("{")
      val data: Process[Task, RenderedJson] = Process.emit(badJson)
      (for {
        tmp    <- genTempName
        before <- fs.ls(testDir)
        rez    <- fs.save(testDir ++ tmp, data).attempt
        after  <- fs.ls(testDir)
      } yield {
        rez.toOption must beNone //throwA[com.mongodb.util.JSONParseException]
        after must_== before  // Note: assumes no other test running, and no other client using the db
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
        tmp  <- genTempName
        _     <- fs.save(testDir ++ tmp, data)
        after <- fs.ls(testDir)
        _     <- fs.delete(testDir ++ tmp)  // clean up this one eagerly, since it's a large file
      } yield {
        after must contain(tmp)
      }).run
    }

    "move file" in {
      val json = RenderedJson("{\"a\": 1}")
      val data: Process[Task, RenderedJson] = Process.emit(json)
      (for {
        tmp1  <- genTempName
        tmp2  <- genTempName
        _     <- fs.save(testDir ++ tmp1, data)
        _     <- fs.move(testDir ++ tmp1, testDir ++ tmp2)
        after <- fs.ls(testDir)
      } yield {
        after must not(contain(tmp1))
        after must contain(tmp2)
      }).run
    }

    "move dir" in {
      val json = RenderedJson("{\"a\": 1}")
      val data: Process[Task, RenderedJson] = Process.emit(json)
      (for {
        tmpDir1  <- genTempName
        tmp1 = tmpDir1.asDir ++ Path("file1")
        tmp2 = tmpDir1.asDir ++ Path("file2")
        _       <- fs.save(testDir ++ tmp1, data)
        _       <- fs.save(testDir ++ tmp2, data)
        tmpDir2 <- genTempName
        _       <- fs.move(tmpDir1, tmpDir2)
        after   <- fs.ls(testDir)
      } yield {
        after must not(contain(tmpDir1))
        after must contain(tmpDir2)
      }).run
    }.pendingUntilFixed("#234")

    "delete file" in {
      val json = RenderedJson("{\"a\": 1}")
      val data: Process[Task, RenderedJson] = Process.emit(json)
      (for {
        tmp   <- genTempName
        _     <- fs.save(testDir ++ tmp, data)
        _     <- fs.delete(testDir ++ tmp)
        after <- fs.ls(testDir)
      } yield {
        after must not(contain(tmp))
      }).run
    }

    "delete dir" in {
      val json = RenderedJson("{\"a\": 1}")
      val data: Process[Task, RenderedJson] = Process.emit(json)
      (for {
        tmpDir <- genTempName.map(_.asDir)
        tmp1 = tmpDir ++ Path("file1")
        tmp2 = tmpDir ++ Path("file2")
        _      <- fs.save(testDir ++ tmp1, data)
        _      <- fs.save(testDir ++ tmp2, data)
        _      <- fs.delete(testDir ++ tmpDir)
        after  <- fs.ls(testDir)
      } yield {
        after must not(contain(tmpDir))
      }).run
    }.pendingUntilFixed("#234")

    "delete missing file" in {
      (for {
        tmp <- genTempName
        rez <- fs.delete(testDir ++ tmp).attempt
      } yield {
        rez.toOption must beNone
      }).run
    }.pendingUntilFixed
  }

  step {
    val deleteAll = for {
      files <- fs.ls(testDir)
      rez <- files.map(f => fs.delete(testDir ++ f).attempt).sequenceU
    } yield rez
    val (errs, _) = unzipDisj(deleteAll.run)
    if (!errs.isEmpty) println("temp files not deleted: " + errs.map(_.getMessage).mkString("\n"))
  }
}