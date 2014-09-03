package slamdata.engine

import java.io.File

import scala.util.matching.Regex

import org.specs2.mutable._
import org.specs2.execute._

import argonaut._, Argonaut._

import scalaz.{Failure => _, _}, Scalaz._
import scalaz.stream._
import scalaz.concurrent._

import slamdata.engine.fp._
import slamdata.engine.fs._
import slamdata.engine.sql.{Query}

class RegressionSpec extends BackendTest with JsonMatchers {
  val testDir = Path("test/")

  tests { case (config, backend) =>

    val fs = backend.dataSource

    val tmpDir = testDir ++ genTempDir.run

    val testRoot = new File("src/it/resources/tests")

    config.toString should {
  
      for (testFile <- files(testRoot, """.*\.json"""r)) {

        def loadData(name: String): Task[Unit] = {
          val NamePattern: Regex = """(.*)\.[^.*]+"""r
          val base = NamePattern.unapplySeq(name).get.head

          for {
            is <- Task.delay { new java.io.FileInputStream(new File(testFile.getParent, name)) }
            lines = scalaz.stream.io.linesR(is)
            data = lines.flatMap(l => Parse.parse(l).fold(e => Process.fail(sys.error(e)), j => Process.eval(Task.now(j))))
            rez <- fs.save(tmpDir ++ Path(base), data.map(json => RenderedJson(json.toString)))
          } yield rez
        }

        def runQuery(query: String): Task[Unit] =
          (for {
            t    <- backend.eval(QueryRequest(Query(query), Path("/"), Path("/") ++ tmpDir, tmpDir ++ Path("out")))
            (log, rp) = t
            //_ = println(log.last + "\n")
            rez  <- rp.runLog
          } yield ())

        def verifyExpected(exp: ExpectedResult): Task[Result] = (for {
          rez <- fs.scan(tmpDir ++ Path("out"), None, None).runLog
        } yield rez must matchResults(exp.ignoreOrder, exp.matchAll, exp.ignoredFields, exp.rows))

        val text = scala.io.Source.fromInputStream(new java.io.FileInputStream(testFile)).mkString
        
        decodeJson[RegressionTest](text) match {
          case -\/  (err)  => testFile.getName in { Failure(err) }
          
          case  \/- (test) => (test.name + " [" + testFile.getName + "]") in {

            if (!test.backends.list.contains(config)) skipped
            
            val testExec = (for {
              _   <- test.data.map(loadData(_)).getOrElse(Task.now(()))
              _   <- runQuery(test.query)
              rez <- verifyExpected(test.expected)
            } yield rez)
            
            testExec.attemptRun.fold(
              e => Failure(e.getMessage),
              r => r
            )
          }
        }
      }
    }
    
    step {
      deleteTempFiles(fs, testDir)
    }
  }
  
  def files(dir: File, pattern: Regex): Vector[File] = {
    val these = dir.listFiles
    these.filter(f => !pattern.unapplySeq(f.getName).isEmpty).toVector ++ 
      these.filter(_.isDirectory).flatMap(files(_, pattern))
  }
}

case class RegressionTest(
  name: String,
  backends: NonEmptyList[TestConfig],
  data: Option[String],
  query: String,
  expected: ExpectedResult
)
object RegressionTest {
  implicit val RegressionTestDecodeJson: DecodeJson[RegressionTest] = {
    DecodeJson(c => for {
      name          <- (c --\ "name").as[String]
      backends      <- orElse(c --\ "backends", TestConfig.all)
      data          <- optional[String](c --\ "data")
      query         <- (c --\ "query").as[String]

      rows          <- (c --\ "expected").as[List[Json]]
      ignoreOrder   <- orElse(c --\ "ignoreOrder", true)
      matchAll      <- orElse(c --\ "matchAll", true)
      ignoredFields <- orElse(c --\ "ignoredFields", List("_id"))
    } yield RegressionTest(name, backends, data, query, ExpectedResult(rows, ignoreOrder, matchAll, ignoredFields)))
  }
}
case class ExpectedResult(
  rows: List[Json],
  ignoreOrder: Boolean,
  matchAll: Boolean,
  ignoredFields: List[JsonField]
)
