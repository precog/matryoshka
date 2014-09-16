package slamdata.engine

import java.io.File

import scala.util.matching.Regex

import org.specs2.mutable._
import org.specs2.execute._
import org.specs2.specification.{Example}

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
  
      def loadData(testFile: File, name: String): Task[Unit] = {
        val NamePattern: Regex = """(.*)\.[^.*]+"""r
        val base = NamePattern.unapplySeq(name).get.head

        for {
          is <- Task.delay { new java.io.FileInputStream(new File(testFile.getParent, name)) }
          lines = scalaz.stream.io.linesR(is)
          data = lines.flatMap(l => Parse.parse(l).fold(e => Process.fail(sys.error(e)), j => Process.eval(Task.now(j))))
          rez <- fs.save(tmpDir ++ Path(base), data.map(json => RenderedJson(json.toString)))
        } yield rez
      }

      def runQuery(query: String, vars: Map[String, String]): Task[Vector[PhaseResult]] =
        (for {
          t    <- backend.eval {
                    QueryRequest(
                      query     = Query(query), 
                      out       = tmpDir ++ Path("out"), 
                      basePath  = Path("/") ++ tmpDir, 
                      mountPath = Path("/"), 
                      variables = Variables.fromMap(vars))
                  }
          (log, rp) = t
          rez  <- rp.runLog
        } yield log)

      def verifyExpected(exp: ExpectedResult): Task[Result] = (for {
        rez <- fs.scan(tmpDir ++ Path("out"), None, None).runLog
      } yield rez must matchResults(exp.ignoreOrder, exp.matchAll, exp.ignoredFields, exp.rows))

      val examples: StreamT[Task, Example] = for {
        testFile <- StreamT.fromStream(files(testRoot, """.*\.test"""r))
        example  <- StreamT((decodeTest(testFile) flatMap { test =>
                      Task.delay {
                        (test.name + " [" + testFile.getName + "]") in {
                          if (!test.backends.list.contains(config)) skipped
                          else (for {
                            _   <- test.data.map(loadData(testFile, _)).getOrElse(Task.now(()))
                            log <- runQuery(test.query, test.variables)
                             // _ = println(test.name + "\n" + log.last + "\n")
                            rez <- verifyExpected(test.expected)
                          } yield rez).handle { case err => Failure(err.getMessage) }.run
                        }
                      }
                    }).handle(handleError(testFile)).map(toStep[Task,Example]))
      } yield example

      examples.toStream.run.toList

      ()
    }

    step {
      deleteTempFiles(fs, testDir)
    }
  }

  def files(dir: File, pattern: Regex): Task[Stream[File]] = {
    for {
      these <- Task.delay { dir.listFiles.toVector }
      children = these.filter(f => !pattern.unapplySeq(f.getName).isEmpty)
      desc <- these.filter(_.isDirectory).map(files(_, pattern)).sequenceU.map(_.flatten)
    } yield (children ++ desc).toStream
  }

  def readText(file: File): Task[String] = Task.delay {
    scala.io.Source.fromInputStream(new java.io.FileInputStream(file)).mkString
  }

  def decodeTest(file: File): Task[RegressionTest] = for {
    text <- readText(file)
    rez  <- decodeJson[RegressionTest](text).fold(err => Task.fail(new RuntimeException(err)), Task.now(_))
  } yield rez

  def handleError(testFile: File): PartialFunction[Throwable, Example] = {
    case err => testFile.getName in { Failure(err.getMessage) } 
  }

  def toStep[M[_]: Monad, A](a: A): StreamT.Step[A, StreamT[M, A]] = StreamT.Yield(a, StreamT.empty[M, A])
}

case class RegressionTest(
  name:       String,
  backends:   NonEmptyList[TestConfig],
  data:       Option[String],
  query:      String,
  variables:  Map[String, String],
  expected:   ExpectedResult
)
object RegressionTest {
  implicit val RegressionTestDecodeJson: DecodeJson[RegressionTest] = {
    DecodeJson(c => for {
      name          <- (c --\ "name").as[String]
      backends      <- orElse(c --\ "backends", TestConfig.all)
      data          <- optional[String](c --\ "data")
      query         <- (c --\ "query").as[String]
      variables     <- orElse(c --\ "variables", Map.empty[String, String])
      rows          <- (c --\ "expected").as[List[Json]]
      ignoreOrder   <- orElse(c --\ "ignoreOrder", true)
      matchAll      <- orElse(c --\ "matchAll", true)
      ignoredFields <- orElse(c --\ "ignoredFields", List("_id"))
    } yield RegressionTest(name, backends, data, query, variables, ExpectedResult(rows, ignoreOrder, matchAll, ignoredFields)))
  }
}
case class ExpectedResult(
  rows: List[Json],
  ignoreOrder: Boolean,
  matchAll: Boolean,
  ignoredFields: List[JsonField]
)
