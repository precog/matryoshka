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
  tests { case (config, backend) =>

    val fs = backend.dataSource

    val tmpDir = testRootDir ++ genTempDir.run

    val testRoot = new File("it/src/test/resources/tests")

    println("testRoot = " + testRoot.getAbsolutePath)

    config.toString should {

      def dataPath(name: String): Path = {
        val NamePattern: Regex = """(.*)\.[^.*]+"""r
        val path: Path = tmpDir ++ Path(NamePattern.unapplySeq(name).get.head)
        path
      }

      def loadData(testFile: File, name: String): Task[Unit] = {
        val path = dataPath(name)
        fs.exists(path).flatMap(if (_) Task.now(()) else for {
          is    <- Task.delay { new java.io.FileInputStream(new File(testFile.getParent, name)) }
          _ = println("loading: " + name)
          lines = scalaz.stream.io.linesR(is)
          data  = lines.flatMap(l => Parse.parse(l).fold(e => Process.fail(sys.error(e)), j => Process.eval(Task.now(j))))
          _     <- fs.save(path, data.map(json => RenderedJson(json.toString)))
        } yield ())
      }

      def runQuery(query: String, vars: Map[String, String]): (Vector[PhaseResult], Task[ResultPath]) =
        (for {
          t    <- backend.run {
                    QueryRequest(
                      query     = Query(query), 
                      out       = Some(tmpDir ++ Path("out")),
                      basePath  = Path("/") ++ tmpDir,
                      mountPath = Path("/"), 
                      variables = Variables.fromMap(vars))
                  }
          _ = println(query)
        } yield t)

      def verifyExists(name: String): Task[Result] = fs.exists(dataPath(name)).map(_ must_== true)

      def verifyExpected(outPath: Path, exp: ExpectedResult): Task[Result] = (for {
        rez <- fs.scan(outPath, None, None).runLog
      } yield rez must matchResults(exp.ignoreOrder, exp.matchAll, exp.ignoredFields, exp.rows))

      val examples: StreamT[Task, Example] = for {
        testFile <- StreamT.fromStream(files(testRoot, """.*\.test"""r))
        example  <- StreamT((decodeTest(testFile) flatMap { test =>
                        Task.delay {
                          (test.name + " [" + testFile.getPath + "]") in {
                             test.backends(config) match {
                              case Disposition.Skip => skipped
                              case Disposition.Pending => pending
                              case Disposition.Verify =>
                                (for {
                                  _ <- test.data.map(loadData(testFile, _)).getOrElse(Task.now(()))
                                  (log, outPathT) = runQuery(test.query, test.variables)
                                  outPath <- outPathT
                                  // _ = println(test.name + "\n" + log.last + "\n")
                                  _   <- test.data.map(verifyExists(_)).getOrElse(Task.now(success))
                                  rez <- verifyExpected(outPath.path, test.expected)
                                } yield rez).handle { case err => Failure(err.getMessage) }.run
                            }
                          }
                        }
                      }).handle(handleError(testFile)).map(toStep[Task,Example]))
      } yield example

      examples.toStream.run.toList

      ()
    }

    step {
      deleteTempFiles(fs, tmpDir)
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
    case err => testFile.getPath in { Failure(err.getMessage) } 
  }

  def toStep[M[_]: Monad, A](a: A): StreamT.Step[A, StreamT[M, A]] = StreamT.Yield(a, StreamT.empty[M, A])
}

case class RegressionTest(
  name:       String,
  backends:   Map[TestConfig, Disposition],
  data:       Option[String],
  query:      String,
  variables:  Map[String, String],
  expected:   ExpectedResult
)
object RegressionTest {
  implicit val RegressionTestDecodeJson: DecodeJson[RegressionTest] =
    DecodeJson(c => for {
      name          <- (c --\ "name").as[String]
      backends      <- TestConfig.all.list.map(cfg => orElse[Disposition](c --\ "backends" --\ cfg.toString, Disposition.Verify).map(cfg -> _)).sequenceU.map(Map(_: _*))
      data          <- optional[String](c --\ "data")
      query         <- (c --\ "query").as[String]
      variables     <- orElse(c --\ "variables", Map.empty[String, String])
      rows          <- (c --\ "expected").as[List[Json]]
      ignoreOrder   <- orElse(c --\ "ignoreOrder", true)
      matchAll      <- orElse(c --\ "matchAll", true)
      ignoredFields <- orElse(c --\ "ignoredFields", List[String]())
    } yield RegressionTest(name, backends, data, query, variables, ExpectedResult(rows, ignoreOrder, matchAll, ignoredFields)))
}

sealed trait Disposition
object Disposition {
  case object Skip extends Disposition
  case object Pending extends Disposition
  case object Verify extends Disposition

  implicit val DispositionDecodeJson: DecodeJson[Disposition] =
    DecodeJson(c => c.as[String].flatMap {
      case "skip"    => DecodeResult(\/- (Skip))
      case "pending" => DecodeResult(\/- (Pending))
      case "verify"  => DecodeResult(\/- (Verify))
      case str => DecodeResult(-\/ (("skip, pending, or verify (default: verify); found: \"" + str + "\"", c.history)))
    })
}

case class ExpectedResult(
  rows: List[Json],
  ignoreOrder: Boolean,
  matchAll: Boolean,
  ignoredFields: List[JsonField]
)
