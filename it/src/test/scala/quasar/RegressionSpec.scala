package quasar

import quasar.Predef._
import quasar.fp._
import quasar.Backend._
import quasar.Errors._
import quasar.Evaluator._
import quasar.fs._
import quasar.sql._
import quasar.regression._

import java.io.File
import scala.util.matching.Regex

import argonaut._, Argonaut._
import org.specs2.execute._
import org.specs2.matcher._
import org.specs2.specification.{Example}
import pathy.Path.FileName
import scalaz.{Failure => _, _}, Scalaz._
import scalaz.stream._
import scalaz.concurrent._

class RegressionSpec extends BackendTest {

  implicit val codec = DataCodec.Precise
  implicit val ED = EncodeJson[Data](codec.encode(_).fold(err => scala.sys.error(err.message), ι))

  backendShould { (prefix, insertBackend, testBackend, backendName) =>

    val tmpDir = prefix ++ testRootDir ++ genTempDir.run

    val TestRoot = new File("it/src/main/resources/tests")

    def dataPath(filename: String): Path = {
      val name = FileName(filename).dropExtension.value
      tmpDir ++ Path(name)
    }

    def runQuery(query: String, vars: Map[String, String]):
        Task[(Vector[PhaseResult], Process[ProcessingTask, Data])] =
      for {
        expr <- SQLParser.parseInContext(Query(query), tmpDir).fold(e => Task.fail(new RuntimeException(e.message)), Task.now)
        _ <- Task.delay(println(query))
        t <- {
          val (ps, v) = testBackend.eval(
              QueryRequest(expr, Variables.fromMap(vars))).run
          v.fold(
            ce => Task.fail(new RuntimeException(ce.message)),
            pr => Task.now((ps, pr)))
        }
      } yield t

    def verifyExists(name: String): Task[Result] =
      testBackend.exists(dataPath(name)).fold(_ must beNull, _ must beTrue)

    def verifyExpected(actual: Process[ETask[ProcessingError, ?], Data], exp: ExpectedResult)(implicit E: EncodeJson[Data]): ETask[ProcessingError, Result] = {
      val clean: Process[ETask[ProcessingError, ?], Json] =
        actual.map(x => deleteFields(exp.ignoredFields)(E.encode(x)))

      exp.predicate[ETask[ProcessingError, ?]](exp.rows.toVector, clean)
    }

    def optionalMapGet[A, B](m: Option[Map[A, B]], key: A, noneDefault: B, missingDefault: B): B = m match {
     case None      => noneDefault
     case Some(map) => map.get(key).getOrElse(missingDefault)
   }

    val examples: StreamT[Task, Example] = for {
      testFile <- StreamT.fromStream(files(TestRoot, """.*\.test"""r))
      example  <- StreamT((decodeTest(testFile) flatMap { test =>
                      // The data file should be in the same directory as the testFile
                      val dataFile = test.data.map(name => new File(testFile.getParent, name))
                      val loadDataFileIfProvided = dataFile.map(interactive.loadFile(insertBackend, tmpDir, _))
                                                      .getOrElse(().point[ProcessingTask])
                      Task.delay {
                        (test.name + " [" + testFile.getPath + "]") in {
                          def runTest = (for {
                            _ <- loadDataFileIfProvided
                            out <- liftE[EvaluationError](runQuery(test.query, test.variables)).leftMap(PEvalError(_))
                            (log, outP) = out
                            // _ = println(test.name + "\n" + log.last)
                            rez <- verifyExpected(outP, test.expected)
                          } yield rez).run.run.fold(e => Failure("path error: " + e.message), ι)
                          test.backends.get(backendName) match {
                            case Some(SkipDirective.Skip)    => skipped
                            case Some(SkipDirective.Pending) => runTest.pendingUntilFixed
                            case None  => runTest
                          }
                        }
                      }
                    }).handle(handleError(testFile)).map(toStep[Task,Example]))
    } yield example

    examples.toStream.run.toList

    val cleanup = step {
      deleteTempFiles(insertBackend, tmpDir).run
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

    unknownBackends = rez.backends.keySet diff TestConfig.backendNames.toSet
    _    <- if (unknownBackends.nonEmpty) Task.fail(new RuntimeException("unrecognized backend(s): " + unknownBackends.mkString(", "))) else Task.now(())
  } yield rez

  def handleError(testFile: File): PartialFunction[Throwable, Example] = {
    case err => testFile.getPath in { Failure(err.getMessage) }
  }

  def toStep[M[_]: Monad, A](a: A): StreamT.Step[A, StreamT[M, A]] = StreamT.Yield(a, StreamT.empty[M, A])

  private def parse(p: Process[Task, String]): Process[Task, Json] =
    p.flatMap(j => Parse.parse(j).fold(
      e => Process.fail(new RuntimeException("File system returning invalid JSON: " + e)),
      Process.emit _))

  private def deleteFields(ignoredFields: List[String]): Json => Json = j =>
    j.obj.map(j => Json.jObject(ignoredFields.foldLeft(j) { case (j, f) => j - f })).getOrElse(j)
}
