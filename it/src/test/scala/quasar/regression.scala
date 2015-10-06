package quasar

import quasar.Predef._
import quasar.fp._
import quasar.Backend._
import quasar.Errors._
import quasar.Evaluator._
import quasar.fs._
import quasar.sql._

import java.io.File
import scala.util.matching.Regex

import argonaut._, Argonaut._
import org.specs2.execute._
import org.specs2.matcher._
import org.specs2.mutable._
import org.specs2.specification.{Example}
import scalaz.{Failure => _, _}, Scalaz._
import scalaz.stream._
import scalaz.concurrent._

import pathy.Path.FileName

class RegressionSpec extends BackendTest {

  implicit val codec = DataCodec.Precise
  implicit val ED = EncodeJson[Data](codec.encode(_).fold(err => scala.sys.error(err.message), ι))

  backendShould { (prefix, backend, backendName) =>

    val tmpDir = prefix ++ testRootDir ++ genTempDir.run

    val TestRoot = new File("it/src/main/resources/tests")

    def dataPath(filename: String): Path = {
      val name = FileName(filename).dropExtension.value
      tmpDir ++ Path(name)
    }

    def runQuery(query: String, vars: Map[String, String]):
        EvaluationTask[(Vector[PhaseResult], ResultPath)] =
      for {
        expr <- liftE[EvaluationError](SQLParser.parseInContext(Query(query), tmpDir).fold(e => Task.fail(new RuntimeException(e.message)), Task.now))
        t <- liftE(genTempFile.map(file =>
          backend.run {
            QueryRequest(
              query     = expr,
              out       = Some(tmpDir ++ file),
              variables = Variables.fromMap(vars))
          }))
          (log, outT) = t.run
        out <- outT.fold(e => liftE[EvaluationError](Task.fail(new RuntimeException(e.message))), ι)
          _ <- liftE(Task.delay(println(query)))
      } yield (log, out)

    def verifyExists(name: String): Task[Result] =
      backend.exists(dataPath(name)).fold(_ must beNull, _ must beTrue)

    def verifyExpected(outPath: Path, exp: ExpectedResult)(implicit E: EncodeJson[Data]): ETask[ResultError, Result] = {
      val clean: Process[ETask[ResultError, ?], Json] =
        backend.scan(outPath, 0, None).map(x => deleteFields(exp.ignoredFields)(E.encode(x)))

      exp.predicate[ETask[ResultError, ?]](exp.rows.toVector, clean)
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
                      val loadDataFileIfProvided = dataFile.map(interactive.loadFile(backend, tmpDir, _))
                                                      .getOrElse(().point[ProcessingTask])
                      Task.delay {
                        (test.name + " [" + testFile.getPath + "]") in {
                          def runTest = (for {
                            _ <- loadDataFileIfProvided
                            out <- runQuery(test.query, test.variables).leftMap(PEvalError(_))
                            (log, outPath) = out
                            // _ = println(test.name + "\n" + log.last)
                            _   <- liftE(test.data.fold(Task.now[Result](success))(verifyExists(_)))
                            rez <- verifyExpected(outPath.path, test.expected).leftMap(PResultError(_))
                            _   <- backend.delete(outPath.path).leftMap(PPathError(_))
                          } yield rez).run.handle { case err => \/-(Failure(err.getMessage)) }.run.fold(e => Failure("path error: " + e.message), ι)
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
      deleteTempFiles(backend, tmpDir).run
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

case class RegressionTest(
                           name:       String,
                           backends:   Map[String, SkipDirective],
                           data:       Option[String],
                           query:      String,
                           variables:  Map[String, String],
                           expected:   ExpectedResult
)
object RegressionTest {
  import DecodeResult.{ok, fail}

  private val SkipAll = ({
    case _ => SkipDirective.Skip
  }): PartialFunction[String, SkipDirective]

  implicit val RegressionTestDecodeJson: DecodeJson[RegressionTest] =
    DecodeJson(c => for {
      name          <-  (c --\ "name").as[String]
      backends      <-  if ((c --\ "backends").succeeded)
                          ((c --\ "backends").as[Map[String, SkipDirective]])
                        else ok(Map[String, SkipDirective]())
      data          <-  optional[String](c --\ "data")
      query         <-  (c --\ "query").as[String]
      variables     <-  orElse(c --\ "variables", Map.empty[String, String])
      ignoredFields <-  orElse(c --\ "ignoredFields", List.empty[String])
      rows          <-  (c --\ "expected").as[List[Json]]
      predicate     <-  (c --\ "predicate").as[Predicate]
    } yield RegressionTest(name, backends, data, query, variables, ExpectedResult(rows, predicate, ignoredFields)))
}

sealed trait SkipDirective
object SkipDirective {
  final case object Skip    extends SkipDirective
  final case object Pending extends SkipDirective

  import DecodeResult.{ok, fail}

  implicit val SkipDirectiveDecodeJson: DecodeJson[SkipDirective] =
    DecodeJson(c => c.as[String].flatMap {
      case "skip"     => ok(Skip)
      case "pending"  => ok(Pending)
      case str        => fail("skip, pending; found: \"" + str + "\"", c.history)
    })
}

sealed trait Predicate {
  def apply[F[_]: Catchable: Monad](expected: Vector[Json], actual: Process[F, Json]):
      F[Result]
}
object Predicate extends Specification {
  import process1._
  import DecodeResult.{ok => jok, fail => jfail}

  def matchJson(expected: Option[Json]): Matcher[Option[Json]] = new Matcher[Option[Json]] {
    def apply[S <: Option[Json]](s: Expectable[S]) = {
      (expected, s.value) match {
        case (Some(expected),
              Some(actual))   =>  (actual.obj |@| expected.obj) { (actual, expected) =>
                                    if (actual.toList == expected.toList) success(s"matches $expected", s)
                                    else if (actual == expected) failure(s"$actual matches $expected, but order differs", s)
                                    else failure(s"$actual does not match $expected", s)
                                  }.getOrElse(result(actual == expected, s"matches $expected", s"$actual does not match $expected", s))
        case (Some(_), None)  =>  failure(s"ran out before expected", s)
        case (None, Some(v))  =>  failure(s"had more than expected: ${v}", s)
        case (None, None)     =>  success(s"matches (empty)", s)
        case _                =>  failure(s"scalac is weird", s)
      }
    }
  }

  private def jsonMatches(j1: Json, j2: Json): Boolean =
    (j1.obj.map(_.toList) |@| j2.obj.map(_.toList))(_ == _).getOrElse(j1 == j2)

  private def jsonMatches(j1: Option[Json], j2: Option[Json]): Boolean = (j1, j2) match {
    case (Some(j1), Some(j2)) => jsonMatches(j1, j2)
    case _ => false
  }

  // Must contain ALL the elements in some order.
  final case object ContainsAtLeast extends Predicate {
    def apply[F[_]: Catchable: Monad](expected: Vector[Json], actual: Process[F, Json]): F[Result] = {
      (for {
        expected <- actual.pipe(scan(expected.toSet) {
          case (expected, e) => expected.filterNot(jsonMatches(_, e))
        }).pipe(dropWhile(_.size > 0)).pipe(take(1))
      } yield (expected aka "unmatched expected values" must be empty) : Result).runLastOr(failure)
    }
  }
  // Must contain ALL and ONLY the elements in some order.
  final case object ContainsExactly extends Predicate {
    def apply[F[_]: Catchable: Monad](expected: Vector[Json], actual: Process[F, Json]): F[Result] = {
      (for {
        t <-  actual.pipe(scan((expected.toSet, Set.empty[Json])) {
          case ((expected, extra), e) =>
            if (expected.contains(e)) (expected.filterNot(jsonMatches(_, e)), extra)
            else (expected, extra + e)
        }).pipe(dropWhile(t => t._1.size > 0 && t._2.size == 0)).pipe(take(1))

        (expected, extra) = t
      } yield (extra aka "unexpected values" must be empty) and (expected aka "unmatched expected values" must be empty): Result).runLastOr(failure)
    }
  }
  // Must EXACTLY match the elements, in order.
  final case object EqualsExactly extends Predicate {
    def apply[F[_]: Catchable: Monad](expected0: Vector[Json], actual0: Process[F, Json]): F[Result] = {
      val actual   = actual0.map(Some(_))
      val expected = Process.emitAll(expected0).map(Some(_))

      val zipped = actual.tee(expected)(tee.zipAll(None, None))

      zipped.flatMap {
        case ((a, e)) => if (jsonMatches(a, e)) Process.empty else Process.emit(a must matchJson(e) : Result)
      }.pipe(take(1)).runLastOr(success)
    }
  }
  // Must START WITH the elements, in order.
  final case object EqualsInitial extends Predicate {
    def apply[F[_]: Catchable: Monad](expected0: Vector[Json], actual0: Process[F, Json]): F[Result] = {
      val actual   = actual0.map(Some(_))
      val expected = Process.emitAll(expected0).map(Some(_))

      val zipped = actual.tee(expected)(tee.zipAll(None, None))

      zipped.flatMap {
        case ((a, None))  => Process.halt
        case ((a, e))     => if (jsonMatches(a, e)) Process.empty else Process.emit(a must matchJson(e) : Result)
      }.pipe(take(1)).runLastOr(success)
    }
  }
  // Must NOT contain ANY of the elements.
  final case object DoesNotContain extends Predicate {
    def apply[F[_]: Catchable: Monad](expected0: Vector[Json], actual: Process[F, Json]): F[Result] = {
      val expected = expected0.toSet
      (for {
        found <-  actual.pipe(scan(expected) {
                    case (expected, e) => expected.filterNot(jsonMatches(_, e))
                  }).pipe(dropWhile(_.size == expected.size)).pipe(take(1))
      } yield (found must_== expected) : Result).runLastOr(failure)
    }
  }

  implicit val PredicateDecodeJson: DecodeJson[Predicate] =
    DecodeJson(c => c.as[String].flatMap {
      case "containsAtLeast"  => jok(ContainsAtLeast)
      case "containsExactly"  => jok(ContainsExactly)
      case "doesNotContain"   => jok(DoesNotContain)
      case "equalsExactly"    => jok(EqualsExactly)
      case "equalsInitial"    => jok(EqualsInitial)
      case str                => jfail("Expected one of: containsAtLeast, containsExactly, doesNotContain, equalsExactly, equalsInitial, but found: " + str, c.history)
    })
}

case class ExpectedResult(
  rows:           List[Json],
  predicate:      Predicate,
  ignoredFields:  List[JsonField]
)
