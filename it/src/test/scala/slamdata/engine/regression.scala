package slamdata.engine

import java.io.File

import scala.util.matching.Regex

import org.specs2.mutable._
import org.specs2.execute._
import org.specs2.matcher._
import org.specs2.specification.{Example}

import argonaut._, Argonaut._

import scalaz.{Failure => _, _}, Scalaz._
import scalaz.stream._
import scalaz.concurrent._

import slamdata.engine.fp._
import slamdata.engine.fs._
import slamdata.engine.sql.{Query}

class RegressionSpec extends BackendTest {
  tests { case (backendName, backend) =>

    val fs = backend.dataSource

    val tmpDir = TestRootDir ++ genTempDir.run

    val TestRoot = new File("it/src/test/resources/tests")

    println("TestRoot = " + TestRoot.getAbsolutePath)

    backendName should {

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

      def verifyExpected(outPath: Path, exp: ExpectedResult): Task[Result] = {
        val clean: Process[Task, Json] = parse(fs.scan(outPath, None, None)).map(deleteFields(exp.ignoredFields))

        exp.predicate(exp.rows.toVector, clean)
      }

      val examples: StreamT[Task, Example] = for {
        testFile <- StreamT.fromStream(files(TestRoot, """.*\.test"""r))
        example  <- StreamT((decodeTest(testFile) flatMap { test =>
                        Task.delay {
                          (test.name + " [" + testFile.getPath + "]") in {
                             test.backends(backendName) match {
                              case Disposition.Skip     => skipped
                              case Disposition.Pending  => pending
                              case Disposition.Verify   =>
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

  private def parse(p: Process[Task, RenderedJson]): Process[Task, Json] = 
    p.flatMap(j => j.toJson.fold(
      e => Process.fail(new RuntimeException("File system returning invalid JSON: " + e)),
      Process.emit _))

  private def deleteFields(ignoredFields: List[String]): Json => Json = j =>
    j.obj.map(j => Json.jObject(ignoredFields.foldLeft(j) { case (j, f) => j - f })).getOrElse(j)
}

case class RegressionTest(
  name:       String,
  backends:   String => Disposition,
  data:       Option[String],
  query:      String,
  variables:  Map[String, String],
  expected:   ExpectedResult
)
object RegressionTest {
  import DecodeResult.{ok, fail}
  
  private val VerifyAll: String => Disposition = Function.const(Disposition.Verify)

  private val SkipAll = ({
    case _ => Disposition.Skip
  }): PartialFunction[String, Disposition]

  implicit val RegressionTestDecodeJson: DecodeJson[RegressionTest] =
    DecodeJson(c => for {
      name          <-  (c --\ "name").as[String]
      backends      <-  if ((c --\ "backends").succeeded) 
                          ((c --\ "backends").as[Map[String, Disposition]]).map(_.orElse(SkipAll))
                        else ok(VerifyAll)
      data          <-  optional[String](c --\ "data")
      query         <-  (c --\ "query").as[String]
      variables     <-  orElse(c --\ "variables", Map.empty[String, String])
      ignoredFields <-  orElse(c --\ "ignoredFields", List.empty[String])
      rows          <-  (c --\ "expected").as[List[Json]]
      predicate     <-  (c --\ "predicate").as[Predicate]
    } yield RegressionTest(name, backends, data, query, variables, ExpectedResult(rows, predicate, ignoredFields)))
}

sealed trait Disposition
object Disposition {
  case object Skip    extends Disposition
  case object Pending extends Disposition
  case object Verify  extends Disposition

  import DecodeResult.{ok, fail}

  implicit val DispositionDecodeJson: DecodeJson[Disposition] =
    DecodeJson(c => c.as[String].flatMap {
      case "skip"     => ok(Skip)
      case "pending"  => ok(Pending)
      case "verify"   => ok(Verify)
      case str        => fail("skip, pending, or verify (default: verify); found: \"" + str + "\"", c.history)
    })
}

sealed trait Predicate {
  def apply(expected: Vector[Json], actual: Process[Task, Json]): Task[Result]
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
                                    else if (actual == expected) failure(s"matches $expected, but order is not the same", s)    
                                    else failure(s"does not match $expected", s)
                                  }.getOrElse(result(actual == expected, s"matches $expected", s"does not match $expected", s))
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
  case object ContainsAtLeast extends Predicate {
    def apply(expected: Vector[Json], actual: Process[Task, Json]): Task[Result] = {
      (for {
        expected <- actual.pipe(scan(expected.toSet) {
                      case (expected, e) => expected.filterNot(jsonMatches(_, e))
                    }).pipe(dropWhile(_.size > 0)).pipe(take(1))
      } yield (expected must be empty) : Result).runLastOr(failure)
    }
  }
  // Must contain ALL and ONLY the elements in some order.
  case object ContainsExactly extends Predicate {
    def apply(expected: Vector[Json], actual: Process[Task, Json]): Task[Result] = {
      (for {
        t <-  actual.pipe(scan((expected.toSet, Set.empty[Json])) {
                case ((expected, extra), e) => 
                  if (expected.contains(e)) (expected.filterNot(jsonMatches(_, e)), extra)
                  else (expected, extra + e)
              }).pipe(dropWhile(t => t._1.size > 0 && t._2.size == 0)).pipe(take(1))

        (expected, extra) = t
      } yield (expected must be empty) and (extra must be empty): Result).runLastOr(failure)
    }
  }
  // Must EXACTLY match the elements, in order.
  case object EqualsExactly extends Predicate {
    def apply(expected0: Vector[Json], actual0: Process[Task, Json]): Task[Result] = {
      val actual   = actual0.map(Some(_))
      val expected = Process.emitAll(expected0).map(Some(_))

      val zipped = actual.tee(expected)(tee.zipAll(None, None))

      zipped.flatMap {
        case ((a, e)) => if (jsonMatches(a, e)) Process.empty else Process.emit(a must matchJson(e) : Result)
      }.pipe(take(1)).runLastOr(success)
    }
  }
  // Must START WITH the elements, in order.
  case object EqualsInitial extends Predicate {
    def apply(expected0: Vector[Json], actual0: Process[Task, Json]): Task[Result] = {
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
  case object DoesNotContain extends Predicate {
    def apply(expected0: Vector[Json], actual: Process[Task, Json]): Task[Result] = {
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
