package quasar.regression

import quasar._
import quasar.Predef._
import quasar.config._
import quasar.fp._
import quasar.fs.{Path => QPath, _}
import quasar.sql._

import java.io.{File, FileInputStream}
import scala.io.Source
import scala.util.matching.Regex

import argonaut._, Argonaut._
import org.specs2.specification._
import org.specs2.execute._
import pathy.Path._
import scalaz._, Scalaz._
import scalaz.concurrent.Task
import scalaz.stream.{merge => pmerge, _}

abstract class QueryRegressionTest[S[_]: Functor](
  fileSystems: Task[IList[FileSystemUT[S]]])(
  implicit S0: QueryFileF :<: S, S1: ManageFileF :<: S,
           S2: WriteFileF :<: S, S3: Task :<: S
) extends FileSystemTest[S](fileSystems) {

  import QueryRegressionTest._

  type FsErr[A] = FileSystemErrT[F, A]

  val qfTransforms = QueryFile.Transforms[F]
  import qfTransforms._

  val injectTask: Task ~> F =
    liftFT[S].compose(injectNT[Task, S])

  val TestsRoot = new File("it/src/main/resources/tests")
  val DataDir: ADir = rootDir </> dir("regression")
  val DataPath: QPath = QPath(posixCodec.printPath(DataDir))

  def dataFile(fileName: String): AFile =
    DataDir </> file1(FileName(fileName).dropExtension)

  val query  = QueryFile.Ops[S]
  val write  = WriteFile.Ops[S]
  val manage = ManageFile.Ops[S]

  /** A name to idetify the suite in test output. */
  def suiteName: String

  /** Return the results of evaluating the given query as a stream. */
  def queryResults(expr: Expr, vars: Variables): Process[CompExecM, Data]

  ////

  lazy val tests = regressionTests(TestsRoot, knownFileSystems).run

  fileSystemShould { name => implicit run =>
    suiteName should {
      step(prepareTestData(tests, run).run)

      tests.toList foreach { case (f, t) =>
        regressionExample(f, t, name, run)
      }

      step(runT(run)(manage.delete(DataDir)).runVoid)
    }; ()
  }

  ////

  /** Returns an `Example` verifying the given `RegressionTest`. */
  def regressionExample(
    loc: File,
    test: RegressionTest,
    backendName: BackendName,
    run: Run
  ): Example = {
    def runTest = (for {
      _    <- Task.delay(println(test.query))
      _    <- run(test.data.cata(verifyDataExists, success.point[F]))
      data =  testQuery(test.query, test.variables)
      res  <- verifyResults(test.expected, data, run)
    } yield res).run

    s"${test.name} [${loc.getPath}]" >> {
      test.backends.get(backendName) match {
        case Some(SkipDirective.Skip)    => skipped
        case Some(SkipDirective.Pending) => runTest.pendingUntilFixed
        case None                        => runTest
      }
    }
  }

  /** Verify that the given data file exists in the filesystem. */
  def verifyDataExists(dataFileName: String): F[Result] =
    query.fileExists(dataFile(dataFileName)).run map (_ ==== true.right)

  /** Verify the given results according to the provided expectation. */
  def verifyResults(
    exp: ExpectedResult,
    act: Process[CompExecM, Data],
    run: Run
  ): Task[Result] = {
    val liftRun: CompExecM ~> Task = {
      type H1[A] = PhaseResultT[Task, A]
      type H2[A] = SemanticErrsT[H1, A]
      type H3[A] = FileSystemErrT[H2, A]

      val h1: G ~> H1 = Hoist[PhaseResultT].hoist(run)
      val h2: H ~> H2 = Hoist[SemanticErrsT].hoist(h1)
      val h3: CompExecM ~> H3 = Hoist[FileSystemErrT].hoist(h2)

      new (CompExecM ~> Task) {
        def apply[A](fa: CompExecM[A]) =
          rethrow[H1, NonEmptyList[SemanticError]].apply(
            rethrow[H2, FileSystemError].apply(
              h3(fa))).value
      }
    }

    def deleteFields: Json => Json =
      _.withObject(obj => exp.ignoredFields.foldLeft(obj)(_ - _))

    exp.predicate(
      exp.rows.toVector,
      act.map(deleteFields.compose[Data](_.asJson)).translate[Task](liftRun))
  }

  /** Parse and execute the given query, returning a stream of results. */
  def testQuery(
    qry: String,
    vars: Map[String, String]
  ): Process[CompExecM, Data] = {
    val f: Task ~> CompExecM =
      toCompExec compose injectTask

    val parseTask: Task[Expr] =
      SQLParser.parseInContext(Query(qry), DataPath)
        .fold(e => Task.fail(new RuntimeException(e.message)), _.point[Task])

    f(parseTask).liftM[Process] flatMap (queryResults(_, Variables.fromMap(vars)))
  }

  /** Loads all the test data needed by the given tests into the filesystem. */
  def prepareTestData(tests: Map[File, RegressionTest], run: Run): Task[Unit] = {
    val throwFsError = rethrow[Task, FileSystemError]

    val dataLoc: ((File, RegressionTest)) => Set[File] = {
      case (f, t) => t.data.map(new File(f.getParent, _)).toSet
    }

    val loads: Process[Task, Process[Task, FileSystemError]] =
      Process.emitAll(tests.toList.foldMap(dataLoc).toVector) map { file =>
        write.saveChunked(
          dataFile(file.getName),
          testData(file).chunk(500).translate(injectTask)
        ).translate[Task](throwFsError.compose[FsErr](runT(run)))
      }

    throwFsError(EitherT(pmerge.mergeN(loads).take(1).runLast.map(_ <\/ (()))))
  }

  /** Returns a stream of `Data` representing the lines of the given data file. */
  def testData(file: File): Process[Task, Data] = {
    def parse(line: String): Process0[Data] =
      DataCodec.parse(line)(DataCodec.Precise).fold(
        err => Process.fail(new RuntimeException(
                 s"Failed to parse `$line` as Data: ${err.message}"
               )),
        Process.emit(_))

    io.linesR(new FileInputStream(file)) flatMap parse
  }

  /** Returns all the `RegressionTest`s found in the given directory, keyed by
    * file path.
    */
  def regressionTests(
    testDir: File,
    knownBackends: Set[BackendName]
  ): Task[Map[File, RegressionTest]] =
    descendantsMatching(testDir, """.*\.test"""r)
      .map(f =>
        (loadRegressionTest(f) >>= verifyBackends(knownBackends)) strengthL f)
      .gather(4)
      .runLog
      .map(_.toMap)

  /** Verifies there are no unknown backends specified in a `RegressionTest`. */
  def verifyBackends(knownBackends: Set[BackendName])
                    : RegressionTest => Task[RegressionTest] = { rt =>

    val unknown = rt.backends.keySet diff knownBackends
    def errMsg = s"Unrecognized backend(s): ${unknown.mkString(", ")}"

    if (unknown.isEmpty) Task.now(rt)
    else Task.fail(new RuntimeException(errMsg))
  }

  /** Loads a `RegressionTest` from the given file. */
  def loadRegressionTest: File => Task[RegressionTest] =
    file => textContents(file) flatMap { text =>
      decodeJson[RegressionTest](text) fold (
        err => Task.fail(new RuntimeException(err)),
        Task.now(_))
    }

  /** Returns all descendant files in the given dir matching `pattern`. */
  def descendantsMatching(dir: File, pattern: Regex): Process[Task, File] =
    Process.eval(Task.delay(dir.listFiles.toVector))
      .flatMap(Process.emitAll(_))
      .flatMap(f =>
        if (f.isDirectory)
          descendantsMatching(f, pattern)
        else if (pattern.findFirstIn(f.getName).isDefined)
          Process.emit(f)
        else
          Process.halt)

  /** Returns the contents of the file as a `String`. */
  def textContents(file: File): Task[String] =
    Task.delay(Source.fromInputStream(new FileInputStream(file)).mkString)
}

object QueryRegressionTest {
  import quasar.physical.mongodb.{filesystems => mongofs}

  lazy val knownFileSystems = TestConfig.backendNames.toSet

  def externalFS: Task[IList[FileSystemUT[FileSystemIO]]] = {
    val extFs = TestConfig.externalFileSystems {
      case (MongoDbConfig(cs), dir) =>
        lazy val f = mongofs.testFileSystemIO(cs, dir).run
        Task.delay(f)
    }

    extFs map (_ map (ut => ut.contramap(chroot.fileSystem(ut.testDir))))
  }

  implicit val dataEncodeJson: EncodeJson[Data] =
    EncodeJson(d =>
      DataCodec.Precise
        .encode(d)
        .fold(err => scala.sys.error(err.message), ι))
}
