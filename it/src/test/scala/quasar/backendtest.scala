package quasar

import quasar.Backend.ProcessingError
import quasar.Predef._
import quasar.config._
import quasar.fs._

import java.lang.System

import org.specs2.mutable._
import quasar.interactive.DataSource

import scala.io.Source
import scalaz._, Scalaz._
import scalaz.concurrent._

import argonaut._

final case class BackendName(name: String)

object TestConfig {

  /** The path prefix under which test data may be found as well as where tests
    * can write test data/output.
    */
  val DefaultTestPathPrefix = Path("/quasar-test/")

  /** The environment variable used to externally specify the test path prefix.
    *
    * NB: The same path prefix is used for all backends under test.
    */
  val TestPathPrefixEnvName = "QUASAR_TEST_PATH_PREFIX"

  val MONGO_2_6 = BackendName("mongodb_2_6")
  val MONGO_3_0 = BackendName("mongodb_3_0")
  val MONGO_READ_ONLY = BackendName("mongodb_read_only")
  lazy val backendNames: List[BackendName] = List(MONGO_2_6, MONGO_3_0, MONGO_READ_ONLY)

  def envName(b: BackendName) = "QUASAR_" + b.name.toUpperCase
  def insertEnvName(b: BackendName) = envName(b) + "_INSERT"

  private def fail[A](msg: String): Task[A] = Task.fail(new RuntimeException(msg))

  /** Read the value of an environment variable. */
  def readEnv(varName: String): OptionT[Task, String] =
    Task.delay(System.getenv).liftM[OptionT]
      .flatMap(env => OptionT(Task.delay(Option(env.get(varName)))))

  /** Load backend config from environment variable.
    *
    * Fails if it cannot parse the config and returns None if there is no config.
    */
  def loadConfig(envVar: String): OptionT[Task, MountConfig] =
    readEnv(envVar).flatMapF(value =>
      Parse.decodeEither[MountConfig](value).fold(
        e => fail("Failed to parse $" + envVar + ": " + e),
        _.point[Task]))

  /** Load a pair of backend configs, the first for inserting test data, and
    * the second for actually running tests. If no config is specified for
    * inserting, then the test config is just returned twice.
    */
  def loadConfigPair(name: BackendName): OptionT[Task, (MountConfig, MountConfig)] = {
    OptionT((loadConfig(insertEnvName(name)).run |@| loadConfig(envName(name)).run) { (c1, c2) =>
      c2.map(c2 => (c1.getOrElse(c2), c2))
    })
  }

  /** Returns the absolute path within a backend to the directory containing test
    * data.
    *
    * One may specify this externally by setting the [[TestPathPrefixEnvName]].
    * The returned [[Task]] will fail if an invalid path is provided from the
    * environment and return the [[DefaultTestPathPrefix]] if nothing is provided.
    */
  def testDataPathPrefix: Task[Path] =
    readEnv(TestPathPrefixEnvName).map(Path(_)).flatMapF { path =>
      if (path.absolute) Task.now(path)
      else fail("Test path prefix must be an absolute dir, got: " + path.shows)
    } getOrElse DefaultTestPathPrefix

}

trait BackendTest extends Specification {
  sequential  // makes it easier to clean up
  args.report(showtimes=true)

  lazy val AllBackends: Task[NonEmptyList[(BackendName, (Backend, Backend))]] = {
    def backendNamed(name: BackendName): OptionT[Task, (Backend, Backend)] = {
      def mount(cfg: MountConfig) =
        BackendDefinitions.All(cfg).fold(
          e => Task.fail(new RuntimeException(
            "Invalid config for backend " + name + ": " + cfg + ", error: " + e.message)),
          Task.now(_)).join

      TestConfig.loadConfigPair(name).flatMapF { case (icfg, tcfg) =>
        Applicative[Task].tuple2(mount(icfg), mount(tcfg)) }
    }

    def noBackendsFound: Throwable = new RuntimeException(
      "No backend to test. Consider setting one of these environment variables: " +
      TestConfig.backendNames.map(TestConfig.envName).mkString(", ")
    )

    TestConfig.backendNames
      .traverse(n => backendNamed(n).strengthL(n).run)
      .flatMap(_.flatten.toNel.cata(Task.now, Task.fail(noBackendsFound)))
  }

  val testRootDir = Path("test_tmp/")

  def genTempFile: Task[Path] = Task.delay {
    Path("gen_" + scala.util.Random.nextInt().toHexString)
  }

  def genTempDir: Task[Path] = genTempFile.map(_.asDir)

  /** Run the supplied tests against all backends.
    *
    * If the fixture fails to load any backends for some reason, an exception is
    * thrown stating so.
    *
    * @param data A list of data sources to use in this test. The data will be loaded if
    *             not already loaded and deleted at the end of the test. Currently, the data is actually
    *             NOT deleted at the end of the test, but that is an undesired behavior that should hopefully
    *             be fixed in the future.
    *             Currently as well, the sources must have a non-empty [[String]] as a description
    *             in order for the fixture to derive a [[Path]] at which to store the data. Eventually, this path
    *             location could be randomized since we provide the paths as an argument for the body, so it should
    *             not hardcode any [[String]]s in theory. Under such circumstances, a [[DataSource]] with a empty string
    *             description would be accepted without issues.
    * @param runTests A function that accepts a path prefix under which both test
    *                 data may be found and tests may use as a sandbox, a backend
    *                 (filesystem) and an associated name for purposes of
    *                 identifying the backend in messages as well as a list of fully qualified paths to the test data
    *                 requested (in the same order as supplied)
    */
  private def backendShould0(data: DataSource*)(runTests: (Path, Backend, Backend, BackendName, List[Path]) => Unit): Unit = {
    import interactive._
    val dataSources = data.toList
    val emptyNameMessage = "Every Source of data must have a name so that we know where to temporarly store it"
    scala.Predef.require(dataSources.all(_.name != ""), emptyNameMessage)
    (TestConfig.testDataPathPrefix |@| AllBackends)((dir, backends) =>
      backends.traverse_ { case (n, (ib, tb)) =>
        val loadAll = dataSources.map(source =>
          safeTaskToNormalTask[ProcessingError].apply(loadData(ib, dir, source))
        )
        val deleteData = Monad[Task].sequence(dataSources.map(source => delete(ib, dir ++ Path(source.name))))
        val filesPaths = dataSources.map(source => dir ++ Path(source.name))
        for {
          _ <- Monad[Task].sequence(loadAll)
                                                                   // It would be nice to clean up after ourselves but
                                                                   // that is probably not worth the effort working
                                                                   // around specs2's execution model at least until we
                                                                   // upgrade to version 3.0 where the execution is more
                                                                   // sane as far as my understanding goes.
                                                                   // Currently, if this below code is uncommented, the
                                                                   // cleanup will happen after the tests are
                                                                   // "constructed" but before they are run,
                                                                   // resulting in no more data when the tests are
                                                                   // actually run
          _ <- Task.delay(n.name should runTests(dir, ib, tb, n, filesPaths))//.onFinish(_ => deleteData.void)
        } yield ()
      }
    ).join.run
  }

  /** Run the supplied tests against all backends.
    *
    * If the fixture fails to load any backends for some reason, an exception is
    * thrown stating so.
    *
    * @param data A list of data sources to use in this test. The data will be loaded if
    *             not already loaded and deleted at the end of the test. Currently, the data is actually
    *             NOT deleted at the end of the test, but that is an undesired behavior that should hopefully
    *             be fixed in the future.
    *             Currently as well, the sources must have a non-empty [[String]] as a description
    *             in order for the fixture to derive a [[Path]] at which to store the data. Eventually, this path
    *             location could be randomized since we provide the paths as an argument for the body, so it should
    *             not hardcode any [[String]]s in theory. Under such circumstances, a [[DataSource]] with a empty string
    *             description would be accepted without issues.
    * @param runTests A function that accepts a path prefix under which both test
    *                 data may be found and tests may use as a sandbox, a backend
    *                 (filesystem) and an associated name for purposes of
    *                 identifying the backend in messages as well as a list of fully qualified paths to the test data
    *                 requested (in the same order as supplied)
    */
  def backendShould(data: DataSource*)(runTests: (Path, Backend, BackendName, List[Path]) => Unit): Unit =
    backendShould0(data: _*) { case (p, _, b, n, ps) => runTests(p, b, n, ps) }

  /** Run the supplied tests against all backends.
    *
    * If the fixture fails to load any backends for some reason, an exception is
    * thrown stating so.
    *
    * @param runTests A function that accepts a path prefix under which both test
    *                 data may be found and tests may use as a sandbox, a backend
    *                 (filesystem) for inserting test data, another backend to be
    *                 tested, and an associated name for purposes of
    *                 identifying the backend in messages.
    */
  def backendShould(runTests: (Path, Backend, Backend, BackendName) => Unit): Unit =
    backendShould0(){ case (p, ib, tb, n, _) => runTests(p,ib,tb,n)}

  def deleteTempFiles(backend: Backend, dir: Path): Task[Unit] =
    backend.ls(dir).run.flatMap(_.fold(
      err => Task.delay { System.err.println("could not list temp files: " + err) },
      nodes => for {
        rs <- nodes.toList.map(n => backend.delete(dir ++ n.path).run.attempt).sequence
        _  <- (rs.map {
          case -\/(err)      => Task.delay { System.err.println("path error: " + err) }
          case \/-(-\/(err)) => Task.delay { System.err.println("delete failed: " + err) }
          case \/-(\/-(()))  => Task.now(())
        }).sequence
      } yield ()))
}
