package quasar

import quasar.Predef._

import java.lang.System

import org.specs2.mutable._

import scalaz._, Scalaz._
import scalaz.concurrent._

import argonaut._

import quasar.config._
import quasar.fs._

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

  val MONGO_2_6 = "mongodb_2_6"
  val MONGO_3_0 = "mongodb_3_0"
  lazy val backendNames: List[String] = List(MONGO_2_6, MONGO_3_0)

  private def fail[A](msg: String): Task[A] = Task.fail(new RuntimeException(msg))

  def backendEnvName(backend: String): String = "QUASAR_" + backend.toUpperCase

  /** Read the value of an envrionment variable. */
  def readEnv(name: String): OptionT[Task, String] =
    Task.delay(System.getenv).liftM[OptionT]
      .flatMap(env => OptionT(Task.delay(Option(env.get(name)))))

  /** Load backend config from environment variable.
    *
    * Fails if it cannot parse the config and returns None if there is no config.
    */
  def loadConfig(name: String): OptionT[Task, BackendConfig] =
    readEnv(backendEnvName(name)).flatMapF(value =>
      Parse.decodeEither[BackendConfig](value).fold(
        e => fail("Failed to parse $" + backendEnvName(name) + ": " + e),
        _.point[Task]))

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

  lazy val AllBackends: Task[NonEmptyList[(String, Backend)]] = {
    def backendNamed(name: String): OptionT[Task, Backend] =
      TestConfig.loadConfig(name).flatMapF(bcfg =>
        BackendDefinitions.All(bcfg).getOrElse(Task.fail(
          new RuntimeException("Invalid config for backend " + name + ": " + bcfg))))

    def noBackendsFound: Throwable = new RuntimeException(
      "No backend to test. Consider setting one of these environment variables: " +
      TestConfig.backendNames.map(TestConfig.backendEnvName).mkString(", ")
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
    * @param runTests A function that accepts a path prefix under which both test
    *                 data may be found and tests may use as a sandbox, a backend
    *                 (filesystem) and an associated name for purposes of
    *                 identifying the backend in messages.
    */
  def backendShould(runTests: (Path, Backend, String) => Unit): Unit =
    (TestConfig.testDataPathPrefix |@| AllBackends)((dir, backends) =>
      backends.traverse_ { case (n, b) =>
        Task.delay(n should runTests(dir, b, n)).void
      }
    ).join.run

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
