package quasar

import quasar.Predef._
import quasar.config._
import quasar.fs._

import java.lang.System

import argonaut._

import pathy.Path._

import scalaz._, Scalaz._
import scalaz.concurrent._

object TestConfig {

  /** The path prefix under which test data may be found as well as where tests
    * can write test data/output.
    */
  val DefaultTestPathPrefix = Path("/quasar-test/")

  /** The directory under which test data may be found as well as where tests
    * can write test data/output.
    */
  val DefaultTestPrefix: ADir = rootDir </> dir("quasar-test")

  /** The environment variable used to externally specify the test path prefix.
    *
    * NB: The same path prefix is used for all backends under test.
    */
  val TestPathPrefixEnvName = "QUASAR_TEST_PATH_PREFIX"

  /** External Backends. */
  val MONGO_2_6 = BackendName("mongodb_2_6")
  val MONGO_3_0 = BackendName("mongodb_3_0")
  val MONGO_READ_ONLY = BackendName("mongodb_read_only")

  lazy val readWriteBackends: List[BackendName] = List(MONGO_2_6, MONGO_3_0)
  lazy val readOnlyBackends: List[BackendName]  = List(MONGO_READ_ONLY)
  lazy val backendNames: List[BackendName]      = readWriteBackends ::: readOnlyBackends

  /** Returns the name of the environment variable used to configure the
    * given backend.
    */
  def backendEnvName(backendName: BackendName): String =
    "QUASAR_" + backendName.name.toUpperCase

  /** The name of the environment variable to configure the insert connection
    * for a read-only backend.
    */
  def insertEnvName(b: BackendName) = backendEnvName(b) + "_INSERT"

  /** Returns the list of filesystems to test, using the provided function
    * to select an interpreter for a given config.
    */
  def externalFileSystems[S[_]](
    pf: PartialFunction[(MountConfig, ADir), Task[S ~> Task]]
  ): Task[IList[FileSystemUT[S]]] = {
    def fileSystemNamed(
      n: BackendName,
      p: ADir
    ): OptionT[Task, FileSystemUT[S]] =
      TestConfig.loadConfig(backendEnvName(n)) flatMapF { c =>
        val run = pf.lift((c, p)) getOrElse Task.fail(new RuntimeException(
          s"Unsupported filesystem config: $c"
        ))

        (run |@| NameGenerator.salt)((r, s) => FileSystemUT(n, r, p </> dir(s)))
      }

    def noBackendsFound: Throwable = new RuntimeException(
      "No external backends to test. Consider setting one of these environment variables: " +
      TestConfig.backendNames.map(TestConfig.backendEnvName).mkString(", ")
    )

    /** NB: We only fail if no backends are found, regardless if they're RW
      * or RO, even though we currently only use RW ones here as, if
      * the RW were omitted but a RO is specified, it was likely intentional.
      */
    TestConfig.testDataPrefix flatMap { prefix =>
      if (TestConfig.backendNames.isEmpty)
        Task.fail(noBackendsFound)
      else
        TestConfig.readWriteBackends.toIList
          .traverse(n => fileSystemNamed(n, prefix).run)
          .map(_.unite)
    }
  }

  /** Read the value of an envrionment variable. */
  def readEnv(name: String): OptionT[Task, String] =
    Task.delay(System.getenv).liftM[OptionT]
      .flatMap(env => OptionT(Task.delay(Option(env.get(name)))))

  /** Load backend config from environment variable.
    *
    * Fails if it cannot parse the config and returns None if there is no config.
    */
  def loadConfig(envName: String): OptionT[Task, MountConfig] =
    readEnv(envName).flatMapF(value =>
      Parse.decodeEither[MountConfig](value).fold(
        e => fail("Failed to parse $" + envName + ": " + e),
        _.point[Task]))

  /** Load a pair of backend configs, the first for inserting test data, and
    * the second for actually running tests. If no config is specified for
    * inserting, then the test config is just returned twice.
    */
  def loadConfigPair(name: BackendName): OptionT[Task, (MountConfig, MountConfig)] = {
    OptionT((loadConfig(insertEnvName(name)).run |@| loadConfig(backendEnvName(name)).run) { (c1, c2) =>
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

  /** Returns the absolute path within a filesystem to the directory where tests
    * may write data.
    *
    * One may specify this externally by setting the [[TestPathPrefixEnvName]].
    * The returned [[Task]] will fail if an invalid path is provided from the
    * environment and return the [[DefaultTestPrefix]] if nothing is provided.
    */
  def testDataPrefix: Task[ADir] =
    readEnv(TestPathPrefixEnvName) flatMap { s =>
      posixCodec.parseAbsDir(s).cata(
        d => OptionT(sandbox(rootDir, d).map(rootDir </> _).point[Task]),
        fail[ADir](s"Test data dir must be an absolute dir, got: $s").liftM[OptionT])
    } getOrElse DefaultTestPrefix

  ////

  private def fail[A](msg: String): Task[A] = Task.fail(new RuntimeException(msg))
}
