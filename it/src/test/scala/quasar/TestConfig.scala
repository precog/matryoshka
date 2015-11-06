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
  val DefaultTestPrefix: AbsDir[Sandboxed] = rootDir </> dir("quasar-test")

  /** The environment variable used to externally specify the test path prefix.
    *
    * NB: The same path prefix is used for all backends under test.
    */
  val TestPathPrefixEnvName = "QUASAR_TEST_PATH_PREFIX"

  /** External Backends. */
  val MONGO_2_6 = "mongodb_2_6"
  val MONGO_3_0 = "mongodb_3_0"
  lazy val backendNames: List[String] = List(MONGO_2_6, MONGO_3_0)

  /** Returns the name of the environment variable used to configure the
    * given backend.
    */
  def backendEnvName(backendName: String): String =
    "QUASAR_" + backendName.toUpperCase

  /** Returns the list of filesystems to test, using the provided function
    * to select an interpreter for a given config.
    */
  def externalFileSystems[S[_]](
    pf: PartialFunction[(MountConfig, AbsDir[Sandboxed]), Task[S ~> Task]]
  ): Task[NonEmptyList[FileSystemUT[S]]] = {
    def fileSystemNamed(
      n: String,
      p: AbsDir[Sandboxed]
    ): OptionT[Task, FileSystemUT[S]] =
      TestConfig.loadConfig(n) flatMapF { c =>
        val run = pf.lift((c, p)) getOrElse Task.fail(new RuntimeException(
          s"Unsupported filesystem config: $c"
        ))

        (run |@| NameGenerator.salt)((r, s) => FileSystemUT(n, r, p </> dir(s)))
      }

    def noBackendsFound: Throwable = new RuntimeException(
      "No external backends to test. Consider setting one of these environment variables: " +
      TestConfig.backendNames.map(TestConfig.backendEnvName).mkString(", ")
    )

    TestConfig.testDataPrefix flatMap (prefix =>
      TestConfig.backendNames
        .traverse(n => fileSystemNamed(n, prefix).run)
        .flatMap(_.flatten.toNel.cata(Task.now, Task.fail(noBackendsFound))))
  }

  /** Load backend config from environment variable.
    *
    * Fails if it cannot parse the config and returns None if there is no config.
    */
  def loadConfig(name: String): OptionT[Task, MountConfig] =
    readEnv(backendEnvName(name)).flatMapF(value =>
      Parse.decodeEither[MountConfig](value).fold(
        e => fail("Failed to parse $" + backendEnvName(name) + ": " + e),
        _.point[Task]))

  /** Read the value of an envrionment variable. */
  def readEnv(name: String): OptionT[Task, String] =
    Task.delay(System.getenv).liftM[OptionT]
      .flatMap(env => OptionT(Task.delay(Option(env.get(name)))))

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
  def testDataPrefix: Task[AbsDir[Sandboxed]] =
    readEnv(TestPathPrefixEnvName) flatMap { s =>
      posixCodec.parseAbsDir(s).cata(
        d => OptionT(sandbox(rootDir, d).map(rootDir </> _).point[Task]),
        fail[AbsDir[Sandboxed]](s"Test data dir must be an absolute dir, got: $s").liftM[OptionT])
    } getOrElse DefaultTestPrefix

  ////

  private def fail[A](msg: String): Task[A] = Task.fail(new RuntimeException(msg))
}
