package quasar

import quasar.Backend.ProcessingError
import quasar.Predef._
import quasar.fs._
import quasar.specs2._
import quasar.interactive.DataSource

import java.lang.System
import scala.io.Source

import org.specs2.mutable._
import scalaz._, Scalaz._
import scalaz.concurrent._

trait BackendTest extends Specification with ExclusiveExecution {

  args.report(showtimes=true)

  lazy val AllBackends: Task[NonEmptyList[(String, Backend)]] = {
    def backendNamed(name: String): OptionT[Task, Backend] =
      TestConfig.loadConfig(name).flatMapF(bcfg =>
        BackendDefinitions.All(bcfg).fold(
          e => Task.fail(new RuntimeException(
            "Invalid config for backend " + name + ": " + bcfg + ", error: " + e.message)),
          Task.now(_)).join)

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
  def backendShould(data: DataSource*)(runTests: (Path, Backend, String, List[Path]) => Unit): Unit = {
    import interactive._
    val dataSources = data.toList
    val emptyNameMessage = "Every Source of data must have a name so that we know where to temporarly store it"
    scala.Predef.require(dataSources.all(_.name != ""), emptyNameMessage)
    (TestConfig.testDataPathPrefix |@| AllBackends)((dir, backends) =>
      backends.traverse_ { case (n, b) =>
        val loadAll = dataSources.map(source =>
          safeTaskToNormalTask[ProcessingError].apply(loadData(b, dir,source))
        )
        val deleteData = Monad[Task].sequence(dataSources.map(source => delete(b,dir ++ Path(source.name))))
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
          _ <- Task.delay(n should runTests(dir, b, n, filesPaths))//.onFinish(_ => deleteData.void)
        } yield ()
      }
    ).join.run
  }

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
    backendShould(){ case (p, b, n, _) => runTests(p,b,n)}

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
