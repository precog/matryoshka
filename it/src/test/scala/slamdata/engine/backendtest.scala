package slamdata.engine

import slamdata.Predef._

import java.lang.System

import org.specs2.mutable._

import scalaz._, Scalaz._
import scalaz.concurrent._

import argonaut._

import slamdata.engine.config._
import slamdata.engine.fs._

object TestConfig {

  lazy val backendNames: List[String] = List("mongodb_2_6", "mongodb_3_0")

  private def fail[A](msg: String): Task[A] = Task.fail(new RuntimeException(msg))

  def envName(backend: String): String = "SLAMDATA_" + backend.toUpperCase

  /**
   * Load backend config from environment variable.
   * Fails if it cannot parse the config
   * Returns None if there is no config.
   */
  def loadConfig(name: String): Task[Option[BackendConfig]] = {
    Task.delay(System.getenv()).flatMap { env =>
      Option(env.get(envName(name))).map { value =>
        Parse.decodeEither[BackendConfig](value).fold(
          e => fail("Failed to parse $" + envName(name) + ": " + e),
          cfg => Task.now(Some(cfg)))
      }.getOrElse(Task.now(None))
    }
  }
}

trait BackendTest extends Specification {
  sequential  // makes it easier to clean up
  args.report(showtimes=true)

  lazy val AllBackends: Task[NonEmptyList[(String, Backend)]] = (TestConfig.backendNames.map { name =>
    for {
      config  <-  TestConfig.loadConfig(name)
      backend <- config.fold[Task[Option[Backend]]](Task.now(None)) { cfg =>
        BackendDefinitions.All(cfg).fold[Task[Option[Backend]]](Task.fail(new RuntimeException("Invalid config for backend " + name + ": " + cfg))) { tb =>
          tb.map(Some(_))
        }
      }
    } yield backend.map(name -> _)
  }).sequenceU.flatMap(_.flatten match {
    case Nil => Task.fail(new java.lang.Exception(
      s"No backend to test. Consider setting one of these environment variables ${TestConfig.backendNames.map(TestConfig.envName)}"
    ))
    case h::t => Task.now(NonEmptyList.nel(h, t))
  })

  def testRootDir(back: Backend) = back.defaultPath ++ Path("test_tmp/")

  def genTempFile: Task[Path] = Task.delay {
    Path("gen_" + scala.util.Random.nextInt().toHexString)
  }

  def genTempDir: Task[Path] = genTempFile.map(_.asDir)

  /**
   * Run the supplied tests against all backends.
   * If the fixture fails to load any backends for some reason, will print a message to the developer with the reason
   * why we were unable to load a backend.
   * @param runTests A function that accepts a backend (filesystem) as well as a nane associated with the backend.
   *                 the functions type is Unit, but it should be running tests. I am not sure what specs2 magic is
   *                 happening for this to  work.
   */
  def backendShould(runTests: (Backend, String) => Unit): Unit = {
    (AllBackends.flatMap { backends =>
      (backends.map {
        case (name, backend) => Task.delay(name should runTests(backend, name))
      }).sequenceU
    }).map(_ => ()).run
  }

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
