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
  private val defaultConfig: Map[String, Option[BackendConfig]] = Map(
    "mongodb_2_6" -> None,
    "mongodb_3_0" -> Some(MongoDbConfig("mongodb://slamengine:slamengine@ds045089.mongolab.com:45089/slamengine-test-01"))
  )

  lazy val AllBackends: List[String] = defaultConfig.keys.toList

  private def fail[A](msg: String): Task[A] = Task.fail(new RuntimeException(msg))

  private def envName(backend: String): String = "SLAMDATA_" + backend.toUpperCase

  def loadConfig(name: String): Task[Option[BackendConfig]] = {
    for {
      env <-  Task.delay(System.getenv())
      cfg <-  Option(env.get(envName(name))).map { value =>
        Parse.decodeEither[BackendConfig](value).fold(
          e => fail("Failed to parse $" + envName(name) + ": " + e),
          cfg => Task.now(Some(cfg)))
      }.getOrElse {
        defaultConfig.get(name).fold[Task[Option[BackendConfig]]](fail("No config for: " + name))(Task.delay(_))
      }
    } yield cfg
  }
}

trait BackendTest extends Specification {
  sequential  // makes it easier to clean up
  args.report(showtimes=true)

  lazy val AllBackends: Task[List[(String, Backend)]] = (TestConfig.AllBackends.map { name =>
    for {
      config  <-  TestConfig.loadConfig(name)
      backend <- config.fold[Task[Option[Backend]]](Task.now(None)) { cfg =>
        BackendDefinitions.All(cfg).fold[Task[Option[Backend]]](Task.fail(new RuntimeException("Invalid config for backend " + name + ": " + cfg))) { tb =>
          tb.map(Some(_))
        }
      }
    } yield backend.map(name -> _)
  }).sequenceU.map(_.flatten)

  def testRootDir(back: Backend) = back.defaultPath ++ Path("test_tmp/")

  def genTempFile: Task[Path] = Task.delay {
    Path("gen_" + scala.util.Random.nextInt().toHexString)
  }

  def genTempDir: Task[Path] = genTempFile.map(_.asDir)

  def tests(f: (String, Backend) => Unit): Unit = {
    (AllBackends.flatMap { backends =>
      (backends.map {
        case (name, backend) => Task.delay(f(name, backend))
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
