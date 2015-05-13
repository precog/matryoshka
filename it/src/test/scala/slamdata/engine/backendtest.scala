package slamdata.engine

import org.specs2.mutable._

import scalaz._, Scalaz._
import scalaz.concurrent._

import argonaut._

import slamdata.engine.config._
import slamdata.engine.fp._
import slamdata.engine.fs._

object TestConfig {
  private val defaultConfig: Map[String, Option[BackendConfig]] = Map(
    "mongodb_2_6" -> Some(MongoDbConfig("mongodb://slamengine:slamengine@ds045089.mongolab.com:45089/slamengine-test-01")),
    "mongodb_3_0" -> None
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

  def testRootDir(fs: FileSystem) = fs.defaultPath ++ Path("test_tmp/")

  def genTempFile(fs: FileSystem): Task[Path] = Task.delay {
    Path("gen_" + scala.util.Random.nextInt().toHexString)
  }

  def genTempDir(fs: FileSystem): Task[Path] = genTempFile(fs).map(_.asDir)

  def tests(f: (String, Backend) => Unit): Unit = {
    (AllBackends.flatMap { backends =>
      (backends.map {
        case (name, backend) => Task.delay(f(name, backend))
      }).sequenceU
    }).map(_ => ()).run
  }

  def deleteTempFiles(fs: FileSystem, dir: Path) = {
    val deleteAll = for {
      files <- fs.ls(dir)
      rez <- files.map(f => fs.delete(dir ++ f).attempt).sequenceU
    } yield rez
    val (errs, _) = unzipDisj(deleteAll.run)
    if (!errs.isEmpty) println("temp files not deleted: " + errs.map(_.getMessage).mkString("\n"))
  }
}
