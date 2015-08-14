package slamdata.engine.config

import slamdata.Predef._
import slamdata.fp._
import slamdata.engine._, Evaluator._
import slamdata.engine.fs.{Path => EnginePath}

import pathy._, Path._
import scalaz._, concurrent.Task, Scalaz._
import org.specs2.mutable._
import org.specs2.scalaz._

class ConfigSpec extends Specification with DisjunctionMatchers {
  import FsPath._

  val TestConfig = Config(
    server = SDServerConfig(Some(92)),
    mountings = Map(
      EnginePath.Root -> MongoDbConfig("mongodb://slamengine:slamengine@ds045089.mongolab.com:45089/slamengine-test-01")))

  val BrokenTestConfig = Config(
    server = SDServerConfig(Some(92)),
    mountings = Map(
      EnginePath.Root -> MongoDbConfig("mongodb://slamengine:slamengine@ds045088.mongolab.com:45089/slamengine-test-01")))

  def testConfigFile: Task[FsPath.Aux[Rel, File, Sandboxed]] =
    Task.delay(scala.util.Random.nextInt.toString)
      .map(i => Uniform(currentDir </> file(s"test-config-${i}.json")))

  def withTestConfigFile[A](f: FsPath.Aux[Rel, File, Sandboxed] => Task[A]): Task[A] = {
    import java.nio.file._

    def deleteIfExists(fp: FsPath[File, Sandboxed]): Task[Unit] =
      systemCodec
        .map(c => printFsPath(c, fp))
        .flatMap(s => Task.delay(Files.deleteIfExists(Paths.get(s))))
        .void

    testConfigFile >>= (fp => f(fp) onFinish κ(deleteIfExists(fp)))
  }

  val OldConfigStr =
    """{
      |  "server": {
      |    "port": 92
      |  },
      |  "mountings": {
      |    "/": {
      |      "mongodb": {
      |        "database": "slamengine-test-01",
      |        "connectionUri": "mongodb://slamengine:slamengine@ds045089.mongolab.com:45089/slamengine-test-01"
      |      }
      |    }
      |  }
      |}""".stripMargin

  val ConfigStr =
    """{
      |  "server": {
      |    "port": 92
      |  },
      |  "mountings": {
      |    "/": {
      |      "mongodb": {
      |        "connectionUri": "mongodb://slamengine:slamengine@ds045089.mongolab.com:45089/slamengine-test-01"
      |      }
      |    }
      |  }
      |}""".stripMargin

  "fromString" should {
    "parse valid config" in {
      Config.fromString(ConfigStr) must beRightDisjunction(TestConfig)
    }

    "parse previous config" in {
      Config.fromString(OldConfigStr) must beRightDisjunction(TestConfig)
    }
  }

  "toString" should {
    "render same config" in {
      Config.toString(TestConfig) must_== ConfigStr
    }
  }

  "write" should {
    "create loadable config" in {
      withTestConfigFile(fp =>
        Config.toFile(TestConfig, fp) *>
        Config.fromFile(fp).run
      ).run must beRightDisjunction(TestConfig)
    }
  }

  "loadAndTest" should {
    "load a correct config" in {
      withTestConfigFile(fp =>
        Config.toFile(TestConfig, fp) *>
        Config.loadAndTest(fp).run
      ).run must beRightDisjunction(TestConfig)
    }

    "fail on a config with incorrect mounting" in {
      withTestConfigFile(fp =>
        Config.toFile(BrokenTestConfig, fp) *>
        Config.loadAndTest(fp).run
      ).run must beLeftDisjunction(InvalidConfig("mounting(s) failed"))
    }
  }

  "MongoDbConfig.UriPattern" should {
    import MongoDbConfig._

    "parse simple URI" in {
      "mongodb://localhost" match {
        case ParsedUri(_, _, host, _, _, _, _) => host must_== "localhost"
      }
    }

    "parse simple URI with trailing slash" in {
      "mongodb://localhost/" match {
        case ParsedUri(_, _, host, _, _, _, _) => host must_== "localhost"
      }
    }

    "parse user/password" in {
      "mongodb://me:pwd@localhost" match {
        case ParsedUri(user, pwd, _, _, _, _, _) =>
          user must beSome("me")
          pwd must beSome("pwd")
      }
    }

    "parse port" in {
      "mongodb://localhost:5555" match {
        case ParsedUri(_, _, _, port, _, _, _) => port must beSome(5555)
      }
    }

    "parse database" in {
      "mongodb://localhost/test" match {
        case ParsedUri(_, _, _, _, _, db, _) => db must beSome("test")
      }
    }

    "parse additional host(s)" in {
      "mongodb://host1,host2,host3:5555" match {
        case ParsedUri(_, _, _, _, hosts, _, _) => hosts must beSome(",host2,host3:5555")
      }
    }

    "parse options" in {
      "mongodb://host/?option1=foo&option2=bar" match {
        case ParsedUri(_, _, _, _, _, _, options) => options must beSome("option1=foo&option2=bar")
      }
    }

    "parse all of the above" in {
      "mongodb://me:pwd@host1:5555,host2:5559,host3/test?option1=foo&option2=bar" match {
        case ParsedUri(user, pwd, host, port, extraHosts, db, options) =>
          user must beSome("me")
          pwd must beSome("pwd")
          host must_== "host1"
          port must beSome(5555)
          extraHosts must beSome(",host2:5559,host3")
          db must beSome("test")
          options must beSome("option1=foo&option2=bar")
      }
    }
  }
}
