package slamdata.engine.config

import slamdata.engine.fs._

import org.specs2.mutable._

class ConfigSpec extends Specification {
  val TestConfig = Config(
    server = SDServerConfig(Some(92)),
    mountings = Map(
      Path.Root -> MongoDbConfig("mongodb://slamengine:slamengine@ds045089.mongolab.com:45089/slamengine-test-01")))

  val BrokenTestConfig = Config(
    server = SDServerConfig(Some(92)),
    mountings = Map(
      Path.Root -> MongoDbConfig("mongodb://slamengine:slamengine@ds045088.mongolab.com:45089/slamengine-test-01")))

  val testConfigFile = "test-config.json"

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
      Config.fromString(ConfigStr).toOption must beSome(TestConfig)
    }

    "parse previous config" in {
      Config.fromString(OldConfigStr).toOption must beSome(TestConfig)
    }
  }

  "toString" should {
    "render same config" in {
      Config.toString(TestConfig) must_== ConfigStr
    }
  }

  "write" should {
    "create loadable config" in {
      val fileName = scala.util.Random.nextInt.toString + testConfigFile
      (for {
        _ <- Config.write(TestConfig, Some(fileName))
        config <- Config.load(Some(fileName))
        _ = java.nio.file.Files.delete(java.nio.file.Paths.get(fileName))
      } yield config must_== TestConfig).run
    }
  }

  "loadAndTest" should {
    "load a correct config" in {
      val fileName = scala.util.Random.nextInt.toString + testConfigFile
      (for {
        _ <- Config.write(TestConfig, Some(fileName))
        config <- Config.loadAndTest(Some(fileName))
        _ = java.nio.file.Files.delete(java.nio.file.Paths.get(fileName))
      } yield config must_== TestConfig).run
    }

    "fail on a config with incorrect mounting" in {
      val fileName = scala.util.Random.nextInt.toString + testConfigFile
      val rez = (for {
        _ <- Config.write(BrokenTestConfig, Some(fileName))
        config <- Config.loadAndTest(Some(fileName))
      } yield config).run must throwA(new RuntimeException("mounting(s) failed"))
      java.nio.file.Files.delete(java.nio.file.Paths.get(fileName))

      rez
    }
  }

  "MongoDbConfig.UriPattern" should {
    import MongoDbConfig._

    "parse simple URI" in {
      "mongodb://localhost" match {
        case UriPattern(_, _, host, _, _, _, _) => host must_== "localhost"
      }
    }

    "parse simple URI with trailing slash" in {
      "mongodb://localhost/" match {
        case UriPattern(_, _, host, _, _, _, _) => host must_== "localhost"
      }
    }

    "parse user/password" in {
      "mongodb://me:pwd@localhost" match {
        case UriPattern(user, pwd, _, _, _, _, _) => user must_== "me"; pwd must_== "pwd"
      }
    }

    "parse port" in {
      "mongodb://localhost:5555" match {
        case UriPattern(_, _, _, port, _, _, _) => port.toInt must_== 5555
      }
    }

    "parse database" in {
      "mongodb://localhost/test" match {
        case UriPattern(_, _, _, _, _, db, _) => db must_== "test"
      }
    }

    "parse additional host(s)" in {
      "mongodb://host1,host2,host3:5555" match {
        case UriPattern(_, _, _, _, hosts, _, _) => hosts must_== ",host2,host3:5555"
      }
    }

    "parse options" in {
      "mongodb://host/?option1=foo&option2=bar" match {
        case UriPattern(_, _, _, _, _, _, options) => options must_== "option1=foo&option2=bar"
      }
    }

    "parse all of the above" in {
      "mongodb://me:pwd@host1:5555,host2:5559,host3/test?option1=foo&option2=bar" match {
        case UriPattern(user, pwd, host, port, extraHosts, db, options) =>
          user must_== "me"
          pwd must_== "pwd"
          host must_== "host1"
          port.toInt must_== 5555
          extraHosts must_== ",host2:5559,host3"
          db must_== "test"
          options must_== "option1=foo&option2=bar"
      }
    }
  }
}
