package slamdata.engine.config

import slamdata.engine.fs._

import org.specs2.mutable._

class ConfigSpec extends Specification {
  val TestConfig = Config(
    server = SDServerConfig(Some(92)),
    mountings = Map(
      Path.Root -> MongoDbConfig("slamengine-test-01", "mongodb://slamengine:slamengine@ds045089.mongolab.com:45089/slamengine-test-01")
    )
  )

  val ConfigStr =
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

  "fromString" should {
    "parse valid config" in {
      Config.fromString(ConfigStr).toOption must beSome(TestConfig)
    }
  }

  "toString" should {
    "render same config" in {
      Config.toString(TestConfig) must_== ConfigStr
    }
  }
}
