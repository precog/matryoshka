package slamdata.engine.config

import slamdata.engine.DisjunctionMatchers
import slamdata.engine.fs._

import org.specs2.mutable._

class ConfigSpec extends Specification with DisjunctionMatchers {
  val TestConfig = Config(
    server = SDServerConfig(Some(92)),
    mountings = Map(
      Path.Root -> MongoDbConfig("mongodb://slamengine:slamengine@ds045089.mongolab.com:45089/slamengine-test-01")
    )
  )

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
      Config.fromString(ConfigStr) must beRightDisj(TestConfig)
    }

    "parse previous config" in {
      Config.fromString(OldConfigStr) must beRightDisj(TestConfig)
    }
  }

  "toString" should {
    "render same config" in {
      Config.toString(TestConfig) must_== ConfigStr
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
        case ParsedUri(_, _, _, _, hosts, _, _) => hosts must_== ",host2,host3:5555"
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
          extraHosts must_== ",host2:5559,host3"
          db must beSome("test")
          options must beSome("option1=foo&option2=bar")
      }
    }
  }
}
