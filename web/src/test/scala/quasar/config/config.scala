package quasar.config

import quasar.Predef._
import quasar.fs.{Path => EnginePath}

import com.mongodb.ConnectionString

class WebConfigSpec extends ConfigSpec[WebConfig] {

  def configOps: ConfigOps[WebConfig] = WebConfig

  def sampleConfig(uri: ConnectionString): WebConfig = WebConfig(
    server = ServerConfig(92),
    mountings = Map(
      EnginePath.Root -> MongoDbConfig(uri)))

  override val ConfigStr =
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

}
