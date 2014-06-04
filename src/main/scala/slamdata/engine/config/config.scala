package slamdata.engine.config

import argonaut._, Argonaut._

import scalaz.concurrent.Task

import scalaz.\/

final case class SDServerConfig(port: Option[Int])

object SDServerConfig {
  implicit def Codec = casecodec1(SDServerConfig.apply, SDServerConfig.unapply)("port")
}

final case class Credentials(username: String, password: String)

object Credentials {
  implicit def Codec = casecodec2(Credentials.apply, Credentials.unapply)("username", "password")
}

sealed trait BackendConfig
final case class MongoDbConfig(database: String, connectionUri: String) extends BackendConfig
object MongoDbConfig { implicit def Codec = casecodec2(MongoDbConfig.apply, MongoDbConfig.unapply)("database", "connectionUri") }

object BackendConfig {
  implicit def BackendConfig = CodecJson[BackendConfig](
    encoder = _ match {
      case x : MongoDbConfig => ("mongodb", MongoDbConfig.Codec.encode(x)) ->: jEmptyObject
    },
    decoder = { cursor =>
      cursor.get[MongoDbConfig]("mongodb").map(v => v : BackendConfig)
    }
  )
}

final case class Config(
  server:    SDServerConfig,
  mountings: Map[String, BackendConfig]
)

object Config {
  val DefaultConfig = Config(
    server = SDServerConfig(port = None),
    mountings = Map(
      "/" -> MongoDbConfig("slamengine-test-01", "mongodb://slamengine:slamengine@ds045089.mongolab.com:45089/slamengine-test-01")
    )
  )

  implicit def Codec = casecodec2(Config.apply, Config.unapply)("server", "mountings")

  def fromFile(path: String): Task[Config] = Task.delay {
    import java.nio.file._
    import java.nio.charset._

    val text = new String(Files.readAllBytes(Paths.get("file")), StandardCharsets.UTF_8);

    fromString(text).fold(
      error => throw new RuntimeException(error),
      identity
    )
  }

  def fromString(value: String): String \/ Config = Parse.decodeEither[Config](value)
}