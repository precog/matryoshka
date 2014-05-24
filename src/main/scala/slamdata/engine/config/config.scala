package slamdata.engine.config

import argonaut._, Argonaut._

import scalaz.concurrent.Task

import scalaz.\/

final case class Server(host: String, port: Option[Int])

object Server {
  implicit def Codec = casecodec2(Server.apply, Server.unapply)("host", "port")
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
      case x : MongoDbConfig =>  ("mongodb", MongoDbConfig.Codec.encode(x)) ->: jEmptyObject
    },
    decoder = { cursor =>
      cursor.get[MongoDbConfig]("mongodb").map(v => v : BackendConfig)
    }
  )
}

final case class Config(
  mountings: Map[String, BackendConfig]
)

object Config {
  implicit def Codec = casecodec1(Config.apply, Config.unapply)("mountings")

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