package slamdata.engine.config

import argonaut._, Argonaut._

sealed trait BackendConfig

final case class MongoDbConfig(server: String, port: Option[Int], username: Option[String], password: Option[String]) extends BackendConfig
object MongoDbConfig { implicit def Codec = casecodec4(MongoDbConfig.apply, MongoDbConfig.unapply)("server", "port", "username", "password") }

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
}