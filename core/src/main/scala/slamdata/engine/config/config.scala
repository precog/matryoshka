package slamdata.engine.config

import argonaut._, Argonaut._

import scalaz.concurrent.Task
import slamdata.engine.fs.Path

import scalaz.\/

final case class SDServerConfig(port: Option[Int])

object SDServerConfig {
  val DefaultPort = 20223

  implicit def Codec = casecodec1(SDServerConfig.apply, SDServerConfig.unapply)("port")
}

final case class Credentials(username: String, password: String)

object Credentials {
  implicit def Codec = casecodec2(Credentials.apply, Credentials.unapply)("username", "password")
}

sealed trait BackendConfig
final case class MongoDbConfig(connectionUri: String) extends BackendConfig {

}
object MongoDbConfig {
  implicit def Codec = casecodec1(MongoDbConfig.apply, MongoDbConfig.unapply)("connectionUri")

  /** This pattern is as lenient as possible, so that we can parse out the parts of any possible URI. */
  val UriPattern = (
    "^mongodb://" +
    "(?:" +
      "([^:]+):([^@]+)" +  // 0: username, 1: password
    "@)?" +
    "([^:/@,]+)" +         // 2: (primary) host [required]
    "(?::([0-9]+))?" +     // 3: (primary) port
    "((?:,[^,/]+)*)" +     // 4: additional hosts
    "(?:/" +
      "([^?]+)?" +         // 5: database
      "(?:\\?(.+))?" +     // 6: options
    ")?$").r
}

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
  mountings: Map[Path, BackendConfig]
)

object Config {
  private implicit val MapCodec = CodecJson[Map[Path, BackendConfig]](
    encoder = map => map.map(t => t._1.pathname -> t._2).asJson,
    decoder = cursor => implicitly[DecodeJson[Map[String, BackendConfig]]].decode(cursor).map(_.map(t => Path(t._1) -> t._2))
  )

  val DefaultConfig = Config(
    server = SDServerConfig(port = None),
    mountings = Map(
      Path.Root -> MongoDbConfig("mongodb://slamengine:slamengine@ds045089.mongolab.com:45089/slamengine-test-01")
    )
  )

  implicit def Codec = casecodec2(Config.apply, Config.unapply)("server", "mountings")

  def fromFile(path: String): Task[Config] = Task.delay {
    import java.nio.file._
    import java.nio.charset._

    val text = new String(Files.readAllBytes(Paths.get(path)), StandardCharsets.UTF_8);

    fromString(text).fold(
      error => throw new RuntimeException(error),
      identity
    )
  }

  def toFile(config: Config, path: String)(implicit encoder: EncodeJson[Config]): Task[Unit] = Task.delay {
    import java.nio.file._
    import java.nio.charset._

    val text = toString(config)

    val p = Paths.get(path)
    Option(p.getParent).map(Files.createDirectories(_))
    Files.write(p, text.getBytes(StandardCharsets.UTF_8))
  }

  def fromString(value: String): String \/ Config = Parse.decodeEither[Config](value)

  def toString(config: Config)(implicit encoder: EncodeJson[Config]): String = encoder.encode(config).pretty(slamdata.engine.fp.multiline)
}
