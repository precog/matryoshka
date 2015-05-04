package slamdata.engine.config

import argonaut._, Argonaut._

import scalaz.concurrent.Task

import slamdata.engine.Backend
import slamdata.engine.fp._
import slamdata.engine.fs.Path

import scalaz._
import Scalaz._

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

  object ParsedUri {
    /** This pattern is as lenient as possible, so that we can parse out the
        parts of any possible URI. */
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

    def orNone(s: String) = if (s == "") None else Some(s)

    // TODO: Convert host/addHosts to NonEmptyList[(String, Option[Int])] and
    //       opts to a Map[String, String]
    def unapply(uri: String):
        Option[(Option[String], Option[String], String, Option[Int], Option[String], Option[String], Option[String])] =
      uri match {
        case UriPattern(user, pass, host, port, addHosts, authDb, opts) =>
          Some((Option(user), Option(pass), host, Option(port).flatMap(_.parseInt.toOption), orNone(addHosts), Option(authDb), Option(opts)))
        case _ => None
      }
  }
}

object BackendConfig {
  implicit def BackendConfig = CodecJson[BackendConfig](
    encoder = _ match {
      case x : MongoDbConfig => ("mongodb", MongoDbConfig.Codec.encode(x)) ->: jEmptyObject
    },
    decoder = { cursor =>
      cursor.get[MongoDbConfig]("mongodb").map(v => v : BackendConfig)
    })
}

final case class Config(
  server:    SDServerConfig,
  mountings: Map[Path, BackendConfig])

object Config {
  private implicit val MapCodec = CodecJson[Map[Path, BackendConfig]](
    encoder = map => map.map(t => t._1.pathname -> t._2).asJson,
    decoder = cursor => implicitly[DecodeJson[Map[String, BackendConfig]]].decode(cursor).map(_.map(t => Path(t._1) -> t._2)))

  private def defaultPath: Task[String] = Task.delay {
    import scala.util.Properties._

    val commonPath = "SlamData/slamengine-config.json"

    if (isWin)
      envOrElse("LOCALAPPDATA", propOrElse("user.home", ".")) + commonPath
    else
      propOrElse("user.home", ".") +
        (if (isMac) "/Library/Application Support/" else "/.config/") +
        commonPath
  }

  def load(path: Option[String]): Task[Config] =
    path.fold(defaultPath.flatMap(fromFile(_)))(fromFile(_))

  implicit def Codec = casecodec2(Config.apply, Config.unapply)("server", "mountings")

  def fromFile(path: String): Task[Config] = {
    import java.nio.file._
    import java.nio.charset._

    for {
      text <- Task.delay(new String(Files.readAllBytes(Paths.get(path)),
                                    StandardCharsets.UTF_8))
      path <- fromString(text).fold(
                e => Task.fail(new RuntimeException("Failed to parse " + path + ": " + e)),
                _.pure[Task])
    } yield path
  }

  def loadAndTest(path: Option[String]): Task[Config] = for {
    config <- load(path)
    tests  <- config.mountings.values.map(Backend.test).toList.sequence
    rez    <- if (tests.isEmpty || tests.collect { case Backend.TestResult.Failure(_, _) => () }.nonEmpty)
                Task.fail(new RuntimeException("mounting(s) failed"))
              else Task.now(config)
  } yield rez

  def toFile(config: Config, path: String)(implicit encoder: EncodeJson[Config]): Task[Unit] = Task.delay {
    import java.nio.file._
    import java.nio.charset._

    val text = toString(config)

    val p = Paths.get(path)
    ignore(Option(p.getParent).map(Files.createDirectories(_)))
    ignore(Files.write(p, text.getBytes(StandardCharsets.UTF_8)))
    ()
  }

  def write(config: Config, path: Option[String]): Task[Unit] =
    path.fold(defaultPath.flatMap(toFile(config, _)))(toFile(config, _))

  def fromString(value: String): String \/ Config =
    Parse.decodeEither[Config](value)

  def toString(config: Config)(implicit encoder: EncodeJson[Config]): String =
    encoder.encode(config).pretty(slamdata.engine.fp.multiline)

  implicit val ShowConfig = new Show[Config] {
    override def shows(f: Config) = Config.toString(f)
  }
}
