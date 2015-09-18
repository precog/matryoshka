/*
 * Copyright 2014 - 2015 SlamData Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package quasar.config

import quasar.Predef._
import quasar.fp._
import quasar._, Evaluator._, Errors._
import quasar.fs.{Path => EnginePath}

import java.io.{File => JFile}
import scala.util.Properties._
import argonaut._, Argonaut._
import scalaz._, Scalaz._
import scalaz.concurrent.Task
import pathy._, Path._

final case class SDServerConfig(port0: Option[Int]) {
  val port = port0.getOrElse(SDServerConfig.DefaultPort)
}

object SDServerConfig {
  val DefaultPort = 20223

  implicit def Codec = casecodec1(SDServerConfig.apply, SDServerConfig.unapply)("port")
}

final case class Credentials(username: String, password: String)

object Credentials {
  implicit def Codec = casecodec2(Credentials.apply, Credentials.unapply)("username", "password")
}

sealed trait BackendConfig {
  def validate(path: EnginePath): EnvironmentError \/ Unit
}
final case class MongoDbConfig(connectionUri: String) extends BackendConfig {
  def validate(path: EnginePath) = for {
    _ <- MongoDbConfig.ParsedUri.unapply(connectionUri).map(κ(())) \/> (InvalidConfig("invalid connection URI: " + connectionUri))
    _ <- if (path.relative) -\/(InvalidConfig("Not an absolute path: " + path)) else \/-(())
    _ <- if (!path.pureDir) -\/(InvalidConfig("Not a directory path: " + path)) else \/-(())
  } yield ()
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
        "([^/?]+)?" +        // 5: database
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
      case x @ MongoDbConfig(_) => ("mongodb", MongoDbConfig.Codec.encode(x)) ->: jEmptyObject
    },
    decoder = _.get[MongoDbConfig]("mongodb").map(v => v: BackendConfig))
}

final case class Config(
  server:    SDServerConfig,
  mountings: Map[EnginePath, BackendConfig])

object Config {
  import FsPath._

  val empty = Config(SDServerConfig(None), Map())

  private implicit val MapCodec = CodecJson[Map[EnginePath, BackendConfig]](
    encoder = map => map.map(t => t._1.pathname -> t._2).asJson,
    decoder = cursor => implicitly[DecodeJson[Map[String, BackendConfig]]].decode(cursor).map(_.map(t => EnginePath(t._1) -> t._2)))

  implicit def configCodecJson = casecodec2(Config.apply, Config.unapply)("server", "mountings")

  def defaultPathForOS(file: RelFile[Sandboxed])(os: OS): Task[FsPath[File, Sandboxed]] = {
    def localAppData: OptionT[Task, FsPath.Aux[Abs, Dir, Sandboxed]] =
      OptionT(Task.delay(envOrNone("LOCALAPPDATA")))
        .flatMap(s => OptionT(parseWinAbsAsDir(s).point[Task]))

    def homeDir: OptionT[Task, FsPath.Aux[Abs, Dir, Sandboxed]] =
      OptionT(Task.delay(propOrNone("user.home")))
        .flatMap(s => OptionT(parseAbsAsDir(os, s).point[Task]))

    val dirPath: RelDir[Sandboxed] = os.fold(
      currentDir,
      dir("Library") </> dir("Application Support"),
      dir(".config"))

    val baseDir = OptionT.some[Task, Boolean](os.isWin)
      .ifM(localAppData, OptionT.none)
      .orElse(homeDir)
      .map(_.forgetBase)
      .getOrElse(Uniform(currentDir))

    baseDir map (_ </> dirPath </> file)
  }

  /**
   * The default path to the configuration file for the current operating system.
   *
   * NB: Paths read from environment/props are assumed to be absolute.
   */
  private def defaultPath: Task[FsPath[File, Sandboxed]] =
    OS.currentOS >>= defaultPathForOS(dir("quasar") </> file("quasar-config.json"))

  private def alternatePath: Task[FsPath[File, Sandboxed]] =
    OS.currentOS >>= defaultPathForOS(dir("SlamData") </> file("slamengine-config.json"))

  def fromFile(path: FsPath[File, Sandboxed]): EnvTask[Config] = {
    import java.nio.file._
    import java.nio.charset._

    for {
      codec  <- liftE[EnvironmentError](systemCodec)
      strPath = printFsPath(codec, path)
      text   <- liftE[EnvironmentError](Task.delay(
                  new String(Files.readAllBytes(Paths.get(strPath)), StandardCharsets.UTF_8)))
      config <- EitherT(Task.now(fromString(text).leftMap {
                  case InvalidConfig(message) => InvalidConfig("Failed to parse " + path + ": " + message)
                  case e => e
                }))
    } yield config
  }

  def fromFileOrEmpty(path: Option[FsPath[File, Sandboxed]]): EnvTask[Config] = {
    def loadOr(path: FsPath[File, Sandboxed], alt: EnvTask[Config]): EnvTask[Config] = 
      handleWith(fromFile(path)) { 
        case _: java.nio.file.NoSuchFileException => alt
      }
    
    val empty = liftE[EnvironmentError](Task.now(Config.empty))
    
    path match {
      case Some(path) => 
        loadOr(path, empty)
      case None =>
        liftE(defaultPath).flatMap { p =>
          loadOr(p, liftE(alternatePath).flatMap { p =>
            loadOr(p, empty)
          })
        }
    }
  }

  def loadAndTest(path: FsPath[File, Sandboxed]): EnvTask[Config] = for {
    config <- fromFile(path)
    _      <- config.mountings.values.toList.map(Backend.test).sequenceU
  } yield config

  def toFile(config: Config, path: Option[FsPath[File, Sandboxed]])(implicit encoder: EncodeJson[Config]): Task[Unit] = {
    import java.nio.file._
    import java.nio.charset._

    for {
      codec <- systemCodec
      p1    <- path.fold(defaultPath)(Task.now)
      cfg   <- Task.delay {
        val text = toString(config)
        val p = Paths.get(printFsPath(codec, p1))
        ignore(Option(p.getParent).map(Files.createDirectories(_)))
        ignore(Files.write(p, text.getBytes(StandardCharsets.UTF_8)))
        ()
      }
    } yield cfg
  }

  def fromString(value: String): EnvironmentError \/ Config =
    Parse.decodeEither[Config](value).leftMap(InvalidConfig(_))

  def toString(config: Config)(implicit encoder: EncodeJson[Config]): String =
    encoder.encode(config).pretty(quasar.fp.multiline)

  implicit val ShowConfig = new Show[Config] {
    override def shows(f: Config) = Config.toString(f)
  }
}
