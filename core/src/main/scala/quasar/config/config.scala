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

import com.mongodb.ConnectionString
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

sealed trait MountConfig {
  def validate(path: EnginePath): EnvironmentError \/ Unit
}
final case class MongoDbConfig(uri: ConnectionString) extends MountConfig {
  def validate(path: EnginePath) = for {
    _ <- if (path.relative) -\/(InvalidConfig("Not an absolute path: " + path)) else \/-(())
    _ <- if (!path.pureDir) -\/(InvalidConfig("Not a directory path: " + path)) else \/-(())
  } yield ()
}
object MongoConnectionString {
  def parse(uri: String): String \/ ConnectionString =
    \/.fromTryCatchNonFatal(new ConnectionString(uri)).leftMap(_.toString)

  def decode(uri: String): DecodeResult[ConnectionString] = {
    DecodeResult(parse(uri).leftMap(κ((s"invalid connection URI: $uri", CursorHistory(Nil)))))
  }
  implicit val codec = CodecJson[ConnectionString](
    c => jString(c.getURI),
    cursor => cursor.as[String].flatMap(decode))
}
object MongoDbConfig {
  import MongoConnectionString.codec
  implicit def Codec = casecodec1(MongoDbConfig.apply, MongoDbConfig.unapply)("connectionUri")
}

final case class ViewConfig(tempPath: Option[EnginePath], query: sql.Expr) extends MountConfig {
  def validate(path: EnginePath) = for {
    _ <- if (path.relative) -\/(InvalidConfig("Not an absolute path: " + path)) else \/-(())
    _ <- if (path.pureDir) -\/(InvalidConfig("Not a file path: " + path)) else \/-(())
  } yield ()
}
object ViewConfig {
  private val UriPattern = "([a-z][a-z0-9+-.]*):///([^?]*)\\?q=(.+)".r

  private def fromUri(uri: String): String \/ ViewConfig = for {
    gs       <- UriPattern.unapplySeq(uri) \/> ("could not parse URI: " + uri)
    scheme   <- nonEmpty(gs(0))            \/> ("missing URI scheme: " + uri)
    _        <- if (scheme == "sql2") \/-(()) else -\/("unrecognized scheme: " + scheme)
    path     =  nonEmpty(gs(1)).map(EnginePath(_))
    queryEnc <- nonEmpty(gs(2))            \/> ("missing query: " + uri)
    queryStr <- \/.fromTryCatchNonFatal(java.net.URLDecoder.decode(queryEnc, "UTF-8")).leftMap(_.getMessage + "; " + queryEnc)
    query <- new sql.SQLParser().parse(sql.Query(queryStr)).leftMap(_.message)
  } yield ViewConfig(path, query)

  private def toUri(cfg: ViewConfig) =
    "sql2:///" + cfg.tempPath.map(_.simplePathname).getOrElse("") + "?q=" + java.net.URLEncoder.encode(sql.pprint(cfg.query), "UTF-8")

  implicit def Codec = CodecJson[ViewConfig](
    cfg => Json("uri" := toUri(cfg)),
    c => {
      val uriC = (c --\ "uri")
      for {
        uri <- uriC.as[String]
        cfg <- DecodeResult(fromUri(uri).leftMap(e => (e.toString, uriC.history)))
      } yield cfg
    })

  private def nonEmpty(strOrNull: String) =
    if (strOrNull == "") None else Option(strOrNull)
}

object MountConfig {
  implicit val Codec = CodecJson[MountConfig](
    encoder = _ match {
      case x @ MongoDbConfig(_) => ("mongodb", MongoDbConfig.Codec.encode(x)) ->: jEmptyObject
      case x @ ViewConfig(_, _) => ("view", ViewConfig.Codec.encode(x)) ->: jEmptyObject
    },
    decoder = c => (c.fields match {
      case Some("mongodb" :: Nil) => c.get[MongoDbConfig]("mongodb")
      case Some("view" :: Nil)    => c.get[ViewConfig]("view")
      case Some(t :: Nil)         => DecodeResult.fail("unrecognized mount type: " + t, c.history)
      case _                      => DecodeResult.fail("invalid mount: " + c.focus, c.history)
    }).map(v => v: MountConfig))
}

final case class Config(
  server:    SDServerConfig,
  mountings: Map[EnginePath, MountConfig])

object Config {
  import FsPath._

  val empty = Config(SDServerConfig(None), Map())

  private implicit val MapCodec = CodecJson[Map[EnginePath, MountConfig]](
    encoder = map => map.map(t => t._1.pathname -> t._2).asJson,
    decoder = cursor => implicitly[DecodeJson[Map[String, MountConfig]]].decode(cursor).map(_.map(t => EnginePath(t._1) -> t._2)))

  implicit def Codec = casecodec2(Config.apply, Config.unapply)("server", "mountings")

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
