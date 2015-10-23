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
import monocle._
import scalaz.{Lens => _, _}, Scalaz._
import scalaz.concurrent.Task
import simulacrum.typeclass
import pathy._, Path._

sealed trait BackendConfig {
  def validate(path: EnginePath): EnvironmentError \/ Unit
}
final case class MongoDbConfig(uri: ConnectionString) extends BackendConfig {
  def validate(path: EnginePath) =
    if (path.relative) -\/(InvalidConfig("Not an absolute path: " + path))
    else if (!path.pureDir) -\/(InvalidConfig("Not a directory path: " + path))
    else \/-(())
}
object MongoConnectionString {
  def parse(uri: String): String \/ ConnectionString =
    \/.fromTryCatchNonFatal(new ConnectionString(uri)).leftMap(_.toString)

  def decode(uri: String): DecodeResult[ConnectionString] = {
    DecodeResult(parse(uri).leftMap(κ((s"invalid connection URI: $uri", CursorHistory(Nil)))))
  }
  implicit val codec: CodecJson[ConnectionString] =
    CodecJson[ConnectionString](
      c => jString(c.getURI),
      _.as[String].flatMap(decode))
}
object MongoDbConfig {
  import MongoConnectionString.codec
  implicit def Codec: CodecJson[MongoDbConfig] =
    casecodec1(MongoDbConfig.apply, MongoDbConfig.unapply)("connectionUri")
}

object BackendConfig {
  implicit def BackendConfig: CodecJson[BackendConfig] =
    CodecJson[BackendConfig](
      encoder = _ match {
        case x @ MongoDbConfig(_) => ("mongodb", MongoDbConfig.Codec.encode(x)) ->: jEmptyObject
      },
      decoder = _.get[MongoDbConfig]("mongodb").map(v => v: BackendConfig))
}

@typeclass trait Empty[A] {
  def empty: A
}

trait ConfigOps[C] {
  import FsPath._

  def mountingsLens: Lens[C, MountingsConfig]

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

  def fromFile(path: FsPath[File, Sandboxed])(implicit ev: DecodeJson[C]): EnvTask[C] = {
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

  def fromFileOrEmpty(path: Option[FsPath[File, Sandboxed]])(implicit ev1: DecodeJson[C], ev2: Empty[C])
    : EnvTask[C] = {
    def loadOr(path: FsPath[File, Sandboxed], alt: EnvTask[C]): EnvTask[C] =
      handleWith(fromFile(path)) {
        case _: java.nio.file.NoSuchFileException => alt
      }

    val empty = liftE[EnvironmentError](Task.now(Empty[C].empty))

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

  def loadAndTest(path: FsPath[File, Sandboxed])(implicit ev: DecodeJson[C]): EnvTask[C] =
    for {
      config <- fromFile(path)
      _      <- mountingsLens.get(config).values.toList.map(Backend.test).sequenceU
    } yield config


  def toFile(config: C, path: Option[FsPath[File, Sandboxed]])(implicit E: EncodeJson[C]): Task[Unit] = {
    import java.nio.file._
    import java.nio.charset._

    for {
      codec <- systemCodec
      p1    <- path.fold(defaultPath)(Task.now)
      cfg   <- Task.delay {
        val text = config.shows
        val p = Paths.get(printFsPath(codec, p1))
        ignore(Option(p.getParent).map(Files.createDirectories(_)))
        ignore(Files.write(p, text.getBytes(StandardCharsets.UTF_8)))
        ()
      }
    } yield cfg
  }

  def fromString(value: String)(implicit D: DecodeJson[C]): EnvironmentError \/ C =
    Parse.decodeEither[C](value).leftMap(InvalidConfig(_))

  implicit def showInstance(implicit ev: EncodeJson[C]): Show[C] = new Show[C] {
    override def shows(f: C): String = EncodeJson.of[C].encode(f).pretty(quasar.fp.multiline)
  }

}
