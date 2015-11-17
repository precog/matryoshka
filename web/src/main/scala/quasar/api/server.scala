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

package quasar.api

import quasar.Predef._
import quasar.fp._
import quasar.api.ServerOps._
import quasar.console._
import quasar._, Errors._, Evaluator._
import quasar.config._

import java.io.File
import java.lang.System
import scala.concurrent.duration._

import argonaut.CodecJson
import scalaz._, Scalaz._
import scalaz.concurrent._
import scalaz.stream._

import org.http4s.server.{Server => Http4sServer, HttpService}
import org.http4s.server.blaze.BlazeBuilder

object ServerOps {
  type Builders = List[(Int, BlazeBuilder)]
  type Servers = List[(Int, Http4sServer)]
  type ServersErrors = List[(Int, Throwable \/ Http4sServer)]
  type Services = List[(String, HttpService)]

  final case class Options(
    config: Option[String],
    contentLoc: Option[String],
    contentPath: Option[String],
    contentPathRelative: Boolean,
    openClient: Boolean,
    port: Option[Int])
}

abstract class ServerOps[WC: CodecJson, SC](
  configOps: ConfigOps[WC],
  fileSystemApi: FileSystemApi.Apply[WC, SC],
  defaultWC: WC,
  val webConfigLens: WebConfigLens[WC, SC]) {
  import webConfigLens._

  // NB: This is a terrible thing.
  //     Is there a better way to find the path to a jar?
  val jarPath: Task[String] =
    Task.delay {
      val uri = getClass.getProtectionDomain.getCodeSource.getLocation.toURI
      val path0 = uri.getPath
      val path =
        java.net.URLDecoder.decode(
          Option(uri.getPath)
            .getOrElse(uri.toURL.openConnection.asInstanceOf[java.net.JarURLConnection].getJarFileURL.getPath),
          "UTF-8")
      (new File(path)).getParentFile().getPath() + "/"
    }

  /** Returns why the given port is unavailable or None if it is available. */
  def unavailableReason(port: Int): OptionT[Task, String] =
    OptionT(Task.delay(new java.net.ServerSocket(port)).attempt.flatMap {
      case -\/(err: java.net.BindException) => Task.now(Some(err.getMessage))
      case -\/(err)                         => Task.fail(err)
      case \/-(s)                           => Task.delay(s.close()).as(None)
    })

  /** An available port number. */
  def anyAvailablePort: Task[Int] = Task.delay {
    val s = new java.net.ServerSocket(0)
    val p = s.getLocalPort
    s.close()
    p
  }

  /** Returns the requested port if available, or the next available port. */
  def choosePort(requested: Int): Task[Int] =
    unavailableReason(requested)
      .flatMapF(rsn => stderr("Requested port not available: " + requested + "; " + rsn) *>
                       anyAvailablePort)
      .getOrElse(requested)

  def builders(config: WC, idleTimeout: Duration): Task[Builders]

  def createServers(config: WC, idleTimeout: Duration, svcs: EnvTask[Services])
    : EnvTask[ServersErrors] = {

    def servicesBuilder(services: Services, builders: Builders): Builders =
      services.foldRight(builders) {
          case ((path, svc), bs) => bs.map { case (p, b) => (p, b.mountService(Prefix(path)(svc))) }
        }

    def startBuilder(builders: Builders): EnvTask[ServersErrors] = EitherT.right {
      builders.traverse { case (p, b) =>
        b.start.map(s => (p, s.right)).handleWith { case ex => Task.now((p, ex.left)) }
      }
    }

    (svcs ⊛ EitherT.right(builders(config, idleTimeout)))(servicesBuilder) >>= startBuilder
  }

  case class StaticContent(loc: String, path: String)

  /**
   * Returns a process of (port, server) and an effectful function which will
   * start a server using the provided configuration.
   *
   * The process will emit each time a new server is started and ensures only
   * one server is running at a time, i.e. calling the function to start a
   * server automatically stops any running server.
   *
   * Pass [[scala.None]] to the returned function to shutdown any running server and
   * prevent new ones from being started.
   */
  def servers(staticContent: List[StaticContent], redirect: Option[String],
              idleTimeout: Duration, tester: MountConfig => EnvTask[Unit],
              mounter: WC => EnvTask[Backend], configWriter: WC => Task[Unit])
             : (Process[Task, Servers], Option[WC] => Task[Unit]) = {

    val configQ = async.boundedQueue[Option[WC]](2)(Strategy.DefaultStrategy)
    val reload = (cfg: WC) => configQ.enqueueOne(Some(cfg))

    val fileSvcs = staticContent.map { case StaticContent(l, p) => l -> staticFileService(p) }
    val redirSvc = List("/" -> redirectService(redirect.getOrElse("/welcome")))

    def portsString(servers: Servers): String = servers.map { case (p, _) => p }.mkString(" ")

    def start(config: WC): EnvTask[Servers] =
      for {
        port <- choosePort(wcPort.get(config)).liftM[EnvErrT]
        fsApi = fileSystemApi(config, mounter, tester, reload, configWriter, webConfigLens)
        updCfg =  wcPort.set(port)(config)
        serversErrors <- createServers(updCfg, idleTimeout, fsApi.AllServices.map(_.toList ++ fileSvcs ++ redirSvc))
        servers <- serversErrors.traverseM {
          case (p, -\/(ex)) => Task.now(List()) <* stdout(s"Server failed to start listening on port $p. ${ex.getMessage}")
          case (p, \/-(s)) => Task.now(List((p, s))) <* stdout(s"Server started listening on port $p")
        }.liftM[EnvErrT]
      } yield servers

    def shutdown(srv: Option[Servers], log: Boolean): Task[Unit] =
      Foldable[Option].compose[List].traverse_(srv) { case (p, s) =>
        s.shutdown *> (if (log) stdout(s"Stopped server listening on port $p") else Task.now(()))
      }

    def go(prevServers: Option[Servers]): Process[Task, Servers] =
      configQ.dequeue.take(1) flatMap {
        case Some(cfg) =>
          Process.await(shutdown(prevServers, true) *> start(cfg).run)(_.fold(
            err => Process.halt.causedBy(Cause.Error(new RuntimeException(err.message))),
            tpl => Process.emit(tpl) ++ go(Some(tpl))
          ))

        case None =>
          Process.eval_(shutdown(prevServers, true))
      }

    (go(None).onComplete(Process.eval_(configQ.kill)), configQ.enqueueOne)
  }

  // Lifted from unfiltered.
  // NB: available() returns 0 when the stream is closed, meaning the server
  //     will run indefinitely when started from a script.
  private def waitForInput: Task[Unit] = for {
    _    <- Task.delay(java.lang.Thread.sleep(250))
                .handle { case _: java.lang.InterruptedException => () }
    test <- Task.delay(Option(System.console).isEmpty || System.in.available() <= 0)
                .handle { case _ => true }
    done <- if (test) waitForInput else Task.now(())
  } yield done

  private def openBrowser(port: Int): Task[Unit] = {
    val url = "http://localhost:" + port + "/"
    Task.delay(java.awt.Desktop.getDesktop().browse(java.net.URI.create(url)))
        .or(stderr("Failed to open browser, please navigate to " + url))
  }

  // scopt's recommended OptionParser construction involves side effects
  @SuppressWarnings(Array("org.brianmckenna.wartremover.warts.NonUnitStatements"))
  val optionParser = new scopt.OptionParser[Options]("quasar") {
    head("quasar")
    opt[String]('c', "config") action { (x, c) => c.copy(config = Some(x)) } text("path to the config file to use")
    opt[String]('L', "content-location") action { (x, c) => c.copy(contentLoc = Some(x)) } text("location where static content is hosted")
    opt[String]('C', "content-path") action { (x, c) => c.copy(contentPath = Some(x)) } text("path where static content lives")
    opt[Unit]('r', "content-path-relative") action { (_, c) => c.copy(contentPathRelative = true) } text("specifies that the content-path is relative to the install directory (not the current dir)")
    opt[Unit]('o', "open-client") action { (_, c) => c.copy(openClient = true) } text("opens a browser window to the client on startup")
    opt[Int]('p', "port") action { (x, c) => c.copy(port = Some(x)) } text("the port to run Quasar on")
    help("help") text("prints this usage text")
  }

  def interpretPaths(options: Options): EnvTask[Option[StaticContent]] = {
    val defaultLoc = "/files"

    def path(p: String): EnvTask[String] =
      liftE(
        if (options.contentPathRelative) jarPath.map(_ + p)
        else Task.now(p))

    (options.contentLoc, options.contentPath) match {
      case (None, None) => none.point[EnvTask]

      case (Some(_), None) => EitherT.left(Task.now(InvalidConfig("content-location specified but not content-path")))

      case (loc, Some(p)) => path(p).map(p => Some(StaticContent(loc.getOrElse(defaultLoc), p)))
    }
  }

  def main(args: Array[String]): Unit = {
    val idleTimeout = Duration.Inf

    def reactToFirstServersStarted(openClient: Boolean): Sink[Task, Servers] =
      Process.emit[Servers => Task[Unit]] { m =>
        val msg = stdout("Press Enter to stop.")
        if (openClient) m.map { case (p, _) => openBrowser(p) }.sequence *> msg else msg
      } ++ Process.constant(κ(Task.now(())))

    val exec: EnvTask[Unit] = for {
      opts           <- optionParser.parse(args, Options(None, None, None, false, false, None)).cata(
                          _.point[EnvTask],
                          EitherT.left(Task.now(InvalidConfig("couldn’t parse options"))))
      content <- interpretPaths(opts)
      redirect = content.map(_.loc).getOrElse("/welcome")
      cfgPath        <- opts.config.fold[EnvTask[Option[FsPath[pathy.Path.File, pathy.Path.Sandboxed]]]](
          liftE(Task.now(None)))(
          cfg => FsPath.parseSystemFile(cfg).toRight(InvalidConfig("Invalid path to config file: " + cfg)).map(Some(_)))
      config         <- configOps.fromFileOrDefaultPaths(cfgPath).orElse(EitherT.right(Task.now(defaultWC)))
      port           =  opts.port getOrElse wcPort.get(config)
      updCfg         =  wcPort.set(port)(config)
      (proc, useCfg) =  servers(content.toList, Some(redirect), idleTimeout, Backend.test,
                                cfg => Mounter.defaultMount(mountings.get(cfg)),
                                cfg => configOps.toFile(cfg, cfgPath))
      _              <- Task.gatherUnordered(List(
                          proc.observe(reactToFirstServersStarted(opts.openClient)).run,
                          useCfg(Some(updCfg)),
                          waitForInput *> useCfg(None)
                        )).liftM[EnvErrT]
    } yield ()

    exec.swap
      .flatMap(e => stderr(e.message).liftM[EitherT[?[_], Unit, ?]])
      .merge
      .handleWith { case err => stderr(err.getMessage) }
      .run
  }
}

// https://github.com/puffnfresh/wartremover/issues/149
@SuppressWarnings(Array("org.brianmckenna.wartremover.warts.NonUnitStatements"))
object Server extends ServerOps(
  WebConfig,
  FileSystemApi.apply,
  WebConfig(ServerConfig(None), Map()),
  WebConfigLens(
    WebConfig.server,
    WebConfig.mountings,
    WebConfig.server composeLens ServerConfig.port,
    ServerConfig.port)) {
  import webConfigLens._

  def builders(config: WebConfig, idleTimeout: Duration): Task[Builders] = Task.now(List(
    wcPort.get(config) -> BlazeBuilder
      .withIdleTimeout(idleTimeout)
      .bindHttp(wcPort.get(config), "0.0.0.0")))

}
