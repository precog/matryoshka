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

package slamdata.engine.api

import slamdata.Predef._
import slamdata.fp._
import slamdata.engine._, Errors._, Evaluator._
import slamdata.engine.config._

import java.io.File
import java.lang.System
import scala.concurrent.duration._

import scalaz._, Scalaz._
import scalaz.concurrent._
import scalaz.stream._

import org.http4s.server.{Server => Http4sServer}
import org.http4s.server.blaze.BlazeBuilder

object Server {
  private def info(msg: => String): Task[Unit] =
    Task.delay(println(msg))

  private def error(msg: => String): Task[Unit] =
    Task.delay(System.err.println(msg))

  // NB: This is a terrible thing.
  //     Is there a better way to find the path to a jar?
  val jarPath: Task[String] =
    Task.delay {
      val uri = Server.getClass.getProtectionDomain.getCodeSource.getLocation.toURI
      val path0 = uri.getPath
      val path =
        java.net.URLDecoder.decode(
          if (path0 == null)
            uri.toURL.openConnection.asInstanceOf[java.net.JarURLConnection].getJarFileURL.getPath
          else path0,
          "UTF-8")
      (new File(path)).getParentFile().getPath() + "/docroot"
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
      .flatMapF(rsn => error(s"Requested port not available: $requested; $rsn") *>
                       anyAvailablePort)
      .getOrElse(requested)

  def createServer(port: Int, idleTimeout: Duration, api: FileSystemApi): Task[Http4sServer] = {
    val builder = BlazeBuilder
                  .withIdleTimeout(idleTimeout)
                  .bindHttp(port, "0.0.0.0")

    api.AllServices.flatMap(_.toList.reverse.foldLeft(builder) {
      case (b, (path, svc)) => b.mountService(Prefix(path)(svc))
    }.start)
  }

  /**
   * Returns a process of (port, server) and an effectful function which will
   * start a server using the provided configuration.
   *
   * The process will emit each time a new server is started and ensures only
   * one server is running at a time, i.e. calling the function to start a
   * server automatically stops any running server.
   *
   * Pass [[None]] to the returned function to shutdown any running server and
   * prevent new ones from being started.
   */
  def servers(contentPath: String, idleTimeout: Duration, tester: BackendConfig => EnvTask[Unit],
              mounter: Config => EnvTask[Backend], configWriter: Config => Task[Unit])
             : (Process[Task, (Int, Http4sServer)], Option[(Int, Config)] => Task[Unit]) = {

    val configQ = async.boundedQueue[Option[(Int, Config)]](2)(Strategy.DefaultStrategy)
    val reload = (cfg: Config) => configWriter(cfg) *> configQ.enqueueOne(Some((cfg.server.port, cfg)))

    def start(port0: Int, config: Config): EnvTask[(Int, Http4sServer)] =
      for {
        mounted <- mounter(config)
        port    <- liftE(choosePort(port0))
        fsApi   =  FileSystemApi(mounted, contentPath, config, tester, reload, configWriter)
        server  <- liftE(createServer(port, idleTimeout, fsApi))
        _       <- liftE(info(s"Server started listening on port $port"))
      } yield (port, server)

    def shutdown(srv: Option[(Int, Http4sServer)], log: Boolean): Task[Unit] =
      srv.traverse_ { case (p, s) =>
        s.shutdown *> (if (log) info(s"Stopped server listening on port $p") else Task.now(()))
      }

    def go(prevServer: Option[(Int, Http4sServer)]): Process[Task, (Int, Http4sServer)] =
      configQ.dequeue.take(1) flatMap {
        case Some((port, cfg)) =>
          Process.await(shutdown(prevServer, true) *> start(port, cfg).run)(_.fold(
            err => Process.halt.causedBy(Cause.Error(new RuntimeException(err.message))),
            tpl => Process.emit(tpl) ++ go(Some(tpl))
          ))

        case None =>
          Process.eval_(shutdown(prevServer, true))
      }

    (go(None).onComplete(Process.eval_(configQ.kill)), configQ.enqueueOne)
  }

  // Lifted from unfiltered.
  // NB: available() returns 0 when the stream is closed, meaning the server
  //     will run indefinitely when started from a script.
  private def waitForInput: Task[Unit] = for {
    _    <- Task.delay(java.lang.Thread.sleep(250))
                .handle { case _: java.lang.InterruptedException => () }
    test <- Task.delay(System.console == null || System.in.available() <= 0)
                .handle { case _ => true }
    done <- if (test) waitForInput else Task.now(())
  } yield done

  private def openBrowser(port: Int): Task[Unit] = {
    val url = s"http://localhost:$port/"
    Task.delay(java.awt.Desktop.getDesktop().browse(java.net.URI.create(url)))
        .or(error(s"Failed to open browser, please navigate to $url"))
  }

  case class Options(
    config: Option[String],
    contentPath: String,
    openClient: Boolean,
    port: Option[Int])

  val optionParser = new scopt.OptionParser[Options]("slamengine") {
    head("slamengine")
    opt[String]('c', "config") action { (x, c) => c.copy(config = Some(x)) } text("path to the config file to use")
    opt[String]('C', "content-path") action { (x, c) => c.copy(contentPath = x) } text("path where static content lives")
    opt[Unit]('o', "open-client") action { (_, c) => c.copy(openClient = true) } text("opens a browser window to the client on startup")
    opt[Int]('p', "port") action { (x, c) => c.copy(port = Some(x)) } text("the port to run slamengine on")
    help("help") text("prints this usage text")
  }

  def main(args: Array[String]): Unit = {
    val idleTimeout = Duration.Inf

    def reactToFirstServerStarted(openClient: Boolean): Sink[Task, (Int, Http4sServer)] =
      Process.emit[((Int, Http4sServer)) => Task[Unit]] {
        case (port, _) =>
          val msg = info("Press Enter to stop.")
          if (openClient) openBrowser(port) *> msg else msg
      } ++ Process.constant(κ(Task.now(())))

    val exec: EnvTask[Unit] = for {
      jp             <- liftE(jarPath)
      opts           <- optionParser.parse(args, Options(None, jp, false, None)).cata(
                          o => liftE[EnvironmentError](Task.now(o)),
                          EitherT.left(Task.now(InvalidConfig("couldn’t parse options"))))
      cfgPath        <- opts.config.cata(
                          cfg => FsPath.parseSystemFile(cfg)
                                       .toRight(InvalidConfig(s"Invalid path to config file: $cfg")),
                          liftE[EnvironmentError](Config.defaultPath))
      config         <- Config.fromFileOrEmpty(cfgPath)
      port           =  opts.port getOrElse config.server.port
      (proc, useCfg) =  servers(opts.contentPath, idleTimeout, Backend.test, Mounter.mount,
                                cfg => Config.toFile(cfg, cfgPath))
      _              <- liftE(Task.gatherUnordered(List(
                          proc.observe(reactToFirstServerStarted(opts.openClient)).run,
                          useCfg(Some((port, config))),
                          waitForInput *> useCfg(None)
                        )))
    } yield ()

    exec.swap
      .flatMap(e => liftE(error(e.message)))
      .merge
      .handleWith { case err => error(err.getMessage) }
      .run
  }
}
