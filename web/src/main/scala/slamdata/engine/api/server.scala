package slamdata.engine.api

import java.io.File

import slamdata.engine._
import slamdata.engine.fp._
import slamdata.engine.config._

import scalaz.concurrent._

object Server {
  var serv: Option[org.http4s.server.Server] = None

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

  def reloader(contentPath: String, configPath: Option[String]):
      Config => Task[Unit] = {
    def restart(config: Config) = for {
      _       <- serv.fold(Task.now(()))(_.shutdown.map(ignore))
      mounted <- Mounter.mount(config)
      server  <- run(config.server.port, mounted, contentPath, config, configPath)
      _       <- Task.delay { println("Server restarted on port " + config.server.port) }
      _       <- Task.delay { serv = Some(server) }
    } yield ()

    def runAsync(t: Task[Unit]) = Task.delay {
      new java.lang.Thread {
        override def run = {
          java.lang.Thread.sleep(250)
          t.run
        }
      }.start
    }

    config => for {
      _       <- Config.write(config, configPath)
      _       <- runAsync(restart(config))
    } yield ()
  }

  def run(port: Int, backend: Backend, contentPath: String, config: Config, configPath: Option[String]):
      Task[org.http4s.server.Server] = {
    val api = new FileSystemApi(backend)
    org.http4s.server.jetty.JettyBuilder
      .bindHttp(port, "0.0.0.0")
      .mountService(api.compileService,               "/compile/fs")
      .mountService(api.dataService,                  "/data/fs")
      .mountService(api.metadataService,              "/metadata/fs")
      .mountService(api.mountService(config, reloader(contentPath, configPath)),
                                                      "/mount/fs")
      .mountService(api.queryService,                 "/query/fs")
      .mountService(api.serverService(config, reloader(contentPath, configPath)),
                                                      "/server")
      .mountService(api.staticFileService(contentPath + "/slamdata"),
                                                      "/slamdata")
      .mountService(api.redirectService("/slamdata"), "/")
      .start
  }

  private def waitForInput: Task[Unit] = {
    // Lifted from unfiltered.
    // NB: available() returns 0 when the stream is closed, meaning
    // the server will run indefinitely when started from a script.
    def loop: Unit = {
      try { Thread.sleep(250) } catch { case _: InterruptedException => () }
      if (System.in.available() <= 0) loop
    }

    Task.delay(loop)
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

  def openBrowser(port: Int): Task[Unit] =
    Task.delay(java.awt.Desktop.getDesktop().browse(
      java.net.URI.create(s"http://localhost:$port/")))

  def main(args: Array[String]): Unit = {
    serv = jarPath.flatMap { jp =>
      optionParser.parse(args, Options(None, jp, false, None)) match {
        case Some(options) =>
          for {
            config  <- Config.loadOrEmpty(options.config)
            mounted <- Mounter.mount(config)
            port = options.port.getOrElse(config.server.port)
            server  <- run(port, mounted, options.contentPath, config, options.config)
            _       <- if (options.openClient) openBrowser(port) else Task.now(())
            _       <- Task.delay { println("Embedded server listening at port " + port) }
            _       <- Task.delay { println("Press Enter to stop.") }
          } yield Some(server)
        case None => Task.now(None)
      }
    }.run
    serv match {
      case None    => ()
      case Some(_) =>
        waitForInput.run
        serv.fold(())(x => ignore(x.shutdownNow))
    }
  }
}
