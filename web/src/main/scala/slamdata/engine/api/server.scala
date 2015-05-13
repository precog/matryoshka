package slamdata.engine.api

import java.io.File

import slamdata.engine._
import slamdata.engine.fs._
import slamdata.engine.config._

import scalaz.concurrent._

object Server {
  val jarPath: Task[String] =
    Task.delay((new File(Server.getClass.getProtectionDomain().getCodeSource().getLocation().toURI().getPath()))
      .getParentFile()
      .getPath())

  def run(port: Int, fs: FSTable[Backend]): Task[org.http4s.server.Server] = {
    val api = new FileSystemApi(fs)
    jarPath.flatMap(jp =>
      org.http4s.server.jetty.JettyBuilder
        .bindHttp(port, "0.0.0.0")
        .mountService(api.queryService,                                "/query/fs")
        .mountService(api.compileService,                              "/compile/fs")
        .mountService(api.metadataService,                             "/metadata/fs")
        .mountService(api.dataService,                                 "/data/fs")
        .mountService(api.staticFileService(jp + "/docroot/slamdata"), "/slamdata")
        .mountService(api.redirectService("/slamdata"),                "/")
        .start)
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
    openClient: Boolean,
    port: Option[Int])

  val optionParser = new scopt.OptionParser[Options]("slamengine") {
    head("slamengine")
    opt[String]('c', "config") action { (x, c) => c.copy(config = Some(x)) } text("path to the config file to use")
    opt[Unit]('o', "open-client") action { (_, c) => c.copy(openClient = true) } text("opens a browser window to the client on startup")
    opt[Int]('p', "port") action { (x, c) => c.copy(port = Some(x)) } text("the port to run slamengine on")
    help("help") text("prints this usage text")
  }

  def openBrowser(port: Int): Task[Unit] =
    Task.delay(java.awt.Desktop.getDesktop().browse(
      java.net.URI.create(s"http://localhost:$port/")))

  def main(args: Array[String]): Unit = {
    optionParser.parse(args, Options(None, false, None)) match {
      case Some(options) =>
        val serve = for {
          config  <- Config.loadOrEmpty(options.config)
          mounted <- Mounter.mount(config)
          port = options.port.getOrElse(config.server.port)
          server  <- run(port, mounted)
          _       <- if (options.openClient) openBrowser(port) else Task.now(())
          _       <- Task.delay { println("Embedded server listening at port " + port) }
          _       <- Task.delay { println("Press Enter to stop.") }
          _       <- waitForInput

          _       <- server.shutdown
        } yield ()

        serve.run

      case None => ()
    }
  }
}
