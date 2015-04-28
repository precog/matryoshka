package slamdata.engine.api

import slamdata.engine._
import slamdata.engine.fs._
import slamdata.engine.config._

import scalaz.concurrent._

object Server {
  def run(port: Int, fs: FSTable[Backend]): Task[org.http4s.server.Server] = {
    val api = new FileSystemApi(fs)
    org.http4s.server.jetty.JettyBuilder
      .bindHttp(port, "0.0.0.0")
      .mountService(api.queryService,    "/query/fs")
      .mountService(api.compileService,  "/compile/fs")
      .mountService(api.metadataService, "/metadata/fs")
      .mountService(api.dataService,     "/data/fs")
      .start
  }

  private def waitForInput: Task[Unit] = {
    // Lifted from unfiltered.
    // NB: available() returns 0 when the stream is closed, meaning
    // the server will run indefinitely when started from a script.
    def loop() {
      try { Thread.sleep(250) } catch { case _: InterruptedException => () }
      if (System.in.available() <= 0)
        loop()
    }

    Task.delay { loop() }
  }

  def main(args: Array[String]) {
    val serve = for {
      config  <- Config.load(args.headOption)
      mounted <- Mounter.mount(config)
      port = config.server.port.getOrElse(8080)

      server  <- run(port, mounted)
      _       <- Task.delay { println("Embedded server listening at port " + port) }

      _       <- Task.delay { println("Press Enter to stop.") }
      _       <- waitForInput

      _       <- server.shutdown
    } yield ()

    serve.run
  }
}
