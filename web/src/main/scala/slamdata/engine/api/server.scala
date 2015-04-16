package slamdata.engine.api

import slamdata.engine._
import slamdata.engine.fs._
import slamdata.engine.config._

import scalaz.concurrent._

object Server {
  def run(port: Int, fs: FSTable[Backend]): Task[org.http4s.server.Server] = {
    val api = new FileSystemApi(fs)
    org.http4s.server.jetty.JettyBuilder
      .bindHttp(port)
      .mountService(api.queryService,    "/query/fs")
      .mountService(api.compileService,  "/compile/fs")
      .mountService(api.metadataService, "/metadata/fs")
      .mountService(api.dataService,     "/data/fs")
      .start
  }

  def runAndWaitForInput(port: Int, fs: FSTable[Backend]): Task[Unit] = for {
    server <- run(port, fs)
    _ = println("Embedded server listening at port " + port)
    _ = println("Press Enter to stop.")

    // Block until input arrives:
    _ = java.lang.System.in.read()
    _ <- server.shutdown
  } yield ()

  def main(args: Array[String]) {
    val serve = for {
      config  <- args.headOption.map(Config.fromFile _).getOrElse(Task.now(Config.DefaultConfig))
      mounted <- Mounter.mount(config)
      server  <- runAndWaitForInput(config.server.port.getOrElse(8080), mounted)
    } yield server

    serve.run
  }
}
