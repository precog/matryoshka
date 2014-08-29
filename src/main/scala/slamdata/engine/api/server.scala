package slamdata.engine.api

import unfiltered.request._
import unfiltered.response._

import slamdata.engine._
import slamdata.engine.fs._
import slamdata.engine.config._

import scalaz.concurrent._

object Server {
  def run(port: Int, fs: FSTable[Backend]): Task[Unit] = Task.delay {
    unfiltered.netty.Server.http(port).chunked(1048576).plan(new FileSystemApi(fs).api).run()
  }

  def main(args: Array[String]) {
    val serve = for {
      config  <- args.headOption.map(Config.fromFile _).getOrElse(Task.now(Config.DefaultConfig))
      mounted <- Mounter.mount(config)
      _       <- run(config.server.port.getOrElse(8080), mounted)
    } yield ()
    
    // Move the server off of the main thread so unfiltered will think it's running 
    // under SBT and listen for keystrokes to stop.
    new Thread(new Runnable { def run = serve.run }).start()
  }
}