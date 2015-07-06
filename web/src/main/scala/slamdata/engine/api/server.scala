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

import java.io.File

import slamdata.engine._
import slamdata.engine.fp._
import slamdata.engine.config._

import scalaz._
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
      server  <- run(config.server.port, FileSystemApi(mounted, contentPath, config, reloader(contentPath, configPath)))
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

  def run(port: Int, api: FileSystemApi): Task[org.http4s.server.Server] = {
    val builder = org.http4s.server.jetty.JettyBuilder.bindHttp(port, "0.0.0.0")
    api.AllServices.toList.foldLeft(builder) {
      case (b, (path, svc)) => b.mountService(svc, path)
    }.start
  }

  // Lifted from unfiltered.
  // NB: available() returns 0 when the stream is closed, meaning the server
  //     will run indefinitely when started from a script.
  private def waitForInput: Task[Unit] = for {
    _    <- Task.delay(Thread.sleep(250))
                .handle { case _: InterruptedException => () }
    test <- Task.delay(System.console == null || System.in.available() <= 0)
                .handle { case _ => true }
    done <- if (test) waitForInput else Task.now(())
  } yield done

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

  def openBrowser(port: Int): Task[Unit] = {
    val url = s"http://localhost:$port/"
    Task.delay(java.awt.Desktop.getDesktop().browse(java.net.URI.create(url)))
      .handle { case _ =>
        System.err.println("Failed to open browser, please navigate to " + url)
    }
  }

  // NB: returns (), or else an explanation of why the port is not available,
  // or fails if some other error occurs.
  def available(port: Int): Task[String \/ Unit] = Task.delay {
    \/.fromTryCatchNonFatal(new java.net.ServerSocket(port)).fold(
      {
        case err: java.net.BindException => -\/(err.getMessage)
        case err                         => throw err
      },
      { s =>
        s.close()
        \/-(())
      })
  }

  def anyAvailablePort: Task[Int] = Task.delay {
    val s = new java.net.ServerSocket(0)
    val p = s.getLocalPort
    s.close()
    p
  }

  def choosePort(requested: Int): Task[Int] = for {
    avail <- available(requested)
    port  <- avail.fold(
      err => for {
        p <- anyAvailablePort
        _ <- Task.delay { println("Requested port not available: " + requested + "; " + err) }
      } yield p,
      κ(Task.now(requested)))
  } yield port

  def main(args: Array[String]): Unit = {
    serv = jarPath.flatMap { jp =>
      optionParser.parse(args, Options(None, jp, false, None)) match {
        case Some(options) =>
          for {
            config  <- Config.loadOrEmpty(options.config)
            mounted <- Mounter.mount(config)
            port    <- choosePort(options.port.getOrElse(config.server.port))
            server  <- run(port, FileSystemApi(mounted, options.contentPath, config, reloader(options.contentPath, options.config)))
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
