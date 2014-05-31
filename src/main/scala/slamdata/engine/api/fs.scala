package slamdata.engine.api

import unfiltered.request._
import unfiltered.response._

object FileSystem {
  val api = unfiltered.netty.cycle.Planify {
    case GET(Path("/")) => new ResponseStreamer {
      def stream(os: java.io.OutputStream): Unit = {

        ()
      }
    }
  }
  unfiltered.netty.Http(8080).plan(api).run()
}