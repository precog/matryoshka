package slamdata.engine.api

import slamdata.engine._
import slamdata.engine.config._

import unfiltered.request._
import unfiltered.response._

import scodec.bits.ByteVector

class FileSystem(fs: Map[String, Backend]) {
  val api = unfiltered.netty.cycle.Planify {
    case x @ GET(Path(path0)) if path0 startsWith ("/query/") => 
      val path = path0.substring("/query".length)

      (for {
        out   <- x.parameterValues("out").headOption
        query <- x.parameterValues("query").headOption
      } yield {      
        (fs.get(path) map { (backend: Backend) =>
          JsonContent ~> ResponseProcess(backend.execResults(query, out)) { json =>
            ByteVector(json.value.getBytes)
          }
        }) getOrElse (NotFound ~> ResponseString("No data source is mounted to the path " + path))
      }) getOrElse (BadRequest ~> ResponseString("The 'out'and 'q' query parameters must both be specified"))
  }
}