package slamdata.engine.api

import slamdata.engine._
import slamdata.engine.config._

import unfiltered.request._
import unfiltered.response._

import scodec.bits.ByteVector

import argonaut._
import Argonaut._

import slamdata.engine.fp._

import scalaz._
import Scalaz._

class FileSystem(fs: Map[String, Backend]) {
  import java.io._

  private def notEmpty(string0: String): Option[String] = {
    val string = string0.trim

    if (string.length == 0) None else Some(string)
  }

  private def execQuery(backend: Backend, query: String, out: String): ResponseFunction[Any] = {
    JsonContent ~> ResponseProcess(backend.execResults(query, out)) { json =>
      ByteVector(json.value.getBytes)
    }
  }

  val api = unfiltered.netty.cycle.Planify {
    case x @ POST(Path(path0)) if path0 startsWith ("/query/fs/") => 
      val path = path0.substring("/query/fs".length)

      (for {
        out     <- x.parameterValues("out").headOption \/> (BadRequest ~> ResponseString("The 'out' query string parameter must be specified"))
        query   <- notEmpty(Body.string(x))            \/> (BadRequest ~> ResponseString("The body of the POST must contain a query"))
        backend <- fs.get(path)                        \/> (NotFound   ~> ResponseString("No data source is mounted to the path " + path))
      } yield execQuery(backend, query, out)).fold(identity, identity)

    case x @ GET(Path(path0)) if path0 startsWith ("/metadata/fs/") => 
      val path = path0.substring("/metadata/fs".length)

      val children = fs.keys.filter(_ startsWith path).toList.map { path =>
        path.split("/").drop(1).headOption.getOrElse(".")
      }

      JsonContent ~> ResponseJson(
        Json.obj("children" -> jArray(children.map(jString)))
      )
  }
}