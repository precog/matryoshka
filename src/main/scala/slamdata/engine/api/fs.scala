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

import scalaz.concurrent._
import scalaz.stream._

class FileSystem(fs: Map[String, Backend]) {
  import java.io._

  type Resp = ResponseFunction[Any]

  private def notEmpty(string0: String): Option[String] = {
    val string = string0.trim

    if (string.length == 0) None else Some(string)
  }

  private def jsonStream(v: Process[Task, RenderedJson]): Resp = {
    JsonContent ~> ResponseProcess(v) { json =>
      ByteVector(json.value.getBytes)
    }
  }

  private def execQuery(backend: Backend, query: String, out: String): Resp = jsonStream(backend.evalResults(query, out))

  private def backendFor(path: String): ResponseFunction[Any] \/ Backend = 
    fs.get(path) \/> (NotFound ~> ResponseString("No data source is mounted to the path " + path))

  private def dataSourceFor(path: String): ResponseFunction[Any] \/ DataSource = 
    backendFor(path).map(_.dataSource)

  val api = unfiltered.netty.cycle.Planify {
    // API to create synchronous queries
    case x @ POST(Path(path0)) if path0 startsWith ("/query/fs/") => AccessControlAllowOriginAll ~> {
      val path = path0.substring("/query/fs".length)

      (for {
        out     <- x.parameterValues("out").headOption \/> (BadRequest ~> ResponseString("The 'out' query string parameter must be specified"))
        query   <- notEmpty(Body.string(x))            \/> (BadRequest ~> ResponseString("The body of the POST must contain a query"))
        backend <- backendFor(path)
        t       <- backend.run(query, out).attemptRun.leftMap(e => InternalServerError ~> ResponseString(e.getMessage))
      } yield {
        val (log, out) = t

        JsonContent ~> ResponseJson(Json.obj("out" -> jString(path + out), "log" -> jString(log.toString)))
      }).fold(identity, identity)
    }

    // API to get metadata:
    case x @ GET(Path(path0)) if path0 startsWith ("/metadata/fs/") => AccessControlAllowOriginAll ~> {
      val path = path0.substring("/metadata/fs".length)

      val dirChildren = (fs.keys.filter(_ startsWith path).toList.map { path =>
        path.split("/").drop(1).headOption.getOrElse(".")
      }).map { name =>
        Json.obj("name" -> jString(name), "type" -> jString("directory"))
      }

      val fileChildren = dataSourceFor(path).map(_.ls).getOrElse(Task.now(Nil)).run.map { name =>
        Json.obj("name" -> jString(name), "type" -> jString("file"))
      }

      // TODO: Use typesafe data structure and just serialize that.

      JsonContent ~> ResponseJson(
        Json.obj("children" -> jArray(dirChildren ++ fileChildren))
      )
    }

    // API to get data:
    case x @ GET(Path(path0)) if path0 startsWith ("/data/fs/") => AccessControlAllowOriginAll ~> {
      val path = path0.substring("/data/fs".length)

      val segs = path.split("/")

      val dir  = ("/" + segs.take(segs.length - 1).mkString("/")).replaceAll("/+", "/")
      val file = segs.last

      val dataSource = (dataSourceFor(dir) | DataSource.Null)

      jsonStream(dataSource.scan(file))
    }
  }
}