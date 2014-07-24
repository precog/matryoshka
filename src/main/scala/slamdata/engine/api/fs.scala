package slamdata.engine.api

import slamdata.engine._
import slamdata.engine.sql.Query
import slamdata.engine.config._
import slamdata.engine.fs._

import unfiltered.request.{Path => PathP, _}
import unfiltered.response._

import scodec.bits.ByteVector

import argonaut._
import Argonaut._

import slamdata.engine.fp._

import scalaz._
import Scalaz._

import scalaz.concurrent._
import scalaz.stream._

class FileSystemApi(fs: Map[Path, Backend]) {
  import java.io.{FileSystem => _, _}

  type Resp = ResponseFunction[Any]

  private def notEmpty(string0: String): Option[String] = {
    val string = string0.trim

    if (string.length == 0) None else Some(string)
  }

  private def jsonStream(v: Process[Task, RenderedJson]): Resp = {
    JsonContent ~> ResponseProcess(v) { json =>
      ByteVector(json.value.getBytes ++ "\r\n".getBytes)
    }
  }

  private def backendFor(path: Path): ResponseFunction[Any] \/ Backend = 
    fs.get(path) \/> (NotFound ~> ResponseString("No data source is mounted to the path " + path))

  private def dataSourceFor(path: Path): ResponseFunction[Any] \/ FileSystem = 
    backendFor(path).map(_.dataSource)

  private def errorResponse(e: Throwable) = e match {
    case PhaseError(phases, causedBy) => JsonContent ~>
      ResponseJson(Json.obj(
        "error"  := causedBy.getMessage,
        "phases" := phases))
        
    case _ => ResponseString(e.getMessage)
  }

  def api = unfiltered.netty.cycle.Planify {
    // API to create synchronous queries
    case x @ POST(PathP(path0)) if path0 startsWith ("/query/fs/") => AccessControlAllowOriginAll ~> {
      val path = Path(path0.substring("/query/fs".length))
      
      (for {
        out     <- x.parameterValues("out").headOption \/> (BadRequest ~> ResponseString("The 'out' query string parameter must be specified"))
        query   <- notEmpty(Body.string(x))            \/> (BadRequest ~> ResponseString("The body of the POST must contain a query"))
        backend <- backendFor(path)
        t       <- backend.run(Query(query), Path(out)).attemptRun.leftMap(e => InternalServerError ~> errorResponse(e))
      } yield {
        val (phases, out) = t

        JsonContent ~> ResponseJson(Json.obj(
                        "out"    := path.withFile(out),
                        "phases" := phases
                      ))
      }).fold(identity, identity)
    }

    // API to get metadata:
    case x @ GET(PathP(path0)) if path0 startsWith ("/metadata/fs/") => AccessControlAllowOriginAll ~> {
      val path = Path(path0.substring("/metadata/fs".length))

      val dirChildren = (fs.keys.filter(path contains _).toList.map { path =>
        path.dir.headOption.map(_.value).getOrElse(".")
      }).map { name =>
        Json.obj("name" -> jString(name), "type" -> jString("directory"))
      }

      val fileChildren = dataSourceFor(path).map(_.ls).getOrElse(Task.now(Nil)).run.flatMap { path =>
        if (path.pureFile) Json.obj("name" -> jString(path.filename), "type" -> jString("file")) :: Nil
        else Nil
      }

      // TODO: Use typesafe data structure and just serialize that.

      JsonContent ~> ResponseJson(
        Json.obj("children" -> jArray(dirChildren ++ fileChildren))
      )
    }

    // API to get data:
    case x @ GET(PathP(path0)) if path0 startsWith ("/data/fs/") => AccessControlAllowOriginAll ~> {
      val path = Path(path0.substring("/data/fs".length))

      val offset = x.parameterValues("offset").headOption.map(_.toLong)
      val limit  = x.parameterValues("limit").headOption.map(_.toLong)

      val dataSource = (dataSourceFor(path.dirOf) | FileSystem.Null)

      jsonStream(dataSource.scan(path.fileOf, offset, limit))
    }
  }
}