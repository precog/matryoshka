package slamdata.engine.api

import scala.collection.immutable.{TreeSet}

import slamdata.engine._
import slamdata.engine.sql.Query
import slamdata.engine.config._
import slamdata.engine.fs._
import slamdata.engine.fp._

import unfiltered.request.{Path => PathP, _}
import unfiltered.response._

import scodec.bits.ByteVector

import argonaut._
import Argonaut._

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

  private def dataSourceFor(path: Path): ResponseFunction[Any] \/ (FileSystem, Path) =
    path.ancestors.map(p => backendFor(p).toOption.map(_ -> p)).flatten.headOption.map {
      case (be, p) => path.relativeTo(p).map(relPath => \/- ((be.dataSource, relPath))).getOrElse(-\/ (InternalServerError))
    }.getOrElse(-\/ (NotFound ~> ResponseString("No data source is mounted to the path " + path)))

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
                        "out"    := path ++ out,
                        "phases" := phases
                      ))
      }).fold(identity, identity)
    }

    // API to get metadata:
    case x @ GET(PathP(path0)) if path0 startsWith ("/metadata/fs/") => AccessControlAllowOriginAll ~> {
      val path = Path(path0.substring("/metadata/fs".length))

      dataSourceFor(path) match {
        case \/- ((ds, relPath)) => {
          val paths = ds.ls.run
          
          // Note: a mongo db can contain a collection named "foo" as well as "foo.bar" and "foo.baz", 
          // in which case "foo" acts as both a directory and a file, as far as slamengine is concerned.
          val childrenAndTypes = paths.foldLeft(TreeSet[(String, String)]()) { (s, p) =>
            p.relativeTo(relPath) match {
              case Some(local) =>
                if (local.pureFile) s + (local.filename -> "file")
                else                s + (local.dir(1).value -> "directory")

              case None => s
            }
          }
          val children = childrenAndTypes.toList.map { case (name, typ) => Json.obj("name" := name, "type" := typ) }

          if (!children.empty)
            JsonContent ~> ResponseJson(
              Json.obj("children" := children)
            )
          else NotFound
        }

        case _ => {
          val fsChildren = (fs.keys.filter(path contains _).toList.map { path =>
            path.dir.headOption.map(_.value).getOrElse(".")
          }).map { name =>
            Json.obj("name" := name, "type" := "directory")
          }

          JsonContent ~> ResponseJson(
            Json.obj("children" := fsChildren)
          )
        }
      }

      // TODO: Use typesafe data structure and just serialize that.
    }

    // API to get data:
    case x @ GET(PathP(path0)) if path0 startsWith ("/data/fs/") => AccessControlAllowOriginAll ~> {
      val path = Path(path0.substring("/data/fs".length))

      val offset = x.parameterValues("offset").headOption.map(_.toLong)
      val limit  = x.parameterValues("limit").headOption.map(_.toLong)

      val (dataSource, relPath) = (dataSourceFor(path) | (FileSystem.Null -> Path(".")))

      jsonStream(dataSource.scan(relPath, offset, limit))
    }
  }
}