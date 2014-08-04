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

class FileSystemApi(fs: FSTable[Backend]) {
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
    fs.lookup(path).map(_._1) \/> (NotFound ~> ResponseString("No data source is mounted to the path " + path))

  private def dataSourceFor(path: Path): ResponseFunction[Any] \/ (FileSystem, Path) =
    fs.lookup(path).map { case (backend, relPath) => (backend.dataSource, relPath) } \/> (NotFound ~> ResponseString("No data source is mounted to the path " + path))

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
                        "out"    := (path ++ out).pathname,
                        "phases" := phases
                      ))
      }).fold(identity, identity)
    }

    // API to get metadata:
    case x @ GET(PathP(path0)) if path0 startsWith ("/metadata/fs/") => AccessControlAllowOriginAll ~> {
      val path = Path(path0.substring("/metadata/fs".length))

      if (path == Path("/") && fs.isEmpty)
        JsonContent ~> ResponseJson(
          Json.obj("children" := List[Path]())
        )
      else
        dataSourceFor(path) match {
          case \/- ((ds, relPath)) =>
            ds.ls(relPath).attemptRun.fold(
              e => e match {
                case f: FileSystem.FileNotFoundError => NotFound
                case _ => throw e
              },
              paths =>
                JsonContent ~> ResponseJson(
                  Json.obj("children" := paths))
            )

          case _ => {
            val fsChildren = fs.children(path)

            if (fsChildren.isEmpty) NotFound
            else
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

      (for {
        t <- dataSourceFor(path)
        (dataSource, relPath) = t
      } yield jsonStream(dataSource.scan(relPath, offset, limit))).getOrElse(NotFound)
    }

    // API to overwrite data:
    case x @ PUT(PathP(path0)) if path0 startsWith ("/data/fs/") => AccessControlAllowOriginAll ~> {
      val path = Path(path0.substring("/data/fs".length))

      upload(x, path, (ds, p, json) => ds.save(p, json).attemptRun.leftMap(_ :: Nil))
    }

    // API to append data:
    case x @ POST(PathP(path0)) if path0 startsWith ("/data/fs/") => AccessControlAllowOriginAll ~> {
      val path = Path(path0.substring("/data/fs".length))

      upload(x, path, (ds, p, json) => {
        val errors = ds.append(p, json).runLog
        errors.attemptRun.fold(err => -\/ (err :: Nil), errs => if (!errs.isEmpty) -\/ (errs.toList) else \/- (()))
      })
    }

    // API to delete data:
    case x @ DELETE(PathP(path0)) if path0 startsWith ("/data/fs/") => AccessControlAllowOriginAll ~> {
      val path = Path(path0.substring("/data/fs".length))

      (for {
          t <- dataSourceFor(path)
          (dataSource, relPath) = t
          _ <- dataSource.delete(relPath).attemptRun.leftMap(e => InternalServerError ~> errorResponse(e))
        } yield ResponseString("")
      ).fold(identity, identity)
    }
  }

  private def upload[A](req: HttpRequest[A], path: Path, f: (FileSystem, Path, Process[Task, RenderedJson]) => List[Throwable] \/ Unit) = {
    def parseJsonLines(str: String): (List[JsonWriteError], List[RenderedJson]) =
      unzipDisj(str.split("\n").map(line => Parse.parse(line).bimap(
        e => JsonWriteError(RenderedJson(line), Some(e)),
        _ => RenderedJson(line)
      )).toList)

    def errorBody(errs: List[Throwable])(implicit EJ: EncodeJson[JsonWriteError]) = 
      ResponseJson(Json(
        "errors" := errs.map { 
          case e @ JsonWriteError(_, _) => EJ.encode(e)
          case e: slamdata.engine.Error => Json("detail" := e.message)
          case e => Json("detail" := e.toString)
        }))

    (for {
        t1   <- dataSourceFor(path)
        (dataSource, relPath) = t1

        body <- notEmpty(Body.string(req)) \/> (BadRequest ~> ResponseString("The body of the POST must contain data"))

        (errs, json) = parseJsonLines(body)
        _    <- if (!errs.isEmpty) -\/ (BadRequest ~> errorBody(errs)) else \/- (())

        _    <- f(dataSource, relPath, Process.emitAll(json)).leftMap {
          case (pe: PathError) :: Nil => BadRequest ~> errorBody(pe :: Nil)
          case es                     => InternalServerError ~> errorBody(es)
        }
      } yield ResponseString("")
    ).fold(identity, identity)
  }
}
