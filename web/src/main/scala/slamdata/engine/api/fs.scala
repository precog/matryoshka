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

  object MOVE extends unfiltered.request.Method("MOVE")

  object Decode {
    import java.net.URLDecoder

    def unapply(raw: String): Option[List[String]] = {
      val segs = raw.split("/", -1).toList match {  // NB: do _not_ discard a trailing empty string
        case "" :: rest => rest  // NB: _do_ discard a leading empty string (from the leading slash)
        case all        => all
      }
      segs.map(seg => \/.fromTryCatchNonFatal(URLDecoder.decode(seg, "UTF-8")).toOption).sequenceU
    }
  }
  object AsPath {
    def unapply(segs: List[String]): Option[Path] = Some(Path("/" + segs.mkString("/")))
  }
  object AsDirPath {
    def unapply(segs: List[String]): Option[Path] = AsPath.unapply(segs).map(_.asDir)
  }

  private def notEmpty(string0: String): Option[String] = {
    val string = string0.trim

    if (string.length == 0) None else Some(string)
  }

  private def jsonStream(v: Process[Task, Data])(implicit EE: EncodeJson[DataEncodingError]): Resp = {
    val codec = DataCodec.Readable
    JsonContent ~> ResponseProcess(v) { data =>
      ByteVector(DataCodec.render(data)(codec).fold(err => EE.encode(err).toString, identity).getBytes ++ "\r\n".getBytes)
    }
  }

  private def lookupBackend(path: Path): ResponseFunction[Any] \/ (Backend, Path, Path) =
    fs.lookup(path) \/> (NotFound ~> ResponseString("No data source is mounted to the path " + path))

  private def backendFor(path: Path): ResponseFunction[Any] \/ (Backend, Path) =
    lookupBackend(path).map { case (backend, mountPath, _) => (backend, mountPath) }

  private def dataSourceFor(path: Path): ResponseFunction[Any] \/ (FileSystem, Path) =
    lookupBackend(path).map { case (backend, _, relPath) => (backend.dataSource, relPath) }

  private def errorResponse(e: Throwable) = e match {
    case PhaseError(phases, causedBy) => JsonContent ~>
      ResponseJson(Json.obj(
        "error"  := causedBy.getMessage,
        "phases" := phases))

    case _ => ResponseString(e.getMessage)
  }

  private def vars(x: HttpRequest[_]): Variables = {
    Variables((x.parameterNames.map(n => n -> x.parameterValues(n)).toList.flatMap {
      case (name, values) => values.headOption.toList.map(v => VarName(name) -> VarValue(v))
    }).toMap)
  }

  private val QueryParameterMustContainQuery = (BadRequest ~> ResponseString("The request must contain a query"))
  private val POSTContentMustContainQuery = (BadRequest ~> ResponseString("The body of the POST must contain a query"))
  private val DestinationHeaderMustExist  = (BadRequest ~> ResponseString("The 'Destination' header must be specified"))

  def api = unfiltered.netty.cycle.Planify {
    // API to create synchronous queries, returning the result:
    case x @ GET(PathP(Decode("query" :: "fs" :: AsDirPath(path)))) => AccessControlAllowOriginAll ~> {
      (for {
        query <- x.parameterValues("q").headOption.map(_.toString) \/> QueryParameterMustContainQuery
        b     <- backendFor(path)
        (backend, mountPath) = b

        (phases, resultT) = backend.eval(QueryRequest(Query(query), None, mountPath, path))
        result <- resultT.attemptRun.leftMap(e => BadRequest ~> errorResponse(e))
      } yield jsonStream(result)).fold(identity, identity)
    }

    // API to create synchronous queries, storing the result:
    case x @ POST(PathP(Decode("query" :: "fs" :: AsDirPath(path)))) => AccessControlAllowOriginAll ~>
      (for {
        outRaw  <- x.headers("Destination").toList.headOption \/> DestinationHeaderMustExist
        query   <- notEmpty(Body.string(x))                   \/> POSTContentMustContainQuery
        b       <- backendFor(path)
        (backend, mountPath) = b
        out     <- Path(outRaw).interpret(mountPath, path).leftMap(e => BadRequest ~> errorResponse(e))
        (phases, resultT) = backend.run(QueryRequest(Query(query), Some(out), mountPath, path, vars(x)))
        out     <- resultT.attemptRun.leftMap(e => InternalServerError ~> errorResponse(e))
      } yield {
        JsonContent ~> ResponseJson(Json.obj(
                        "out"    := (mountPath ++ out.path).pathname,
                        "phases" := phases
                      ))
      }).fold(identity, identity)

    // API to get metadata:
    case x @ GET(PathP(Decode("metadata" :: "fs" :: AsPath(path)))) => AccessControlAllowOriginAll ~> {
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
                Json.obj("children" := fsChildren.map(p =>
                    Json("name" := p.simplePathname, "type" := "mount"))))
          }
        }
      // TODO: Use typesafe data structure and just serialize that.
    }

    // API to get data:
    case x @ GET(PathP(Decode("data" :: "fs" :: AsPath(path)))) => AccessControlAllowOriginAll ~> {
      val offset = x.parameterValues("offset").headOption.map(_.toLong)
      val limit  = x.parameterValues("limit").headOption.map(_.toLong)

      (for {
        t <- dataSourceFor(path)
        (dataSource, relPath) = t
      } yield jsonStream(dataSource.scan(relPath, offset, limit))).getOrElse(NotFound)
    }

    // API to overwrite data:
    case x @ PUT(PathP(Decode("data" :: "fs" :: AsPath(path)))) => AccessControlAllowOriginAll ~>
      upload(x, path, (ds, p, json) => ds.save(p, json).attemptRun.leftMap(_ :: Nil))

    // API to append data:
    case x @ POST(PathP(Decode("data" :: "fs" :: AsPath(path)))) => AccessControlAllowOriginAll ~>
      upload(x, path, (ds, p, json) => {
        val errors = ds.append(p, json).runLog
        errors.attemptRun.fold(err => -\/ (err :: Nil), errs => if (!errs.isEmpty) -\/ (errs.toList) else \/- (()))
      })

    // API to rename data:
    case x @ MOVE(PathP(Decode("data" :: "fs" :: AsPath(path)))) => AccessControlAllowOriginAll ~>
      (for {
          dstRaw <- x.headers("Destination").toList.headOption \/> (BadRequest ~> ResponseString("Destination header required"))
          dst <- Path(dstRaw).from(path.dirOf).leftMap(e => BadRequest ~> ResponseString("Invalid destination path: " + e.getMessage))

          t1 <- dataSourceFor(path)
          (srcDataSource, srcPath) = t1

          t2 <- dataSourceFor(dst)
          (dstDataSource, dstPath) = t2

          _ <- if (srcDataSource != dstDataSource) -\/ (InternalServerError ~> ResponseString("Cannot copy/move across backends"))
               else srcDataSource.move(srcPath, dstPath).attemptRun.leftMap(e => InternalServerError ~> errorResponse(e))
        } yield Created ~> ResponseString("")
      ).fold(identity, identity)

    // API to delete data:
    case x @ DELETE(PathP(Decode("data" :: "fs" :: AsPath(path)))) => AccessControlAllowOriginAll ~>
      (for {
          t <- dataSourceFor(path)
          (dataSource, relPath) = t
          _ <- dataSource.delete(relPath).attemptRun.leftMap(e => InternalServerError ~> errorResponse(e))
        } yield ResponseString("")
      ).fold(identity, identity)

    case x @ OPTIONS(PathP(Seg("query" :: _))) =>
      AccessControlAllowOriginAll ~>
      AccessControlAllowMethods("GET, POST, OPTIONS") ~>
      AccessControlMaxAge((20*24*60*60).toString) ~> {
        x.headers("Access-Control-Request-Method").toList match {
          case "POST" :: Nil => AccessControlAllowHeaders("Destination")
          case _ => Ok
        }
      }

    case x @ OPTIONS(PathP(Seg("data" :: _))) =>
      AccessControlAllowOriginAll ~>
      AccessControlAllowMethods("GET, PUT, POST, DELETE, MOVE, OPTIONS") ~>
      AccessControlMaxAge((20*24*60*60).toString) ~> {
        x.headers("Access-Control-Request-Method").toList match {
          case "MOVE" :: Nil => AccessControlAllowHeaders("Destination")
          case _ => Ok
        }
      }
  }

  private def upload[A](req: HttpRequest[A], path: Path, f: (FileSystem, Path, Process[Task, Data]) => List[Throwable] \/ Unit) = {
    val codec = DataCodec.Precise
    def parseJsonLines(str: String): (List[WriteError], List[Data]) =
      unzipDisj(str.split("\n").map(line => DataCodec.parse(line)(codec).leftMap(
        e => WriteError(Data.Str("parse error: " + line), Some(e.message)))).toList)

    def errorBody(errs: List[Throwable])(implicit EJ: EncodeJson[WriteError]) =
      ResponseJson(Json(
        "errors" := errs.map {
          case e @ WriteError(_, _) => EJ.encode(e)
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
          case (pe @ PathError(_)) :: Nil => BadRequest ~> errorBody(pe :: Nil)
          case es                         => InternalServerError ~> errorBody(es)
        }
      } yield ResponseString("")
    ).fold(identity, identity)
  }
}
