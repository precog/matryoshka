package slamdata.engine.api

import scala.collection.immutable.{ListMap, TreeSet}

import slamdata.engine._
import slamdata.engine.config._
import slamdata.engine.sql.Query
import slamdata.engine.fs._
import slamdata.engine.fp._

import argonaut.{DecodeResult => _, _ }
import Argonaut._

import org.http4s.{Query => HQuery, _}
import org.http4s.dsl.{Path => HPath, _}
import org.http4s.argonaut._
import org.http4s.headers._
import org.http4s.server._
import org.http4s.util.{CaseInsensitiveString, Renderable}

import scalaz._
import Scalaz._

import scalaz.concurrent._
import scalaz.stream._

sealed trait ResponseFormat {
  def mediaType: MediaType
}
object ResponseFormat {
  val JsonMediaType = new MediaType("application", "ldjson")
  val CsvMediaType = new MediaType("text", "csv")

  final case class Json private[ResponseFormat] (codec: DataCodec, mediaType: MediaType) extends ResponseFormat
  private def jsonType(mode: String) = JsonMediaType.withExtensions(Map("mode" -> mode))
  val Precise = Json(DataCodec.Precise, jsonType("precise"))
  val Readable = Json(DataCodec.Readable, jsonType("readable"))

  case object Csv extends ResponseFormat {
    val mediaType = CsvMediaType
  }

  def fromAccept(accept: Option[Accept]): ResponseFormat = {
    val mediaTypes = NonEmptyList(
      JsonMediaType,
      new MediaType("application", "x-ldjson"),
      new MediaType("application", "json",
        extensions = Map("boundary" -> "NL")),
      CsvMediaType)

    (for {
      acc       <- accept
      // TODO: MediaRange needs an Order instance – combining QValue ordering
      //       with specificity (EG, application/json sorts before
      //       application/* if they have the same q-value).
      mediaType <- acc.values.toList.sortBy(_.qValue).find(a => mediaTypes.toList.exists(a.satisfies(_)))
      format    <-
        if (mediaType satisfies CsvMediaType) Some(Csv)
        else for {
          mode <- mediaType.extensions.get("mode")
          fmt  <- if (mode == "precise") Some(ResponseFormat.Precise) else None
        } yield fmt
    } yield format).getOrElse(ResponseFormat.Readable)
  }
}

final case class RequestError(message: String)
object RequestError {
  implicit def EntityEncoderRequestError: EntityEncoder[RequestError] =
    EntityEncoder[Json].contramap[RequestError] { err =>
      Json("error" := err.toString)
    }
}

final case class FileSystemApi(fs: FSTable[Backend], contentPath: String, config: Config, reloader: Config => Task[Unit]) {
  import Method.{MOVE, OPTIONS}

  val CsvColumnsFromInitialRowsCount = 1000

  private def jsonStream(codec: DataCodec, v: Process[Task, Data])(implicit EE: EncodeJson[DataEncodingError]): Task[Response] =
    Ok(v.map(
      DataCodec.render(_)(codec).fold(EE.encode(_).toString, identity) + "\r\n"))

  private def csvStream(v: Process[Task, Data]): Task[Response] = {
    import slamdata.engine.repl.Prettify

    Ok(Prettify.renderStream(v, CsvColumnsFromInitialRowsCount).map { v =>
      import com.github.tototoshi.csv._
      val w = new java.io.StringWriter
      val cw = CSVWriter.open(w)
      cw.writeRow(v)
      cw.close
      w.toString
    })
  }

  private def responseStream(accept: Option[Accept], v: Process[Task, Data]):
      Task[Response] = {
    ResponseFormat.fromAccept(accept) match {
      case ResponseFormat.Json(codec, mediaType) =>
        jsonStream(codec, v).map(_.putHeaders(`Content-Type`(mediaType, Some(Charset.`UTF-8`))))
      case ResponseFormat.Csv =>
        csvStream(v).map(_.putHeaders(`Content-Type`(ResponseFormat.Csv.mediaType, Some(Charset.`UTF-8`))))
    }
  }

  private def lookupBackend(path: Path): Task[Response] \/ (Backend, Path, Path) =
    fs.lookup(path) \/> (NotFound("No data source is mounted to the path " + path))

  private def backendFor(path: Path): Task[Response] \/ (Backend, Path) =
    lookupBackend(path).map { case (backend, mountPath, _) => (backend, mountPath) }

  private def dataSourceFor(path: Path): Task[Response] \/ (FileSystem, Path) =
    lookupBackend(path).map { case (backend, _, relPath) => (backend.dataSource, relPath) }

  private def errorResponse(status: org.http4s.dsl.impl.EntityResponseGenerator, e: Throwable) = {
    e match {
      case PhaseError(phases, causedBy) => status(Json.obj(
          "error"  := causedBy.getMessage,
          "phases" := phases))

      case _ => status(e.getMessage)
    }
  }

  private def vars(req: Request) = Variables(req.params.map { case (k, v) => (VarName(k), VarValue(v)) })

  private val QueryParameterMustContainQuery = BadRequest("The request must contain a query")
  private val POSTContentMustContainQuery    = BadRequest("The body of the POST must contain a query")
  private val DestinationHeaderMustExist     = BadRequest("The '" + Destination.name + "' header must be specified")
  private val FileNameHeaderMustExist        = BadRequest("The '" + FileName.name + "' header must be specified")

  private def upload[A](errors: List[WriteError], path: Path, f: (FileSystem, Path) => List[Throwable] \/ Unit) = {
    def errorBody(status: org.http4s.dsl.impl.EntityResponseGenerator, errs: List[Throwable])(implicit EJ: EncodeJson[WriteError]) =
      status(Json(
        "errors" := errs.map {
          case e @ WriteError(_, _) => EJ.encode(e)
          case e: slamdata.engine.Error => Json("detail" := e.message)
          case e => Json("detail" := e.toString)
        }))

    (for {
      t1 <- dataSourceFor(path)
      (dataSource, relPath) = t1

      _  <- if (!errors.isEmpty) -\/ (errorBody(BadRequest, errors)) else \/- (())

      _  <- f(dataSource, relPath).leftMap {
        case (pe @ PathError(_)) :: Nil => errorBody(BadRequest, pe :: Nil)
        case es                         => errorBody(InternalServerError, es)
      }
    } yield Ok("")).fold(identity, identity)
  }

  object AsPath {
    def unapply(p: HPath): Option[Path] = {
      Some(Path("/" + p.toList.map(java.net.URLDecoder.decode(_, "UTF-8")).mkString("/")))
    }
  }

  object AsDirPath {
    def unapply(p: HPath): Option[Path] = AsPath.unapply(p).map(_.asDir)
  }

  implicit val QueryDecoder = new QueryParamDecoder[Query] {
    def decode(value: QueryParameterValue): ValidationNel[ParseFailure, Query] =
      Query(value.value).successNel[ParseFailure]
  }
  object Q extends QueryParamDecoderMatcher[Query]("q")

  object Offset extends OptionalQueryParamDecoderMatcher[Long]("offset")
  object Limit extends OptionalQueryParamDecoderMatcher[Long]("limit")

  object Cors extends Middleware {
    // Note: CORS middleware is coming in http4s post-0.6.5
    val corsHeaders = List(
      AccessControlAllowOriginAll,
      `Access-Control-Allow-Methods`(List("GET", "PUT", "POST", "DELETE", "MOVE", "OPTIONS")),
      `Access-Control-Max-Age`(20*24*60*60),
      `Access-Control-Allow-Headers`(List(Destination)))  // NB: actually needed for POST only

    def apply(service: HttpService): HttpService =
      Service.lift { req =>
        service(req).flatMap {
          case r@Some(_) => Task.now(r)
          case None => Ok().map(resp => Some(resp.putHeaders(corsHeaders: _*)))
        }
      }
  }

  /** Handle failure in Task by returning a 500. Otherwise http4s hangs for 30 seconds and then returns 200. */
  object FailSafe extends Middleware {
    def apply(service: HttpService): HttpService =
      Service.lift { req =>
        service.run(req).handleWith {
          case err => InternalServerError(Json("error" := err.toString)).map(Some(_))
        }
      }
  }

  type M[A] = EitherT[Task, RequestError, A]
  def liftT[A](t: Task[A]): M[A] = EitherT.right(t)
  def liftE[A](v: RequestError \/ A): M[A] = EitherT(Task.now(v))

  def respond(v: M[String]): Task[Response] =
    v.fold(BadRequest(_), Ok(_)).join

  def queryService =
    HttpService {
      case req @ GET -> AsDirPath(path) :? Q(query) =>
        (for {
          b     <- backendFor(path)
          (backend, mountPath) = b

          (phases, resultT) = backend.eval(QueryRequest(query, None, mountPath, path, Variables(Map())))
          result <- resultT.attemptRun.leftMap(e =>  errorResponse(BadRequest, e))
        } yield responseStream(req.headers.get(Accept), result)).fold(identity, identity)

      case GET -> _ => QueryParameterMustContainQuery

      case req @ POST -> AsDirPath(path) =>
        def go(query: Query): Task[Response] =
          (for {
            outRaw <- req.headers.get(Destination).map(_.value) \/> DestinationHeaderMustExist

            b     <- backendFor(path)
            (backend, mountPath) = b

            out     <- Path(outRaw).interpret(mountPath, path).leftMap(e => errorResponse(BadRequest, e))
            (phases, resultT) = backend.run(QueryRequest(query, Some(out), mountPath, path, vars(req)))
            out     <- resultT.attemptRun.leftMap(e => errorResponse(InternalServerError, e))
          } yield {
            Ok(Json.obj(
              "out"    := (mountPath ++ out.path).pathname,
              "phases" := phases))
          }).fold(identity, identity)

        for {
          query <- EntityDecoder.decodeString(req)
          resp <- if (query != "") go(Query(query)) else POSTContentMustContainQuery
        } yield resp
    }

  def compileService = {
    def go(path: Path, query: Query) =
      (for {
        b     <- backendFor(path)
        (backend, mountPath) = b

        (phases, resultT) = backend.eval(QueryRequest(query, None, mountPath, path, Variables(Map())))

        plan  <- phases.lastOption \/> InternalServerError("no plan")
      } yield plan match {
          case PhaseResult.Error(name, value)  => errorResponse(BadRequest, value)
          case PhaseResult.Tree(name, value)   => Ok(Json(name := value))
          case PhaseResult.Detail(name, value) => Ok(name + "\n" + value)
      }).fold(identity, identity)

    HttpService {
      case GET -> AsDirPath(path) :? Q(query) => go(path, query)

      case GET -> _ => QueryParameterMustContainQuery

      case req @ POST -> AsDirPath(path) => for {
        query <- EntityDecoder.decodeString(req)
        resp  <- if (query != "") go(path, Query(query)) else POSTContentMustContainQuery
      } yield resp
    }
  }

  def serverService(config: Config, reloader: Config => Task[Unit]) = {
    HttpService {
      case req @ PUT -> Root / "port" => for {
        body <- EntityDecoder.decodeString(req)
        r    <- body.parseInt.fold(
          e => NotFound(e.getMessage),
          i => for {
            _    <- reloader(config.copy(server = SDServerConfig(Some(i))))
            resp <- Ok("changed port to " + i)
          } yield resp)
      } yield r
      case DELETE -> Root / "port" => for {
        _    <- reloader(config.copy(server = SDServerConfig(None)))
        resp <- Ok("reverted to default port")
      } yield resp
    }
  }

  def mountService(config: Config, reloader: Config => Task[Unit]) = {
    def addPath(path: Path, req: Request): M[Boolean] = for {
      body <- liftT(EntityDecoder.decodeString(req))
      conf <- liftE(Parse.decodeEither[BackendConfig](body).leftMap(RequestError(_)))
      _    <- liftE(conf.validate(path).leftMap(RequestError(_)))
      _    <- liftT(reloader(config.copy(mountings = config.mountings + (path -> conf))))
    } yield config.mountings.keySet contains path

    HttpService {
      case GET -> AsPath(path) =>
        config.mountings.find { case (k, _) => k.equals(path) }.fold(
          NotFound("There is no mount point at " + path))(
          v => Ok(BackendConfig.BackendConfig.encode(v._2)))
      case req @ MOVE -> AsPath(path) =>
        (config.mountings.get(path), req.headers.get(Destination).map(_.value)) match {
          case (Some(mounting), Some(newPath)) =>
            mounting.validate(Path(newPath)).fold(
              BadRequest(_),
              κ(for {
                _    <- reloader(config.copy(mountings = config.mountings - path + (Path(newPath) -> mounting)))
                resp <- Ok("moved " + path + " to " + newPath)
              } yield resp))
          case (None, _) => NotFound("There is no mount point at " + path)
          case (_, None) => DestinationHeaderMustExist
        }
      case req @ POST -> AsPath(path) =>
        def addMount(newPath: Path) =
          respond(for {
            _ <- addPath(newPath, req)
          } yield "added " + newPath)
        req.headers.get(FileName).map(nh => path ++ Path(nh.value)).map { newPath =>
          config.mountings.find { case (k, _) => k.contains(newPath) }.map { case (k, v) =>
            // TODO: make sure path+resource doesn’t conflict, too
            if (k.equals(newPath))
              MethodNotAllowed("There’s already a mount point at " + newPath)
            else
              addMount(newPath)
          }.getOrElse(addMount(newPath))
        }.getOrElse(FileNameHeaderMustExist)
      case req @ PUT -> AsPath(path) =>
        respond(for {
          upd <- addPath(path, req)
        } yield (if (upd) "updated" else "added") + " " + path)
      case DELETE -> AsPath(path) =>
        if (config.mountings.contains(path))
          for {
            _    <- reloader(config.copy(mountings = config.mountings - path))
            resp <- Ok("deleted " + path)
          } yield resp
        else
          Ok()
    }
  }

  def metadataService = HttpService {
    case GET -> AsPath(path) =>
      if (path == Path("/") && fs.isEmpty)
        Ok(Json.obj("children" := List[Path]()))
      else
        dataSourceFor(path) match {
          case \/- ((ds, relPath)) =>
            if (relPath.pureDir)
              ds.ls(relPath).attemptRun.fold(
                e => e match {
                  case f: FileSystem.FileNotFoundError => NotFound()
                  case _ => throw e
                },
                paths =>
                  Ok(Json.obj("children" := paths)))
            else
              ds.ls(relPath.dirOf).attemptRun.fold(
                e => e match {
                  case f: FileSystem.FileNotFoundError => NotFound()
                  case _ => throw e
                },
                paths => {
                  if (paths contains relPath.fileOf) Ok(Json.obj())
                  else NotFound()
                })


          case _ => {
            val fsChildren = fs.children(path)

            if (fsChildren.isEmpty) NotFound()
            else
              Ok(Json.obj("children" := fsChildren.map(p =>
                    Json("name" := p.simplePathname, "type" := "mount"))))
          }
        }
      // TODO: Use typesafe data structure and just serialize that.
  }

  // NB: EntityDecoders handle media types but not streaming, so the entire body is
  // parsed at once.
  implicit val dataDecoder: EntityDecoder[(List[WriteError], List[Data])] = {
    import ResponseFormat._

    val csv: EntityDecoder[(List[WriteError], List[Data])] = EntityDecoder.decodeBy(CsvMediaType) { msg =>
      val t = EntityDecoder.decodeString(msg).map { body =>
        import scalaz.std.option._
        import slamdata.engine.repl.Prettify

        CsvDetect.parse(body).fold(
          err => List(WriteError(Data.Str("parse error: " + err), None)) -> Nil,
          lines => lines.headOption.map { header =>
            val paths = header.fold(κ(Nil), _.fields.map(Prettify.Path.parse(_).toOption))
            val rows = lines.drop(1).map(_.bimap(
                err => WriteError(Data.Str("parse error: " + err), None),
                rec => {
                  val pairs = (paths zip rec.fields.map(Prettify.parse))
                  val good = pairs.map { case (p, s) => (p |@| s).tupled }.flatten
                  Prettify.unflatten(good.toListMap)
                }
              )).toList
            unzipDisj(rows)
          }.getOrElse(List(WriteError(Data.Obj(ListMap()), Some("no CSV header in body"))) -> Nil))
      }
      DecodeResult.success(t)
    }
    def json(mt: MediaRange)(implicit codec: DataCodec): EntityDecoder[(List[WriteError], List[Data])] = EntityDecoder.decodeBy(mt) { msg =>
      val t = EntityDecoder.decodeString(msg).map { body =>
        unzipDisj(body.split("\n").map(line => DataCodec.parse(line).leftMap(
          e => WriteError(Data.Str("parse error: " + line), Some(e.message)))).toList)
      }
      DecodeResult.success(t)
    }
    csv orElse
      json(ResponseFormat.Readable.mediaType)(DataCodec.Readable) orElse
      json(MediaRange.`*/*`)(DataCodec.Precise)
  }

  def dataService = HttpService {
    case req @ GET -> AsPath(path) :? Offset(offset) +& Limit(limit) =>
      (for {
        _ <- offset match { case Some(o) if o < 0 => -\/(BadRequest("Negative offset: " + o)); case _ =>  \/-(()) }
        _ <- limit match { case Some(l) if l < 1 => -\/(BadRequest("Limit not positive: " + l)); case _ =>  \/-(()) }
        t <- dataSourceFor(path)
        (dataSource, relPath) = t
      } yield responseStream(req.headers.get(Accept), dataSource.scan(relPath, offset, limit))).fold(identity, identity)

    case req @ PUT -> AsPath(path) =>
      req.decode[(List[WriteError], List[Data])] { case (errors, rows) =>
        upload(errors, path, (ds, p) => ds.save(p, Process.emitAll(rows)).attemptRun.leftMap(_ :: Nil))
      }

    case req @ POST -> AsPath(path) =>
      req.decode[(List[WriteError], List[Data])] { case (errors, rows) =>
        upload(errors, path, (ds, p) => {
          val errors0 = ds.append(p, Process.emitAll(rows)).runLog
          errors0.attemptRun.fold(err => -\/(err :: Nil), errs => if (!errs.isEmpty) -\/(errs.toList) else \/-(()))
        })
      }

    case req @ MOVE -> AsPath(path) =>
      (for {
          dstRaw <- req.headers.get(Destination).map(_.value) \/> DestinationHeaderMustExist
          dst <- Path(dstRaw).from(path.dirOf).leftMap(e => BadRequest("Invalid destination path: " + e.getMessage))

          t1 <- dataSourceFor(path)
          (srcDataSource, srcPath) = t1

          t2 <- dataSourceFor(dst)
          (dstDataSource, dstPath) = t2

          _ <- if (srcDataSource != dstDataSource) -\/(InternalServerError("Cannot copy/move across backends"))
               else srcDataSource.move(srcPath, dstPath).attemptRun.leftMap(e => errorResponse(InternalServerError, e))
        } yield Created("")
      ).fold(identity, identity)

    case DELETE -> AsPath(path) =>
      (for {
          t <- dataSourceFor(path)
          (dataSource, relPath) = t
          _ <- dataSource.delete(relPath).attemptRun.leftMap(e => errorResponse(InternalServerError, e))
        } yield Ok("")
      ).fold(identity, identity)
  }

  def fileMediaType(file: String): Option[MediaType] =
    MediaType.forExtension(file.split('.').last)

  def staticFileService(basePath: String) = HttpService {
    case GET -> AsPath(path) =>
      // NB: http4s/http4s#265 should give us a simple way to handle this stuff.
      val filePath = basePath + path.toString
      StaticFile.fromString(filePath).fold(
        StaticFile.fromString(filePath + "/index.html").fold(
          NotFound("Couldn’t find page " + path.toString))(
          Task.now))(
        resp => path.file.flatMap(f => fileMediaType(f.value)).fold(
          Task.now(resp))(
          mt => Task.delay(resp.withContentType(Some(`Content-Type`(mt))))))
  }

  def redirectService(basePath: String) = HttpService {
    case GET -> AsPath(path) =>
      TemporaryRedirect(Uri(path = basePath + path.toString))
  }

  def AllServices = ListMap(
    "/compile/fs"  -> FailSafe(Cors(compileService)),
    "/data/fs"     -> FailSafe(Cors(dataService)),
    "/metadata/fs" -> FailSafe(Cors(metadataService)),
    "/mount/fs"    -> FailSafe(Cors(mountService(config, reloader))),
    "/query/fs"    -> FailSafe(Cors(queryService)),
    "/server"      -> FailSafe(Cors(serverService(config, reloader))),
    "/slamdata"    -> FailSafe(Cors(staticFileService(contentPath + "/slamdata"))),
    "/"            -> FailSafe(Cors(redirectService("/slamdata"))))
}
