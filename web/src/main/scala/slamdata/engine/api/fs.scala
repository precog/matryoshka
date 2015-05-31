package slamdata.engine.api

import scala.collection.immutable.{TreeSet}

import slamdata.engine._; import Backend._
import slamdata.engine.config._
import slamdata.engine.sql._
import slamdata.engine.fs._
import slamdata.engine.fp._

import argonaut._
import Argonaut._

import org.http4s.{Query => HQuery, _}; import EntityEncoder._
import org.http4s.dsl.{Path => HPath, listInstance => _, _}
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

class FileSystemApi(backend: Backend) {
  import Method.{MOVE, OPTIONS}

  val CsvColumnsFromInitialRowsCount = 1000

  private def jsonStream(codec: DataCodec, v: Process[Task, Data])(implicit EE: EncodeJson[DataEncodingError]): Task[Response] =
    Ok(v.map(
      DataCodec.render(_)(codec).fold(EE.encode(_).toString, ɩ) + "\r\n"))

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

  private def errorResponse(
    status: org.http4s.dsl.impl.EntityResponseGenerator,
    e: Throwable):
      Task[Response] = {
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
  private val DestinationHeaderMustExist     = BadRequest("The 'Destination' header must be specified")

  private def upload[A](body: String, path: Path, f: (Backend, Path, Process[Task, Data]) => PathTask[List[WriteError] \/ Unit]): Task[Response] = {
    val codec = DataCodec.Precise
    def parseJsonLines(str: String): (List[WriteError], List[Data]) =
      unzipDisj(str.split("\n").map(line => DataCodec.parse(line)(codec).leftMap(
        e => WriteError(Data.Str("parse error: " + line), Some(e.message)))).toList)

    def errorBody(status: org.http4s.dsl.impl.EntityResponseGenerator, errs: List[WriteError])(implicit EJ: EncodeJson[WriteError]) =
      status(Json("errors" := errs.map(EJ.encode)))

    val (errs, json) = parseJsonLines(body)
    if (!errs.isEmpty)
      errorBody(BadRequest, errs)
    else f(backend, path, Process.emitAll(json)).fold(
      handlePathError,
      _.fold(errorBody(InternalServerError, _), κ(Ok("")))).join
      .handleWith { case e => errorResponse(InternalServerError, e) }
  }

  object AsPath {
    def unapply(p: HPath): Option[Path] = {
      Some(Path("/" + p.toList.map(java.net.URLDecoder.decode(_, "UTF-8")).mkString("/")))
    }
  }

  object AsDirPath {
    def unapply(p: HPath): Option[Path] = AsPath.unapply(p).map(_.asDir)
  }

  implicit def FilesystemNodeEncodeJson = EncodeJson[FilesystemNode] { fsn =>
    Json(
      "name" := fsn.path.simplePathname,
      "type" := (fsn.typ match {
        case Mount => "mount"
        case Plain => fsn.path.file.fold("directory")(κ("file"))
      }))
  }

  implicit val QueryDecoder = new QueryParamDecoder[Query] {
    def decode(value: QueryParameterValue): ValidationNel[ParseFailure, Query] =
      Query(value.value).successNel[ParseFailure]
  }
  object Q extends QueryParamDecoderMatcher[Query]("q")

  object Offset extends OptionalQueryParamDecoderMatcher[Long]("offset")
  object Limit extends OptionalQueryParamDecoderMatcher[Long]("limit")

  // Note: CORS middleware is coming in http4s post-0.6.5
  val corsHeaders = List(
    AccessControlAllowOriginAll,
    `Access-Control-Allow-Methods`(List("GET", "PUT", "POST", "DELETE", "MOVE", "OPTIONS")),
    `Access-Control-Max-Age`(20*24*60*60),
    `Access-Control-Allow-Headers`(List(Destination)))  // NB: actually needed for POST only

  // TODO: Using the Option Kleisli category or scalaz.NullResult instead of PartialFunction.
  def corsService(pf: PartialFunction[Request, Task[Response]]) =
    HttpService(pf.orElse[Request, Task[Response]] {
      case OPTIONS -> _ => Ok()
    }).map(_.putHeaders(corsHeaders: _*))

  def queryService = {
    corsService {
      case req @ GET -> AsDirPath(path) :? Q(query) => {
        SQLParser.parseInContext(query, path).fold(
          err => BadRequest("query error: " + err),
          expr => {
            val (phases, resultT) = backend.eval(QueryRequest(expr, None, Variables(Map())))
            resultT.fold(
              handlePathError,
              responseStream(req.headers.get(Accept), _)).join
              .handleWith { case e => errorResponse(BadRequest, e) }
          })
      }

      case GET -> _ => QueryParameterMustContainQuery

      case req @ POST -> AsDirPath(path) =>
        def go(query: Query): Task[Response] =
          req.headers.get(Destination).fold(
            DestinationHeaderMustExist)(
            x =>
            (SQLParser.parseInContext(query, path).leftMap(err => BadRequest("query error: " + err)) |@| Path(x.value).from(path).leftMap(errorResponse(BadRequest, _)))((expr, out) => {
              val (phases, resultT) = backend.run(QueryRequest(expr, Some(out), vars(req)))
              resultT.run
                .flatMap(_.fold(
                  handlePathError,
                  out => Ok(Json.obj(
                    "out"    := out.path.pathname,
                    "phases" := phases))))
                .handleWith { case e => errorResponse(InternalServerError, e) }
            }).fold(identity, identity))

        for {
          query <- EntityDecoder.decodeString(req)
          resp <- if (query != "") go(Query(query)) else POSTContentMustContainQuery
        } yield resp
    }
  }

  def compileService = {
    def go(path: Path, query: Query): Task[Response] = {
      (for {
        expr  <- SQLParser.parseInContext(query, path).leftMap(err => BadRequest("query error: " + err))

        (phases, _) = backend.eval(QueryRequest(expr, None, Variables(Map())))

        plan  <- phases.lastOption \/> InternalServerError("no plan")
      } yield plan match {
          case PhaseResult.Error(_, value) => errorResponse(BadRequest, value)
          case PhaseResult.Tree(name, value)   => Ok(Json(name := value))
          case PhaseResult.Detail(name, value) => Ok(name + "\n" + value)
      }).fold(identity, identity)
    }

    corsService {
      case GET -> AsDirPath(path) :? Q(query) => go(path, query)

      case GET -> _ => QueryParameterMustContainQuery

      case req @ POST -> AsDirPath(path) => for {
        query <- EntityDecoder.decodeString(req)
        resp  <- if (query != "") go(path, Query(query)) else POSTContentMustContainQuery
      } yield resp
    }
  }

  def serverService(config: Config, reloader: Config => Task[Unit]) = {
    corsService {
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
    def addPath(path: Path, req: Request): Task[Unit] = for {
      body <- EntityDecoder.decodeString(req)
      conf <- Parse.decodeEither[BackendConfig](body).fold(
        e => Task.fail(new RuntimeException(e)),
        Task.now)
      _    <- reloader(config.copy(mountings = config.mountings + (path -> conf)))
    } yield ()

    corsService {
      case GET -> AsPath(path) =>
        config.mountings.find { case (k, _) => k.equals(path) }.fold(
          NotFound("There is no mount point at " + path))(
          v => Ok(BackendConfig.BackendConfig.encode(v._2).pretty(slamdata.engine.fp.multiline)))
      case req @ POST -> AsPath(path) =>
        def addMount = for {
          _    <- addPath(path, req)
          resp <- Ok("added " + path)
        } yield resp
        config.mountings.find { case (k, _) => k.contains(path) }.fold(
          addMount) {
          case (k, v) =>
            // TODO: make sure path+resource doesn’t conflict, too
            if (k.equals(path))
              Conflict("There’s already a mount point at " + path)
            else
              addMount
        }
      case req @ PUT -> AsPath(path) =>
        if (config.mountings.contains(path))
          NotFound("There is no mount point at " + path)
        else for {
          _    <- addPath(path, req)
          resp <- Ok("updated " + path)
        } yield resp
      case DELETE -> AsPath(path) =>
        if (config.mountings.contains(path))
          NotFound("There is no mount point at " + path)
        else for {
          _    <- reloader(config.copy(mountings = config.mountings - path))
          resp <- Ok("deleted " + path)
        } yield resp
    }
  }

  def metadataService = corsService {
    case GET -> AsPath(path) =>
      path.file.fold(
        backend.ls(path).fold(
          handlePathError,
          // NB: we sort only for deterministic results, since JSON lacks `Set`
          paths => Ok(Json.obj("children" := paths.toList.sorted))))(
        κ(backend.exists(path).fold(
          handlePathError,
          x => if (x) Ok(Json.obj()) else NotFound()))).join
        .handleWith { case e => errorResponse(InternalServerError, e) }
  }

  def handlePathError(error: PathError): Task[Response] =
    errorResponse(
      error match {
        case ExistingPathError(_, _)    => Conflict
        case NonexistentPathError(_, _) => NotFound
        case InternalPathError(_)       => NotImplemented
        case _                          => BadRequest
      },
      error)

  def dataService = corsService {
    case req @ GET -> AsPath(path) :? Offset(offset) +& Limit(limit) =>
      backend.scan(path, offset, limit).fold(
        handlePathError,
        responseStream(req.headers.get(Accept), _)).join

    case req @ PUT -> AsPath(path) => for {
      body <- EntityDecoder.decodeString(req)
      resp <- upload(body, path, _.save(_, _).map(κ(\/-(()))))
    } yield resp

    case req @ POST -> AsPath(path) => for {
      body <- EntityDecoder.decodeString(req)
      resp <- upload(body, path, _.append(_, _).flatMap(x => liftP(x.runLog.map(x => if (x.isEmpty) \/-(()) else -\/(x.toList)))))
    } yield resp

    case req @ MOVE -> AsPath(path) =>
      req.headers.get(Destination).fold(
        DestinationHeaderMustExist)(
        x =>
        Path(x.value).from(path.dirOf).fold(
          handlePathError,
          backend.move(path, _).run.flatMap(_.fold(handlePathError, κ(Created(""))))))

    case DELETE -> AsPath(path) =>
      backend.delete(path).run.flatMap(_.fold(handlePathError, κ(Ok(""))))
  }

  def fileMediaType(file: String): Option[MediaType] =
    MediaType.forExtension(file.split('.').last)

  def staticFileService(basePath: String) = corsService {
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

  def redirectService(basePath: String) = corsService {
    case GET -> AsPath(path) =>
      TemporaryRedirect(Uri(path = basePath + path.toString))
  }
}
