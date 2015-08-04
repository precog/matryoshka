/*
 * Copyright 2014 - 2015 SlamData Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package slamdata.engine.api

import slamdata.Predef._
import scala.collection.immutable.TreeSet

import slamdata.engine._; import Backend._; import Errors._; import Evaluator._; import Planner._
import slamdata.engine.config._
import slamdata.engine.sql._
import slamdata.engine.fs._; import Path.{Root => _, _}
import slamdata.engine.fp._

import argonaut.{DecodeResult => _, _ }
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
  def disposition: Option[`Content-Disposition`]
}
object ResponseFormat {
  final case class JsonStream private[ResponseFormat] (codec: DataCodec, mode: String, disposition: Option[`Content-Disposition`]) extends ResponseFormat {
    def mediaType = JsonStream.mediaType.withExtensions(Map("mode" -> mode))
  }
  object JsonStream {
    val mediaType = new MediaType("application", "ldjson", compressible = true)

    val Readable = JsonStream(DataCodec.Readable, "readable", None)
    val Precise  = JsonStream(DataCodec.Precise,  "precise", None)
  }

  final case class JsonArray private[ResponseFormat] (codec: DataCodec, mode: String, disposition: Option[`Content-Disposition`]) extends ResponseFormat {
    def mediaType = JsonArray.mediaType.withExtensions(Map("mode" -> mode))
  }
  object JsonArray {
    def mediaType = new MediaType("application", "json", compressible = true)

    val Readable = JsonArray(DataCodec.Readable, "readable", None)
    val Precise  = JsonArray(DataCodec.Precise,  "precise", None)
  }

  final case class Csv(columnDelimiter: Char, rowDelimiter: String, quoteChar: Char, escapeChar: Char, disposition: Option[`Content-Disposition`]) extends ResponseFormat {
    import Csv._

    def mediaType = Csv.mediaType.withExtensions(Map(
      "columnDelimiter" -> escapeNewlines(columnDelimiter.toString),
      "rowDelimiter" -> escapeNewlines(rowDelimiter),
      "quoteChar" -> escapeNewlines(quoteChar.toString),
      "escapeChar" -> escapeNewlines(escapeChar.toString)))
  }
  object Csv {
    val mediaType = new MediaType("text", "csv", compressible = true)

    val Default = Csv(',', "\r\n", '"', '"', None)

    def escapeNewlines(str: String): String =
      str.replace("\r", "\\r").replace("\n", "\\n")

    def unescapeNewlines(str: String): String =
      str.replace("\\r", "\r").replace("\\n", "\n")
  }

  def fromAccept(accept: Option[Accept]): ResponseFormat = {
    val mediaTypes = NonEmptyList(
      JsonStream.mediaType,
      new MediaType("application", "x-ldjson"),
      JsonArray.mediaType,
      Csv.mediaType)

    (for {
      acc       <- accept
      // TODO: MediaRange needs an Order instance – combining QValue ordering
      //       with specificity (EG, application/json sorts before
      //       application/* if they have the same q-value).
      mediaType <- acc.values.toList.sortBy(_.qValue).find(a => mediaTypes.toList.exists(a.satisfies(_)))
    } yield {
      import org.http4s.parser.HttpHeaderParser.parseHeader
      val disposition = mediaType.extensions.get("disposition").flatMap { str =>
        parseHeader(Header.Raw(CaseInsensitiveString("Content-Disposition"), str)).toOption.map(_.asInstanceOf[`Content-Disposition`])
      }
      if (mediaType satisfies Csv.mediaType) {
        def toChar(str: String): Option[Char] = str.toList match {
          case c :: Nil => Some(c)
          case _ => None
        }
        Csv(mediaType.extensions.get("columnDelimiter").map(Csv.unescapeNewlines).flatMap(toChar).getOrElse(','),
          mediaType.extensions.get("rowDelimiter").map(Csv.unescapeNewlines).getOrElse("\r\n"),
          mediaType.extensions.get("quoteChar").map(Csv.unescapeNewlines).flatMap(toChar).getOrElse('"'),
          mediaType.extensions.get("escapeChar").map(Csv.unescapeNewlines).flatMap(toChar).getOrElse('"'),
          disposition)
      }
      else {
        ((mediaType satisfies JsonArray.mediaType) && mediaType.extensions.get("boundary") != Some("NL"),
            mediaType.extensions.get("mode")) match {
          case (true, Some("precise"))  => JsonArray.Precise.copy(disposition = disposition)
          case (true, _)                => JsonArray.Readable.copy(disposition = disposition)
          case (false, Some("precise")) => JsonStream.Precise.copy(disposition = disposition)
          case (false, _)               => JsonStream.Readable.copy(disposition = disposition)
        }
      }
    }).getOrElse(JsonStream.Readable)
  }
}

final case class FileSystemApi(backend: Backend, contentPath: String, config: Config, reloader: Config => Task[Unit]) {
  import Method.{MOVE, OPTIONS}

  val CsvColumnsFromInitialRowsCount = 1000

  val LineSep = "\r\n"

  private def rawJsonLines[F[_]](codec: DataCodec, v: Process[F, Data])(implicit EE: EncodeJson[DataEncodingError]): Process[F, String] =
    v.map(DataCodec.render(_)(codec).fold(EE.encode(_).toString, ɩ))

  private def jsonStreamLines[F[_]](codec: DataCodec, v: Process[F, Data]): Process[F, String] =
    rawJsonLines(codec, v).map(_ + LineSep)

  private def jsonArrayLines[F[_]](codec: DataCodec, v: Process[F, Data]): Process[F, String] =
    // NB: manually wrapping the stream with "[", commas, and "]" allows us to still stream it,
    // rather than having to construct the whole response in Json form at once.
    Process.emit("[" + LineSep) ++ rawJsonLines(codec, v).intersperse("," + LineSep) ++ Process.emit(LineSep + "]" + LineSep)

  private def csvLines[F[_]](v: Process[F, Data], format: Option[CsvParser.Format]): Process[F, String] = {
    import slamdata.engine.repl.Prettify
    import com.github.tototoshi.csv._

    Prettify.renderStream(v, CsvColumnsFromInitialRowsCount).map { v =>
      val w = new java.io.StringWriter
      val cw = format.map(f => CSVWriter.open(w)(f)).getOrElse(CSVWriter.open(w))
      cw.writeRow(v)
      cw.close
      w.toString
    }
  }

  val unstack = new (ProcessingTask ~> Task) {
    def apply[A](t: ProcessingTask[A]): Task[A] =
      t.fold(e => Task.fail(new RuntimeException(e.message)), Task.now).join
  }

  private def linesResponse[A: EntityEncoder](v: Process[ProcessingTask, A]):
      Task[Response] =
    v.unconsOption.fold(
      handleProcessingError(_),
      _.fold(
        Ok("")) {
        case (first, rest) => Ok(Process.emit(first) ++ rest.translate(unstack))
      }).join
  

  private def responseLines[F[_]](accept: Option[Accept], v: Process[F, Data]) =
    ResponseFormat.fromAccept(accept) match {
      case f @ ResponseFormat.JsonStream(codec, _, disposition) =>
        (f.mediaType, jsonStreamLines(codec, v), disposition)
      case f @ ResponseFormat.JsonArray(codec, _, disposition) =>
        (f.mediaType, jsonArrayLines(codec, v), disposition)
      case f @ ResponseFormat.Csv(r, c, q, e, disposition) =>
        (f.mediaType, csvLines(v, Some(CsvParser.Format(r, q, e, c))), disposition)
    }

  private def responseStream(accept: Option[Accept], v: Process[ProcessingTask, Data]):
      Task[Response] = {
    val (mediaType, lines, disposition) = responseLines(accept, v)
    linesResponse(lines).map(_.putHeaders(
      `Content-Type`(mediaType, Some(Charset.`UTF-8`)) ::
      disposition.toList: _*))
  }

  private def errorResponse(
    status: org.http4s.dsl.impl.EntityResponseGenerator,
    e: Throwable):
      Task[Response] =
    failureResponse(status, e.getMessage)

  private def failureResponse(
    status: org.http4s.dsl.impl.EntityResponseGenerator,
    message: String):
      Task[Response] =
    status(errorBody(message, None))

  private def errorBody(message: String, phases: Option[Vector[PhaseResult]]) =
    Json((("error" := message) :: phases.map("phases" := _).toList): _*)

  private def vars(req: Request) = Variables(req.params.map { case (k, v) => (VarName(k), VarValue(v)) })

  private val QueryParameterMustContainQuery = BadRequest(errorBody("The request must contain a query", None))
  private val POSTContentMustContainQuery    = BadRequest(errorBody("The body of the POST must contain a query", None))
  private val DestinationHeaderMustExist     = BadRequest(errorBody("The '" + Destination.name + "' header must be specified", None))
  private val FileNameHeaderMustExist        = BadRequest(errorBody("The '" + FileName.name + "' header must be specified", None))

  private def upload[A](errors: List[WriteError], path: Path, f: (Backend, Path) => ProcessingTask[List[WriteError] \/ Unit]): Task[Response] = {
    def dataErrorBody(status: org.http4s.dsl.impl.EntityResponseGenerator, errs: List[WriteError])(implicit EJ: EncodeJson[WriteError]) =
      status(Json(
        "error" := "some uploaded value(s) could not be processed",
        "details" := errs.map(e => Json("detail" := e.message))))

    if (!errors.isEmpty)
      dataErrorBody(BadRequest, errors)
    else {
      f(backend, path).run.flatMap(_.fold(
        handleProcessingError,
        _.fold(dataErrorBody(InternalServerError, _), κ(Ok("")))))
    }
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

  def queryService = {
    HttpService {
      case req @ GET -> AsDirPath(path) :? Q(query) => {
        SQLParser.parseInContext(query, path).fold(
          handleParsingError,
          expr => backend.eval(QueryRequest(expr, None, Variables(Map()))).run._2.fold(
            handleCompilationError,
            responseStream(req.headers.get(Accept), _)))
      }

      case GET -> _ => QueryParameterMustContainQuery

      case req @ POST -> AsDirPath(path) =>
        def go(query: Query): Task[Response] =
          req.headers.get(Destination).fold(
            DestinationHeaderMustExist)(
            x =>
            (SQLParser.parseInContext(query, path)
              .leftMap(handleParsingError) |@|
              Path(x.value).from(path)
              .leftMap(handlePathError))((expr, out) => {
                val (phases, resultT) = backend.run(QueryRequest(expr, Some(out), vars(req))).run
                resultT.fold(
                  handleCompilationError,
                  _.fold(
                    handleEvalError,
                    out => Ok(Json.obj(
                      "out"    := out.path.pathname,
                      "phases" := phases))).join)
              }).fold(ɩ, ɩ))

        for {
          query <- EntityDecoder.decodeString(req)
          resp <- if (query != "") go(Query(query)) else POSTContentMustContainQuery
        } yield resp
    }
  }

  def compileService = {
    def go(path: Path, query: Query): Task[Response] = {
      (for {
        expr  <- SQLParser.parseInContext(query, path).leftMap(handleParsingError)

        phases = backend.evalLog(QueryRequest(expr, None, Variables(Map())))

        plan  <- phases.lastOption \/> InternalServerError("no plan")
      } yield plan match {
        case PhaseResult.Tree(name, value)   => Ok(Json(name := value))
        case PhaseResult.Detail(name, value) => Ok(name + "\n" + value)
      }).fold(ɩ, ɩ)
    }

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
      case req @ GET -> Root / "info" =>
        Ok(versionAndNameInfo)
    }
  }

  def liftT[A](t: Task[A]): EnvTask[A] = EitherT.right(t)
  def liftE[A](v: EnvironmentError \/ A): EnvTask[A] = EitherT(Task.now(v))

  def respond(v: EnvTask[String]): Task[Response] =
    v.fold(err => BadRequest(errorBody(err.message, None)), Ok(_)).join

  def mountService(config: Config, reloader: Config => Task[Unit]) = {
    def addPath(path: Path, req: Request): EnvTask[Boolean] = for {
      body <- liftT(EntityDecoder.decodeString(req))
      conf <- liftE(Parse.decodeEither[BackendConfig](body).leftMap(err => InvalidConfig("input error: " + err)))
      _    <- liftE(conf.validate(path))
      _    <- liftT(reloader(config.copy(mountings = config.mountings + (path -> conf))))
    } yield config.mountings.keySet contains path

    HttpService {
      case GET -> AsPath(path) =>
        config.mountings.find { case (k, _) => k == path }.fold(
          NotFound("There is no mount point at " + path))(
          v => Ok(BackendConfig.BackendConfig.encode(v._2)))
      case req @ MOVE -> AsPath(path) =>
        (config.mountings.get(path), req.headers.get(Destination).map(_.value)) match {
          case (Some(mounting), Some(newPath)) =>
            mounting.validate(Path(newPath)).fold(
              err => BadRequest(errorBody(err.message, None)),
              κ(for {
                _    <- reloader(config.copy(mountings = config.mountings - path + (Path(newPath) -> mounting)))
                resp <- Ok("moved " + path + " to " + newPath)
              } yield resp))
          case (None, _) => NotFound(errorBody("There is no mount point at " + path, None))
          case (_, None) => DestinationHeaderMustExist
        }
      case req @ POST -> AsPath(path) =>
        def addMount(newPath: Path) =
          respond(for {
            _ <- addPath(newPath, req)
          } yield "added " + newPath)
        req.headers.get(FileName).fold(
          FileNameHeaderMustExist) { nh =>
          val newPath = path ++ Path(nh.value)
          config.mountings.toList.map { case (k, _) =>
            // FIXME: This should really be checked in the backend, not here
            k.rebase(newPath).fold(
              κ(newPath.rebase(k).fold(
                κ(\/-(())),
                κ(-\/(Conflict(errorBody("Can't add a mount point below the existing mount point at  " + k, None)))))),
              κ(-\/(Conflict(errorBody("Can't add a mount point above the existing mount point at " + k, None)))))
          }.sequenceU.fold(ɩ, κ(addMount(newPath)))
        }
      case req @ PUT -> AsPath(path) =>
        config.mountings.toList.map { case (k, _) =>
          // FIXME: This should really be checked in the backend, not here
          k.rebase(path).fold(
            κ(path.rebase(k).fold(
              κ(\/-(())),
              κ(-\/(Conflict(errorBody("Can't add a mount point below the existing mount point at  " + k, None)))))),
            κ(if (k == path)
              \/-(())
            else
              -\/(Conflict(errorBody("Can't add a mount point above the existing mount point at " + k, None)))))
        }.sequenceU.fold(
          ɩ,
          κ(respond(for {
            upd <- addPath(path, req)
          } yield (if (upd) "updated" else "added") + " " + path)))
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
      path.file.fold(
        backend.ls(path).fold(
          handlePathError,
          // NB: we sort only for deterministic results, since JSON lacks `Set`
          paths => Ok(Json.obj("children" := paths.toList.sorted))))(
        κ(backend.exists(path).fold(
          handlePathError,
          x => if (x) Ok(Json.obj()) else NotFound()))).join
  }

  def handlePathError(error: PathError): Task[Response] =
    failureResponse(
      error match {
        case ExistingPathError(_, _)    => Conflict
        case NonexistentPathError(_, _) => NotFound
        case InternalPathError(_)       => NotImplemented
        case _                          => BadRequest
      },
      error.message)

  def handleResultError(error: ResultError): Task[Response] = error match {
    case ResultPathError(e) => handlePathError(e)
    case e: ScanError => failureResponse(BadRequest, error.message)
    case _ => failureResponse(InternalServerError, error.message)
  }

  def handleProcessingError(error: ProcessingError): Task[Response] = error match {
    case PPathError(e) => handlePathError(e)
    case PResultError(e) => handleResultError(e)
    case _ => failureResponse(InternalServerError, error.message)
  }

  def handleEvalError(error: EvaluationError): Task[Response] = error match {
    case EvalPathError(e) => handlePathError(e)
    case _ => failureResponse(InternalServerError, error.message)
  }

  def handleParsingError(error: ParsingError): Task[Response] = error match {
    case ParsingPathError(e) => handlePathError(e)
    case _ => failureResponse(BadRequest, error.message)
  }

  def handleCompilationError(error: CompilationError): Task[Response] =
    error match {
      case CompilePathError(e) => handlePathError(e)
      case _ => failureResponse(InternalServerError, error.message)
    }

  // NB: EntityDecoders handle media types but not streaming, so the entire body is
  // parsed at once.
  implicit val dataDecoder: EntityDecoder[(List[WriteError], List[Data])] = {
    import ResponseFormat._

    val csv: EntityDecoder[(List[WriteError], List[Data])] = EntityDecoder.decodeBy(Csv.mediaType) { msg =>
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
      json(ResponseFormat.JsonStream.Readable.mediaType)(DataCodec.Readable) orElse
      json(new MediaType("*", "*"))(DataCodec.Precise)
  }

  def dataService = HttpService {
    case req @ GET -> AsPath(path) :? Offset(offset0) +& Limit(limit) =>
      val offset = offset0.getOrElse(0L)
      val accept = req.headers.get(Accept)
      if (path.pureDir) {
        backend.lsAll(path).fold(
          handlePathError,
          ns => {
            val bytes = Zip.zipFiles(ns.toList.map { n =>
              val (_, lines, _) = responseLines(accept, backend.scan(path ++ n.path, offset, limit).translate[ProcessingTask](convertError(PResultError(_))))
              n.path -> lines.map(str => scodec.bits.ByteVector.view(str.getBytes(java.nio.charset.StandardCharsets.UTF_8)))
            })
            val ct = `Content-Type`(MediaType.`application/zip`)
            val disp = ResponseFormat.fromAccept(accept).disposition
            linesResponse(bytes).map(_.withHeaders((ct :: disp.toList): _*))
          }).join
      }
      else
        responseStream(accept, backend.scan(path, offset, limit).translate[ProcessingTask](convertError(PResultError(_))))

    case req @ PUT -> AsPath(path) =>
      req.decode[(List[WriteError], List[Data])] { case (errors, rows) =>
        upload(errors, path, _.save(_, Process.emitAll(rows)).map(κ(\/-(()))))
      }

    case req @ POST -> AsPath(path) =>
      req.decode[(List[WriteError], List[Data])] { case (errors, rows) =>
        upload(errors, path, _.append(_, Process.emitAll(rows)).runLog.bimap(PPathError(_), x => if (x.isEmpty) \/-(()) else -\/(x.toList)))
      }

    case req @ MOVE -> AsPath(path) =>
      req.headers.get(Destination).fold(
        DestinationHeaderMustExist)(
        x =>
        Path(x.value).from(path.dirOf).fold(
          handlePathError,
          backend.move(path, _, FailIfExists).run.flatMap(_.fold(handlePathError, κ(Created(""))))))

    case DELETE -> AsPath(path) =>
      backend.delete(path).run.flatMap(_.fold(handlePathError, κ(Ok(""))))
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
    "/compile/fs"  -> compileService,
    "/data/fs"     -> dataService,
    "/metadata/fs" -> metadataService,
    "/mount/fs"    -> mountService(config, reloader),
    "/query/fs"    -> queryService,
    "/server"      -> serverService(config, reloader),
    "/slamdata"    -> staticFileService(contentPath + "/slamdata"),
    "/"            -> redirectService("/slamdata")) ∘
      (svc => Cors(middleware.GZip(HeaderParam(svc))))
}
