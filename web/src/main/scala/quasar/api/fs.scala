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

package quasar.api

import quasar.Predef._
import quasar.fp._
import quasar._, Backend._, Errors._, Evaluator._, Planner._
import quasar.config._
import quasar.fs._, Path.{Root => _, _}
import quasar.sql._

import scala.collection.immutable.TreeSet
import scala.concurrent.duration.DurationInt
import java.nio.charset.StandardCharsets

import argonaut.{DecodeResult => _, _ }, Argonaut._
import org.http4s.{Query => HQuery, _}, EntityEncoder._
import org.http4s.argonaut._
import org.http4s.dsl.{Path => HPath, _}
import org.http4s.headers._
import org.http4s.server._, middleware.{CORS, GZip}, syntax.ServiceOps
import org.http4s.util.{CaseInsensitiveString, Renderable}
import scalaz._, Scalaz._
import scalaz.concurrent._
import scalaz.stream._
import scodec.bits.ByteVector

object FileSystemApi {
  type Apply[WC, SC] =
    (
    WC,
    WC => EnvTask[Backend],
    MountConfig => EnvTask[Unit],
    WC => Task[Unit],
    WC => Task[Unit],
    WebConfigLens[WC, SC]
    )
    => FileSystemApi[WC, SC]

  def apply: Apply[WebConfig, ServerConfig] = new FileSystemApi(_, _, _, _, _, _)
}

/**
 * The REST API to the Quasar Engine
 * @param initialConfig The config with which the server will be started initially
 * @param createBackend create a backend from the given config
 * @param validateConfig Is called to validate a `MountConfig` when the client changes a mount
 * @param restartServer Expected to restart server when called using the provided Configuration. Called only when the port changes.
 * @param configChanged Expected to persist a Config when called. Called whenever the Config changes.
 */
class FileSystemApi[WC, SC](
  initialConfig: WC,
  createBackend: WC => EnvTask[Backend],
  validateConfig: MountConfig => EnvTask[Unit],
  restartServer: WC => Task[Unit],
  configChanged: WC => Task[Unit],
  webConfigLens: WebConfigLens[WC, SC]) {

  import Method.{MOVE, OPTIONS}
  import webConfigLens._

  type S = (WC, Backend)

  val CsvColumnsFromInitialRowsCount = 1000
  val LineSep = "\r\n"

  /** Sets the configured mounts to the given mountings. */
  private def setMounts(mounts: Map[Path, MountConfig], sref: TaskRef[S]): EnvTask[Unit] =
    for {
      s           <- sref.read.liftM[EnvErrT]
      (cfg, bknd) =  s
      updCfg      =  mountings.set(mounts)(cfg)
      updBknd     <- createBackend(updCfg)
      _           <- configChanged(updCfg).liftM[EnvErrT]
      _           <- sref.write((updCfg, updBknd)).liftM[EnvErrT]
    } yield ()

  /**
   * Mounts the backend represented by the given config at the given path,
   * returning true if the path already existed.
   */
  private def upsertMount(path: Path, backendCfg: MountConfig, sref: TaskRef[S]): EnvTask[Boolean] =
    sref.read.liftM[EnvErrT].flatMap { case (cfg, _) =>
      setMounts(mountings.get(cfg).updated(path, backendCfg), sref)
        .as(mountings.get(cfg).keySet contains path)
    }

  protected def updateWebServerConfig(config: Task[WC]): Task[Unit] =
    for {
      cfg <- config
      _   <- configChanged(cfg)
      _   <- restartServer(cfg)
    } yield ()

  private def rawJsonLines[F[_]](codec: DataCodec, v: Process[F, Data]): Process[F, String] =
    v.map(DataCodec.render(_)(codec).fold(EncodeJson.of[DataEncodingError].encode(_).toString, ι))

  private def jsonStreamLines[F[_]](codec: DataCodec, v: Process[F, Data]): Process[F, String] =
    rawJsonLines(codec, v).map(_ + LineSep)

  private def jsonArrayLines[F[_]](codec: DataCodec, v: Process[F, Data]): Process[F, String] =
    // NB: manually wrapping the stream with "[", commas, and "]" allows us to still stream it,
    // rather than having to construct the whole response in Json form at once.
    Process.emit("[" + LineSep) ++ rawJsonLines(codec, v).intersperse("," + LineSep) ++ Process.emit(LineSep + "]" + LineSep)

  private def csvLines[F[_]](v: Process[F, Data], format: Option[CsvParser.Format]): Process[F, String] = {
    import quasar.repl.Prettify
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
      handleProcessingError,
      _.fold(Ok(""))({ case (first, rest) =>
        Ok(Process.emit(first) ++ rest.translate(unstack))
      })).join

  private def responseLines[F[_]](accept: Option[Accept], v: Process[F, Data]) =
    MessageFormat.fromAccept(accept) match {
      case f @ MessageFormat.JsonStream(mode, disposition) =>
        (f.mediaType, jsonStreamLines(mode.codec, v), disposition)
      case f @ MessageFormat.JsonArray(mode, disposition) =>
        (f.mediaType, jsonArrayLines(mode.codec, v), disposition)
      case f @ MessageFormat.Csv(r, c, q, e, disposition) =>
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

  private def responseForUpload[A](errors: List[WriteError], task: => ProcessingTask[List[WriteError] \/ Unit]): Task[Response] = {
    def dataErrorBody(status: org.http4s.dsl.impl.EntityResponseGenerator, errs: List[WriteError]) =
      status(Json(
        "error" := "some uploaded value(s) could not be processed",
        "details" := errs.map(e => Json("detail" := e.message))))

      if (!errors.isEmpty)
        dataErrorBody(BadRequest, errors)
      else
        task.run.flatMap(_.fold(
          handleProcessingError,
          _.as(Ok("")) valueOr (dataErrorBody(InternalServerError, _))))
  }

  implicit def FilesystemNodeEncodeJson = EncodeJson[FilesystemNode] { fsn =>
    Json(
      (("name" := fsn.path.simplePathname) ::
        ("type" := fsn.path.file.fold("directory")(κ("file"))) ::
        fsn.mountType.toList.map("mount" := _)): _*)
  }

  implicit val QueryDecoder = new QueryParamDecoder[Query] {
    def decode(value: QueryParameterValue): ValidationNel[ParseFailure, Query] =
      Query(value.value).successNel[ParseFailure]
  }

  // https://github.com/puffnfresh/wartremover/issues/149
  @SuppressWarnings(Array("org.brianmckenna.wartremover.warts.NonUnitStatements"))
  object Q extends QueryParamDecoderMatcher[Query]("q")

  // https://github.com/puffnfresh/wartremover/issues/149
  @SuppressWarnings(Array("org.brianmckenna.wartremover.warts.NonUnitStatements"))
  object Offset extends OptionalQueryParamDecoderMatcher[Long]("offset")

  // https://github.com/puffnfresh/wartremover/issues/149
  @SuppressWarnings(Array("org.brianmckenna.wartremover.warts.NonUnitStatements"))
  object Limit extends OptionalQueryParamDecoderMatcher[Long]("limit")

  def queryService(backend: Task[Backend]) =
    HttpService {
      case req @ GET -> AsDirPath(path) :? Q(query) => {
        SQLParser.parseInContext(query, path).fold(
          handleParsingError,
          expr => backend.flatMap(_.eval(QueryRequest(expr, Variables(Map()))).run._2.fold(
            handleCompilationError,
            responseStream(req.headers.get(Accept), _))))
      }

      case GET -> _ => QueryParameterMustContainQuery

      case req @ POST -> AsDirPath(path) =>
        def go(query: Query): Task[Response] =
          req.headers.get(Destination).fold(DestinationHeaderMustExist) { x =>
            val parseRes = SQLParser.parseInContext(query, path).leftMap(handleParsingError)
            val pathRes = Path(x.value).from(path).leftMap(handlePathError)

            backend.flatMap { bknd =>
              (parseRes |@| pathRes)((expr, out) => {
                val (phases, resultT) = bknd.run(QueryRequest(expr, vars(req)), out).run

                resultT.fold(
                  handleCompilationError,
                  _.fold(
                    handleEvalError,
                    out => Ok(Json.obj(
                      "out"    := out.path.pathname,
                      "phases" := phases))).join)
              }).merge
            }
          }

        for {
          query <- EntityDecoder.decodeString(req)
          resp <- if (query != "") go(Query(query)) else POSTContentMustContainQuery
        } yield resp
    }

  def compileService(backend: Task[Backend]) = {
    def go(path: Path, query: Query, bknd: Backend): Task[Response] = {
      (for {
        expr  <- SQLParser.parseInContext(query, path).leftMap(handleParsingError)

        phases = bknd.evalLog(QueryRequest(expr, Variables(Map())))

        plan  <- phases.lastOption \/> InternalServerError("no plan")
      } yield plan match {
        case PhaseResult.Tree(name, value)   => Ok(Json(name := value))
        case PhaseResult.Detail(name, value) => Ok(name + "\n" + value)
      }).merge
    }

    HttpService {
      case GET -> AsDirPath(path) :? Q(query) => backend.flatMap(go(path, query, _))

      case GET -> _ => QueryParameterMustContainQuery

      case req @ POST -> AsDirPath(path) => for {
        query <- EntityDecoder.decodeString(req)
        resp  <- if (query != "") backend.flatMap(go(path, Query(query), _))
                 else POSTContentMustContainQuery
      } yield resp
    }
  }

  def serverService(config: Task[WC]) =
    HttpService {
      case req @ PUT -> Root / "port" =>
        EntityDecoder.decodeString(req).flatMap(body =>
          body.parseInt.fold(
            e => NotFound(e.getMessage),
            // TODO: If the requested port is unavailable the server will restart
            //       on a random one, thus this response text may not be accurate.
            i => updateWebServerConfig(config.map(wcPort.set(i))) *>
                 Ok("changed port to " + i)))

      case DELETE -> Root / "port" =>
        updateWebServerConfig(config.map(wcPort.set(ServerConfig.DefaultPort))) *>
        Ok("reverted to default port " + ServerConfig.DefaultPort)

      case req @ GET -> Root / "info" =>
        Ok(versionAndNameInfo)
    }

  def liftE[A](v: EnvironmentError \/ A): EnvTask[A] = EitherT(Task.now(v))

  def respond(v: EnvTask[String]): Task[Response] =
    v.fold(handleEnvironmentError, Ok(_)).join

  def mountService(ref: TaskRef[S]) = {
    def addPath(path: Path, req: Request): EnvTask[Boolean] = for {
      body  <- EntityDecoder.decodeString(req).liftM[EnvErrT]
      bConf <- liftE(Parse.decodeWith[String \/ MountConfig, MountConfig](body,_.right[String],
                 parseErrorMsg => s"input error: $parseErrorMsg".left[MountConfig],
                 {case (msg, _) => msg.left[MountConfig]}).leftMap(msg => InvalidConfig(msg)))
      _     <- liftE(bConf.validate(path))
      _     <- validateConfig(bConf)
      isUpd <- upsertMount(path, bConf, ref)
    } yield isUpd

    /**
     * Returns an error response if the given path overlaps any existing ones,
     * otherwise returns None.
     *
     * FIXME: This should really be checked in the backend, not here
     */
    def ensureNoOverlaps(cfg: WC, path: Path): Option[Task[Response]] =
      // NB: this check applies only to non-view mounts, which (at the moment),
      // also happen to have directory paths. And we haven't parsed the config
      // yet, so it's expedient to just check the path type here.
      if (path.pureDir)
        mountings.get(cfg).keys.toList.traverseU(k =>
          k.rebase(path)
            .as(Conflict(errorBody("Can't add a mount point above the existing mount point at " + k, None)))
            .swap *>
          path.rebase(k)
            .as(Conflict(errorBody("Can't add a mount point below the existing mount point at " + k, None)))
            .swap
        ).swap.toOption
      else None


    HttpService {
      case GET -> AsPath(path) =>
        ref.read.flatMap { case (cfg, _) =>
          mountings.get(cfg).find { case (k, _) => k == path }.cata(
            v => Ok(MountConfig.Codec.encode(v._2)),
            NotFound("There is no mount point at " + path))
        }

      case req @ MOVE -> AsPath(path) =>
        ref.read.flatMap { case (cfg, _) =>
          (mountings.get(cfg).get(path), req.headers.get(Destination).map(_.value)) match {
            case (Some(mounting), Some(newPath)) =>
              mounting.validate(Path(newPath)).fold(
                err => BadRequest(errorBody(err.message, None)),
                κ(for {
                  newMnt <- (Path(newPath), mounting).point[Task]
                  r      <- setMounts(mountings.get(cfg) - path + newMnt, ref).run
                  resp   <- r.as(Ok("moved " + path + " to " + newPath)) valueOr handleEnvironmentError
                } yield resp))

            case (None, _) =>
              NotFound(errorBody("There is no mount point at " + path, None))

            case (_, None) =>
              DestinationHeaderMustExist
          }
        }

      case req @ POST -> AsPath(path) =>
        req.headers.get(FileName) map { nh =>
          val newPath = path ++ Path(nh.value)
          ref.read.flatMap { case (cfg, _) =>
            ensureNoOverlaps(cfg, newPath)
              .getOrElse(respond(addPath(newPath, req) as ("added " + newPath)))
          }
        } getOrElse FileNameHeaderMustExist

      case req @ PUT -> AsPath(path) =>
        ref.read flatMap { case (cfg, _) =>
          (if (mountings.get(cfg) contains path) None else ensureNoOverlaps(cfg, path))
            .getOrElse(respond(addPath(path, req).map(_.fold("updated", "added") + " " + path)))
        }

      case DELETE -> AsPath(path) =>
        ref.read.flatMap { case (cfg, _) =>
          if (mountings.get(cfg) contains path)
            setMounts(mountings.get(cfg) - path, ref)
              .as(Ok("deleted " + path))
              .valueOr(handleEnvironmentError)
              .join
          else
            NotFound()
        }
    }
  }

  def metadataService(backend: Task[Backend]) = HttpService {
    case GET -> AsPath(path) => backend flatMap { bknd =>
      bknd.exists(path).fold(
        handlePathError,
        x =>
          if (!x) NotFound(errorBody("There is no file/directory at " + path, None))
          else if (path.pureDir)
            bknd.ls(path).fold(
              handlePathError,
              // NB: we sort only for deterministic results, since JSON lacks `Set`
              paths => Ok(Json("children" := paths.toList.sorted))).join
          else Ok(Json.obj())).join
    }
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

  def handleEnvironmentError(error: EnvironmentError): Task[Response] =
    BadRequest(EncodeJson.of[EnvironmentError].encode(error))

  // NB: EntityDecoders handle media types but not streaming, so the entire body is
  // parsed at once.
  implicit val dataDecoder: EntityDecoder[(List[WriteError], List[Data])] = {
    import MessageFormat._

    val csv: EntityDecoder[(List[WriteError], List[Data])] = EntityDecoder.decodeBy(Csv.mediaType) { msg =>
      val t = EntityDecoder.decodeString(msg).map { body =>
        import scalaz.std.option._
        import quasar.repl.Prettify

        CsvDetect.parse(body).fold(
          err => List(WriteError(Data.Str("parse error: " + err), None)) -> Nil,
          lines => lines.headOption.map { header =>
            val paths = header.fold(κ(Nil), _.fields.map(Prettify.Path.parse(_).toOption))
            val rows = lines.drop(1).map(_.bimap(
                err => WriteError(Data.Str("parse error: " + err), None),
                rec => {
                  val pairs = (paths zip rec.fields.map(Prettify.parse))
                  val good = pairs.map { case (p, s) => (p |@| s).tupled }.foldMap(_.toList)
                  Prettify.unflatten(good.toListMap)
                }
              )).toList
            unzipDisj(rows)
          }.getOrElse(List(WriteError(Data.Obj(ListMap()), Some("no CSV header in body"))) -> Nil))
      }
      DecodeResult.success(t)
    }
    def json(format: JsonFormat): EntityDecoder[(List[WriteError], List[Data])] = EntityDecoder.decodeBy(format.mediaType) { msg =>
      implicit val codec = format.mode.codec
      val t = EntityDecoder.decodeString(msg).map { body =>
        if (format.mediaType.satisfies(MediaType.`application/json`)) {
          DataCodec.parse(body).fold(
            err => (List(WriteError(Data.Str("parse error: " + err.message), None)), List()),
            data => (List(),List(data))
          )
        }
        else {
          unzipDisj(
            body.split("\n").map(line => DataCodec.parse(line).leftMap(
              e => WriteError(Data.Str("parse error: " + line), Some(e.message))
            )).toList
          )
        }
      }
      DecodeResult.success(t)
    }
    csv orElse
      json(MessageFormat.JsonStream.Readable) orElse
      json(MessageFormat.JsonStream.Precise) orElse
      json(MessageFormat.JsonArray.Readable) orElse
      json(MessageFormat.JsonArray.Precise) orElse EntityDecoder.error(MessageFormat.UnsupportedContentType)
  }

  def handleMissingContentType(response: Task[Response]) =
    response.handleWith{ case MessageFormat.UnsupportedContentType =>
      UnsupportedMediaType("Media-Type is missing")
        .withContentType(Some(`Content-Type`(MediaType.`text/plain`)))
    }

  def dataService(backend: Task[Backend]) = HttpService {
    case req @ GET -> AsPath(path) :? Offset(offset0) +& Limit(limit) =>
      val offset = offset0.getOrElse(0L)
      val accept = req.headers.get(Accept)

      def scan(p: Path, b: Backend) =
        b.scan(p, offset, limit).translate[ProcessingTask](convertError(PResultError(_)))

      if (path.pureDir) {
        backend.flatMap(bknd => bknd.lsAll(path).fold(
          handlePathError,
          ns => {
            val bytes = Zip.zipFiles(ns.toList.map { n =>
              val (_, lines, _) = responseLines(accept, scan(path ++ n.path, bknd))
              (n.path, lines.map(str => ByteVector.view(str.getBytes(StandardCharsets.UTF_8))))
            })
            val ct = `Content-Type`(MediaType.`application/zip`)
            val disp = MessageFormat.fromAccept(accept).disposition
            linesResponse(bytes).map(_.replaceAllHeaders((ct :: disp.toList): _*))
          }).join)
      } else {
        backend.flatMap(bknd => responseStream(accept, scan(path, bknd)))
      }

    case req @ PUT -> AsPath(path) =>
      handleMissingContentType {
        req.decode[(List[WriteError], List[Data])] { case (errors, rows) =>
          backend.flatMap(bknd =>
            responseForUpload(errors, bknd.save(path, Process.emitAll(rows)) map (_.right)))
        }
      }

    case req @ POST -> AsPath(path) =>
      handleMissingContentType {
        req.decode[(List[WriteError], List[Data])] { case (errors, rows) =>
          backend.flatMap(bknd =>
            responseForUpload(errors,
              bknd.append(path, Process.emitAll(rows))
                .runLog
                .bimap(PPathError(_), x => if (x.isEmpty) ().right else x.toList.left)))
        }
      }

    case req @ MOVE -> AsPath(path) =>
      req.headers.get(Destination).fold(
        DestinationHeaderMustExist)(
        x => Path(x.value).from(path.dirOf).fold(
          handlePathError,
          p => backend.liftM[PathErrT]
            .flatMap(_.move(path, p, FailIfExists))
            .run.flatMap(_.as(Created("")) valueOr handlePathError)))

    case DELETE -> AsPath(path) =>
      backend.liftM[PathErrT]
        .flatMap(_.delete(path))
        .run.flatMap(_.as(Ok("")) valueOr handlePathError)
  }

  val welcomeService = {
    def resource(path: String): Task[String] = Task.delay {
      scala.io.Source.fromInputStream(getClass.getResourceAsStream(path), "UTF-8").getLines.toList.mkString("\n")
    }

    HttpService {
      case GET -> Root =>
        resource("/quasar/api/index.html").flatMap { html =>
          Ok(html
            .replaceAll("__version__", quasar.build.BuildInfo.version))
            .withContentType(Some(`Content-Type`(MediaType.`text/html`)))
        }

      case GET -> Root / path =>
        StaticFile.fromResource("/quasar/api/" + path).fold(NotFound())(Task.now)
    }
  }

  def cors(svc: HttpService): HttpService = CORS(
    svc,
    middleware.CORSConfig(
      anyOrigin = true,
      allowCredentials = false,
      maxAge = 20.days.toSeconds,
      allowedMethods = Some(Set("GET", "PUT", "POST", "DELETE", "MOVE", "OPTIONS")),
      allowedHeaders = Some(Set(Destination.name.value)))) // NB: actually needed for POST only

  def AllServices =
    createBackend(initialConfig)
      .flatMap(bknd => TaskRef((initialConfig, bknd)).liftM[EnvErrT])
      .map { ref =>
        val cfg = ref.read.map(_._1)
        val bknd = ref.read.map(_._2)

        ListMap(
          "/compile/fs"  -> compileService(bknd),
          "/data/fs"     -> dataService(bknd),
          "/metadata/fs" -> metadataService(bknd),
          "/mount/fs"    -> mountService(ref),
          "/query/fs"    -> queryService(bknd),
          "/server"      -> serverService(cfg),
          "/welcome"     -> welcomeService
        ) ∘ { svc =>
          cors(GZip(HeaderParam(svc.orElse {
            HttpService {
              case req if req.method == OPTIONS => Ok()
            }
          })))
        }
      }
}
