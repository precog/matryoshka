package quasar.api

import quasar.Predef._
import quasar.recursionschemes.Fix
import quasar.fp._
import quasar._, Backend._, Evaluator._
import quasar.config._
import quasar.fs._, Path._
import quasar.specs2._

import scala.concurrent.duration._
import scala.collection.mutable

import argonaut._, Argonaut._
import com.ning.http.client.Response
import dispatch._
import org.specs2.mutable._
import scalaz._, Scalaz._
import scalaz.concurrent._
import scalaz.stream._

import Utils._

object Utils {

  def tester(cfg: BackendConfig) = Errors.liftE[quasar.Evaluator.EnvironmentError](Task.now(()))

  /**
   * Start a server, with the given backend, execute something, and then tear
   * down the server.
   */
  def withServer[A](backend: Backend, config: Config)(body: Req => A): A =
    withServer(_ => backend.point[EnvTask], config)(body)

  def withServer[A](createBackend: Config => EnvTask[Backend], config: Config)(body: Req => A): A =
    withServerRecordConfigChange(createBackend, config)((req, _) => body(req))

  /**
   * Start a server with the given backend function and initial config, execute something,
   * and then tear down the server.
   *
   * The body receives an accessor function that, when called, returns the list of
   * configs asked to be reloaded since the server started.
   */
  def withServerRecordConfigChange[A](createBackend: Config => EnvTask[Backend], config: Config)(body: (Req, () => List[Config]) => A): A = {
    import shapeless._
    // TODO: Extend specs2 to understand Task and avoid all the runs in this implementation. See SD-945
    val port = Server.anyAvailablePort.run
    val client = dispatch.host("localhost", port)
    val reloads = mutable.ListBuffer[Config]()
    def recordConfigChange(cfg: Config) = Task.delay { ignore(reloads += cfg) }

    val updatedConfig = (lens[Config] >> 'server >> 'port0).set(config)(Some(port))
    def unexpectedRestart(config: Config) = Task.fail(new java.lang.AssertionError("Did not expect the server to be restarted with this config: " + config))
    val api = FileSystemApi(".", updatedConfig, createBackend, tester,
                            restartServer = unexpectedRestart,
                            configChanged = recordConfigChange)
    val srv = Server.createServer(port, 1.seconds, api).run.run
    try { body(client, () => reloads.toList) } finally { srv.traverse_(_.shutdown.void).run }
  }

  /** Handler for response bodies containing newline-separated JSON documents, for use with Dispatch. */
  object asJson extends (Response => String \/ (String, List[Json])) {
    private def sequenceStrs[A](vs: scala.collection.Seq[String \/ A]): String \/ List[A] =
      vs.toList.map(_.validation.toValidationNel).sequenceU.leftMap(_.list.mkString("; ")).disjunction

    private def parseJsonLines(str: String): String \/ List[Json] =
      if (str ≟ "") \/-(Nil)
      else sequenceStrs(str.split("\n").map(Parse.parse(_)))

    def apply(r: Response) =
      (dispatch.as.String andThen parseJsonLines)(r).map((r.getContentType, _))
  }


  val jsonContentType = "application/json"

  val preciseContentType = "application/ldjson; mode=\"precise\"; charset=UTF-8"
  val readableContentType = "application/ldjson; mode=\"readable\"; charset=UTF-8"
  val arrayContentType = "application/json; mode=\"readable\"; charset=UTF-8"
  val csvContentType = "text/csv"
  val charsetParam = "; charset=UTF-8"
  val csvResponseContentType = csvContentType + "; columnDelimiter=\",\"; rowDelimiter=\"\\\\r\\\\n\"; quoteChar=\"\\\"\"; escapeChar=\"\\\"\"" + charsetParam
}

object Mock {

  sealed trait Action
  object Action {
    final case class Save(path: Path, rows: List[Data]) extends Action
    final case class Append(path: Path, rows: List[Data]) extends Action
  }

  implicit val PlanRenderTree = new RenderTree[Plan] {
    def render(v: Plan) = Terminal(List("Stub.Plan"), None)
  }

  case class Plan(description: String)

  object JournaledBackend {
    def apply(files: Map[Path, Process[Task, Data]]): Backend =
      new JournaledBackend(files)
  }

  /**
   * A mock backend that records the actions taken on it and exposes this through the mutable `actions` buffer
   */
  class JournaledBackend(files: Map[Path, Process[Task, Data]]) extends PlannerBackend[Plan] {

    private val pastActions = scala.collection.mutable.ListBuffer[Action]()

    val planner = new Planner[Plan] {
      def plan(logical: Fix[LogicalPlan]) = Planner.emit(Vector.empty, \/-(Plan("logical: " + logical.toString)))
    }
    val evaluator = new Evaluator[Plan] {
      val name = "Stub"
      def execute(physical: Plan) =
        EitherT.right(Task.now(ResultPath.Temp(Path("tmp/out"))))
      def compile(physical: Plan) = "Stub" -> Cord(physical.toString)
    }
    val RP = PlanRenderTree

    def scan0(path: Path, offset: Long, limit: Option[Long]) =
      files.get(path).fold(
        Process.eval[Backend.ResTask, Data](EitherT.left(Task.now(Backend.ResultPathError(NonexistentPathError(path, Some("no backend"))))))) { p =>
        val limited = p.drop(offset.toInt).take(limit.fold(Int.MaxValue)(_.toInt))
        limited.translate(liftP).translate[Backend.ResTask](Errors.convertError(Backend.ResultPathError(_)))
      }
    def count0(path: Path) =
      EitherT[Task, PathError, Long](files.get(path).fold[Task[PathError \/ Long]](Task.now(-\/(NonexistentPathError(path, Some("no backend"))))) { p =>
          p.map(κ(1)).sum.runLast.map(n => \/-(n.get))
        })

    def save0(path: Path, values: Process[Task, Data]) =
      if (path.pathname.contains("pathError"))
        EitherT.left(Task.now(PPathError(InvalidPathError("simulated (client) error"))))
      else if (path.pathname.contains("valueError"))
        EitherT.left(Task.now(PWriteError(WriteError(Data.Str(""), Some("simulated (value) error")))))
      else Errors.liftE[ProcessingError](values.runLog.map { rows =>
          pastActions += Action.Save(path, rows.toList)
          ()
        })

    def append0(path: Path, values: Process[Task, Data]) =
      if (path.pathname.contains("pathError"))
        Process.eval[Backend.PathTask, WriteError](EitherT.left(Task.now(InvalidPathError("simulated (client) error"))))
      else if (path.pathname.contains("valueError"))
        Process.eval(WriteError(Data.Str(""), Some("simulated (value) error")).point[Backend.PathTask])
      else Process.eval_(Backend.liftP(values.runLog.map { rows =>
        pastActions += Action.Append(path, rows.toList)
        ()
      }))

    def delete0(path: Path) = ().point[Backend.PathTask]

    def move0(src: Path, dst: Path, semantics: Backend.MoveSemantics) = ().point[Backend.PathTask]

    def ls0(dir: Path): Backend.PathTask[Set[Backend.FilesystemNode]] = {
      val children = files.keys.toList.map(_.rebase(dir).toOption.map(p => Backend.FilesystemNode(p.head, Backend.Plain))).flatten
      children.toSet.point[Backend.PathTask]
    }

    def defaultPath = Path.Current

    def actions: List[Action] = pastActions.toList
  }

  def simpleFiles(files: Map[Path, List[Data]]): Map[Path, Process[Task, Data]] =
    files ∘ { ds => Process.emitAll(ds) }

  val emptyBackend = JournaledBackend(ListMap())
}

class ApiSpecs extends Specification with DisjunctionMatchers with PendingWithAccurateCoverage with org.specs2.time.NoTimeConversions {
  sequential  // The tests around restarting the server do not pass if run in parallel.
              // TODO: Explore why that is and/or find a way to extract them so the rest of the tests can run in parallel
  args.report(showtimes = true)

  def mounter(backend: Backend)(cfg: Config) = Errors.liftE[quasar.Evaluator.EnvironmentError](Task.now(backend))

  def asLines(r: Response): (String, List[String]) = (r.getContentType, dispatch.as.String(r).split("\r\n").toList)


  /** Handlers for use with Dispatch. */
  val code: Response => Int = _.getStatusCode
  def header(name: String): Response => Option[String] = r => Option(r.getHeader(name))
  def commaSep: Option[String] => List[String] = _.fold(List[String]())(_.split(", ").toList)

  def errorFromBody(resp: Response): String \/ String = {
    val mt = resp.getContentType.split(";").head
    (for {
      _      <- if (mt ≟ "application/json" || mt ≟ "application/ldjson") \/-(())
                else -\/("bad content-type: " + mt + " (body: " + resp.getResponseBody + ")")
      json   <- Parse.parse(resp.getResponseBody)
      err    <- json.field("error") \/> ("`error` missing: " + json)
      errStr <- err.string \/> ("`error` not a string: " + err)
    } yield errStr)
  }

  /**
   * Mounts a backend without any data at each of the mount points described in
   * the [[Config]].
   */
  val backendForConfig: Config => EnvTask[Backend] = {
    val emptyFiles = Map.empty.withDefault((_: Path) => Process.halt)
    val bdefn = BackendDefinition(_ => Mock.JournaledBackend(emptyFiles).point[EnvTask])
    Mounter.mount(_, bdefn)
  }

  val files1 = ListMap(
    Path("bar") -> List(
      Data.Obj(ListMap("a" -> Data.Int(1))),
      Data.Obj(ListMap("b" -> Data.Int(2))),
      Data.Obj(ListMap("c" -> Data.Set(List(Data.Int(3)))))),
    Path("dir/baz") -> List(),
    Path("tmp/out") -> List(Data.Obj(ListMap("0" -> Data.Str("ok")))),
    Path("tmp/dup") -> List(Data.Obj(ListMap("4" -> Data.Str("ok")))),
    Path("a file") -> List(Data.Obj(ListMap("1" -> Data.Str("ok")))),
    Path("quoting") -> List(
      Data.Obj(ListMap(
        "a" -> Data.Str("\"Hey\""),
        "b" -> Data.Str("a, b, c")))),
    Path("empty") -> List())
  val bigFiles = ListMap(
    // Something simple:
    Path("range") -> Process.range(0, 100*1000).map(n => Data.Obj(ListMap("n" -> Data.Int(n)))),
    // A closer analog to what we do in the MongoDB backend:
    Path("resource") -> {
      class Count { var n = 0 }
      val acquire = Task.delay { new Count }
      def release(r: Count) = Task.now(())
      def step(r: Count) = Task.delay {
        if (r.n < 100*1000) {
          r.n += 1
          Data.Obj(ListMap("n" -> Data.Int(r.n)))
        }
        else throw Cause.End.asThrowable
      }
      scalaz.stream.io.resource(acquire)(release)(step)
    })
  val noBackends = NestedBackend(Map())
  val backends1 = createBackendsFromMock(Mock.JournaledBackend(bigFiles), Mock.JournaledBackend(Mock.simpleFiles(files1)))

  def createBackendsFromMock(large: Backend, normal: Backend) =
    NestedBackend(ListMap(
      DirNode("empty") -> Mock.emptyBackend,
      DirNode("foo") -> normal,
      DirNode("non") -> NestedBackend(ListMap(
        DirNode("root") -> NestedBackend(ListMap(
          DirNode("mounting") -> normal)))),
      DirNode("large") -> large,
      DirNode("badPath1") -> Mock.emptyBackend,
      DirNode("badPath2") -> Mock.emptyBackend))
  // We don't put the port here as the `withServer` function will supply the port based on it's optional input.
  val config1 = Config(SDServerConfig(None), ListMap(
    Path("/foo/") -> MongoDbConfig("mongodb://localhost/foo"),
    Path("/non/root/mounting/") -> MongoDbConfig("mongodb://localhost/mounting")))

  val corsMethods = header("Access-Control-Allow-Methods") andThen commaSep
  val corsHeaders = header("Access-Control-Allow-Headers") andThen commaSep

  def mockTest[A](body: (Mock.JournaledBackend, Req) => A) = {
    val mock = new Mock.JournaledBackend(Mock.simpleFiles(files1))
    val backends = createBackendsFromMock(Mock.JournaledBackend(bigFiles), mock)
    withServer(backends, config1) { client =>
      body(mock, client)
    }
  }

  "OPTIONS" should {

    "advertise GET and POST for /query path" in {
      withServer(noBackends, config1) { client =>
        val methods = Http(client.OPTIONS / "query" / "fs" / "" > corsMethods)

        methods() must contain(allOf("GET", "POST"))
      }
    }

    "advertise Destination header for /query path and method POST" in {
      withServer(noBackends, config1) { client =>
        val headers = Http((client.OPTIONS / "query" / "fs" / "").setHeader("Access-Control-Request-Method", "POST") > corsHeaders)

        headers() must contain(allOf("Destination"))
      }
    }

    "advertise GET, PUT, POST, DELETE, and MOVE for /data path" in {
      withServer(noBackends, config1) { client =>
        val methods = Http(client.OPTIONS / "data" / "fs" / "" > corsMethods)

        methods() must contain(allOf("GET", "PUT", "POST", "DELETE", "MOVE"))
      }
    }

    "advertise Destination header for /data path and method MOVE" in {
      withServer(noBackends, config1) { client =>
        val headers = Http((client.OPTIONS / "data" / "fs" / "").setHeader("Access-Control-Request-Method", "MOVE") > corsHeaders)

        headers() must contain(allOf("Destination"))
      }
    }
  }

  "/metadata/fs" should {
    def metadata(client: Req): Req = client / "metadata" / "fs" / ""  // Note: trailing slash required

    "return no filesystems" in {
      withServer(noBackends, config1) { client =>
        val meta = Http(metadata(client) OK asJson)

        meta() must beRightDisjunction((jsonContentType, List(Json("children" := List[Json]()))))
      }
    }

    "be 404 with missing backend" in {
      withServer(noBackends, config1) { client =>
        val req = metadata(client) / "missing"
        val meta = Http(req)

        val resp = meta()
        resp.getStatusCode must_== 404
        resp.getResponseBody must_== ""
      }
    }

    "return empty for null fs" in {
      withServer(backends1, config1) { client =>
        val path = metadata(client) / "empty" / ""
        val meta = Http(path OK asJson)

        meta() must beRightDisjunction((jsonContentType, List(Json("children" := List[Json]()))))
      }
    }

    "return empty for missing path" in {
      withServer(backends1, config1) { client =>
        val path = metadata(client) / "foo" / "baz" / ""
        val meta = Http(path OK asJson)

        meta() must beRightDisjunction((jsonContentType, List(Json("children" := List[Json]()))))
      }
    }

    "find stubbed filesystems" in {
      withServer(backends1, config1) { client =>
        val meta = Http(metadata(client) OK asJson)

        meta() must beRightDisjunction((
          jsonContentType,
          List(
            Json("children" := List(
              Json("name" := "badPath1", "type" := "mount"),
              Json("name" := "badPath2", "type" := "mount"),
              Json("name" := "empty",    "type" := "mount"),
              Json("name" := "foo",      "type" := "mount"),
              Json("name" := "large",    "type" := "mount"),
              Json("name" := "non",      "type" := "directory"))))))
      }
    }

    "find stubbed files" in {
      withServer(backends1, config1) { client =>
        val path = metadata(client) / "foo" / ""
        val meta = Http(path OK asJson)

        meta() must beRightDisjunction((
          jsonContentType,
          List(
            Json("children" := List(
              Json("name" := "a file",  "type" := "file"),
              Json("name" := "bar",     "type" := "file"),
              Json("name" := "dir",     "type" := "directory"),
              Json("name" := "empty",   "type" := "file"),
              Json("name" := "quoting", "type" := "file"),
              Json("name" := "tmp",     "type" := "directory"))))))
      }
    }

    "find intermediate directory" in {
      withServer(backends1, config1) { client =>
        val path = metadata(client) / "non" / ""
        val meta = Http(path OK asJson)

        meta() must beRightDisjunction((
          jsonContentType,
          List(
            Json("children" := List(
              Json("name" := "root", "type" := "directory"))))))
      }
    }

    "find nested mount" in {
      withServer(backends1, config1) { client =>
        val path = metadata(client) / "non" / "root" / ""
        val meta = Http(path OK asJson)

        meta() must beRightDisjunction((
          jsonContentType,
          List(
            Json("children" := List(
              Json("name" := "mounting", "type" := "mount"))))))
      }
    }

    "be 404 for file with same name as existing directory (minus the trailing slash)" in {
      withServer(backends1, config1) { client =>
        val req = metadata(client) / "foo"
        val meta = Http(req)

        val resp = meta()

        resp.getStatusCode must_== 404
        resp.getResponseBody must_== ""
      }
    }

    "be empty for file" in {
      withServer(backends1, config1) { client =>
        val path = metadata(client) / "foo" / "bar"
        val meta = Http(path OK asJson)

        meta() must beRightDisjunction((
          jsonContentType,
          List(
            Json())))
      }
    }

    "also contain CORS headers" in {
      withServer(noBackends, config1) { client =>
        val methods = Http(metadata(client) > corsMethods)

        methods() must contain(allOf("GET", "POST"))
      }
    }
  }

  "/data/fs" should {
    def data(client: Req) = client / "data" / "fs" / ""

    "GET" should {
      "be 404 for missing backend" in {
        withServer(noBackends, config1) { client =>
          val req = data(client) / "missing"
          val meta = Http(req)

          val resp = meta()
          resp.getStatusCode must_== 404
          errorFromBody(resp) must_== \/-("./missing: doesn't exist")
        }
      }

      "be 404 for missing file" in {
        withServer(backends1, config1) { client =>
          val req = data(client) / "empty" / "anything"
          val meta = Http(req)

          val resp = meta()
          resp.getStatusCode must_== 404
          errorFromBody(resp) must_== \/-("./anything: no backend")
        }
      }

      "read entire file readably by default" in {
        withServer(backends1, config1) { client =>
          val path = data(client) / "foo" / "bar"
          val meta = Http(path OK asJson)

          meta() must beRightDisjunction((
            readableContentType,
            List(Json("a" := 1), Json("b" := 2), Json("c" := List(3)))))
        }
      }

      "read empty file" in {
        withServer(backends1, config1) { client =>
          val path = data(client) / "foo" / "empty"
          val meta = Http(path OK asJson)

          meta() must beRightDisjunction((
            readableContentType,
            List()))
        }
      }

      "read entire file precisely when specified" in {
        withServer(backends1, config1) { client =>
          val path = data(client) / "foo" / "bar"
          val meta = Http(path.setHeader("Accept", "application/ldjson;mode=precise") OK asJson)

          meta() must beRightDisjunction((
            preciseContentType,
            List(Json("a" := 1), Json("b" := 2), Json("c" := Json("$set" := List(3))))))
        }
      }

      "read entire file precisely when specified in request-headers" in {
        withServer(backends1, config1) { client =>
          val req = data(client) / "foo" / "bar" <<? Map("request-headers" -> """{"Accept": "application/ldjson; mode=precise" }""")
          val meta = Http(req OK asJson)

          meta() must beRightDisjunction((
            preciseContentType,
            List(Json("a" := 1), Json("b" := 2), Json("c" := Json("$set" := List(3))))))
        }
      }

      "read entire file precisely with complicated Accept" in {
        withServer(backends1, config1) { client =>
          val path = data(client) / "foo" / "bar"
          val meta = Http(path.setHeader("Accept", "application/ldjson;q=0.9;mode=readable,application/json;boundary=NL;mode=precise") OK asJson)

          meta() must beRightDisjunction((
            preciseContentType,
            List(Json("a" := 1), Json("b" := 2), Json("c" := Json("$set" := List(3))))))
        }
      }

      "read entire file in JSON array when specified" in {
        withServer(backends1, config1) { client =>
          val req = (data(client) / "foo" / "bar").setHeader("Accept", "application/json")
          val meta = Http(req OK as.String)

          meta() must_==
            """[
               |{ "a": 1 },
               |{ "b": 2 },
               |{ "c": [ 3 ] }
               |]
               |""".stripMargin.replace("\n", "\r\n")
        }
      }

      "read entire file with gzip encoding" in {
        withServer(backends1, config1) { client =>
          val req = (data(client) / "foo" / "bar").setHeader("Accept-Encoding", "gzip")
          val meta = Http(req)

          val resp = meta()
          resp.getStatusCode must_== 200
          resp.getHeader("Content-Encoding") must_== "gzip"
        }
      }

      "read entire file (with space)" in {
        withServer(backends1, config1) { client =>
          val path = data(client) / "foo" / "a file"
          val meta = Http(path OK asJson)

          meta() must beRightDisjunction((readableContentType, List(Json("1" := "ok"))))
        }
      }

      "read entire file as CSV" in {
        withServer(backends1, config1) { client =>
          val path = data(client) / "foo" / "bar"
          val meta = Http(path.setHeader("Accept", csvContentType) OK asLines)

          meta() must_==
            csvResponseContentType ->
            List("a,b,c[0]", "1,,", ",2,", ",,3")
        }
      }

      "read entire file as CSV with quoting" in {
        withServer(backends1, config1) { client =>
          val path = data(client) / "foo" / "quoting"
          val meta = Http(path.setHeader("Accept", csvContentType) OK asLines)

          meta() must_==
            csvResponseContentType ->
            List("a,b", "\"\"\"Hey\"\"\",\"a, b, c\"")
        }
      }

      "read entire file as CSV with alternative delimiters" in {
        val mt = List(
          csvContentType,
          "columnDelimiter=\"\t\"",
          "rowDelimiter=\";\"",
          "quoteChar=\"'\"",  // NB: probably doesn't need quoting, but http4s renders it that way
          "escapeChar=\"\\\\\"").mkString("; ")

        withServer(backends1, config1) { client =>
          val req = (data(client) / "foo" / "bar")
                      .setHeader("Accept", mt)
          val meta = Http(req OK asLines)

          meta() must_==
            mt + charsetParam ->
            List("a\tb\tc[0];1\t\t;\t2\t;\t\t3;")
        }
      }

      "read entire file as CSV with standard delimiters specified" in {
        val mt = List(
          csvContentType,
          "columnDelimiter=\",\"",
          "rowDelimiter=\"\\\\r\\\\n\"",
          "quoteChar=\"\"",
          "escapeChar=\"\\\"\"").mkString("; ")

        withServer(backends1, config1) { client =>
          val req = (data(client) / "foo" / "bar")
                      .setHeader("Accept", mt)
          val meta = Http(req OK asLines)

          meta() must_==
            csvResponseContentType ->
            List("a,b,c[0]", "1,,", ",2,", ",,3")
        }
      }

      "read with disposition" in {
        withServer(backends1, config1) { client =>
          val req = (data(client) / "foo" / "bar")
                    .setHeader("Accept", "application/ldjson; disposition=\"attachment; filename=data.json\"")
          val meta = Http(req)

          val resp = meta()
          resp.getHeader("Content-Disposition") must_== "attachment; filename=\"data.json\""
        }
      }

      "read partial file with offset and limit" in {
        withServer(backends1, config1) { client =>
          val path = data(client) / "foo" / "bar" <<? Map("offset" -> "1", "limit" -> "1")
          val meta = Http(path OK asJson)

          meta() must beRightDisjunction((
            readableContentType,
            List(Json("b" := 2))))
        }
      }

      "download zipped directory" in {
        withServer(backends1, config1) { client =>
          val req = (data(client) / "foo" / "")
                    .setHeader("Accept", "text/csv; disposition=\"attachment; filename=foo.zip\"")
          val meta = Http(req)

          val resp = meta()
          resp.getStatusCode must_== 200
          resp.getHeader("Content-Type") must_== "application/zip"
          resp.getHeader("Content-Disposition") must_== "attachment; filename=\"foo.zip\""
        }
      }

      "be 400 with negative offset" in {
        withServer(backends1, config1) { client =>
          val req = data(client) / "foo" / "bar" <<? Map("offset" -> "-10", "limit" -> "10")
          val meta = Http(req)

          val resp = meta()
          resp.getStatusCode must_== 400
          errorFromBody(resp) must_== \/-("invalid offset: -10 (must be >= 0)")
        }
      }

      "be 400 with negative limit" in {
        withServer(backends1, config1) { client =>
          val req = data(client) / "foo" / "bar" <<? Map("offset" -> "10", "limit" -> "-10")
          val meta = Http(req)

          val resp = meta()
          resp.getStatusCode must_== 400
          errorFromBody(resp) must_== \/-("invalid limit: -10 (must be >= 1)")
        }
      }

      "be 400 with unparsable limit" in {
        withServer(backends1, config1) { client =>
          val req = data(client) / "foo" / "bar" <<? Map("limit" -> "a")
          val meta = Http(req)

          val resp = meta()
          resp.getStatusCode must_== 400
          errorFromBody(resp) must_== \/-("???")
        }
      }.pendingUntilFixed("#773")

      def count(is: java.io.InputStream): Int = {
        def loop(acc: Int): Int = {
          if (is.read() < 0) acc else loop(acc+1)
        }
        loop(0)
      }

      def countLines(is: java.io.InputStream): Int = {
        def loop(acc: Int): Int = {
          val c = is.read()
          if (c < 0) acc
          else if (c ≟ '\n') loop(acc + 1)
          else loop(acc)
        }
        loop(0)
      }

      "read very large data (generated with Process.range)" in {
        withServer(backends1, config1) { client =>
          val req = data(client) / "large" / "range"
          val meta = Http(req)

          val resp = meta()
          countLines(resp.getResponseBodyAsStream) must_== 100*1000
        }
      }

      "read very large data (generated with Process.range, gzipped)" in {
        withServer(backends1, config1) { client =>
          val req = (data(client) / "large" / "range").setHeader("Accept-Encoding", "gzip")
          val meta = Http(req)

          val resp = meta()
          count(resp.getResponseBodyAsStream) must beBetween(200*1000, 250*1000)
        }
      }

      "read very large data (generated with Process.resource)" in {
        withServer(backends1, config1) { client =>
          val req = (data(client) / "large" / "resource")
          val meta = Http(req)

          val resp = meta()
          countLines(resp.getResponseBodyAsStream) must_== 100*1000
        }
      }

      "read very large data (generated with Process.resource, gzipped)" in {
        withServer(backends1, config1) { client =>
          val req = (data(client) / "large" / "resource").setHeader("Accept-Encoding", "gzip")
          val meta = Http(req)

          val resp = meta()
          count(resp.getResponseBodyAsStream) must beBetween(200*1000, 250*1000)
        }
      }
    }

    "PUT" should {
      "be 404 for missing backend" in {
        withServer(noBackends, config1) { client =>
          val req = (data(client) / "missing").PUT.setBody("{\"a\": 1}\n{\"b\": 2}")
          val meta = Http(req)

          val resp = meta()
          resp.getStatusCode must_== 404
          errorFromBody(resp) must_== \/-("./missing: doesn't exist")
        }
      }

      "be 400 with no body" in {
        withServer(backends1, config1) { client =>
          val req = (data(client) / "foo" / "bar").PUT
          val meta = Http(req)

          val resp = meta()
          resp.getStatusCode must_== 400
          errorFromBody(resp) must_== \/-("some uploaded value(s) could not be processed")
        }
      }

      "be 400 with invalid JSON" in {
        withServer(backends1, config1) { client =>
          val req = (data(client) / "foo" / "bar").PUT
                    .setBody("{")
          val meta = Http(req)

          val resp = meta()
          resp.getStatusCode must_== 400
          errorFromBody(resp) must_== \/-("some uploaded value(s) could not be processed")
        }
      }

      "accept valid (Precise) JSON" in {
        mockTest { (mock, client) =>
          val path = data(client) / "foo" / "bar"
          val meta = Http(path.PUT.setBody("{\"a\": 1}\n{\"b\": \"12:34:56\"}") OK as.String)

          meta() must_== ""
          mock.actions must_== List(
            Mock.Action.Save(
              Path("./bar"),
              List(
                Data.Obj(ListMap("a" -> Data.Int(1))),
                Data.Obj(ListMap("b" -> Data.Str("12:34:56"))))))
        }
      }

      "accept valid (Readable) JSON" in {
        mockTest { (mock, client) =>
          val path = (data(client) / "foo" / "bar").setHeader("Content-Type", readableContentType)
          val meta = Http(path.PUT.setBody("{\"a\": 1}\n{\"b\": \"12:34:56\"}") OK as.String)

          meta() must_== ""
          mock.actions must_== List(
            Mock.Action.Save(
              Path("./bar"),
              List(
                Data.Obj(ListMap("a" -> Data.Int(1))),
                Data.Obj(ListMap("b" -> Data.Time(org.threeten.bp.LocalTime.parse("12:34:56")))))))
        }
      }

      "accept valid (standard) CSV" in {
        mockTest { (mock, client) =>
          val req = (data(client) / "foo" / "bar").PUT
            .setHeader("Content-Type", csvContentType)
            .setBody("a,b\n1,\n,12:34:56")
          val meta = Http(req OK as.String)

          meta() must_== ""
          mock.actions must_== List(
            Mock.Action.Save(
              Path("./bar"),
              List(
                Data.Obj(ListMap("a" -> Data.Int(1))),
                Data.Obj(ListMap("b" -> Data.Time(org.threeten.bp.LocalTime.parse("12:34:56")))))))
        }
      }

      "accept valid (weird) CSV" in {
        mockTest { (mock, client) =>
          val req = (data(client) / "foo" / "bar").PUT
            .setHeader("Content-Type", csvContentType)
            .setBody("a|b\n1|\n|'[1|2|3]'\n")
          val meta = Http(req OK as.String)

          meta() must_== ""
          mock.actions must_== List(
            Mock.Action.Save(
              Path("./bar"),
              List(
                Data.Obj(ListMap("a" -> Data.Int(1))),
                Data.Obj(ListMap("b" -> Data.Str("[1|2|3]"))))))
        }
      }

      "be 400 with empty CSV (no headers)" in {
        mockTest { (mock, client) =>
          val req = (data(client) / "foo" / "bar").PUT
            .setHeader("Content-Type", csvContentType)
            .setBody("")
          val meta = Http(req)

          val resp = meta()
          resp.getStatusCode must_== 400
          errorFromBody(resp) must_== \/-("some uploaded value(s) could not be processed")
          mock.actions must_== Nil
        }
      }

      "be 400 with broken CSV (after the tenth data line)" in {
        mockTest { (mock, client) =>
          val req = (data(client) / "foo" / "bar").PUT
            .setHeader("Content-Type", csvContentType)
            .setBody("\"a\",\"b\"\n1,2\n3,4\n5,6\n7,8\n9,10\n11,12\n13,14\n15,16\n17,18\n19,20\n\",\n") // NB: missing quote char _after_ the tenth data row
          val meta = Http(req)

          val resp = meta()
          resp.getStatusCode must_== 400
          errorFromBody(resp) must_== \/-("some uploaded value(s) could not be processed")
          mock.actions must_== Nil
        }
      }

      "be 400 with simulated path error" in {
        withServer(backends1, config1) { client =>
          val req = (data(client) / "foo" / "pathError").PUT
                    .setBody("{\"a\": 1}")
          val meta = Http(req)

          val resp = meta()
          resp.getStatusCode must_== 400
          errorFromBody(resp) must_== \/-("simulated (client) error")
        }
      }

      "be 500 with simulated error on a particular value" in {
        withServer(backends1, config1) { client =>
          val req = (data(client) / "foo" / "valueError").PUT
                    .setBody("{\"a\": 1}")
          val meta = Http(req)

          val resp = meta()
          resp.getStatusCode must_== 500
          errorFromBody(resp) must_== \/-("simulated (value) error; value: Str()")
        }
      }
    }

    "POST" should {
      "be 404 for missing backend" in {
        withServer(noBackends, config1) { client =>
          val req = (data(client) / "missing").POST
                    .setBody("{\"a\": 1}\n{\"b\": 2}")
          val meta = Http(req)

          val resp = meta()
          resp.getStatusCode must_== 404
          errorFromBody(resp) must_== \/-("./missing: doesn't exist")
        }
      }

      "be 400 with no body" in {
        withServer(backends1, config1) { client =>
          val req = (data(client) / "foo" / "bar").POST
          val meta = Http(req)

          val resp = meta()
          resp.getStatusCode must_== 400
          errorFromBody(resp) must_== \/-("some uploaded value(s) could not be processed")
        }
      }

      "be 400 with invalid JSON" in {
        withServer(backends1, config1) { client =>
          val req = (data(client) / "foo" / "bar").POST
                      .setBody("{")
          val meta = Http(req)

          val resp = meta()
          resp.getStatusCode must_== 400
          errorFromBody(resp) must_== \/-("some uploaded value(s) could not be processed")
        }
      }

      "produce two errors with partially invalid JSON" in {
        withServer(backends1, config1) { client =>
          val req = (data(client) / "foo" / "bar").POST.setBody(
            """{"a": 1}
              |"unmatched
              |{"b": 2}
              |}
              |{"c": 3}""".stripMargin)
          val meta = Http(req > asJson)

          meta() must beRightDisjunction { (resp: (String, List[Json])) =>
            val (_, json) = resp
            json.length ≟ 1 &&
            (for {
              obj <- json.head.obj
              errors <- obj("details")
              eArr <- errors.array
            } yield eArr.length ≟ 2).getOrElse(false)
          }
        }
      }

      "accept valid (Precise) JSON" in {
        mockTest { (mock, client) =>
          val req = (data(client) / "foo" / "bar").POST
            .setBody("{\"a\": 1}\n{\"b\": \"12:34:56\"}")
          val meta = Http(req OK as.String)

          meta() must_== ""
          mock.actions must_== List(
            Mock.Action.Append(
              Path("./bar"),
              List(
                Data.Obj(ListMap("a" -> Data.Int(1))),
                Data.Obj(ListMap("b" -> Data.Str("12:34:56"))))))
        }
      }

      "accept valid (Readable) JSON" in {
        mockTest { (mock, client) =>
          val req = (data(client) / "foo" / "bar").POST
            .setHeader("Content-Type", readableContentType)
            .setBody("{\"a\": 1}\n{\"b\": \"12:34:56\"}")
          val meta = Http(req OK as.String)

          meta() must_== ""
          mock.actions must_== List(
            Mock.Action.Append(
              Path("./bar"),
              List(
                Data.Obj(ListMap("a" -> Data.Int(1))),
                Data.Obj(ListMap("b" -> Data.Time(org.threeten.bp.LocalTime.parse("12:34:56")))))))
        }
      }

      "accept valid (standard) CSV" in {
        mockTest { (mock, client) =>
          val req = (data(client) / "foo" / "bar").POST
            .setHeader("Content-Type", csvContentType)
            .setBody("a,b\n1,\n,12:34:56")
          val meta = Http(req OK as.String)

          meta() must_== ""
          mock.actions must_== List(
            Mock.Action.Append(
              Path("./bar"),
              List(
                Data.Obj(ListMap("a" -> Data.Int(1))),
                Data.Obj(ListMap("b" -> Data.Time(org.threeten.bp.LocalTime.parse("12:34:56")))))))
        }
      }

      "accept valid (weird) CSV" in {
        mockTest { (mock, client) =>
          val req = (data(client) / "foo" / "bar").POST
            .setHeader("Content-Type", csvContentType)
            .setBody("a|b\n1|\n|'[1|2|3]'")
          val meta = Http(req OK as.String)

          meta() must_== ""
          mock.actions must_== List(
            Mock.Action.Append(
              Path("./bar"),
              List(
                Data.Obj(ListMap("a" -> Data.Int(1))),
                Data.Obj(ListMap("b" -> Data.Str("[1|2|3]"))))))
        }
      }

      "be 400 with empty CSV (no headers)" in {
        mockTest { (mock, client) =>
          val req = (data(client) / "foo" / "bar").POST
            .setHeader("Content-Type", csvContentType)
            .setBody("")
          val meta = Http(req)

          val resp = meta()
          resp.getStatusCode must_== 400
          errorFromBody(resp) must_== \/-("some uploaded value(s) could not be processed")
          mock.actions must_== Nil
        }
      }

      "be 400 with broken CSV (after the tenth data line)" in {
        mockTest { (mock, client) =>
          val req = (data(client) / "foo" / "bar").POST
            .setHeader("Content-Type", csvContentType)
            .setBody("\"a\",\"b\"\n1,2\n3,4\n5,6\n7,8\n9,10\n11,12\n13,14\n15,16\n17,18\n19,20\n\",\n") // NB: missing quote char _after_ the tenth data row
          val meta = Http(req)

          val resp = meta()
          resp.getStatusCode must_== 400
          errorFromBody(resp) must_== \/-("some uploaded value(s) could not be processed")
          mock.actions must_== Nil
        }
      }

      "be 400 with simulated path error" in {
        withServer(backends1, config1) { client =>
          val req = (data(client) / "foo" / "pathError").POST
                      .setBody("{\"a\": 1}")
          val meta = Http(req)

          val resp = meta()
          resp.getStatusCode must_== 400
          errorFromBody(resp) must_== \/-("simulated (client) error")
        }
      }

      "be 500 with simulated error on a particular value" in {
        withServer(backends1, config1) { client =>
          val req = (data(client) / "foo" / "valueError").POST
                      .setBody("{\"a\": 1}")
          val meta = Http(req)

          val resp = meta()
          resp.getStatusCode must_== 500
          errorFromBody(resp) must_== \/-("some uploaded value(s) could not be processed")
        }
      }
    }

    "MOVE" should {
      def move(client: Req) = data(client).setMethod("MOVE")

      "be 400 for missing src backend" in {
        withServer(noBackends, config1) { client =>
          val req = move(client) / "foo"
          val meta = Http(req)

          val resp = meta()
          resp.getStatusCode must_== 400
          errorFromBody(resp) must_== \/-("The 'Destination' header must be specified")
        }
      }

      "be 404 for missing source file" in {
        withServer(backends1, config1) { client =>
          val req = (move(client) / "missing" / "a" ).setHeader("Destination", "/foo/bar")
          val meta = Http(req)

          val resp = meta()
          resp.getStatusCode must_== 404
          errorFromBody(resp) must_== \/-("./missing/a: doesn't exist")
        }
      }

      "be 404 for missing dst backend" in {
        withServer(backends1, config1) { client =>
          val req = (move(client) / "foo" / "bar").setHeader("Destination", "/missing/a")
          val meta = Http(req)

          val resp = meta()
          resp.getStatusCode must_== 404
          errorFromBody(resp) must_== \/-("./missing/a: doesn't exist")
        }
      }

      "be 201 for file" in {
        withServer(backends1, config1) { client =>
          val req = (move(client) / "foo" / "bar").setHeader("Destination", "/foo/baz")
          val meta = Http(req > code)

          meta() must_== 201
        }
      }

      "be 201 for dir" in {
        withServer(backends1, config1) { client =>
          val req = (move(client) / "foo" / "dir" / "").setHeader("Destination", "/foo/dir2/")
          val meta = Http(req > code)

          meta() must_== 201
        }
      }

      "be 501 for src and dst not in same backend" in {
        withServer(backends1, config1) { client =>
          val req = (move(client) / "foo" / "bar").setHeader("Destination", "/empty/a")
          val meta = Http(req)

          val resp = meta()

          resp.getStatusCode must_== 501
          errorFromBody(resp) must_== \/-("src and dst path not in the same backend")
        }
      }

    }

    "DELETE" should {
      "be 404 for missing backend" in {
        withServer(noBackends, config1) { client =>
          val req = (data(client) / "missing").DELETE
          val meta = Http(req)

          val resp = meta()
          resp.getStatusCode must_== 404
          errorFromBody(resp) must_== \/-("./missing: doesn't exist")
        }
      }

      "be 200 with existing file" in {
        withServer(backends1, config1) { client =>
          val path = data(client) / "foo" / "bar"
          val meta = Http(path.DELETE > code)

          meta() must_== 200
        }
      }

      "be 200 with existing dir" in {
        withServer(backends1, config1) { client =>
          val path = data(client) / "foo" / "dir" / ""
          val meta = Http(path.DELETE > code)

          meta() must_== 200
        }
      }

      "be 200 with missing file (idempotency)" in {
        withServer(backends1, config1) { client =>
          val path = data(client) / "foo" / "missing"
          val meta = Http(path.DELETE > code)

          meta() must_== 200
        }
      }

      "be 200 with missing dir (idempotency)" in {
        withServer(backends1, config1) { client =>
          val path = data(client) / "foo" / "missingDir" / ""
          val meta = Http(path.DELETE > code)

          meta() must_== 200
        }
      }
    }
  }

  "/query/fs" should {
    def query(client: Req)= client / "query" / "fs" / ""

    "GET" should {
      "be 404 for missing backend" in {
        withServer(noBackends, config1) { client =>
          val req = query(client) / "missing" <<? Map("q" -> "select * from bar")
          val meta = Http(req)

          val resp = meta()
          resp.getStatusCode must_== 404
          errorFromBody(resp) must_== \/-("???")
        }
      }.pendingUntilFixed("#771")

      "be 400 for missing query" in {
        withServer(backends1, config1) { client =>
          val req = query(client) / "foo" / ""
          val meta = Http(req)

          val resp = meta()

          resp.getStatusCode must_== 400
          errorFromBody(resp) must_== \/-("The request must contain a query")
        }
      }

      "execute simple query" in {
        withServer(backends1, config1) { client =>
          val path = query(client) / "foo" / "" <<? Map("q" -> "select * from bar")
          val result = Http(path OK asJson)

          result() must beRightDisjunction((
            readableContentType,
            List(Json("0" := "ok"))))
        }
      }

      "be 400 for query error" in {
        withServer(backends1, config1) { client =>
          val req = query(client) / "foo" / "" <<? Map("q" -> "select date where")
          val meta = Http(req)

          val resp = meta()

          resp.getStatusCode must_== 400
          errorFromBody(resp) must_== \/-("keyword 'case' expected; `where'")
        }
      }
    }

    "POST" should {
      "be 404 with missing backend" in {
        withServer(noBackends, config1) { client =>
          val req = (query(client) / "missing" / "").POST.setBody("select * from bar").setHeader("Destination", "/tmp/gen0")
          val meta = Http(req)

          val resp = meta()
          resp.getStatusCode must_== 404
          errorFromBody(resp) must_== \/-("???")
        }
      }.pendingUntilFixed("#771")

      "be 400 with missing query" in {
        withServer(backends1, config1) { client =>
          val req = (query(client) / "foo" / "").POST.setHeader("Destination", "/foo/tmp/gen0")
          val meta = Http(req)

          val resp = meta()

          resp.getStatusCode must_== 400
          errorFromBody(resp) must_== \/-("The body of the POST must contain a query")
        }
      }

      "be 400 with missing Destination header" in {
        withServer(backends1, config1) { client =>
          val req = (query(client) / "foo" / "").POST.setBody("select * from bar")
          val meta = Http(req)

          val resp = meta()

          resp.getStatusCode must_== 400
          errorFromBody(resp) must_== \/-("The 'Destination' header must be specified")
        }
      }

      "execute simple query" in {
        withServer(backends1, config1) { client =>
          val req = (query(client) / "foo" / "").POST.setBody("select * from bar").setHeader("Destination", "/foo/tmp/gen0")

          val meta = Http(req)
          val resp = meta()

          resp.getStatusCode must_== 200
          (for {
            json   <- Parse.parse(resp.getResponseBody).toOption
            out    <- json.field("out")
            outStr <- out.string
          } yield outStr) must beSome("/foo/tmp/gen0")
        }
      }

      "be 400 for query error" in {
        withServer(backends1, config1) { client =>
          val req = (query(client) / "foo" / "").POST
                      .setBody("select date where")
                      .setHeader("Destination", "tmp0")
          val meta = Http(req)

          val resp = meta()

          resp.getStatusCode must_== 400
          errorFromBody(resp) must_== \/-("keyword 'case' expected; `where'")
        }
      }
    }
  }

  "/compile/fs" should {
    def compile(client: Req) = client / "compile" / "fs" / ""

    "GET" should {
      "be 404 with missing backend" in {
        withServer(noBackends, config1) { client =>
          val req = compile(client) / "missing" / "" <<? Map("q" -> "select * from bar")
          val meta = Http(req)

          val resp = meta()
          resp.getStatusCode must_== 404
          errorFromBody(resp) must_== \/-("???")
        }
      }.pendingUntilFixed("#771")

      "be 400 with missing query" in {
        withServer(backends1, config1) { client =>
          val req = compile(client) / "foo" / ""
          val meta = Http(req)

          val resp = meta()

          resp.getStatusCode must_== 400
          errorFromBody(resp) must_== \/-("The request must contain a query")
        }
      }

      "plan simple query" in {
        withServer(backends1, config1) { client =>
          val path = compile(client) / "foo" / "" <<? Map("q" -> "select * from bar")
          val result = Http(path OK as.String)

          result() must_== "Stub\nPlan(logical: Squash(Read(Path(\"bar\"))))"
        }
      }

      "be 400 for query error" in {
        withServer(backends1, config1) { client =>
          val req = compile(client) / "foo" / "" <<? Map("q" -> "select date where")
          val meta = Http(req)

          val resp = meta()

          resp.getStatusCode must_== 400
          errorFromBody(resp) must_== \/-("keyword 'case' expected; `where'")
        }
      }
    }

    "POST" should {
      "be 404 with missing backend" in {
        withServer(noBackends, config1) { client =>
          val req = (compile(client) / "missing" / "").POST.setBody("select * from bar")
          val meta = Http(req)

          val resp = meta()
          resp.getStatusCode must_== 404
          errorFromBody(resp) must_== \/-("???")
        }
      }.pendingUntilFixed("#771")

      "be 400 with missing query" in {
        withServer(backends1, config1) { client =>
          val req = (compile(client) / "foo" / "").POST
          val meta = Http(req)

          val resp = meta()

          resp.getStatusCode must_== 400
          errorFromBody(resp) must_== \/-("The body of the POST must contain a query")
        }
      }

      "plan simple query" in {
        withServer(backends1, config1) { client =>
          val path = (compile(client) / "foo" / "").POST.setBody("select * from bar")
          val result = Http(path OK as.String)

          result() must_== "Stub\nPlan(logical: Squash(Read(Path(\"bar\"))))"
        }
      }

      "be 400 for query error" in {
        withServer(backends1, config1) { client =>
          val req = (compile(client) / "foo" / "").POST.setBody("select date where")
          val meta = Http(req)

          val resp = meta()

          resp.getStatusCode must_== 400
          errorFromBody(resp) must_== \/-("keyword 'case' expected; `where'")
        }
      }
    }
  }

  "/mount/fs" should {
    def mount(client: Req) = client / "mount" / "fs" / ""
    def metadata(client: Req) = client / "metadata" / "fs" / ""

    "GET" should {
      "be 404 with missing mount" in {
        withServer(backendForConfig, config1) { client =>
          val req = mount(client) / "missing" / ""
          val meta = Http(req)

          val resp = meta()
          resp.getStatusCode must_== 404
          resp.getResponseBody must_== "There is no mount point at /missing/"
        }
      }

      "succeed with correct path" in {
        withServer(backendForConfig, config1) { client =>
          val req = mount(client) / "foo" / ""
          val result = Http(req OK asJson)

          result() must beRightDisjunction((
            jsonContentType,
            List(Json("mongodb" := Json("connectionUri" := "mongodb://localhost/foo")))))
        }
      }

      "be 404 with missing trailing slash" in {
        withServer(backendForConfig, config1) { client =>
          val req = mount(client) / "foo"
          val meta = Http(req)

          val resp = meta()
          resp.getStatusCode must_== 404
          resp.getResponseBody must_== "There is no mount point at /foo"
        }
      }
    }

    "MOVE" should {
      "succeed with valid paths" in {
        withServerRecordConfigChange(backendForConfig, config1) { (client, configs) =>
          val req = (mount(client) / "foo" / "")
                    .setMethod("MOVE")
                    .setHeader("Destination", "/foo2/")

          val fooMetadata = metadata(client) / "foo" / ""
          val foo2Metadata = metadata(client) / "foo2" / ""

          val fooExists = Http(fooMetadata)
          val foo2NotExists = Http(foo2Metadata)

          fooExists().getStatusCode must_== 200
          foo2NotExists().getStatusCode must_== 404

          val result = Http(req OK as.String)

          result() must_== "moved /foo/ to /foo2/"

          configs() must_== List(Config(SDServerConfig(Some(client.toRequest.getOriginalURI.getPort)), Map(
            Path("/foo2/") -> MongoDbConfig("mongodb://localhost/foo"),
            Path("/non/root/mounting/") -> MongoDbConfig("mongodb://localhost/mounting"))))

          val fooNotExists = Http(fooMetadata)
          val foo2Exists = Http(foo2Metadata)

          fooNotExists().getStatusCode must_== 404
          foo2Exists.apply().getStatusCode must_== 200
        }
      }

      "be 404 with missing source" in {
        withServerRecordConfigChange(backendForConfig, config1) { (client, configs) =>
          val req = (mount(client) / "missing" / "")
                    .setMethod("MOVE")
                    .setHeader("Destination", "/foo/")
          val meta = Http(req)

          val resp = meta()
          resp.getStatusCode must_== 404
          errorFromBody(resp) must_== \/-("There is no mount point at /missing/")
          configs() must_== Nil
        }
      }

      "be 400 with missing destination" in {
        withServerRecordConfigChange(backendForConfig, config1) { (client, configs) =>
          val req = (mount(client) / "foo" / "")
                    .setMethod("MOVE")
          val meta = Http(req)

          val resp = meta()
          resp.getStatusCode must_== 400
          errorFromBody(resp) must_== \/-("The 'Destination' header must be specified")
          configs() must_== Nil
        }
      }

      "be 400 with relative path" in {
        withServerRecordConfigChange(backendForConfig, config1) { (client, configs) =>
          val req = (mount(client) / "foo" / "")
                    .setMethod("MOVE")
                    .setHeader("Destination", "foo2/")
          val meta = Http(req)

          val resp = meta()
          resp.getStatusCode must_== 400
          errorFromBody(resp) must_== \/-("Not an absolute path: ./foo2/")
          configs() must_== Nil
        }
      }

      "be 400 with non-directory path for MongoDB mount" in {
        withServerRecordConfigChange(backendForConfig, config1) { (client, configs) =>
          val req = (mount(client) / "foo" / "")
                    .setMethod("MOVE")
                    .setHeader("Destination", "/foo2")
          val meta = Http(req)

          val resp = meta()
          resp.getStatusCode must_== 400
          errorFromBody(resp) must_== \/-("Not a directory path: /foo2")
          configs() must_== Nil
        }
      }
    }

    "POST" should {
      "succeed with valid MongoDB config" in {
        withServerRecordConfigChange(backendForConfig, config1) { (client, configs) =>
          val req = mount(client).POST
                    .setHeader("X-File-Name", "local/")
                    .setBody("""{ "mongodb": { "connectionUri": "mongodb://localhost/test" } }""")

          val localMetadata = metadata(client) / "local" / ""

          val metadataNotExists = Http(localMetadata)
          metadataNotExists().getStatusCode must_== 404

          val result = Http(req OK as.String)

          result() must_== "added /local/"

          configs() must_== List(Config(SDServerConfig(Some(client.toRequest.getOriginalURI.getPort)), Map(
            Path("/foo/") -> MongoDbConfig("mongodb://localhost/foo"),
            Path("/non/root/mounting/") -> MongoDbConfig("mongodb://localhost/mounting"),
            Path("/local/") -> MongoDbConfig("mongodb://localhost/test"))))

          val metadataExists = Http(localMetadata)
          metadataExists().getStatusCode must_== 200
        }
      }

      "be 409 with existing path" in {
        withServerRecordConfigChange(backendForConfig, config1) { (client, configs) =>
          val req = mount(client).POST
                    .setHeader("X-File-Name", "foo/")
                    .setBody("""{ "mongodb": { "connectionUri": "mongodb://localhost/foo2" } }""")
          val meta = Http(req)

          val resp = meta()
          resp.getStatusCode must_== 409
          errorFromBody(resp) must_== \/-("Can't add a mount point above the existing mount point at /foo/")
          configs() must_== Nil
        }
      }

      "be 409 for conflicting mount above" in {
        mockTest { (mock, client) =>
          val req = (mount(client) / "non" / "").POST
                    .setHeader("X-File-Name", "root/")
                    .setBody("""{ "mongodb": { "connectionUri": "mongodb://localhost/root" } }""")
          val meta = Http(req)

          val resp = meta()
          resp.getStatusCode must_== 409
          errorFromBody(resp) must_== \/-("Can't add a mount point above the existing mount point at /non/root/mounting/")
          mock.actions must_== Nil
        }
      }

      "be 409 for conflicting mount below" in {
        mockTest { (mock, client) =>
          val req = (mount(client) / "foo" / "").POST
                    .setHeader("X-File-Name", "nope/")
                    .setBody("""{ "mongodb": { "connectionUri": "mongodb://localhost/root" } }""")
          val meta = Http(req)

          val resp = meta()
          resp.getStatusCode must_== 409
          errorFromBody(resp) must_== \/-("Can't add a mount point below the existing mount point at /foo/")
          mock.actions must_== Nil
        }
      }

      "be 400 with missing file-name" in {
        withServerRecordConfigChange(backendForConfig, config1) { (client, configs) =>
          val req = mount(client).POST
                    .setBody("""{ "mongodb": { "connectionUri": "mongodb://localhost/test" } }""")
          val meta = Http(req)

          val resp = meta()
          resp.getStatusCode must_== 400
          errorFromBody(resp) must_== \/-("The 'X-File-Name' header must be specified")
          configs() must_== Nil
        }
      }

      "be 400 with invalid MongoDB path (no trailing slash)" in {
        withServerRecordConfigChange(backendForConfig, config1) { (client, configs) =>
          val req = mount(client).POST
                    .setHeader("X-File-Name", "local")
                    .setBody("""{ "mongodb": { "connectionUri": "mongodb://localhost/test" } }""")
          val meta = Http(req)

          val resp = meta()
          resp.getStatusCode must_== 400
          errorFromBody(resp) must_== \/-("Not a directory path: /local")
          configs() must_== Nil
        }
      }

      "be 400 with invalid JSON" in {
        withServerRecordConfigChange(backendForConfig, config1) { (client, configs) =>
          val req = mount(client).POST
                    .setHeader("X-File-Name", "local/")
                    .setBody("""{ "mongodb":""")
          val meta = Http(req)

          val resp = meta()
          resp.getStatusCode must_== 400
          errorFromBody(resp) must_== \/-("input error: JSON terminates unexpectedly.")
          configs() must_== Nil
        }
      }

      "be 400 with invalid MongoDB URI (extra slash)" in {
        withServerRecordConfigChange(backendForConfig, config1) { (client, configs) =>
          val req = mount(client).POST
                    .setHeader("X-File-Name", "local/")
                    .setBody("""{ "mongodb": { "connectionUri": "mongodb://localhost:8080//test" } }""")
          val meta = Http(req)

          val resp = meta()
          resp.getStatusCode must_== 400
          errorFromBody(resp) must_== \/-("invalid connection URI: mongodb://localhost:8080//test")
          configs() must_== Nil
        }
      }
    }

    "PUT" should {
      "succeed with valid MongoDB config" in {
        withServerRecordConfigChange(backendForConfig, config1) { (client, configs) =>
          val req = (mount(client) / "local" / "").PUT
                    .setBody("""{ "mongodb": { "connectionUri": "mongodb://localhost/test" } }""")

          val localMetadata = metadata(client) / "local" / ""

          val metadataNotExists = Http(localMetadata)
          metadataNotExists().getStatusCode must_== 404

          val result = Http(req OK as.String)

          result() must_== "added /local/"

          configs() must_== List(Config(SDServerConfig(Some(client.toRequest.getOriginalURI.getPort)), Map(
            Path("/foo/") -> MongoDbConfig("mongodb://localhost/foo"),
            Path("/non/root/mounting/") -> MongoDbConfig("mongodb://localhost/mounting"),
            Path("/local/") -> MongoDbConfig("mongodb://localhost/test"))))

          val metadataExists = Http(localMetadata)
          metadataExists().getStatusCode must_== 200
        }
      }

      "succeed with valid, overwritten MongoDB config" in {
        withServerRecordConfigChange(backendForConfig, config1) { (client, configs) =>
          val req = (mount(client) / "foo" / "").PUT
                    .setBody("""{ "mongodb": { "connectionUri": "mongodb://localhost/foo2" } }""")
          val result = Http(req OK as.String)

          result() must_== "updated /foo/"

          configs() must_== List(Config(SDServerConfig(Some(client.toRequest.getOriginalURI.getPort)), Map(
            Path("/foo/") -> MongoDbConfig("mongodb://localhost/foo2"),
            Path("/non/root/mounting/") -> MongoDbConfig("mongodb://localhost/mounting"))))
        }
      }

      "make the new mount immediately available, without restart" in {
        withServer(backendForConfig, config1) { client =>
          val req1 = mount(client) / "bar" / ""

          val result1 = Http(req1 > code)
          result1() must_== 404

          val req2 = req1.PUT.setBody("""{ "mongodb": { "connectionUri": "mongodb://localhost/bar" } }""")
          val result2 = Http(req2 OK as.String)

          result2() must_== "added /bar/"

          val result3 = Http(req1 > code)
          result3() must_== 200
        }
      }

      "be 409 for conflicting mount above" in {
        mockTest { (mock, client) =>
          val req = (mount(client) / "non" / "root" / "").PUT
                    .setBody("""{ "mongodb": { "connectionUri": "mongodb://localhost/root" } }""")
          val meta = Http(req)

          val resp = meta()
          resp.getStatusCode must_== 409
          errorFromBody(resp) must_== \/-("Can't add a mount point above the existing mount point at /non/root/mounting/")
          mock.actions must_== Nil
        }
      }

      "be 409 for conflicting mount below" in {
        mockTest { (mock, client) =>
          val req = (mount(client) / "foo" / "nope" / "").PUT
                    .setBody("""{ "mongodb": { "connectionUri": "mongodb://localhost/root" } }""")
          val meta = Http(req)

          val resp = meta()
          resp.getStatusCode must_== 409
          errorFromBody(resp) must_== \/-("Can't add a mount point below the existing mount point at /foo/")
          mock.actions must_== Nil
        }
      }

      "be 400 with invalid MongoDB path (no trailing slash)" in {
        withServerRecordConfigChange(backendForConfig, config1) { (client, configs) =>
          val req = (mount(client) / "local").PUT
                    .setBody("""{ "mongodb": { "connectionUri": "mongodb://localhost/test" } }""")
          val meta = Http(req)

          val resp = meta()
          resp.getStatusCode must_== 400
          errorFromBody(resp) must_== \/-("Not a directory path: /local")
          configs() must_== Nil
        }
      }

      "be 400 with invalid JSON" in {
        withServerRecordConfigChange(backendForConfig, config1) { (client, configs) =>
          val req = (mount(client) / "local" / "").PUT
                    .setBody("""{ "mongodb":""")
          val meta = Http(req)

          val resp = meta()
          resp.getStatusCode must_== 400
          errorFromBody(resp) must_== \/-("input error: JSON terminates unexpectedly.")
          configs() must_== Nil
        }
      }

      "be 400 with invalid MongoDB URI (extra slash)" in {
        withServerRecordConfigChange(backendForConfig, config1) { (client, configs) =>
          val req = (mount(client) / "local" / "").PUT
                    .setBody("""{ "mongodb": { "connectionUri": "mongodb://localhost:8080//test" } }""")
          val meta = Http(req)

          val resp = meta()
          resp.getStatusCode must_== 400
          errorFromBody(resp) must_== \/-("invalid connection URI: mongodb://localhost:8080//test")
          configs() must_== Nil
        }
      }
    }

    "DELETE" should {
      "succeed with correct path" in {
        withServerRecordConfigChange(backendForConfig, config1) { (client, configs) =>
          val req = (mount(client) / "foo" / "").DELETE

          val fooMetadata = metadata(client) / "foo" / ""

          val fooMetadataExists = Http(fooMetadata)
          fooMetadataExists().getStatusCode must_== 200

          val result = Http(req OK as.String)
          result() must_== "deleted /foo/"

          configs() must_== List(Config(SDServerConfig(Some(client.toRequest.getOriginalURI.getPort)), Map(
            Path("/non/root/mounting/") -> MongoDbConfig("mongodb://localhost/mounting"))))

          val fooMetadataNotExists = Http(fooMetadata)
          fooMetadataNotExists().getStatusCode must_== 404
        }
      }

      "fail for non-existent path" in {
        withServerRecordConfigChange(backendForConfig, config1) { (client, configs) =>
          val req = (mount(client) / "missing" / "").DELETE
          val result = Http(req)

          result().getStatusCode must_== 404
          configs() must_== Nil
        }
      }
    }
  }

  "/server" should {
    def withServerExpectingRestart[A, B](backend: Backend, config: Config, timeoutMillis: Long = 10000, port: Int = 8888)
                                        (causeRestart: Req => A)(afterRestart: => B): B = {
      type S = (Int, org.http4s.server.Server)

      val client = dispatch.host("localhost", port)

      val (servers, forCfg) = Server.servers("", 1.seconds, tester, mounter(backend), _ => Task.now(()))

      val channel = Process[S => Task[A \/ B]](
        κ(Task.delay(\/.left(causeRestart(client)))),
        κ(Task.delay(\/.right(afterRestart)).onFinish(_ => forCfg(None))))

      val exec = servers.through(channel).runLast.flatMap(
        _.flatMap(_.toOption).cata[Task[B]](
          Task.now(_),
          Task.fail(new RuntimeException("impossible!"))))

      (forCfg(Some((port, config))) *> exec).runFor(timeoutMillis)
    }

    "be capable of providing it's name and version" in {
      withServer(noBackends, config1) { client =>
        val req = (client / "server" / "info").GET
        val result = Http(req OK as.String)

        result() must_== versionAndNameInfo.toString
      }
    }

    "restart on new port when PUT /port succeeds" in {
      val newPort = 8889

      withServerExpectingRestart(noBackends, config1)({ client =>
        val result1 = Http((client / "server" / "info") > code)
        result1() must_== 200

        val req2 = (client / "server" / "port").PUT.setBody(newPort.toString)
        val result2 = Http(req2 > code)

        result2() must_== 200
      })({
        val req3 = dispatch.host("localhost", newPort) / "server" / "info"
        val result3 = Http(req3 > code)
        result3() must_== 200
      })
    }

    "restart on default port when DELETE /port successful" in {
      withServerExpectingRestart(noBackends, config1)({ client =>
        val result1 = Http((client / "server" / "info") > code)
        result1() must_== 200

        val req2 = (client / "server" / "port").DELETE
        val result2 = Http(req2 > code)

        result2() must_== 200
      })({
        val req3 = dispatch.host("localhost", SDServerConfig.DefaultPort) / "server" / "info"
        val result3 = Http(req3 > code)
        result3() must_== 200
      })
    }
  }

  step {
    // Explicitly close dispatch's executor, since it no longer detects running in SBT properly.
    Http.shutdown
  }
}

class ResponseFormatSpecs extends Specification {
  import org.http4s._, QValue._
  import org.http4s.headers.{Accept}

  import ResponseFormat._

  "fromAccept" should {
    "be Readable by default" in {
      fromAccept(None) must_== JsonStream.Readable
    }

    "choose precise" in {
      val accept = Accept(
        new MediaType("application", "ldjson").withExtensions(Map("mode" -> "precise")))
      fromAccept(Some(accept)) must_== JsonStream.Precise
    }

    "choose streaming via boundary extension" in {
      val accept = Accept(
        new MediaType("application", "json").withExtensions(Map("boundary" -> "NL")))
      fromAccept(Some(accept)) must_== JsonStream.Readable
    }

    "choose precise list" in {
      val accept = Accept(
        new MediaType("application", "json").withExtensions(Map("mode" -> "precise")))
      fromAccept(Some(accept)) must_== JsonArray.Precise
    }

    "choose streaming and precise via extensions" in {
      val accept = Accept(
        new MediaType("application", "json").withExtensions(Map("mode" -> "precise", "boundary" -> "NL")))
      fromAccept(Some(accept)) must_== JsonStream.Precise
    }

    "choose CSV" in {
      val accept = Accept(
        new MediaType("text", "csv"))
      fromAccept(Some(accept)) must_== Csv.Default
    }

    "choose CSV with custom format" in {
      val accept = Accept(
        new MediaType("text", "csv").withExtensions(Map(
          "columnDelimiter" -> "\t",
          "rowDelimiter" -> ";",
          "quoteChar" -> "'",
          "escapeChar" -> "\\")))
      fromAccept(Some(accept)) must_== Csv('\t', ";", '\'', '\\', None)
    }

    "choose CSV over JSON" in {
      val accept = Accept(
        new MediaType("text", "csv").withQValue(q(1.0)),
        new MediaType("application", "ldjson").withQValue(q(0.9)))
      fromAccept(Some(accept)) must_== Csv.Default
    }

    "choose JSON over CSV" in {
      val accept = Accept(
        new MediaType("text", "csv").withQValue(q(0.9)),
        new MediaType("application", "ldjson"))
      fromAccept(Some(accept)) must_== JsonStream.Readable
    }
  }

  "Csv.escapeNewlines" should {
    """escape \r\n""" in {
      Csv.escapeNewlines("\r\n") must_== """\r\n"""
    }

    """not affect \"""" in {
      Csv.escapeNewlines("\\\"") must_== "\\\""
    }
  }

  "Csv.unescapeNewlines" should {
    """unescape \r\n""" in {
      Csv.unescapeNewlines("""\r\n""") must_== "\r\n"
    }

    """not affect \"""" in {
      Csv.escapeNewlines("""\"""") must_== """\""""
    }
  }
}

class HeaderParamSpecs extends Specification {
  import org.http4s.util._

  import HeaderParam._

  "parse" should {
    "parse one" in {
      parse("""{ "Accept": "text/csv" }""") must_==
        \/-(Map(CaseInsensitiveString("Accept") -> List("text/csv")))
    }

    "parse mulitple values" in {
      parse("""{ "Foo": [ "bar", "baz" ] }""") must_==
        \/-(Map(CaseInsensitiveString("Foo") -> List("bar", "baz")))
    }

    "fail with invalid json" in {
      parse("""{""") must_==
        -\/("parse error (JSON terminates unexpectedly.)")
    }

    "fail with non-object" in {
      parse("""0""") must_==
        -\/("expected a JSON object; found: 0")
    }

    "fail with non-string/array value" in {
      parse("""{ "Foo": 0 }""") must_==
        -\/("expected a string or array of strings; found: 0")
    }

    "fail with non-string value in array" in {
      parse("""{ "Foo": [ 0 ] }""") must_==
        -\/("expected string in array; found: 0")
    }
  }

  "rewrite" should {
    import org.http4s._
    import org.http4s.headers._

    "overwrite conflicting header" in {
      val headers = rewrite(
        Headers(`Accept`(MediaType.`text/csv`)),
        Map(CaseInsensitiveString("accept") -> List("application/json")))

      headers.get(`Accept`) must beSome(`Accept`(MediaType.`application/json`))
    }

    "add non-conflicting header" in {
      val headers = rewrite(
        Headers(`Accept`(MediaType.`text/csv`)),
        Map(CaseInsensitiveString("user-agent") -> List("some_phone_browser/0.0.1")))

      headers.get(`Accept`) must beSome(`Accept`(MediaType.`text/csv`))
      headers.get(`User-Agent`) must beSome(`User-Agent`(AgentProduct("some_phone_browser", Some("0.0.1"))))
    }
  }
}
