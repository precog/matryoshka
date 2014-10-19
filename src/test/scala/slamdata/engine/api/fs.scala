package slamdata.engine.api

import slamdata.engine._
import slamdata.engine.analysis.fixplate.{Term}
import slamdata.engine.fs._

import scalaz._
import scalaz.concurrent._
import scalaz.stream._
import Scalaz._

import org.specs2.mutable._
import org.specs2.specification._
import slamdata.specs2._

import unfiltered.util.{StartableServer}

import argonaut._, Argonaut._

import dispatch._
import com.ning.http.client.{Response}

class ApiSpecs extends Specification with DisjunctionMatchers with PendingWithAccurateCoverage {
  sequential  // Each test binds an arbitrary port

  val port = unfiltered.util.Port.any

 /**
  Start a server, with the given backends, execute something, and then tear
  down the server.
  */
  def withServer[A](fs: Map[Path, Backend])(body: => A): A = {
    val api = new FileSystemApi(FSTable(fs)).api
    val srv = unfiltered.netty.Server.local(port).chunked(1024*1024).plan(api)

    srv.start

    try {
      body
    }
    finally {
      srv.stop

      // Unfiltered does not wait for the netty server to shutdown before returning
      // from stop(), and by default the "quiet period" for netty is two seconds.
      // If we don't wait, each test leaves behind a thread pool for two seconds and
      // eventually the VM might run out of threads (especially on Travis).
      // Here we reach into the server's engine, grab one of the futures which
      // completes when the quiet period expires, and wait on it directly.
      val promise = srv.engine.workers.shutdownGracefully
      promise.get
    }
  }

  object Stub {
    trait Plan
    implicit val PlanRenderTree = new RenderTree[Plan] {
      def render(v: Plan) = Terminal("", List("Stub.Plan"))
    }

    lazy val planner = new Planner[Plan] {
      def plan(logical: Term[LogicalPlan]) = \/- (new Plan {})
    }
    lazy val evaluator: Evaluator[Plan] = new Evaluator[Plan] {
      def execute(physical: Plan) = Task.now(ResultPath.Temp(Path("tmp/out")))
    }
    def showNative(plan: Plan): String = plan.toString

    def fs(files: Map[Path, List[RenderedJson]]): FileSystem = new FileSystem {
      def scan(path: Path, offset: Option[Long], limit: Option[Long]) = 
        files.get(path).map(js => Process.emitAll(js))
          .getOrElse(Process.fail(FileSystem.FileNotFoundError(path)))

      def save(path: Path, values: Process[Task, RenderedJson]) = 
        if (path.pathname.contains("pathError")) Task.fail(PathError(Some("simulated (client) error")))
        else if (path.pathname.contains("valueError")) Task.fail(JsonWriteError(RenderedJson(""), Some("simulated (value) error")))
        else Task.now(())

      def append(path: Path, values: Process[Task, RenderedJson]) = 
        if (path.pathname.contains("pathError")) Process.fail(PathError(Some("simulated (client) error")))
        else if (path.pathname.contains("valueError")) Process.emit(JsonWriteError(RenderedJson(""), Some("simulated (value) error")))
        else Process.halt

      def delete(path: Path): Task[Unit] = Task.now(())

      def move(src: Path, dst: Path): Task[Unit] = Task.now(())

      def ls(dir: Path): Task[List[Path]] = {
        val childrenOpt = files.keys.toList.map(_.rebase(dir).map(_.head)).sequenceU
        childrenOpt.map(Task.now(_)).getOrElse(Task.fail(FileSystem.FileNotFoundError(dir)))
      }
    }

    def backend(fs: FileSystem) = Backend(planner, evaluator, fs, showNative)
  }


  /** Handler for response bodies containing newline-separated JSON documents, for use with Dispatch. */
  object asJson extends (Response => String \/ List[Json]) {
    private def sequenceStrs[A](vs: Seq[String \/ A]): String \/ List[A] =
      vs.toList.map(_.validation.toValidationNel).sequenceU.leftMap(_.list.mkString("; ")).disjunction

    private def parseJsonLines(str: String): String \/ List[Json] =
      sequenceStrs(str.split("\n").map(Parse.parse(_)))

    def apply(r: Response) = (dispatch.as.String andThen parseJsonLines)(r)
  }

  /** Handlers for use with Dispatch. */
  val code: Response => Int = _.getStatusCode
  def header(name: String): Response => Option[String] = r => Option(r.getHeader(name))
  def commaSep: Option[String] => List[String] = _.fold(List[String]())(_.split(", ").toList)

  val svc = dispatch.host("localhost", port)

  val files1 = Map(
    Path("bar") -> List(RenderedJson("{\"a\": 1}\n{\"b\": 2}")),
    Path("dir/baz") -> List(),
    Path("tmp/out") -> List(RenderedJson("{\"0\": \"ok\"}"))
  )
  val backends1 = Map(
    Path("/empty/") -> Stub.backend(FileSystem.Null),
    Path("/foo/") -> Stub.backend(Stub.fs(files1)),
    Path("badPath1/") -> Stub.backend(FileSystem.Null),
    Path("/badPath2") -> Stub.backend(FileSystem.Null))

  "OPTIONS" should {
    val optionsRoot = svc.OPTIONS
    
    val corsMethods = header("Access-Control-Allow-Methods") andThen commaSep
    val corsHeaders = header("Access-Control-Allow-Headers") andThen commaSep

    "advertise GET and POST for /query path" in {
      withServer(Map()) {
        val methods = Http(optionsRoot / "query" / "foo" > corsMethods)
      
        methods() must contain(allOf("GET", "POST"))
      }
    }
    
    "advertise Destination header for /query path and method POST" in {
      withServer(Map()) {
        val headers = Http((optionsRoot / "query" / "foo").setHeader("Access-Control-Request-Method", "POST") > corsHeaders)
      
        headers() must contain(allOf("Destination"))
      }
    }
    
    "advertise GET, PUT, POST, DELETE, and MOVE for /data path" in {
      withServer(Map()) {
        val methods = Http(optionsRoot / "data" / "foo" > corsMethods)
      
        methods() must contain(allOf("GET", "PUT", "POST", "DELETE", "MOVE"))
      }
    }

    "advertise Destination header for /data path and method MOVE" in {
      withServer(Map()) {
        val headers = Http((optionsRoot / "data" / "foo").setHeader("Access-Control-Request-Method", "MOVE") > corsHeaders)
      
        headers() must contain(allOf("Destination"))
      }
    }
  }

  "/metadata/fs" should {
    val root = svc / "metadata" / "fs" / ""  // Note: trailing slash required

    "return no filesystems" in {
      withServer(Map()) {
        val meta = Http(root OK asJson)
        
        meta() must beRightDisj(List(Json("children" := List[Json]())))
      }
    }

    "be 404 with missing backend" in {
      withServer(Map()) {
        val path = root / "missing"
        val meta = Http(path > code)

        meta() must_== 404
      }
    }

    "return empty for null fs" in {
      withServer(backends1) {
        val path = root / "empty" / ""
        val meta = Http(path OK asJson)

        meta() must beRightDisj(List(Json("children" := List[Json]())))
      }
    }

    "be 404 with missing path" in {
      withServer(backends1) {
        val path = root / "foo" / "baz" / ""
        val meta = Http(path > code)

        meta() must_== 404
      }
    }

    "find stubbed filesystems" in {
      withServer(backends1) {
        val meta = Http(root OK asJson)

        // Note: four backends will come in the right order and compare equal, but not 5 or more.
        meta() must beRightDisj(List(
          Json("children" := List(
            Json("name" := "empty", "type" := "mount"),
            Json("name" := "foo", "type" := "mount"),
            Json("name" := "badPath1", "type" := "mount"),
            Json("name" := "badPath2", "type" := "mount")))))
      }
    }

    "find stubbed files" in {
      withServer(backends1) {
        val path = root / "foo" / ""
        val meta = Http(path OK asJson)

        meta() must beRightDisj(List(
          Json("children" := List(
            Json("name" := "bar", "type" := "file"),
            Json("name" := "dir", "type" := "directory"),
            Json("name" := "tmp", "type" := "directory")))))
      }
    }

  }
  
  "/data/fs" should {
    val root = svc / "data" / "fs" / ""

    "GET" should {
      "be 404 for missing backend" in {
        withServer(Map()) {
          val path = root / "missing"
          val meta = Http(path > code)

          meta() must_== 404
        }
      }
    
      "be 404 for missing file" in {
        withServer(backends1) {
          val path = root / "empty" / "anything"
          val meta = Http(path > code)

          meta() must_== 404
        }
      }.pendingUntilFixed  // FIXME: ResponseStreamer does not detect failure
    
      "read entire file" in {
        withServer(backends1) {
          val path = root / "foo" / "bar"
          val meta = Http(path OK asJson)

          meta() must beRightDisj(List(Json("a" := 1), Json("b" := 2)))
        }
      }
    }
  
    "PUT" should {
      "be 404 for missing backend" in {
        withServer(Map()) {
          val path = root / "missing"
          val meta = Http(path.PUT > code)

          meta() must_== 404
        }
      }
  
      "be 400 with no body" in {
        withServer(backends1) {
          val path = root / "foo" / "bar"
          val meta = Http(path.PUT > code)

          meta() must_== 400
        }
      }
  
      "be 400 with invalid JSON" in {
        withServer(backends1) {
          val path = root / "foo" / "bar"
          val meta = Http(path.PUT.setBody("{") > code)

          meta() must_== 400
        }
      }
  
      "accept valid JSON" in {
        withServer(backends1) {
          val path = root / "foo" / "bar"
          val meta = Http(path.PUT.setBody("{\"a\": 1}\n{\"b\": 2}") OK as.String)

          meta() must_== ""
        }
      }

      "be 400 with simulated path error" in {
        withServer(backends1) {
          val path = root / "foo" / "pathError"
          val meta = Http(path.PUT.setBody("{\"a\": 1}") > code)

          meta() must_== 400
        }
      }

      "be 500 with simulated error on a particular value" in {
        withServer(backends1) {
          val path = root / "foo" / "valueError"
          val meta = Http(path.PUT.setBody("{\"a\": 1}") > code)

          meta() must_== 500
        }
      }
    }

    "POST" should {
      "be 404 for missing backend" in {
        withServer(Map()) {
          val path = root / "missing"
          val meta = Http(path.POST > code)

          meta() must_== 404
        }
      }

      "be 400 with no body" in {
        withServer(backends1) {
          val path = root / "foo" / "bar"
          val meta = Http(path.POST > code)

          meta() must_== 400
        }
      }

      "be 400 with invalid JSON" in {
        withServer(backends1) {
          val path = root / "foo" / "bar"
          val meta = Http(path.POST.setBody("{") > code)

          meta() must_== 400
        }
      }

      "produce two errors with partially invalid JSON" in {
        withServer(backends1) {
          val req = (root / "foo" / "bar").POST.setBody(
            """{"a": 1}
              |"unmatched
              |{"b": 2}
              |}
              |{"c": 3}
            """.stripMargin)
          val meta = Http(req > asJson)

          meta() must beRightDisj((json: List[Json]) => 
            json.length == 1 &&
            (for {
              obj <- json.head.obj
              errors <- obj("errors")
              eArr <- errors.array
            } yield eArr.length == 2).getOrElse(false))
        }
      }

      "accept valid JSON" in {
        withServer(backends1) {
          val req = (root / "foo" / "bar").POST.setBody("{\"a\": 1}\n{\"b\": 2}")
          val meta = Http(req OK as.String)

          meta() must_== ""
        }
      }

      "be 400 with simulated path error" in {
        withServer(backends1) {
          val path = root / "foo" / "pathError"
          val meta = Http(path.POST.setBody("{\"a\": 1}") > code)

          meta() must_== 400
        }
      }

      "be 500 with simulated error on a particular value" in {
        withServer(backends1) {
          val path = root / "foo" / "valueError"
          val meta = Http(path.POST.setBody("{\"a\": 1}") > code)

          meta() must_== 500
        }
      }
    }

    "MOVE" should {
      val moveRoot = root.setMethod("MOVE")

      "be 400 for missing src backend" in {
        withServer(Map()) {
          val req = moveRoot / "foo"
          val meta = Http(req > code)

          meta() must_== 400
        }
      }

      "be 404 for missing source file" in {
        withServer(backends1) {
          val req = (moveRoot / "missing" / "a" ).setHeader("Destination", "/foo/bar")
          val meta = Http(req > code)

          meta() must_== 404
        }
      }

      "be 404 for missing dst backend" in {
        withServer(backends1) {
          val req = (moveRoot / "foo" / "bar").setHeader("Destination", "/missing/a")
          val meta = Http(req > code)

          meta() must_== 404
        }
      }

      "be 201 for file" in {
        withServer(backends1) {
          val req = (moveRoot / "foo" / "bar").setHeader("Destination", "/foo/baz")
          val meta = Http(req > code)

          meta() must_== 201
        }
      }

      "be 201 for dir" in {
        withServer(backends1) {
          val req = (moveRoot / "foo" / "dir" / "").setHeader("Destination", "/foo/dir2/")
          val meta = Http(req > code)

          meta() must_== 201
        }
      }

      "be 500 for src and dst not in same backend" in {
        withServer(backends1) {
          val req = (moveRoot / "foo" / "bar").setHeader("Destination", "/empty/a")
          val meta = Http(req > code)

          meta() must_== 500
        }
      }

    }

    "DELETE" should {
      "be 404 for missing backend" in {
        withServer(Map()) {
          val path = root / "missing"
          val meta = Http(path.DELETE > code)

          meta() must_== 404
        }
      }

      "be 200 with existing file" in {
        withServer(backends1) {
          val path = root / "foo" / "bar"
          val meta = Http(path.DELETE > code)

          meta() must_== 200
        }
      }

      "be 200 with existing dir" in {
        withServer(backends1) {
          val path = root / "foo" / "dir" / ""
          val meta = Http(path.DELETE > code)

          meta() must_== 200
        }
      }

      "be 200 with missing file (idempotency)" in {
        withServer(backends1) {
          val path = root / "foo" / "missing"
          val meta = Http(path.DELETE > code)

          meta() must_== 200
        }
      }

      "be 200 with missing dir (idempotency)" in {
        withServer(backends1) {
          val path = root / "foo" / "missingDir" / ""
          val meta = Http(path.DELETE > code)

          meta() must_== 200
        }
      }
    }
  }
  
  "/query/fs" should {
    val root = svc / "query" / "fs" / ""

    "GET" should {
      "be 404 for missing backend" in {
        withServer(Map()) {
          val path = root / "missing" <<? Map("q" -> "select * from bar")
          val meta = Http(path > code)

          meta() must_== 404
        }
      }

      "be 400 for missing query" in {
        withServer(backends1) {
          val path = root / "foo" / ""
          val result = Http(path > code)

          result() must_== 400
        }
      }
      
      "execute simple query" in {
        withServer(backends1) {
          val path = root / "foo" / "" <<? Map("q" -> "select * from bar")
          val result = Http(path OK asJson)

          result() must beRightDisj(List(
            Json("0" := "ok")))
        }
      }
      
      "be 400 for query error" in {
        withServer(backends1) {
          val path = root / "foo" / "" <<? Map("q" -> "error")
          val result = Http(path > code)

          result() must_== 400
        }
      }
    }
  }

  step {
    // Explicitly close dispatch's executor, since it no longer detects running in SBT properly.
    Http.shutdown
  }
}