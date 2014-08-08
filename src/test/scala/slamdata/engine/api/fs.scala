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

import unfiltered.util.{StartableServer}

import argonaut._, Argonaut._

import dispatch._, Defaults._
import com.ning.http.client.{/*AsyncHandler, Request,*/ Response}

class ApiSpecs extends Specification with DisjunctionMatchers {
  sequential  // Each test binds an arbitrary port

  val port = unfiltered.util.Port.any

 /**
  Start a server, with the given backends, execute something, and then tear
  down the server.
  */
  def withServer[A](fs: Map[Path, Backend])(body: => A): A = {
    val api = new FileSystemApi(fs).api
    val srv = unfiltered.netty.Http(port).chunked(1024*1024).plan(api)

    srv.start

    try {
      body
    }
    finally {
      srv.stop

      // Force finalizers to run, to recover file handles:
      java.lang.Runtime.getRuntime.gc
    }
  }

  object Stub {
    trait Plan
    implicit val PlanRenderTree = new RenderTree[Plan] {
      def render(v: Plan) = Terminal("")
    }

    lazy val planner = new Planner[Plan] {
      def plan(logical: Term[LogicalPlan]) = \/- (new Plan {})
    }
    lazy val evaluator: Evaluator[Plan] = new Evaluator[Plan] {
      def execute(physical: Plan, out: Path) = Task.delay(out)
    }
    def showNative(plan: Plan, path: Path): String = ???

    def fs(files: Map[Path, List[RenderedJson]]): FileSystem = new FileSystem {
      def scan(path: Path, offset: Option[Long], limit: Option[Long]) = 
        files.get(path).map(js => Process.emitAll(js))
          .getOrElse(Process.fail(FileSystem.FileNotFoundError(path)))
      
      def delete(path: Path): Task[Unit] = ???  // Not used yet
      
      def ls(dir: Path): Task[List[Path]] = {
        val childrenOpt = files.keys.toList.map(_.relativeTo(dir)).sequenceU
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

  /** Handler for responses that just captures the response code, for use with Dispatch. */
  object code extends (Response => Int) {
    def apply(r: Response) = r.getStatusCode
  }

  val svc = dispatch.host("localhost", port)

  val files1 = Map(
    Path("bar") -> List(RenderedJson("{\"a\": 1}\n{\"b\": 2}")))
  val backends1 = Map(
    Path("/empty/") -> Stub.backend(FileSystem.Null),
    Path("/foo/") -> Stub.backend(Stub.fs(files1)),
    Path("badPath1/") -> Stub.backend(FileSystem.Null),
    Path("/badPath2") -> Stub.backend(FileSystem.Null))

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
            Json("name" := "empty", "type" := "directory"),
            Json("name" := "foo", "type" := "directory"),
            Json("name" := "badPath1", "type" := "directory"),
            Json("name" := "badPath2", "type" := "directory")))))
      }
    }

    "find stubbed files" in {
      withServer(backends1) {
        val path = root / "foo" / ""
        val meta = Http(path OK asJson)

        meta() must beRightDisj(List(
          Json("children" := List(
            Json("name" := "./bar", "type" := "file")))))
      }
    }

  }
  
  "/data/fs" should {
    val root = svc / "data" / "fs" / ""

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
    }.pendingUntilFixed
    
    "read entire file" in {
      withServer(backends1) {
        val path = root / "foo" / "bar"
        val meta = Http(path OK asJson)

        meta() must beRightDisj(List(Json("a" := 1), Json("b" := 2)))
      }
    }
    
  }
}