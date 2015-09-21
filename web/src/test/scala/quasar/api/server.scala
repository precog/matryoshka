package quasar.api

import quasar.Predef._

import org.specs2.mutable._
import org.specs2.scalaz._

import scalaz._

class ServerSpecs extends Specification with DisjunctionMatchers {
  import Server._

  val defaultOpts = Options(None, None, None, false, false, None)

  "interpretPaths" should {
    "be empty with defaults" in {
      interpretPaths(defaultOpts).run.run must beRightDisjunction(None)
    }

    "fail with loc and no path" in {
      val opts = defaultOpts.copy(contentLoc = Some("foo"))
      interpretPaths(opts).run.run must beLeftDisjunction
    }

    "default to /files" in {
      val opts = defaultOpts.copy(contentPath = Some("foo"))
      interpretPaths(opts).run.run must beRightDisjunction(Some(StaticContent("/files", "foo")))
    }

    "handle loc and path" in {
      val opts = defaultOpts.copy(contentLoc = Some("/foo"), contentPath = Some("bar"))
      interpretPaths(opts).run.run must beRightDisjunction(Some(StaticContent("/foo", "bar")))
    }

    "relative" in {
      val opts = defaultOpts.copy(contentPath = Some("foo"), contentPathRelative = true)
      (interpretPaths(opts).run.run match {
        case \/-(Some(StaticContent(_, path))) => path must endWith("/foo")
        case _ => failure
      }): org.specs2.execute.Result
    }
  }
}
