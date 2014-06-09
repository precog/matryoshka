package slamdata.engine.fs

import scalaz._
import Scalaz._

import org.specs2.mutable._

class PathSpecs extends Specification {
  "Path.apply" should {
    "Parse empty string as root" in {
      Path("") must_== Path.Root
    }

    "Parse root string as root" in {
      Path("/") must_== Path.Root
    }

    "Parse multiple slashes as root" in {
      Path("/////////////////////////////") must_== Path.Root
    }

    "Parse trailing slash as pure directory" in {
      Path("/foo/bar/baz/") must_== Path.dir("foo" :: "bar" :: "baz" :: Nil)
    }

    "Parse lack of trailing slash as file" in {
      Path("/foo/bar/baz") must_== Path.file("foo" :: "bar" :: Nil, "baz")
    }

    "Correctly parse root file" in {
      Path("/foo") must_== Path.file(Nil, "foo")
    }
  }

  "Path.pathname" should {
    "render root correctly" in {
      Path.Root.pathname must_== "/"
    }

    "render pure dir correctly" in {
      Path("/foo/bar/baz/").pathname must_== "/foo/bar/baz/"
    }

    "render file correctly" in {
      Path("/foo/bar/baz").pathname must_== "/foo/bar/baz"
    }
  }
}