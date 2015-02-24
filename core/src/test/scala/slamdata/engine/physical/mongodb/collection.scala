package slamdata.engine.physical.mongodb

import org.specs2.mutable._

import slamdata.engine.fs.{Path}
import slamdata.engine.{DisjunctionMatchers}

class CollectionSpec extends Specification with DisjunctionMatchers {

  "Collection.fromPath" should {

    "handle simple name" in {
      Collection.fromPath(Path("foo")) must beRightDisj(Collection("foo"))
    }

    "handle simple relative path" in {
      Collection.fromPath(Path("foo/bar")) must beRightDisj(Collection("foo.bar"))
    }

    "escape leading '.'" in {
      Collection.fromPath(Path(".hidden")) must beRightDisj(Collection("\\.hidden"))
    }

    "escape '.' with path separators" in {
      Collection.fromPath(Path("foo/bar.baz")) must beRightDisj(Collection("foo.bar\\.baz"))
    }

    "escape '$'" in {
      Collection.fromPath(Path("foo$")) must beRightDisj(Collection("foo\\d"))
    }

    "escape '\\'" in {
      Collection.fromPath(Path("foo\\bar")) must beRightDisj(Collection("foo\\\\bar"))
    }

    "accept absolute path" in {
      Collection.fromPath(Path("/foo/bar")) must beRightDisj(Collection("foo.bar"))
    }

    "accept path with 120 characters" in {
      val longName = List.fill(20)("123456789/").mkString.substring(0, 120)
      Collection.fromPath(Path(longName)) must beAnyRightDisj
    }

    "preserve space" in {
      Collection.fromPath(Path("/foo/bar baz")) must beRightDisj(Collection("foo.bar baz"))
    }

    "reject path longer than 120 characters" in {
      val longName = List.fill(20)("123456789/").mkString.substring(0, 121)
      Collection.fromPath(Path(longName)) must beAnyLeftDisj
    }

    "reject path that translates to more than 120 characters" in {
      val longName = List.fill(20)(".2345679/").mkString.substring(0, 120)

      longName.length must_== 120
      Collection.fromPath(Path(longName)) must beAnyLeftDisj
    }
  }

  "Collection.asPath" should {

    "handle simple name" in {
      Collection("foo").asPath must_== Path("foo")
    }

    "handle simple path" in {
      Collection("foo.bar").asPath must_== Path("foo/bar")
    }

    "preserve space" in {
      Collection("foo.bar baz").asPath must_== Path("foo/bar baz")
    }

    "unescape leading '.'" in {
      Collection("\\.hidden").asPath must_== Path(".hidden")
    }

    "unescape '$'" in {
      Collection("foo\\d").asPath must_== Path("foo$")
    }

    "unescape '\\'" in {
      Collection("foo\\\\bar").asPath must_== Path("foo\\bar")
    }

    "unescape '.' with path separators" in {
      Collection("foo.bar\\.baz").asPath must_== Path("foo/bar.baz")
    }

    "ignore slash" in {
      Collection("foo/bar").asPath must_== Path("foo/bar")
    }

  }
}
