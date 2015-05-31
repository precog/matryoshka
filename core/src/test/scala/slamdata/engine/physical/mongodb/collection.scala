package slamdata.engine.physical.mongodb

import org.specs2.mutable._

import slamdata.engine.fp._
import slamdata.engine.fs.{Path}
import slamdata.engine.{DisjunctionMatchers}

class CollectionSpec extends Specification with DisjunctionMatchers {

  "Collection.fromPath" should {

    "handle simple name" in {
      Collection.fromPath(Path("db/foo")) must beRightDisj(Collection("db", "foo"))
    }

    "handle simple relative path" in {
      Collection.fromPath(Path("db/foo/bar")) must beRightDisj(Collection("db", "foo.bar"))
    }

    "escape leading '.'" in {
      Collection.fromPath(Path("db/.hidden")) must beRightDisj(Collection("db", "\\.hidden"))
    }

    "escape '.' with path separators" in {
      Collection.fromPath(Path("db/foo/bar.baz")) must beRightDisj(Collection("db", "foo.bar\\.baz"))
    }

    "escape '$'" in {
      Collection.fromPath(Path("db/foo$")) must beRightDisj(Collection("db", "foo\\d"))
    }

    "escape '\\'" in {
      Collection.fromPath(Path("db/foo\\bar")) must beRightDisj(Collection("db", "foo\\\\bar"))
    }

    "accept absolute path" in {
      Collection.fromPath(Path("/db/foo/bar")) must beRightDisj(Collection("db", "foo.bar"))
    }

    "accept path with 120 characters" in {
      val longName = List.fill(20)("123456789/").mkString.substring(0, 120)
      Collection.fromPath(Path("db/" + longName)) must beAnyRightDisj
    }

    "preserve space" in {
      Collection.fromPath(Path("db/foo/bar baz")) must beRightDisj(Collection("db", "foo.bar baz"))
    }

    "reject path longer than 120 characters" in {
      val longName = List.fill(20)("123456789/").mkString.substring(0, 121)
      Collection.fromPath(Path("db/" + longName)) must beAnyLeftDisj
    }

    "reject path that translates to more than 120 characters" in {
      val longName = List.fill(20)(".2345679/").mkString.substring(0, 120)

      longName.length must_== 120
      Collection.fromPath(Path("db/" + longName)) must beAnyLeftDisj
    }

    "reject path with db but no collection" in {
      Collection.fromPath(Path("db")) must beAnyLeftDisj
    }
  }

  "Collection.asPath" should {

    "handle simple name" in {
      Collection("db", "foo").asPath must_== Path("db/foo")
    }

    "handle simple path" in {
      Collection("db", "foo.bar").asPath must_== Path("db/foo/bar")
    }

    "preserve space" in {
      Collection("db", "foo.bar baz").asPath must_== Path("db/foo/bar baz")
    }

    "unescape leading '.'" in {
      Collection("db", "\\.hidden").asPath must_== Path("db/.hidden")
    }

    "unescape '$'" in {
      Collection("db", "foo\\d").asPath must_== Path("db/foo$")
    }

    "unescape '\\'" in {
      Collection("db", "foo\\\\bar").asPath must_== Path("db/foo\\bar")
    }

    "unescape '.' with path separators" in {
      Collection("db", "foo.bar\\.baz").asPath must_== Path("db/foo/bar.baz")
    }

    "ignore slash" in {
      Collection("db", "foo/bar").asPath must_== Path("db/foo/bar")
    }

  }
}
