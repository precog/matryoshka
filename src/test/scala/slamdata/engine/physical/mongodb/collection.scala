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

    // Potential error cases:
    
    "accept absolute path" in {
      Collection.fromPath(Path("/foo/bar")) must beRightDisj(Collection("foo.bar"))
    }
    
    "not allow name containing '$'" in {
      Collection.fromPath(Path("foo$")) must beAnyLeftDisj
    }
    
    "not allow 'system'" in {
      Collection.fromPath(Path("system")) must beAnyLeftDisj
    }
    
    "not allow name prefixed by 'system.'" in {
      Collection.fromPath(Path("system/foo")) must beAnyLeftDisj
    }
  }

  "Collection.asPath" should {
    
    "handle simple name" in {
      Collection("foo").asPath must_== Path("foo")
    }
    
    "handle simple path" in {
      Collection("foo.bar").asPath must_== Path("foo/bar")
    }
    
    "unescape leading '.'" in {
      Collection("\\.hidden").asPath must_== Path(".hidden")
    }
    
    "unescape '.' with path separators" in {
      Collection("foo.bar\\.baz").asPath must_== Path("foo/bar.baz")
    }
    
    "ignore slash" in {
      Collection("foo/bar").asPath must_== Path("foo/bar")
    }
    
  }
}