package slamdata.engine.fs

import scalaz._
import Scalaz._

import org.specs2.mutable._

import slamdata.engine.{DisjunctionMatchers}

class PathSpecs extends Specification with DisjunctionMatchers {
  "Path.apply" should {
    "Parse empty string as root" in {
      Path("") must_== Path.Root
    }

    "Parse root string as root" in {
      Path("/") must_== Path.Root
    }

    "Parse current as current" in {
      Path(".") must_== Path.Current
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

    "Parse raw file as relative file" in {
      Path("foo") must_== Path.file("." :: Nil, "foo")
    }

    "Parse raw relative file as relative file" in {
      Path("./foo") must_== Path.file("." :: Nil, "foo")
    }

    "Parse raw directory as relative directory" in {
      Path("foo/") must_== Path.dir("." :: "foo" :: Nil)
    }

    "Parse raw relative directory as relative directory" in {
      Path("./foo/") must_== Path.dir("." :: "foo" :: Nil)
    }

    "Parse hidden file as hidden file" in {
      Path(".foo") must_== Path.file("." :: Nil, ".foo")
    }

    "Parse hidden directory as hidden directory" in {
      Path(".foo/") must_== Path.dir("." :: ".foo" :: Nil)
    }
  }

  "Path.++" should {
    "concatentate abs dir with rel file" in {
      Path("/sd/") ++ Path("./tmp/5") must_== Path("/sd/tmp/5")
    }

    "concatentate rel dir with rel dir" in {
      Path("./foo/") ++ Path("./bar/") must_== Path("./foo/bar/")
    }
  }

  "Path.head" should {
    "preserve pure file" in {
      val p = Path("foo")
      p.head must_== p
    }

    "return root for root" in {
      Path("/").head must_== Path("/")
    }

    "return only dir for abs" in {
      Path("/foo/").head must_== Path("/foo/")
    }

    "return parent dir for nested abs" in {
      Path("/foo/bar").head must_== Path("/foo/")
    }

    "return only dir for relative" in {
      Path("foo/").head must_== Path("foo/")
    }

    "return parent dir for relative" in {
      Path("foo/bar/").head must_== Path("foo/")
    }

    "return file for relative fiel" in {
      Path("foo").head must_== Path("foo")
    }

    "return parent dir for relative file" in {
      Path("foo/bar").head must_== Path("foo/")
    }
  }

  "Path.pathname" should {
    "render root correctly" in {
      Path.Root.pathname must_== "/"
    }

    "render current correctly" in {
      Path.Current.pathname must_== "./"
    }

    "render absolute pure dir correctly" in {
      Path("/foo/bar/baz/").pathname must_== "/foo/bar/baz/"
    }

    "render absolute file correctly" in {
      Path("/foo/bar/baz").pathname must_== "/foo/bar/baz"
    }

    "render relative pure dir correctly" in {
      Path("./foo/bar/baz/").pathname must_== "./foo/bar/baz/"
    }

    "render relative file correctly" in {
      Path("./foo/bar/baz").pathname must_== "./foo/bar/baz"
    }
  }

  "Path.relative" should {
    "be false for absolute path" in {
      Path("/foo").relative must beFalse
    }

    "be true for relative path" in {
      Path("./foo").relative must beTrue
    }
  }

  "Path.contains" should {
    "return true when parent contains child dir" in {
      Path("/foo/bar/").contains(Path("/foo/bar/baz/")) must beTrue
    }

    "return true when parent contains child file" in {
      Path("/foo/bar/").contains(Path("/foo/bar/baz")) must beTrue
    }

    "return true for abs path that contains itself" in {
      Path("/foo/bar/").contains(Path("/foo/bar/")) must beTrue
    }

    "return true for rel path when parent contains child dir" in {
      Path("./foo/bar/").contains(Path("./foo/bar/baz/")) must beTrue
    }

    "return true for rel path when parent contains child file" in {
      Path("./foo/bar/").contains(Path("./foo/bar/baz")) must beTrue
    }

    "return true for rel path that contains itself" in {
      Path("./foo/bar/").contains(Path("./foo/bar/")) must beTrue
    }
  }

  "Path.asAbsolute" should {
    "not modify /" in {
      Path("/").asAbsolute must_== Path("/")
    }

    "not modify unnested dir" in {
      Path("/foo/").asAbsolute must_== Path("/foo/")
    }

    "not modify nested dir" in {
      Path("/foo/bar/").asAbsolute must_== Path("/foo/bar/")
    }

    "prefix unnested relative dir" in {
      Path("foo/").asAbsolute must_== Path("/foo/")
    }

    "prefix nested relative dir" in {
      Path("foo/bar/").asAbsolute must_== Path("/foo/bar/")
    }

    "not modify simple file" in {
      Path("/foo").asAbsolute must_== Path("/foo")
    }

    "not modify nested file" in {
      Path("/foo/bar").asAbsolute must_== Path("/foo/bar")
    }

    "prefix simple relative file" in {
      Path("foo").asAbsolute must_== Path("/foo")
    }

    "prefix nested relative file" in {
      Path("foo/bar").asAbsolute must_== Path("/foo/bar")
    }
  }

  "Path.asDir" should {
    "not modify /" in {
      Path("/").asDir must_== Path("/")
    }

    "not modify unnested dir" in {
      Path("/foo/").asDir must_== Path("/foo/")
    }

    "not modify nested dir" in {
      Path("/foo/bar/").asDir must_== Path("/foo/bar/")
    }

    "not modify unnested relative dir" in {
      Path("foo/").asDir must_== Path("foo/")
    }

    "not modify nested relative dir" in {
      Path("foo/bar/").asDir must_== Path("foo/bar/")
    }

    "convert simple file" in {
      Path("/foo").asDir must_== Path("/foo/")
    }

    "convert nested file" in {
      Path("/foo/bar").asDir must_== Path("/foo/bar/")
    }

    "convert simple relative file" in {
      Path("foo").asDir must_== Path("foo/")
    }

    "convert nested relative file" in {
      Path("foo/bar").asDir must_== Path("foo/bar/")
    }
  }

  "Path.parent" should {
    "be root for root" in {
      Path("/").parent must_== Path("/")
    }

    "be root for simple file" in {
      Path("/foo").parent must_== Path("/")
    }

    "be root for dir" in {
      Path("/foo/").parent must_== Path("/")
    }

    "be parent for nested file" in {
      Path("/foo/bar/baz").parent must_== Path("/foo/bar/")
    }

    "be parent for nested dir" in {
      Path("/foo/bar/baz/").parent must_== Path("/foo/bar/")
    }
  }

  "Path.ancestors" should {
    "contain root" in {
      Path("/").ancestors must contain(Path("/"))
    }

    "contain root and not file" in {
      Path("/foo").ancestors must contain(Path("/"))
    }

    "contain root and dir" in {
      Path("/foo/").ancestors must contain(Path("/"), Path("/foo/"))
    }

    "return root, parent, and not file" in {
      Path("/foo/bar").ancestors must contain(Path("/"), Path("/foo/"))
    }

    "return root, parent, and dir" in {
      Path("/foo/bar/").ancestors must contain(Path("/"), Path("/foo/"), Path("/foo/bar/"))
    }
  }

  "Path.rebase" should {
    "match root to root" in {
      Path("/").rebase(Path("/")) must beRightDisj(Path("./"))
    }

    "match dir to same dir" in {
      Path("/foo/").rebase(Path("/foo/")) must beRightDisj(Path("./"))
    }

    "match file to its dir" in {
      Path("/foo/bar").rebase(Path("/foo/")) must beRightDisj(Path("./bar"))
    }

    "match file to parent's dir" in {
      Path("/foo/bar/baz").rebase(Path("/foo/")) must beRightDisj(Path("./bar/baz"))
    }

    "fail with file" in {
      Path("/foo/bar").rebase(Path("/foo")) must beAnyLeftDisj
    }
  }

  "Path.interpret" should {
    "leave relative path intact with matching ref and working dirs" in  {
      Path("foo").interpret(Path("/"), Path("/")) must beRightDisj(Path("foo"))
    }

    "make simple file relative to ref dir" in  {
      Path("bar").interpret(Path("/"), Path("/foo/")) must beRightDisj(Path("foo/bar"))
    }

    "make absolute path relative to ref dir" in  {
      Path("/foo/bar").interpret(Path("/foo/"), Path("/anything/")) must beRightDisj(Path("bar"))
    }

    "fail with path outside ref dir" in {
      Path("/other").interpret(Path("/foo/"), Path("/anything/")) must beAnyLeftDisj
    }

    "fail with relative ref dir" in  {
      Path("foo").interpret(Path("rel/"), Path("/anything/")) must beAnyLeftDisj
    }

    "fail with ref path not a dir" in  {
      Path("foo").interpret(Path("/file"), Path("/anything/")) must beAnyLeftDisj
    }

    "fail with relative working dir" in  {
      Path("foo").interpret(Path("/anything/"), Path("rel/")) must beAnyLeftDisj
    }

    "fail with working path not a dir" in  {
      Path("foo").interpret(Path("/anything/"), Path("/file")) must beAnyLeftDisj
    }
  }

  "FSTable.lookup" should {
    "find root" in {
      FSTable(Map(Path("/") -> "foo")).lookup(Path("/")) must beSome(("foo", Path("/"), Path(".")))
    }

    "find file in root" in {
      FSTable(Map(Path("/") -> "foo")).lookup(Path("/bar")) must beSome(("foo", Path("/"), Path("./bar")))
    }

    "handle no mounts" in {
      FSTable(Map()).lookup(Path("/")) must beNone
    }

    "handle unmounted path" in {
      FSTable(Map(Path("foo") -> "foo")).lookup(Path("/bar")) must beNone
    }

    "find file with two mounts" in {
      FSTable(Map(Path("foo") -> "foo", Path("bar") -> "bar")).lookup(Path("/foo/buz")) must beSome(("foo", Path("/foo/"), Path("./buz")))
    }

    "find nested file with two mounts" in {
      FSTable(Map(Path("foo") -> "foo", Path("bar") -> "bar")).lookup(Path("/bar/buz/quux")) must beSome(("bar", Path("/bar/"), Path("./buz/quux")))
    }
  }

  "FSTable.children" should {
    "find two mounts" in {
      FSTable(Map(Path("foo") -> "foo", Path("bar/buz") -> "buz")).children(Path("/")) must contain(Path("foo/"), Path("bar/"))
    }

    "find one of two mounts" in {
      FSTable(Map(Path("foo") -> "foo", Path("bar/buz") -> "buz")).children(Path("/bar/")) must contain(Path("buz/"))
    }
  }
}
