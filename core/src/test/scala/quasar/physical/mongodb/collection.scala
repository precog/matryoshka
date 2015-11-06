package quasar.physical.mongodb

import quasar.Predef._
import quasar.fs.{Path, PathyGen}

import org.specs2.mutable._
import org.specs2.scalaz._
import org.specs2.ScalaCheck
import org.scalacheck._

class CollectionSpec extends Specification with ScalaCheck with DisjunctionMatchers {

  "Collection.fromPath" should {

    "handle simple name" in {
      Collection.fromPath(Path("db/foo")) must beRightDisjunction(Collection("db", "foo"))
    }

    "handle simple relative path" in {
      Collection.fromPath(Path("db/foo/bar")) must beRightDisjunction(Collection("db", "foo.bar"))
    }

    "escape leading '.'" in {
      Collection.fromPath(Path("db/.hidden")) must beRightDisjunction(Collection("db", "\\.hidden"))
    }

    "escape '.' with path separators" in {
      Collection.fromPath(Path("db/foo/bar.baz")) must beRightDisjunction(Collection("db", "foo.bar\\.baz"))
    }

    "escape '$'" in {
      Collection.fromPath(Path("db/foo$")) must beRightDisjunction(Collection("db", "foo\\d"))
    }

    "escape '\\'" in {
      Collection.fromPath(Path("db/foo\\bar")) must beRightDisjunction(Collection("db", "foo\\\\bar"))
    }

    "accept absolute path" in {
      Collection.fromPath(Path("/db/foo/bar")) must beRightDisjunction(Collection("db", "foo.bar"))
    }

    "accept path with 120 characters" in {
      val longName = ("db/" + List.fill(20)("123456789/").mkString).substring(0, 120)
      Collection.fromPath(Path(longName)) must beRightDisjunction
    }

    "preserve space" in {
      Collection.fromPath(Path("db/foo/bar baz")) must beRightDisjunction(Collection("db", "foo.bar baz"))
    }

    "reject path longer than 120 characters" in {
      val longName = ("db/" + List.fill(20)("123456789/").mkString).substring(0, 121)
      Collection.fromPath(Path(longName)) must beLeftDisjunction
    }

    "reject path that translates to more than 120 characters" in {
      val longName = ("db/" + List.fill(20)(".2345679/").mkString).substring(0, 120)

      longName.length must_== 120
      Collection.fromPath(Path(longName)) must beLeftDisjunction
    }

    "reject path with db but no collection" in {
      Collection.fromPath(Path("db")) must beLeftDisjunction
    }

    "escape space in db name" in {
      Collection.fromPath(Path("db 1/foo")) must beRightDisjunction(Collection("db+1", "foo"))
    }

    "escape leading dot in db name" in {
      Collection.fromPath(Path(".trash/foo")) must beRightDisjunction(Collection("~trash", "foo"))
    }

    "escape MongoDB-reserved chars in db name" in {
      import quasar.fs._

      Collection.fromPath(Path(List(DirNode("db/\\\"")), Some(FileNode("foo")))) must
        beRightDisjunction(Collection("db$div$esc$quot", "foo"))
    }

    "escape Windows-only MongoDB-reserved chars in db name" in {
      Collection.fromPath(Path("db*<>:|?/foo")) must beRightDisjunction(Collection("db$mul$lt$gt$colon$bar$qmark", "foo"))
    }

    "escape escape characters in db name" in {
      Collection.fromPath(Path("db$+~/foo")) must beRightDisjunction(Collection("db$$$add$tilde", "foo"))
    }

    "fail with sequence of escapes exceeding maximum length" in {
      Collection.fromPath(Path("~:?~:?~:?~:/foo")) must beLeftDisjunction
    }

    "succeed with db name of exactly 64 bytes when encoded" in {
      val dbName = List.fill(64/4)("💩").mkString
      Collection.fromPath(Path(dbName + "/foo")) must beRightDisjunction
    }

    "fail with db name exceeding 64 bytes when encoded" in {
      val dbName = List.fill(64/4 + 1)("💩").mkString
      Collection.fromPath(Path(dbName + "/foo")) must beLeftDisjunction
    }

    import PathGen._

    "never emit an invalid db name" ! prop { (p: Path) =>
      // NB: as long as the path is not too long, it should convert to something that's legal
      (p.pathname.length < 30) ==> {
        Collection.fromPath(p).fold(
          err => scala.sys.error(err.toString),
          coll => {
            " ./\\*<>:|?".foreach { c => coll.databaseName.toList must not(contain(c)) }
          })
      }
    }

    "succeed with crazy char" in {
      Collection.fromPath(Path("/*_/_ⶡ\\݅ ᐠ/儨")) must beRightDisjunction
    }

    "round-trip" ! prop { (p: Path) =>
      // NB: the path might be too long to convert
      val v = Collection.fromPath(p)
      (v.isRight) ==> {
        v.fold(
          err => scala.sys.error(err.toString),
          coll => coll.asPath must_== p
        )
      }
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
      import quasar.fs._

      Collection("db", "foo/bar").asPath must_== Path(List(DirNode.Current, DirNode("db")), Some(FileNode("foo/bar")))
    }

    "ignore unrecognized escape in database name" in {
      Collection("$foo", "bar").asPath must_== Path("$foo/bar")
    }

    "not explode on empty collection name" in {
      import quasar.fs._

      Collection("foo", "").asPath must_== Path(List(DirNode.Current, DirNode("foo")), Some(FileNode("")))
    }
  }

  // Pathy versions

  "Collection.fromPathy" should {
    import pathy.Path._

    "handle simple name" in {
      Collection.fromFile(rootDir </> dir("db") </> file("foo")) must
        beRightDisjunction(Collection("db", "foo"))
    }

    "handle simple relative path" in {
      Collection.fromFile(rootDir </> dir("db") </> dir("foo") </> file("bar")) must
        beRightDisjunction(Collection("db", "foo.bar"))
    }

    "escape leading '.'" in {
      Collection.fromFile(rootDir </> dir("db") </> file(".hidden")) must
        beRightDisjunction(Collection("db", "\\.hidden"))
    }

    "escape '.' with path separators" in {
      Collection.fromFile(rootDir </> dir("db") </> dir("foo") </> file("bar.baz")) must
        beRightDisjunction(Collection("db", "foo.bar\\.baz"))
    }

    "escape '$'" in {
      Collection.fromFile(rootDir </> dir("db") </> file("foo$")) must
        beRightDisjunction(Collection("db", "foo\\d"))
    }

    "escape '\\'" in {
      Collection.fromFile(rootDir </> dir("db") </> file("foo\\bar")) must
        beRightDisjunction(Collection("db", "foo\\\\bar"))
    }

    "accept path with 120 characters" in {
      val longName = Stream.continually("A").take(117).mkString
      Collection.fromFile(rootDir </> dir("db") </> file(longName)) must
        beRightDisjunction(Collection("db", longName))
    }

    "reject path longer than 120 characters" in {
      val longName = Stream.continually("B").take(118).mkString
      Collection.fromFile(rootDir </> dir("db") </> file(longName)) must beLeftDisjunction
    }

    "reject path that translates to more than 120 characters" in {
      val longName = "." + Stream.continually("C").take(116).mkString
      Collection.fromFile(rootDir </> dir("db") </> file(longName)) must beLeftDisjunction
    }

    "preserve space" in {
      Collection.fromFile(rootDir </> dir("db") </> dir("foo") </> file("bar baz")) must
        beRightDisjunction(Collection("db", "foo.bar baz"))
    }

    "reject path without db or collection" in {
      Collection.fromDir(rootDir) must beLeftDisjunction
    }

    "reject path with db but no collection" in {
      Collection.fromDir(rootDir </> dir("db")) must beLeftDisjunction
    }

    "escape space in db name" in {
      Collection.fromFile(rootDir </> dir("db 1") </> file("foo")) must
        beRightDisjunction(Collection("db+1", "foo"))
    }

    "escape leading dot in db name" in {
      Collection.fromFile(rootDir </> dir(".trash") </> file("foo")) must
        beRightDisjunction(Collection("~trash", "foo"))
    }

    "escape MongoDB-reserved chars in db name" in {
      Collection.fromFile(rootDir </> dir("db/\\\"") </> file("foo")) must
        beRightDisjunction(Collection("db$div$esc$quot", "foo"))
    }

    "escape Windows-only MongoDB-reserved chars in db name" in {
      Collection.fromFile(rootDir </> dir("db*<>:|?") </> file("foo")) must
        beRightDisjunction(Collection("db$mul$lt$gt$colon$bar$qmark", "foo"))
    }

    "escape escape characters in db name" in {
      Collection.fromFile(rootDir </> dir("db$+~") </> file("foo")) must
        beRightDisjunction(Collection("db$$$add$tilde", "foo"))
    }

    "fail with sequence of escapes exceeding maximum length" in {
      Collection.fromFile(rootDir </> dir("~:?~:?~:?~:") </> file("foo")) must beLeftDisjunction
    }

    "succeed with db name of exactly 64 bytes when encoded" in {
      val dbName = List.fill(64/4)("💩").mkString
      Collection.fromFile(rootDir </> dir(dbName) </> file("foo")) must beRightDisjunction
    }

    "fail with db name exceeding 64 bytes when encoded" in {
      val dbName = List.fill(64/4 + 1)("💩").mkString
      Collection.fromFile(rootDir </> dir(dbName) </> file("foo")) must beLeftDisjunction
    }

    "succeed with crazy char" in {
      Collection.fromFile(rootDir </> dir("*_") </> dir("_ⶡ\\݅ ᐠ") </> file("儨")) must
        beRightDisjunction
    }

    import PathyGen._

    "never emit an invalid db name" ! prop { f: AbsFile[Sandboxed] =>
      val notInRootDir = parentDir(f).fold(false)(_ != rootDir)
      val notTooLong = posixCodec.printPath(f).length < 30
      // NB: as long as the path is not too long, it should convert to something that's legal
      (notInRootDir && notTooLong) ==> {
        Collection.fromFile(f).fold(
          err => scala.sys.error(err.toString),
          coll => {
            " ./\\*<>:|?".foreach { c => coll.databaseName.toList must not(contain(c)) }
          })
      }
    }

    "round-trip" ! prop { f: AbsFile[Sandboxed] =>
      // NB: the path might be too long to convert
      val r = Collection.fromFile(f)
      (r.isRight) ==> {
        r.fold(
          err  => scala.sys.error(err.toString),
          coll => identicalPath(f, coll.asFile) must beTrue)
      }
    }
  }

  "Collection.asFile" should {
    import pathy.Path._

    "handle simple name" in {
      Collection("db", "foo").asFile must_==
        rootDir </> dir("db") </> file("foo")
    }

    "handle simple path" in {
      Collection("db", "foo.bar").asFile must_==
        rootDir </> dir("db") </> dir("foo") </> file("bar")
    }

    "preserve space" in {
      Collection("db", "foo.bar baz").asFile must_==
        rootDir </> dir("db") </> dir("foo") </> file("bar baz")
    }

    "unescape leading '.'" in {
      Collection("db", "\\.hidden").asFile must_==
        rootDir </> dir("db") </> file(".hidden")
    }

    "unescape '$'" in {
      Collection("db", "foo\\d").asFile must_==
        rootDir </> dir("db") </> file("foo$")
    }

    "unescape '\\'" in {
      Collection("db", "foo\\\\bar").asFile must_==
        rootDir </> dir("db") </> file("foo\\bar")
    }

    "unescape '.' with path separators" in {
      Collection("db", "foo.bar\\.baz").asFile must_==
        rootDir </> dir("db") </> dir("foo") </> file("bar.baz")
    }

    "ignore slash" in {
      Collection("db", "foo/bar").asFile must_==
        rootDir </> dir("db") </> file("foo/bar")
    }

    "ignore unrecognized escape in database name" in {
      Collection("$foo", "bar").asFile must_==
        rootDir </> dir("$foo") </> file("bar")
    }

    "not explode on empty collection name" in {
      Collection("foo", "").asFile must_==
        rootDir </> dir("foo") </> file("")
    }
  }
}

object PathGen {
  import quasar.fs._

  implicit val arbitraryPath: Arbitrary[Path] = Arbitrary(Gen.resize(10, pathGen))

  def pathGen: Gen[Path] = for {
    ds <- Gen.nonEmptyListOf(genDir)
    f <- genFile
  } yield Path(DirNode.Current :: ds, Some(f))

  def genDir: Gen[DirNode] = for {
    n <- genName
    d <- Gen.const(DirNode(n))
  } yield d
  def genFile: Gen[FileNode] =  for {
    n <- genName
    f <- Gen.const(FileNode(n))
  } yield f

  def genName: Gen[String] = Gen.nonEmptyListOf(
    Gen.oneOf(
      Gen.oneOf("$./\\_~ *+-".toList),  // NB: boost the frequency of reserved chars
      Arbitrary.arbitrary[Char])).map(_.mkString)
}
