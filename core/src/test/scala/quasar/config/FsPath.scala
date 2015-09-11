package quasar.config

import quasar.Predef._

import pathy._, Path._
import org.specs2.mutable._

class FsPathSpec extends Specification {
  import windowsCodec.printPath
  import FsPath._

  def printWin[T](fp: FsPath[T, Sandboxed]) = printFsPath[T](windowsCodec, fp)
  def printPosix[T](fp: FsPath[T, Sandboxed]) = printFsPath[T](posixCodec, fp)

  "printFsPath" should {
    "equal printPath when uniform" in {
      val p = Uniform(dir[Sandboxed]("foo")) </> dir("bar") </> file("quux.txt")
      printWin(p) ==== printPath(p.path)
    }

    "include volume when in volume" in {
      val p = InVolume("c:", rootDir[Sandboxed]) </> dir("bat") </> dir("quux") </> file("blaat.png")
      printWin(p) ==== s"c:${printPath(p.path)}"
    }
  }

  "sandboxFsPathIn" should {
    def pAbs[S] = rootDir[S] </> dir("foo") </> dir("bar") </> file("baz.txt")
    def pRel[S] = dir[S]("bar") </> dir("quux") </> dir("foo")

    "sandbox uniform abs path when dir is a prefix" in {
      val uni = Uniform(pAbs[Unsandboxed])
      val base = rootDir[Sandboxed] </> dir("foo")
      sandboxFsPathIn(base, uni).map(printWin) ==== Some(printPath(pAbs))
    }

    "sandbox uniform rel path when dir is a prefix" in {
      val uni = Uniform(pRel[Unsandboxed])
      val base = dir[Sandboxed]("bar") </> dir("quux")
      sandboxFsPathIn(base, uni).map(printWin) ==== Some(printPath(pRel))
    }

    "fail when sandbox dir not a prefix" in {
      val uni = Uniform(dir[Unsandboxed]("bar") </> dir("foo"))
      val base = dir[Sandboxed]("sbox")
      sandboxFsPathIn(base, uni) must beNone
    }

    "sandbox volume path in given dir" in {
      val vol = InVolume("e:", pAbs[Unsandboxed])
      val base = rootDir[Sandboxed] </> dir("foo") </> dir("bar")
      sandboxFsPathIn(base, vol).map(printWin) ==== Some("e:\\foo\\bar\\baz.txt")
    }

    "fail for volume path when sandbox dir not a prefix" in {
      val vol = InVolume("f:", rootDir[Unsandboxed] </> file("foo.txt"))
      val base = rootDir[Sandboxed] </> dir("quux")
      sandboxFsPathIn(base, vol) must beNone
    }
  }

  "parseWinAbsDir" should {
    "support volume + path" in {
      val d = "d:\\foo\\bar\\"
      parseWinAbsDir(d).map(printWin(_)) must beSome(d)
    }

    "support UNC paths" in {
      val d = "\\\\d\\foo\\bar\\"
      parseWinAbsDir(d).map(printWin(_)) must beSome(d)
    }
  }

  "parseWinAbsFile" should {
    "support volume + path" in {
      val f = "c:\\over\\there\\pic.jpg"
      parseWinAbsFile(f).map(printWin(_)) must beSome(f)
    }

    "support UNC paths" in {
      val f = "\\\\c\\over\\there\\pic.jpg"
      parseWinAbsFile(f).map(printWin(_)) must beSome(f)
    }
  }

  "parseAbsDir" should {
    "for windows" in {
      parseAbsDir(OS.windows, "c:\\foo\\").map(printWin(_)) must beSome("c:\\foo\\")
    }

    "for mac" in {
      parseAbsDir(OS.mac, "/foo/").map(printPosix(_)) must beSome("/foo/")
    }

    "for posix" in {
      parseAbsDir(OS.posix, "/foo/").map(printPosix(_)) must beSome("/foo/")
    }

    "be none when not dir" in {
      parseAbsDir(OS.posix, "/foo/bar.txt") must beNone
    }

    "be none when not absolute" in {
      parseAbsDir(OS.posix, "./foo/") must beNone
    }
  }

  "parseFile" should {
    "windows relative" in {
      parseFile(OS.windows, ".\\foo\\bar.txt").map(printWin(_)) must beSome(".\\foo\\bar.txt")
    }

    "windows absolute" in {
      parseFile(OS.windows, "d:\\foo\\bar.txt").map(printWin(_)) must beSome("d:\\foo\\bar.txt")
    }

    "windows dir" in {
      parseFile(OS.windows, "c:\\foo\\bar\\") must beNone
    }

    "mac relative" in {
      parseFile(OS.mac, "./foo/bar.txt").map(printPosix(_)) must beSome("./foo/bar.txt")
    }

    "mac absolute" in {
      parseFile(OS.mac, "/foo/bar.txt").map(printPosix(_)) must beSome("/foo/bar.txt")
    }

    "mac dir" in {
      parseFile(OS.mac, "/foo/") must beNone
    }

    "posix relative" in {
      parseFile(OS.posix, "./bar/foo.txt").map(printPosix(_)) must beSome("./bar/foo.txt")
    }

    "posix absolute" in {
      parseFile(OS.posix, "/bar/foo.txt").map(printPosix(_)) must beSome("/bar/foo.txt")
    }

    "posix dir" in {
      parseFile(OS.posix, "/bar/foo/") must beNone
    }
  }

}
