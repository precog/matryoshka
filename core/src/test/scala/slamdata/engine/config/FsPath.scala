package slamdata.engine.config

import slamdata.Predef._

import pathy._, Path._
import org.specs2.mutable._

class FsPathSpec extends Specification {
  import windowsCodec._
  import FsPath._

  "printFsPath" should {
    "equal printPath when uniform" in {
      val p: RelFile[Sandboxed] = dir("foo") </> dir("bar") </> file("quux.txt")
      printFsPath(windowsCodec, Uniform(p)) must_== printPath(p)
    }

    "include volume when in volume" in {
      val s = "c:\\bat\\quux\\blaat.png"

      parseWinAbsFile(s) map { fp =>
        printFsPath(windowsCodec, fp) must_== s"c:${printPath(fp.path)}"
      } getOrElse ko
    }
  }

  "parseWinAbsDir" should {
    "support volume + path" in {
      val d = "d:\\foo\\bar\\"
      parseWinAbsDir(d).map(printFsPath(windowsCodec, _)) must beSome(d)
    }

    "support UNC paths" in {
      val d = "\\\\d\\foo\\bar\\"
      parseWinAbsDir(d).map(printFsPath(windowsCodec, _)) must beSome(d)
    }
  }

  "parseWinAbsFile" should {
    "support volume + path" in {
      val f = "c:\\over\\there\\pic.jpg"
      parseWinAbsFile(f).map(printFsPath(windowsCodec, _)) must beSome(f)
    }

    "support UNC paths" in {
      val f = "\\\\c\\over\\there\\pic.jpg"
      parseWinAbsFile(f).map(printFsPath(windowsCodec, _)) must beSome(f)
    }
  }
}
