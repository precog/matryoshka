package quasar
package fs

import quasar.Predef._

import scalaz.syntax.functor._
import scalaz.std.list._
import org.scalacheck._
import pathy.Path._

object PathyGen {

  implicit val arbitraryAbsFile: Arbitrary[AbsFile[Sandboxed]] =
    Arbitrary(Gen.resize(10, genAbsFile))

  implicit val arbitraryRelFile: Arbitrary[RelFile[Sandboxed]] =
    Arbitrary(Gen.resize(10, genRelFile))

  implicit val arbitraryAbsDir: Arbitrary[AbsDir[Sandboxed]] =
    Arbitrary(Gen.resize(10, genAbsDir))

  implicit val arbitraryRelDir: Arbitrary[RelDir[Sandboxed]] =
    Arbitrary(Gen.resize(10, genRelDir))

  def genAbsFile: Gen[AbsFile[Sandboxed]] =
    genRelFile map (rootDir </> _)

  def genRelFile: Gen[RelFile[Sandboxed]] =
    for {
      d <- genRelDir
      s <- genSegment
    } yield d </> file(s)

  def genAbsDir: Gen[AbsDir[Sandboxed]] =
    genRelDir map (rootDir </> _)

  def genRelDir: Gen[RelDir[Sandboxed]] =
    Gen.frequency(
      (  1, Gen.const(currentDir[Sandboxed])),
      (100, Gen.nonEmptyListOf(genSegment)
        .map(_.foldLeft(currentDir)((d, s) => d </> dir(s)))))

  // TODO: Are these special characters MongoDB-specific?
  def genSegment: Gen[String] =
    Gen.nonEmptyListOf(Gen.frequency(
      (100, Arbitrary.arbitrary[Char]) ::
      "$./\\_~ *+-".toList.map(Gen.const).strengthL(10): _*))
      .map(_.mkString)
}
