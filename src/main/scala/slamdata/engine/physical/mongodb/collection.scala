package slamdata.engine.physical.mongodb

import scalaz._
import Scalaz._

import slamdata.engine.{Error}
import slamdata.engine.fs.{Path}

import scala.util.parsing.combinator._
import scala.util.parsing.combinator.lexical._
import scala.util.parsing.combinator.syntactical._
import scala.util.parsing.combinator.token._

case class PathError(hint: Option[String]) extends Error {
  def message = hint match {
    case Some(msg) => msg
    case None => "invalid path"
  }
}

case class Collection(name: String) {
  def asPath: Path = Path(Collection.PathUnparser(name))
}
object Collection {
  def fromPath(path: Path): PathError \/ Collection = PathParser(path.pathname).map(Collection(_))

  object PathParser extends RegexParsers {
    def path: Parser[PathError \/ String] =
      "/" ~> rel | "./" ~> rel

    def rel: Parser[PathError \/ String] =
      sysPrefix |
      pathChar.* ^^ { _.sequenceU.map(_.mkString) }

    def sysPrefix: Parser[PathError \/ String] =
      "system(/.*)?".r ^^ { _ => -\/ (PathError(Some("path starts with 'system'"))) }

    def pathChar: Parser[PathError \/ String] =
      "/"   ^^ { _ =>  \/- (".") } |
      "."   ^^ { _ =>  \/- ("\\.") } |
      "$"   ^^ { _ => -\/ (PathError(Some("path contains $"))) } |
      ".".r ^^ { c =>  \/- (c) }

    def apply(input: String): PathError \/ String = parseAll(path, input) match {
      case Success(result, _) => result
      case failure : NoSuccess => -\/ (PathError(Some(failure.msg)))
    }
  }

  object PathUnparser extends RegexParsers {
    def name = nameChar.* ^^ { _.sequenceU.mkString }

    def nameChar =
      "\\." ^^ { _ => "." } |
      "."   ^^ { _ => "/" } |
      ".".r

    def apply(input: String): String = parseAll(name, input) match {
      case Success(result, _) => result
      case failure : NoSuccess => scala.sys.error("doesn't happen")
    }
  }

  implicit val ShowCollection = new Show[Collection] {
    override def show(v: Collection) = v.toString
  }
}

