package slamdata.engine.physical.mongodb

import scalaz._
import Scalaz._

import slamdata.engine.fs._

import scala.util.parsing.combinator._
import scala.util.parsing.combinator.lexical._
import scala.util.parsing.combinator.syntactical._
import scala.util.parsing.combinator.token._

case class Collection(name: String) {
  def asPath: Path = Path(Collection.PathUnparser(name))

  override def toString = s"""Collection("$name")"""
}
object Collection {
  def fromPath(path: Path): PathError \/ Collection = PathParser(path.pathname).map(Collection(_))

  object PathParser extends RegexParsers {
    def path: Parser[String] =
      "/" ~> rel | "./" ~> rel

    def rel: Parser[String] =
      pathChar.* ^^ { _.mkString }

    def pathChar: Parser[String] =
      "/"  ^^ { _ => "."    } |
      "."  ^^ { _ => "\\."  } |
      "$"  ^^ { _ => "\\d"  } |
      "\\" ^^ { _ => "\\\\" } |
      ".".r

    def apply(input: String): PathError \/ String = parseAll(path, input) match {
      case Success(result, _) if result.length > 120 => -\/ (PathError(Some("collection name too long (> 120 bytes): " + result)))
      case Success(result, _)                        =>  \/- (result)
      
      case failure : NoSuccess                       => -\/  (PathError(Some(failure.msg)))
    }
  }

  object PathUnparser extends RegexParsers {
    def name = nameChar.* ^^ { _.mkString }

    def nameChar =
      "\\."  ^^ { _ => "." } |
      "\\d"  ^^ { _ => "$" } |
      "\\\\" ^^ { _ => "\\" } |
      "."    ^^ { _ => "/" } |
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
