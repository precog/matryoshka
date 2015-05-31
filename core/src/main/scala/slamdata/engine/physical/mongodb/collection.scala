package slamdata.engine.physical.mongodb

import scalaz._

import slamdata.engine.{RenderTree, Terminal}
import slamdata.engine.fp._
import slamdata.engine.fs._

import scala.util.parsing.combinator._

final case class Collection(databaseName: String, collectionName: String) {
  def asPath: Path = Path(databaseName + '/' + Collection.PathUnparser(collectionName))
}
object Collection {
  def fromPath(path: Path): PathError \/ Collection = PathParser(path.pathname).map((Collection.apply _).tupled)

  object PathParser extends RegexParsers {
    override def skipWhitespace = false

    def path: Parser[(String, Option[String])] =
      ("/" | "./") ~> seg ~ rel ^^ { case db ~ coll => (db, coll) }

    def rel: Parser[Option[String]] =
      opt("/" ~> (repsep(seg, "/")) ^^ (_.mkString(".")))

    def seg: Parser[String] =
      segChar.* ^^ { _.mkString }

    def segChar: Parser[String] =
      "."  ^^ κ("\\.") |
      "$"  ^^ κ("\\d") |
      "\\" ^^ κ("\\\\") |
      "[^/]".r

    def apply(input: String): PathError \/ (String, String) = parseAll(path, input) match {
      case Success((db, Some(coll)), _) =>
        if (coll.length > 120)
          -\/(PathError(Some("collection name too long (> 120 bytes): " + coll)))
        else \/-((db, coll))
      case Success((db, None), _) =>
        -\/(PathError(Some("path names a database, but no collection: " + input)))
      case failure : NoSuccess => -\/(PathError(Some("failed to parse ‘" + input + "’: " + failure.msg)))
    }
  }

  object PathUnparser extends RegexParsers {
    override def skipWhitespace = false

    def name = nameChar.* ^^ { _.mkString }

    def nameChar =
      "\\."  ^^ κ(".") |
      "\\d"  ^^ κ("$") |
      "\\\\" ^^ κ("\\") |
      "."    ^^ κ("/") |
      ".".r

    def apply(input: String): String = parseAll(name, input) match {
      case Success(result, _) => result
      case failure : NoSuccess => scala.sys.error("doesn't happen")
    }
  }

  implicit val CollectionRenderTree = new RenderTree[Collection] {
    def render(v: Collection) = Terminal(List("Collection"), Some(v.databaseName + "; " + v.collectionName))
  }
}
