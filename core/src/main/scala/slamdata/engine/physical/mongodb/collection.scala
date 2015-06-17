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
  def fromPath(path: Path): PathError \/ Collection = {
    // TODO: substitute these in the collection name
    // def segChar: Parser[String] =
    //   "."  ^^ κ("\\.") |
    //   "$"  ^^ κ("\\d") |
    //   "\\" ^^ κ("\\\\") |
    //   "[^/]".r

    val absPath = path.asAbsolute
    absPath.dir.headOption.fold[PathError \/ Collection](
      -\/(InvalidPathError("path names a collection, but no database: " + path)))(
      db => absPath.file.fold[PathError \/ Collection](
        -\/(InvalidPathError("path names a database, but no collection: " + path)))(
        file => {
          val coll = absPath.dir.tail.map(_.value).mkString("", "/", "/") ++ file.value
          if (coll.length > 120)
            -\/(InvalidPathError("collection name too long (> 120 bytes): " + coll))
          else \/-(Collection(db.value, coll))
        }))
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
