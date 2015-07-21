/*
 * Copyright 2014 - 2015 SlamData Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package slamdata.engine.physical.mongodb

import scalaz._
import Scalaz._

import slamdata.engine.{RenderTree, Terminal}
import slamdata.engine.fp._
import slamdata.engine.fs._

import scala.util.parsing.combinator._

final case class Collection(databaseName: String, collectionName: String) {
  import Collection._

  def asPath: Path = {
    val first = DatabaseNameUnparser(databaseName)
    val rest = CollectionNameUnparser(collectionName)
    val segs = NonEmptyList(first, rest: _*)
    Path(DirNode.Current :: segs.list.dropRight(1).map(DirNode(_)), Some(FileNode(segs.last)))
  }
}
object Collection {
  import PathError._

  def fromPath(path: Path): PathError \/ Collection = {
    val rel = path.asRelative
    val segs = rel.dir.map(_.value) ++ rel.file.map(_.value).toList
    for {
      first    <- segs.drop(1).headOption \/> PathTypeError(path, Some("has no segments"))
      rest     =  segs.drop(2)
      db       <- DatabaseNameParser(first)
      collSegs <- rest.map(CollectionSegmentParser(_)).sequenceU
      _        <- if (collSegs.isEmpty)
                    -\/(InvalidPathError("path names a database, but no collection: " + path))
                  else \/-(())
      coll     =  collSegs.mkString(".")
      _        <- if (utf8length(db) + 1 + utf8length(coll) > 120)
                    -\/(InvalidPathError("database/collection name too long (> 120 bytes): " + db + "." + coll))
                  else \/-(())
    } yield Collection(db, coll)
  }

  private trait PathParser extends RegexParsers {
    override def skipWhitespace = false

    protected def substitute(pairs: List[(String, String)]): Parser[String] =
      pairs.foldLeft[Parser[String]](failure("no match")) {
        case (acc, (a, b)) => (a ^^ κ(b)) | acc
      }
  }

  def utf8length(str: String) = str.getBytes("UTF-8").length

  val DatabaseNameEscapes = List(
    " "  -> "+",
    "."  -> "~",
    "$"  -> "$$",
    "+"  -> "$add",
    "~"  -> "$tilde",
    "/"  -> "$div",
    "\\" -> "$esc",
    "\"" -> "$quot",
    "*"  -> "$mul",
    "<"  -> "$lt",
    ">"  -> "$gt",
    ":"  -> "$colon",
    "|"  -> "$bar",
    "?"  -> "$qmark")

  private object DatabaseNameParser extends PathParser {
    def name: Parser[String] =
      char.* ^^ { _.mkString }

    def char: Parser[String] = substitute(DatabaseNameEscapes) | "(?m).".r

    def apply(input: String): PathError \/ String = parseAll(name, input) match {
      case Success(name, _) =>
        if (utf8length(name) > 64)
          -\/(InvalidPathError("database name too long (> 64 bytes): " + name))
        else \/-(name)
      case failure : NoSuccess =>
        -\/(InvalidPathError("failed to parse ‘" + input + "’: " + failure.msg))
    }
  }

  private object DatabaseNameUnparser extends PathParser {
    def name = nameChar.* ^^ { _.mkString }

    def nameChar = substitute(DatabaseNameEscapes.map(_.swap)) | "(?m).".r

    def apply(input: String): String = parseAll(name, input) match {
      case Success(result, _) => result
      case failure : NoSuccess => scala.sys.error("doesn't happen")
    }
  }

  val CollectionNameEscapes = List(
    "."  -> "\\.",
    "$"  -> "\\d",
    "\\" -> "\\\\")

  private object CollectionSegmentParser extends PathParser {
    def seg: Parser[String] =
      char.* ^^ { _.mkString }

    def char: Parser[String] = substitute(CollectionNameEscapes) | "(?m).".r

    def apply(input: String): PathError \/ String = parseAll(seg, input) match {
      case Success(seg, _) => \/-(seg)
      case failure : NoSuccess =>
        -\/(InvalidPathError("failed to parse ‘" + input + "’: " + failure.msg))
    }
  }

  private object CollectionNameUnparser extends PathParser {
    def name = repsep(seg, ".")

    def seg = segChar.* ^^ { _.mkString }

    def segChar = substitute(CollectionNameEscapes.map(_.swap)) | "(?m)[^.]".r

    def apply(input: String): List[String] = parseAll(name, input) match {
      case Success(result, _) => result
      case failure : NoSuccess => scala.sys.error("doesn't happen")
    }
  }

  implicit val CollectionRenderTree = new RenderTree[Collection] {
    def render(v: Collection) = Terminal(List("Collection"), Some(v.databaseName + "; " + v.collectionName))
  }
}
