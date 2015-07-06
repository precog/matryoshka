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
      _        <- if (db.length + 1 + coll.length > 120)
                    -\/(InvalidPathError("database/collection name too long (> 120 bytes): " + db + "." + coll))
                  else \/-(())
    } yield Collection(db, coll)
  }

  object DatabaseNameParser extends RegexParsers {
    override def skipWhitespace = false

    def name: Parser[String] =
      char.* ^^ { _.mkString }

    def char: Parser[String] =
      "$"  ^^ κ("$dollar") |
      "+"  ^^ κ("$plus") |
      "~"  ^^ κ("$tilde") |
      " "  ^^ κ("+") |
      "."  ^^ κ("~") |
      "/"  ^^ κ("$slash") |
      "\\" ^^ κ("$bslash") |
      "\"" ^^ κ("$quote") |
      "*"  ^^ κ("$times") |
      "<"  ^^ κ("$less") |
      ">"  ^^ κ("$greater") |
      ":"  ^^ κ("$colon") |
      "|"  ^^ κ("$bar") |
      "?"  ^^ κ("$qmark") |
      ".".r

    def apply(input: String): PathError \/ String = parseAll(name, input) match {
      case Success(name, _) =>
        if (name.length > 63)
          -\/(InvalidPathError("database name too long (> 63 chars): " + name))
        else \/-(name)
      case failure : NoSuccess =>
        -\/(InvalidPathError("failed to parse ‘" + input + "’: " + failure.msg))
    }
  }

  object CollectionSegmentParser extends RegexParsers {
    override def skipWhitespace = false

    def seg: Parser[String] =
      char.* ^^ { _.mkString }

    def char: Parser[String] =
      "."  ^^ κ("\\.") |
      "$"  ^^ κ("\\d") |
      "\\" ^^ κ("\\\\") |
      ".".r

    def apply(input: String): PathError \/ String = parseAll(seg, input) match {
      case Success(seg, _) => \/-(seg)
      case failure : NoSuccess =>
        -\/(InvalidPathError("failed to parse ‘" + input + "’: " + failure.msg))
    }
  }

  object DatabaseNameUnparser extends RegexParsers {
    override def skipWhitespace = false

    def name = nameChar.* ^^ { _.mkString }

    def nameChar =
      "$dollar"  ^^ κ("$")  |
      "$plus"    ^^ κ("+")  |
      "$tilde"   ^^ κ("~")  |
      "+"        ^^ κ(" ")  |
      "~"        ^^ κ(".")  |
      "$slash"   ^^ κ("/")  |
      "$bslash"  ^^ κ("\\") |
      "$quote"   ^^ κ("\"") |
      "$times"   ^^ κ("*" ) |
      "$less"    ^^ κ("<" ) |
      "$greater" ^^ κ(">")  |
      "$colon"   ^^ κ(":")  |
      "$bar"     ^^ κ("|")  |
      "$qmark"   ^^ κ("?")  |
      ".".r

    def apply(input: String): String = parseAll(name, input) match {
      case Success(result, _) => result
      case failure : NoSuccess => scala.sys.error("doesn't happen")
    }
  }

  object CollectionNameUnparser extends RegexParsers {
    override def skipWhitespace = false

    def name = repsep(seg, ".")

    def seg = segChar.* ^^ { _.mkString }

    def segChar =
      "\\."  ^^ κ(".") |
      "\\d"  ^^ κ("$") |
      "\\\\" ^^ κ("\\") |
      "[^.]".r

    def apply(input: String): List[String] = parseAll(name, input) match {
      case Success(result, _) => result
      case failure : NoSuccess => scala.sys.error("doesn't happen")
    }
  }

  implicit val CollectionRenderTree = new RenderTree[Collection] {
    def render(v: Collection) = Terminal(List("Collection"), Some(v.databaseName + "; " + v.collectionName))
  }
}
