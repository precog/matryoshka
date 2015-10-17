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

package quasar.api

import quasar.Predef._

import org.http4s.util.CaseInsensitiveString
import org.http4s.{Header, MediaType}
import org.http4s.headers.{Accept, `Content-Disposition`}

import quasar.DataCodec

import scalaz.NonEmptyList

sealed trait MessageFormat {
  def mediaType: MediaType
  def disposition: Option[`Content-Disposition`]
}
object MessageFormat {
  final case class JsonStream private[MessageFormat](codec: DataCodec, mode: String, disposition: Option[`Content-Disposition`]) extends MessageFormat {
    def mediaType = JsonStream.mediaType.withExtensions(Map("mode" -> mode))
  }
  object JsonStream {
    val mediaType = new MediaType("application", "ldjson", compressible = true)

    val Readable = JsonStream(DataCodec.Readable, "readable", None)
    val Precise  = JsonStream(DataCodec.Precise,  "precise", None)
  }

  final case class JsonArray private[MessageFormat](codec: DataCodec, mode: String, disposition: Option[`Content-Disposition`]) extends MessageFormat {
    def mediaType = JsonArray.mediaType.withExtensions(Map("mode" -> mode))
  }
  object JsonArray {
    def mediaType = MediaType.`application/json`

    val Readable = JsonArray(DataCodec.Readable, "readable", None)
    val Precise  = JsonArray(DataCodec.Precise,  "precise", None)
  }

  final case class Csv(columnDelimiter: Char, rowDelimiter: String, quoteChar: Char, escapeChar: Char, disposition: Option[`Content-Disposition`]) extends MessageFormat {
    import Csv._

    def mediaType = Csv.mediaType.withExtensions(Map(
      "columnDelimiter" -> escapeNewlines(columnDelimiter.toString),
      "rowDelimiter" -> escapeNewlines(rowDelimiter),
      "quoteChar" -> escapeNewlines(quoteChar.toString),
      "escapeChar" -> escapeNewlines(escapeChar.toString)))
  }
  object Csv {
    val mediaType = MediaType.`text/csv`

    val Default = Csv(',', "\r\n", '"', '"', None)

    def escapeNewlines(str: String): String =
      str.replace("\r", "\\r").replace("\n", "\\n")

    def unescapeNewlines(str: String): String =
      str.replace("\\r", "\r").replace("\\n", "\n")
  }

  case object UnsupportedContentType extends scala.Exception

  def fromAccept(accept: Option[Accept]): MessageFormat = {
    val mediaTypes = NonEmptyList(
      JsonStream.mediaType,
      new MediaType("application", "x-ldjson"),
      JsonArray.mediaType,
      Csv.mediaType)

    (for {
      acc       <- accept
      // TODO: MediaRange needs an Order instance – combining QValue ordering
      //       with specificity (EG, application/json sorts before
      //       application/* if they have the same q-value).
      mediaType <- acc.values.sortBy(_.qValue).list.find(a => mediaTypes.list.exists(a.satisfies(_)))
    } yield {
      import org.http4s.parser.HttpHeaderParser.parseHeader
      val disposition = mediaType.extensions.get("disposition").flatMap { str =>
        parseHeader(Header.Raw(CaseInsensitiveString("Content-Disposition"), str)).toOption.map(_.asInstanceOf[`Content-Disposition`])
      }
      if (mediaType satisfies Csv.mediaType) {
        def toChar(str: String): Option[Char] = str.toList match {
          case c :: Nil => Some(c)
          case _ => None
        }
        Csv(mediaType.extensions.get("columnDelimiter").map(Csv.unescapeNewlines).flatMap(toChar).getOrElse(','),
          mediaType.extensions.get("rowDelimiter").map(Csv.unescapeNewlines).getOrElse("\r\n"),
          mediaType.extensions.get("quoteChar").map(Csv.unescapeNewlines).flatMap(toChar).getOrElse('"'),
          mediaType.extensions.get("escapeChar").map(Csv.unescapeNewlines).flatMap(toChar).getOrElse('"'),
          disposition)
      }
      else {
        ((mediaType satisfies JsonArray.mediaType) && mediaType.extensions.get("boundary") != Some("NL"),
          mediaType.extensions.get("mode")) match {
          case (true, Some("precise"))  => JsonArray.Precise.copy(disposition = disposition)
          case (true, _)                => JsonArray.Readable.copy(disposition = disposition)
          case (false, Some("precise")) => JsonStream.Precise.copy(disposition = disposition)
          case (false, _)               => JsonStream.Readable.copy(disposition = disposition)
        }
      }
    }).getOrElse(JsonStream.Readable)
  }
}
