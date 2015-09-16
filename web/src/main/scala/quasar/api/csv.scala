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
import scala.math.Ordering
import scalaz.{Ordering => _, _}
import Scalaz._

final case class Record(fields: List[String]) {
  def size = fields.length
}

trait CsvParser {
  def parse(text: String): Stream[String \/ Record]
}

object CsvParser {
  import com.github.tototoshi.csv._

  final case class TototoshiCsvParser(format: CSVFormat) extends CsvParser {
    def parse(text: String) = {
      val reader = CSVReader.open(new java.io.StringReader(text))(format)
      // NB: CSVReader's `toStream` does not allow the exceptions to be captured,
      // so here I re-implement it.
      Stream.continually(
        \/.fromTryCatchNonFatal(reader.readNext).fold(
            err => Some(-\/(err.getMessage)),
            _.map(v => \/-(Record(v)))))
        .takeWhile(_.isDefined).collect { case Some(v) => v }
    }
  }

  final case class Format(delimiter: Char, quoteChar: Char, escapeChar: Char, lineTerminator: String) extends CSVFormat {
    val quoting = QUOTE_MINIMAL
    val treatEmptyLineAsNil = false
  }

  // Parsers with all plausible formats, roughly in decreasing order of likeliness.
  val AllParsers = for {
    del  <- List(',', '\t', '|', ':', ';', '\r')
    quot <- List('"', '\'')
    esc  <- List(quot, '\\')
    term <- List("\r\n", "\n", ";")
  } yield TototoshiCsvParser(Format(del, quot, esc, term))
}


object CsvDetect {
  // NB: to avoid parsing a very large text many times, just impose a limit on the
  // number of records that are inspected for format detection. And since a mis-matched
  // format could miss record boundaries, also impose a character limit.
  val TestRecords = 10
  val TestChars = (TestRecords+1)*500

  final case class TestResult(header: Int, rows: List[Int])

  def testParse(parser: CsvParser, text: String): Option[TestResult] = {
    val subtext = text.take(TestChars)
    (parser.parse(subtext) ++ Stream.continually(\/-(Record(Nil)))).take(TestRecords + 1) match {
      case header #:: rows =>
        ((header |@| rows.toList.sequenceU) { (header, rows) =>
          TestResult(header.size, rows.map(_.size))
        }).toOption

      case _ => None
    }
  }

  /**
    Assign a numeric score to the result of `testParse`. 0.0 is the best score, meaning
    no deviation from the expected shape, which is
    - at least two columns and two rows
    - no rows with more fields than the header (large penalty)
    - few rows with less fields than the header (small penalty)
   */
  def score(result: TestResult): Double =
    ((2 - result.header) max 0)*100.0 +
      ((2 - result.rows.size) max 0)*100.0 +
      result.rows.map(n => (n - result.header) max 0).sum*10.0 +
      result.rows.map(n => (result.header - n) max 0).sum*1.0

  def rank[P <: CsvParser](parsers: List[P])(text: String): List[(Double, P)] = {
    parsers.map(p => testParse(p, text).map(score(_) -> p)).flatten.sorted(Ordering.by[(Double, P), Double](_._1))
  }

  def bestParse(parsers: List[CsvParser])(text: String): String \/ Stream[String \/ Record] =
    rank(parsers)(text).headOption match {
      case None              => -\/ ("no successful parse")
      case Some((_, parser)) =>  \/-(parser.parse(text))
    }

  val parse = bestParse(CsvParser.AllParsers) _
}
