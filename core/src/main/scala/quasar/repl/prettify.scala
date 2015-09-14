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

package quasar.repl

import quasar.Predef._
import quasar.fp._
import quasar._

import scala.Predef.{ArrowAssoc, augmentString, intWrapper}

import scalaz._

object Prettify {
  sealed trait Segment
  final case class FieldSeg(name: String) extends Segment
  final case class IndexSeg(index: Int) extends Segment

  final case class Path(segs: List[Segment]) {
    def label = segs.foldLeft(List[String]() -> true) { case ((acc, first), seg) =>
      (acc :+ (seg match {
        case FieldSeg(name) if first => name
        case FieldSeg(name)          => "." + name
        case IndexSeg(index)         => "[" + index + "]"
      })) -> false
    }._1.mkString

    def ::(prefix: Segment): Path = Path(prefix :: segs)
  }
  object Path{
    def apply(segs: Segment*): Path = Path(segs.toList)

    def parse(str: String): String \/ Path = PathParser(str)

    import scala.util.parsing.combinator._

    object PathParser extends RegexParsers {
      override def skipWhitespace = false

      private def path: Parser[List[Segment]] =
        leading ~ rep(trailing) ^^ { case h ~ t => h :: t }

      private def leading: Parser[Segment] =
        field | subscript

      private def trailing: Parser[Segment] =
         ("." ~> field) | subscript

      private def field: Parser[Segment] =
        "[0-9]+".r                   ^^ { digits => IndexSeg(digits.toInt) } |
        """[^0-9.\[\]][^.\[\]]*""".r ^^ { chars => FieldSeg(chars) }

      private def subscript: Parser[IndexSeg] =
        "[" ~> "[0-9]+".r <~ "]" ^^ { digits => IndexSeg(digits.toInt) }

      def apply(input: String): String \/ Path = parseAll(path, input) match {
        case Success(result, _)  => \/-(Path(result))
        case failure : NoSuccess => -\/("failed to parse ‘" + input + "’: " + failure.msg)
      }
    }
  }

  def mergePaths(as: List[Path], bs: List[Path]): List[Path] =
    (as ++ bs).distinct

  def flatten(data: Data): ListMap[Path, Data] = {
    def loop(data: Data): Data \/ List[(Path, Data)] = {
      def prepend(name: Segment, data: Data): List[(Path, Data)] =
        loop(data) match {
          case -\/ (value) => (Path(name) -> value) :: Nil
          case  \/-(map)   => map.map(t => (name :: t._1) -> t._2)
        }
      data match {
        case Data.Arr(value) =>  \/-(value.zipWithIndex.flatMap { case (c, i) => prepend(IndexSeg(i), c) })
        case Data.Set(value) =>  \/-(value.zipWithIndex.flatMap { case (c, i) => prepend(IndexSeg(i), c) })
        case Data.Obj(value) =>  \/-(value.toList.flatMap { case (f, c) => prepend(FieldSeg(f), c) })
        case _               => -\/ (data)
      }
    }

    loop(data) match {
      case -\/ (value) => ListMap(Path(FieldSeg("value")) -> value)
      case  \/-(map)   => map.toListMap
    }
  }

  def unflatten(values: ListMap[Path, Data]): Data = {
    val init = Data.Obj(ListMap())

    def append(v: Data, p: Path, d: Data): Data = (v, p) match {
      case (Data.Obj(values), Path(FieldSeg(s) :: Nil)) => Data.Obj(values + (s -> d))
      case (Data.Obj(values), Path(FieldSeg(s) :: rest)) => Data.Obj(values + (s -> append(values.get(s).getOrElse(init), Path(rest), d)))

      case (Data.Obj(values), Path(IndexSeg(_) :: _)) if values.isEmpty => append(Data.Arr(List()), p, d)

      case (Data.Arr(values), Path(IndexSeg(x) :: _)) if values.size < x => append(Data.Arr(values :+ Data.Null), p, d)
      case (Data.Arr(values), Path(IndexSeg(x) :: Nil)) if values.size == x => Data.Arr(values :+ d)
      case (Data.Arr(values), Path(IndexSeg(x) :: rest)) if values.size == x => Data.Arr(values :+ append(init, Path(rest), d))
      case (Data.Arr(values), Path(IndexSeg(x) :: rest)) if values.size == x+1 => Data.Arr(values.dropRight(1) :+ append(values.lastOption.getOrElse(init), Path(rest), d))

      case _ => v
    }
    values.foldLeft[Data](init) { case (v, (p, str)) => append(v, p, str) }
  }

  sealed trait Aligned[A] {
    def value: A
  }
  object Aligned {
    final case class Left[A](value: A) extends Aligned[A]
    final case class Right[A](value: A) extends Aligned[A]
  }

  /**
   Render any atomic Data value to a String that should either left-aligned (Str values),
   or right-aligned (all others).
   */
  def render(data: Data): Aligned[String] = data match {
    case Data.Str(str) => Aligned.Left(str)
    case _ => Aligned.Right(DataCodec.Readable.encode(data).fold(
      _ => "unexpected: " + data,
      json => json.fold(
        "null",
        _.toString,
        _.asJsonOrString.pretty(minspace),
        ι,
        // NB: the non-atomic types never appear here because the Data has been
        // flattened.
        _ => "unexpected: " + data,
        _ => "unexpected: " + data
      )))
  }

  def parse(str: String): Option[Data] = {
    import argonaut._

    if (str == "") None
    else if (str == "null") Some(Data.Null)
    else if (str == "true") Some(Data.Bool(true))
    else if (str == "false") Some(Data.Bool(false))
    else
      parseBigInt(str).fold(
        parseBigDecimal(str).fold(
          DataCodec.Readable.decode(Json.jString(str)).toOption)(
          x => Some(Data.Dec(x))))(
        n => Some(Data.Int(n)))
  }

  /**
   Render a list of non-atomic values to a table:
   - Str values are left-aligned; all other values are right-aligned
   - There is a column for every field/index, at any depth, that appears in any value.
   - The table is formatted as in GitHub-flavored MarkDown.
   */
  def renderTable(rows: List[Data]): List[String] =
    if (rows.isEmpty) Nil
    else {
      val flat = rows.map(flatten)
      val columnNames0 = flat.map(_.keys.toList).foldLeft[List[Path]](Nil)(mergePaths)
      val columnNames = if (columnNames0.isEmpty) List(Path(FieldSeg("<empty>"))) else columnNames0

      val columns: List[(Path, List[Aligned[String]])] =
        columnNames.map(n => n -> flat.map(m => m.get(n).fold[Aligned[String]](Aligned.Left(""))(render)))

      val widths: List[((Path, List[Aligned[String]]), Int)] =
        columns.map { case (path, vals) => (path, vals) -> (path.label.length :: vals.map(_.value).map(_.length + 1)).max }

      widths.map { case ((path, _), width) => s" %-${width}s |" format path.label }.mkString ::
        widths.map { case (_, width) => List.fill(width+2)('-').mkString + "|" }.mkString ::
        (0 until widths.map(_._1._2.length).max).map { i =>
          widths.map { case ((_, vals), width) => vals(i) match {
           case Aligned.Left(value) => s" %-${width}s |" format value
           case Aligned.Right(value) => s" %${width}s |" format value
          }}.mkString
        }.toList
    }

  import scalaz.concurrent.Task
  import scalaz.stream._

  /**
   Render an unbounded series of values to a series of rows, all with the same
   columns, which are discovered by pre-processing the first `n (>= 1)` values.
   That means we never have more than `n` values in memory, and for the common
   case where all values have the same fields, a small value of `n` (even 1)
   is sufficient. However, if elements contain variable-length arrays/sets and/or
   objects with varying fields, then values are more likely to be omitted.
   - The first row is the names of the columns, based on the first `n` values.
   - Each row contains the "pretty" String values for each of the corresponding
       fields, if present.
   - If new fields appear after the first `n` rows, they're ignored.
   */
  def renderStream[F[_]](src: Process[F, Data], n: Int): Process[F, List[String]] = {
    // Combinator that handles sampling the stream, computing some value from the sample,
    // and emiting a single "header" value, followed by each transformed value. The types
    // make this look generic, but it's not clear what else this would be useful for.
    def sampleMap[A, B, C](src: Process[F, A], n: Int, sample: IndexedSeq[A] => B, prefix: B => C, f: (A, B) => C): Process[F, C] = {
      (src.chunk(n) ++ Process.emit(Vector.empty) ++ Process.emit(Vector.empty)).zipWithState[Option[B]](None) { case (as, optB) =>
        optB.orElse(Some(sample(as)))
      }.zipWithPrevious.flatMap {
        case (None, _)                           => Process.halt
        case (Some((as, None)), (_, Some(b)))    => Process.emit(prefix(b)) ++
                                                      Process.emitAll(as.map(a => f(a, b)))
        case (Some((as, Some(_))), (_, Some(b))) => Process.emitAll(as.map(a => f(a, b)))
        case (Some(_), (_, None))                => Process.halt  // Actually doesn't happen
      }
    }

    sampleMap[Data, List[Path], List[String]](
      src,
      n max 1,
      { rows =>
        val cols = rows.map(flatten(_).keys.toList).foldLeft[List[Path]](Nil)(mergePaths)
        if (cols.isEmpty) List(Path(FieldSeg("<empty>"))) else cols
      },
      _.map(_.label),
      { (row, cols) =>
        val flat = flatten(row)
        cols.map(n => flat.get(n).fold("")(render(_).value))
      })
  }
}
