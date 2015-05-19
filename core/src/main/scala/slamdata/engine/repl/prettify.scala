package slamdata.engine.repl

import scala.collection.immutable.ListMap

import scalaz._

import slamdata.engine._
import slamdata.engine.fp._

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
    case _             => Aligned.Right(data match {
      case Data.Null            =>  "null"
      case Data.True            =>  "true"
      case Data.False           =>  "false"
      case Data.Int(x)          =>  x.toString
      case Data.Dec(x)          =>  x.toString  // NB: always has a trailing zero, unlike the JSON repr.

      case Data.Timestamp(x)    =>  x.toString
      case Data.Date(x)         =>  x.toString
      case Data.Time(x)         =>  x.toString
      case Data.Interval(x)     =>  x.toString

      case Data.Id(x)           =>  x  // NB: we assume oid's are always distinguishable from the rest of the types
      case bin @ Data.Binary(_) => bin.base64

      case Data.NA              => "n/a"

      // NB: the non-atomic types never appear here because the Data has been
      // flattened. Str is handled above.
      case Data.Arr(_) | Data.Set(_) | Data.Obj(_) | Data.Str(_)
                                => "unexpected: " + data
    })
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
  def renderStream(src: Process[Task, Data], n: Int): Process[Task, List[String]] = {
    // Combinator that handles sampling the stream, computing some value from the sample,
    // and emiting a single "header" value, followed by each transformed value. The types
    // make this look generic, but it's not clear what else this would be useful for.
    def sampleMap[A, B, C](src: Process[Task, A], n: Int, sample: IndexedSeq[A] => B, prefix: B => C, f: (A, B) => C): Process[Task, C] = {
      (src.chunk(n) ++ Process.emit(Vector.empty) ++ Process.emit(Vector.empty)).zipWithState[Option[B]](None) { case (as, optB) =>
        optB.orElse(Some(sample(as)))
      }.zipWithNext.flatMap {
        case ((as, b0), Some((_, Some(b)))) =>
          b0.fold[Process[Task, C]](Process.emit(prefix(b)))(κ(Process.halt)) ++
            Process.emitAll(as.map(a => f(a, b)))
        case ((as, _), None) =>
          if (as.nonEmpty) sys.error("!")
          Process.halt
        case (_, Some((_, None))) => sys.error("!")
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
