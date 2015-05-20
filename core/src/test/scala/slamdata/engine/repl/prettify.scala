package slamdata.engine.repl

import org.specs2.mutable._

import scala.collection.immutable.ListMap
import org.threeten.bp._

import slamdata.engine._

class PrettifySpecs extends Specification {
  import Prettify._

  "flatten" should {
    "find single field" in {
      val data = Data.Obj(ListMap("a" -> Data.Int(1)))
      flatten(data) must_== ListMap(
        Path(FieldSeg("a")) -> Data.Int(1))
    }

    "find multiple fields" in {
      val data = Data.Obj(ListMap(
        "a" -> Data.Null, "b" -> Data.True, "c" -> Data.False, "d" -> Data.Dec(1.0), "e" -> Data.Str("foo")))
      flatten(data) must_== ListMap(
        Path(FieldSeg("a")) -> Data.Null,
        Path(FieldSeg("b")) -> Data.True,
        Path(FieldSeg("c")) -> Data.False,
        Path(FieldSeg("d")) -> Data.Dec(1.0),
        Path(FieldSeg("e")) -> Data.Str("foo"))
    }

    "find nested fields" in {
      val data = Data.Obj(ListMap("value" -> Data.Obj(ListMap("a" -> Data.Int(1), "b" -> Data.Int(2)))))
      flatten(data) must_== ListMap(
        Path(FieldSeg("value"), FieldSeg("a")) -> Data.Int(1),
        Path(FieldSeg("value"), FieldSeg("b")) -> Data.Int(2))
    }

    "find array indices" in {
      val data = Data.Obj(ListMap("arr" -> Data.Arr(List(Data.Str("a"), Data.Str("b")))))
      flatten(data) must_== ListMap(
        Path(FieldSeg("arr"), IndexSeg(0)) -> Data.Str("a"),
        Path(FieldSeg("arr"), IndexSeg(1)) -> Data.Str("b"))
    }

    "find set indices" in {
      val data = Data.Obj(ListMap("set" -> Data.Set(List(Data.Str("a"), Data.Str("b")))))
      flatten(data) must_== ListMap(
        Path(FieldSeg("set"), IndexSeg(0)) -> Data.Str("a"),
        Path(FieldSeg("set"), IndexSeg(1)) -> Data.Str("b"))
    }

    "find nested array indices" in {
      val data = Data.Obj(ListMap(
        "arr" -> Data.Arr(List(
          Data.Obj(ListMap("a" -> Data.Str("foo"))),
          Data.Obj(ListMap("b" -> Data.Str("bar")))))))
      flatten(data) must_== ListMap(
        Path(FieldSeg("arr"), IndexSeg(0), FieldSeg("a")) -> Data.Str("foo"),
        Path(FieldSeg("arr"), IndexSeg(1), FieldSeg("b")) -> Data.Str("bar"))
    }
  }

  "render" should {
    implicit val codec = DataCodec.Readable

    "render Str without quotes" in {
      render(Data.Str("abc")) must_== Aligned.Left("abc")
    }

    "render round Dec with trailing zero" in {
      render(Data.Dec(1.0)) must_== Aligned.Right("1.0")
    }

    "render Timestamp" in {
      val now = Instant.now
      render(Data.Timestamp(now)) must_== Aligned.Right(now.toString)
    }
  }

  "renderTable" should {
    "format empty result" in {
      renderTable(Nil) must_== List()
    }

    "format one value" in {
      renderTable(List(
        Data.Int(0))) must_==
        List(
          " value |",
          "-------|",
          "     0 |")
    }

    "format one obj" in {
      renderTable(List(
        Data.Obj(ListMap(
          "a" -> Data.Int(0))))) must_==
        List(
          " a  |",
          "----|",
          "  0 |")
    }

    "format one row" in {
      renderTable(List(
        Data.Obj(ListMap(
          "a" -> Data.Int(0), "b" -> Data.Str("foo"))))) must_==
        List(
          " a  | b    |",
          "----|------|",
          "  0 | foo  |")
    }

    "format one array" in {
      renderTable(List(
        Data.Arr(List(
          Data.Int(0), Data.Str("foo"))))) must_==
        List(
          " [0] | [1]  |",
          "-----|------|",
          "   0 | foo  |")
    }

    "format empty values" in {
      renderTable(List(
        Data.Obj(ListMap()),
        Data.Obj(ListMap()))) must_==
        List(
          " <empty> |",
          "---------|",
          "         |",
          "         |")
    }
  }

  "renderStream" should {
    import scalaz.concurrent.Task
    import scalaz.stream.Process

    "empty stream" in {
      val values: Process[Task, Data] = Process.halt
      val rows = renderStream(values, 100)
      rows.runLog.run must_== Vector(
        List("<empty>"))
    }

    "empty values" in {
      val values: Process[Task, Data] = Process.emitAll(List(
        Data.Obj(ListMap()),
        Data.Obj(ListMap())))
      val rows = renderStream(values, 100)
      rows.runLog.run must_== Vector(
        List("<empty>"),
        List(""),
        List(""))
    }

    "one trivial value" in {
      val values: Process[Task, Data] = Process.emitAll(List(
        Data.Obj(ListMap("a" -> Data.Int(1)))))
      val rows = renderStream(values, 100)
      rows.runLog.run must_== Vector(
        List("a"),
        List("1"))
    }

    "more than n" in {
      val values: Process[Task, Data] = Process.emitAll(List(
        Data.Obj(ListMap(
          "a" -> Data.Int(1))),
        Data.Obj(ListMap(
          "b" -> Data.Int(2))),
        Data.Obj(ListMap(
          "a" -> Data.Int(3),
          "b" -> Data.Int(4)))))
      val rows = renderStream(values, 2)
      rows.runLog.run must_== Vector(
        List("a", "b"),
        List("1", ""),
        List("", "2"),
        List("3", "4"))
    }

    "more than n with new fields after" in {
      val values: Process[Task, Data] = Process.emitAll(List(
        Data.Obj(ListMap(
          "a" -> Data.Int(1))),
        Data.Obj(ListMap(
          "b" -> Data.Int(2))),
        Data.Obj(ListMap(
          "a" -> Data.Int(3),
          "b" -> Data.Int(4),
          "c" -> Data.Int(5)))))
      val rows = renderStream(values, 2)
      rows.runLog.run must_== Vector(
        List("a", "b"),
        List("1", ""),
        List("", "2"),
        List("3", "4"))
    }

    "properly sequence effects" in {
      // A source of values that keeps track of evaluation:
      val history = new collection.mutable.ListBuffer[Int]
      def t(n: Int) = Task.delay { history += n; Data.Obj(ListMap("n" -> Data.Int(n))) }

      val values = Process.eval(t(0)) ++ Process.eval(t(1))

      val rows = renderStream(values, 1)

      // Run the process once:
      val rez = rows.runLog.run

      rez must_== Vector(
        List("n"),
        List("0"),
        List("1"))
      history must_== List(0, 1)
    }
  }
}
