package slamdata.engine.repl

import org.specs2.mutable._

import scala.collection.immutable.ListMap
import scalaz._
import org.threeten.bp._

import slamdata.engine._

class PrettifySpecs extends Specification {
  import Prettify._

  "flatten" should {
    "find single field" in {
      val data = Data.Obj(ListMap("a" -> Data.Int(1)))
      flatten(data) must_== ListMap(Path("a") -> Data.Int(1))
    }

    "find multiple fields" in {
      val data = Data.Obj(ListMap(
        "a" -> Data.Null, "b" -> Data.True, "c" -> Data.False, "d" -> Data.Dec(1.0), "e" -> Data.Str("foo")))
      flatten(data) must_== ListMap(
        Path("a") -> Data.Null,
        Path("b") -> Data.True,
        Path("c") -> Data.False,
        Path("d") -> Data.Dec(1.0),
        Path("e") -> Data.Str("foo"))
    }

    "find nested fields" in {
      val data = Data.Obj(ListMap("value" -> Data.Obj(ListMap("a" -> Data.Int(1), "b" -> Data.Int(2)))))
      flatten(data) must_== ListMap(
        Path("value", "a") -> Data.Int(1),
        Path("value", "b") -> Data.Int(2))
    }

    "find array indices" in {
      val data = Data.Obj(ListMap("arr" -> Data.Arr(List(Data.Str("a"), Data.Str("b")))))
      flatten(data) must_== ListMap(
        Path("arr", "0") -> Data.Str("a"),
        Path("arr", "1") -> Data.Str("b"))
    }

    "find nested array indices" in {
      val data = Data.Obj(ListMap(
        "arr" -> Data.Arr(List(
          Data.Obj(ListMap("a" -> Data.Str("foo"))),
          Data.Obj(ListMap("b" -> Data.Str("bar")))))))
      flatten(data) must_== ListMap(
        Path("arr", "0", "a") -> Data.Str("foo"),
        Path("arr", "1", "b") -> Data.Str("bar"))
    }
  }

  "render" should {
    implicit val codec = DataCodec.Readable

    "render Str without quotes" in {
      render(Data.Str("abc")) must_== -\/("abc")
    }

    "render round Dec with trailing zero" in {
      render(Data.Dec(1.0)) must_== \/-("1.0")
    }

    "render Timestamp" in {
      val now = Instant.now
      render(Data.Timestamp(now)) must_== \/-(now.toString)
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
  }
}
