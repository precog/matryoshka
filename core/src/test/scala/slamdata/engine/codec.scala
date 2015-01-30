package slamdata.engine

import org.specs2.mutable._
import org.specs2.ScalaCheck
import org.scalacheck._

import org.threeten.bp._
import scala.collection.immutable.ListMap
import scalaz._

import slamdata.engine.fp._

class CodecSpecs extends Specification with ScalaCheck with DisjunctionMatchers {
  import DataGen._

  implicit val DataShow = new Show[Data] { override def show(v: Data) = v.toString }
  implicit val ShowStr = new Show[String] { override def show(v: String) = v }

  "Precise" should {
    implicit val codec = DataCodec.Precise

    "render" should {
      // NB: these tests verify that all the formatting matches our documentation

      "encode null"      in { DataCodec.render(Data.Null)     must beRightDisj("null") }
      "encode true"      in { DataCodec.render(Data.True)     must beRightDisj("true") }
      "encode false"     in { DataCodec.render(Data.False)    must beRightDisj("false") }
      "encode int"       in { DataCodec.render(Data.Int(0))   must beRightDisj("0") }
      "encode dec"       in { DataCodec.render(Data.Dec(1.1)) must beRightDisj("1.1") }
      "encode dec with no fractional part" in { DataCodec.render(Data.Dec(2.0)) must beRightDisj("""{ "$dec": 2 }""") }
      "encode timestamp" in { DataCodec.render(Data.Timestamp(Instant.parse("2015-01-31T10:30:00Z"))) must beRightDisj("""{ "$timestamp": "2015-01-31T10:30:00Z" }""") }
      "encode date"      in { DataCodec.render(Data.Date(LocalDate.parse("2015-01-31")))              must beRightDisj("""{ "$date": "2015-01-31" }""") }
      "encode time"      in { DataCodec.render(Data.Time(LocalTime.parse("10:30:00.000")))            must beRightDisj("""{ "$time": "10:30" }""") }
      "encode interval"  in { DataCodec.render(Data.Interval(Duration.parse("PT12H34M")))             must beRightDisj("""{ "$interval": "PT12H34M" }""") }
      "encode obj" in {
        // NB: more than 4, to verify order is preserved
        DataCodec.render(Data.Obj(ListMap("a" -> Data.Int(1), "b" -> Data.Int(2), "c" -> Data.Int(3), "d" -> Data.Int(4), "e" -> Data.Int(5)))) must 
          beRightDisj("""{ "a": 1, "b": 2, "c": 3, "d": 4, "e": 5 }""")
      }  
      "encode obj with leading '$'s" in {
        DataCodec.render(Data.Obj(ListMap("$a" -> Data.Int(1), "$date" -> Data.Timestamp(Instant.parse("2015-01-31T10:30:00Z"))))) must
          beRightDisj("""{ "$obj": { "$a": 1, "$date": { "$timestamp": "2015-01-31T10:30:00Z" } } }""")
        }
      "encode array"     in { DataCodec.render(Data.Arr(List(Data.Int(0), Data.Int(1), Data.Int(2)))) must beRightDisj("[ 0, 1, 2 ]") }
      "encode set"       in { DataCodec.render(Data.Set(List(Data.Int(0), Data.Int(1), Data.Int(2)))) must beRightDisj("""{ "$set": [ 0, 1, 2 ] }""") }
      "encode binary"    in { DataCodec.render(Data.Binary(List(76, 77, 78, 79))) must beRightDisj("""{ "$binary": "TE1OTw==" }""") }
      "encode objectId"  in { DataCodec.render(Data.Id("abc")) must beRightDisj("""{ "$oid": "abc" }""") }
    }

    "round-trip" ! prop { (data: Data) =>
      DataCodec.render(data).flatMap(DataCodec.parse(_)) must beRightDisj(data)
    }
    
    "parse" should {
      // NB: these are just the failures. Everything else is verified by round-tripping.
      
      "round large Int (Long.MaxValue)" in {
        DataCodec.parse("9223372036854775807") must beRightDisj(Data.Dec(BigDecimal("9223372036854776000")))
      }

      "fail with unescaped leading '$'" in {
        DataCodec.parse("""{ "$a": 1 }""") must beAnyLeftDisj
      }

      "fail with bad timestamp value" in {
        DataCodec.parse("""{ "$timestamp": 123456 }""") must beAnyLeftDisj
      }

      "fail with bad timestamp string" in {
        DataCodec.parse("""{ "$timestamp": "10 o'clock this morning" }""") must beAnyLeftDisj
      }
    }
  }

  "Readable" should {
    implicit val codec = DataCodec.Readable

    "render" should {
      // NB: these tests verify that all the formatting matches our documentation

      "encode null"      in { DataCodec.render(Data.Null)     must beRightDisj("null") }
      "encode true"      in { DataCodec.render(Data.True)     must beRightDisj("true") }
      "encode false"     in { DataCodec.render(Data.False)    must beRightDisj("false") }
      "encode int"       in { DataCodec.render(Data.Int(0))   must beRightDisj("0") }
      "encode dec"       in { DataCodec.render(Data.Dec(1.1)) must beRightDisj("1.1") }
      "encode dec with no fractional part" in { DataCodec.render(Data.Dec(2.0)) must beRightDisj("2") }
      "encode timestamp" in { DataCodec.render(Data.Timestamp(Instant.parse("2015-01-31T10:30:00Z"))) must beRightDisj("\"2015-01-31T10:30:00Z\"") }
      "encode date"      in { DataCodec.render(Data.Date(LocalDate.parse("2015-01-31")))              must beRightDisj("\"2015-01-31\"") }
      "encode time"      in { DataCodec.render(Data.Time(LocalTime.parse("10:30:00.000")))            must beRightDisj("\"10:30\"") }
      "encode interval"  in { DataCodec.render(Data.Interval(Duration.parse("PT12H34M")))             must beRightDisj("\"PT12H34M\"") }
      "encode obj" in {
        // NB: more than 4, to verify order is preserved
        DataCodec.render(Data.Obj(ListMap("a" -> Data.Int(1), "b" -> Data.Int(2), "c" -> Data.Int(3), "d" -> Data.Int(4), "e" -> Data.Int(5)))) must 
          beRightDisj("""{ "a": 1, "b": 2, "c": 3, "d": 4, "e": 5 }""")
      }  
      "encode obj with leading '$'s" in {
        DataCodec.render(Data.Obj(ListMap("$a" -> Data.Int(1), "$date" -> Data.Timestamp(Instant.parse("2015-01-31T10:30:00Z"))))) must
          beRightDisj("""{ "$a": 1, "$date": "2015-01-31T10:30:00Z" }""")
        }
      "encode array"     in { DataCodec.render(Data.Arr(List(Data.Int(0), Data.Int(1), Data.Int(2)))) must beRightDisj("[ 0, 1, 2 ]") }
      "encode set"       in { DataCodec.render(Data.Set(List(Data.Int(0), Data.Int(1), Data.Int(2)))) must beRightDisj("[ 0, 1, 2 ]") }
      "encode binary"    in { DataCodec.render(Data.Binary(List(76, 77, 78, 79))) must beRightDisj("\"TE1OTw==\"") }
      "encode objectId"  in { DataCodec.render(Data.Id("abc")) must beRightDisj("\"abc\"") }
    }

    "round-trip" ! prop { (data: Data) =>
      DataCodec.Readable.representable(data) ==> {
        DataCodec.render(data).flatMap(DataCodec.parse) must beRightDisj(data)
      }
    }

    "parse" should {
      // These types get inferred whenever a string matches the expected format:

      "re-parse Str as Timestamp" in {
        val ts = Data.Timestamp(Instant.now)
        val str = Data.Str(ts.value.toString)
        DataCodec.render(str).flatMap(DataCodec.parse) must beRightDisj(ts)
      }

      "re-parse Str as Date" in {
        val date = Data.Date(LocalDate.now)
        val str = Data.Str(date.value.toString)
        DataCodec.render(str).flatMap(DataCodec.parse) must beRightDisj(date)
      }

      "re-parse Str as Time" in {
        val time = Data.Time(LocalTime.now)
        val str = Data.Str(time.value.toString)
        DataCodec.render(str).flatMap(DataCodec.parse) must beRightDisj(time)
      }

      "re-parse Str as Interval" in {
        val interval = Data.Interval(Duration.ofSeconds(1))
        val str = Data.Str(interval.value.toString)
        DataCodec.render(str).flatMap(DataCodec.parse) must beRightDisj(interval)
      }


      // These types get lost on the way through rendering and re-parsing:

      "re-parse Dec as Int" in {
        DataCodec.render(Data.Dec(1.0)).flatMap(DataCodec.parse) must beRightDisj(Data.Int(1))
      }

      "re-parse Set as Arr" in {
        val set = Data.Set(List[BigInt](1, 2, 3).map(Data.Int.apply))
        DataCodec.render(set).flatMap(DataCodec.parse) must beRightDisj(Data.Arr(set.value))
      }

      "re-parse Binary as Str" in {
        val binary = Data.Binary(List(0, 1, 2, 3))
        DataCodec.render(binary).flatMap(DataCodec.parse) must beRightDisj(Data.Str("AAECAw=="))
      }

      "re-parse Id as Str" in {
        val id = Data.Id("abc")
        DataCodec.render(id).flatMap(DataCodec.parse) must beRightDisj(Data.Str("abc"))
      }
    }
  }
 }
 object DataGen {
  implicit val data: Arbitrary[Data] = Arbitrary {
    Gen.oneOf(
      Data.Null, Data.True, Data.False,
      Data.Str("abc"), Data.Int(0), Data.Dec(1.1),
      Data.Obj(ListMap("a" -> Data.Int(0), "b" -> Data.Int(1))),
      Data.Arr(List(Data.Int(0), Data.Int(1))),
      Data.Set(List(Data.Int(0), Data.Int(1))),
      Data.Timestamp(Instant.now),
      Data.Interval(Duration.ofSeconds(1)),
      Data.Date(LocalDate.now),
      Data.Time(LocalTime.now),
      Data.Binary(List(0, 1, 2, 3)),
      Data.Id("012345678901234567890123"),  // NB: a (nominally) valid MongoDB id, because we use this generator to test BSON conversion, too

      // Tricky cases:
      // Data.Int(Long.MaxValue),  // Too big for Double
      Data.Dec(2.0),  // Looks like an Int, so needs special handling
      Data.Obj(ListMap("$date" -> Data.Str("Jan 1"))))
  }
}