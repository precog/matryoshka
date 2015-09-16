package quasar.physical.mongodb

import quasar.Predef._
import quasar.fp._
import quasar._
import quasar.javascript._
import quasar.jscore

import scala.collection.JavaConverters._

import org.specs2.mutable._
import org.specs2.ScalaCheck
import org.threeten.bp._

class BsonSpecs extends Specification with ScalaCheck {
  import Bson._

  "fromRepr" should {
    "handle partially invalid object" in {
      val native = Doc(ListMap(
        "a" -> Int32(0),
        "b" -> JavaScript(Js.Null))).repr

      fromRepr(native) must_==
        Doc(ListMap(
          "a" -> Int32(0),
          "b" -> Undefined))
    }

    "preserve NA" in {
      val b = Doc(ListMap("a" -> Undefined))

      fromRepr(b.repr) must_== b
    }

    "handle completely unexpected object" in {
      // Simulating a type MongoDB uses that we know nothing about:
      class Foo

      val native = new org.bson.Document(Map[String, java.lang.Object]("a" -> new Foo()).asJava)

      Bson.fromRepr(native) must_== Doc(ListMap("a" -> Undefined))
    }

    import BsonGen._

    "be (fully) isomorphic for representable types" ! org.scalacheck.Arbitrary(simpleGen) { (bson: Bson) =>
      val representable = bson match {
        case JavaScript(_)         => false
        case JavaScriptScope(_, _) => false
        case Undefined             => false
        case _ => true
      }

      val wrapped = Doc(ListMap("value" -> bson))

      if (representable)
        fromRepr(wrapped.repr) must_== wrapped
      else
        fromRepr(wrapped.repr) must_== Doc(ListMap("value" -> Undefined))
    }

    "be 'semi' isomorphic for all types" ! prop { (bson: Bson) =>
      val wrapped = Doc(ListMap("value" -> bson)).repr

      // (fromRepr >=> repr >=> fromRepr) == fromRepr
      fromRepr(fromRepr(wrapped).repr) must_== fromRepr(wrapped)
    }
  }

  "toJs" should {
    import BsonGen._

    "correspond to Data.toJs where toData is defined" ! org.scalacheck.Arbitrary(simpleGen) { (bson: Bson) =>
      val data = BsonCodec.toData(bson)
      (data != Data.NA) ==> {
        data match {
          case Data.Int(x) =>
            // NB: encoding int as Data loses size info
            (bson.toJs must_== jscore.Call(jscore.ident("NumberInt"), List(data.toJs)).toJs) or
              (bson.toJs must_== jscore.Call(jscore.ident("NumberLong"), List(data.toJs)).toJs)
          case _ =>
            bson.toJs must_== data.toJs.toJs
        }
      }
    }
  }
}

object BsonGen {
  import org.scalacheck._
  import Gen._
  import Arbitrary._

  import Bson._

  implicit val arbBson: Arbitrary[Bson] = Arbitrary(Gen.oneOf(
    simpleGen,
    resize(5, objGen),
    resize(5, arrGen)))

  val simpleGen = oneOf(
    const(Null),
    const(Bool(true)),
    const(Bool(false)),
    resize(20, arbitrary[String]).map(Text.apply),
    arbitrary[Int].map(Int32.apply),
    arbitrary[Long].map(Int64.apply),
    arbitrary[Double].map(Dec.apply),
    listOf(arbitrary[Byte]).map(bytes => Binary(bytes.toArray)),
    listOfN(12, arbitrary[Byte]).map(bytes => ObjectId(bytes.toArray)),
    const(Date(Instant.now)),
    const(Timestamp(Instant.now, 0)),
    const(Regex("a.*", "")),
    const(JavaScript(Js.Null)),
    const(JavaScriptScope(Js.Null, Doc(ListMap.empty))),
    resize(5, arbitrary[String]).map(Symbol.apply),
    const(MinKey),
    const(MaxKey),
    const(Undefined))

  val objGen = for {
    pairs <- listOf(for {
      n <- resize(5, alphaStr)
      v <- simpleGen
    } yield n -> v)
  } yield Doc(pairs.toListMap)

  val arrGen = listOf(simpleGen).map(Arr.apply)
}
