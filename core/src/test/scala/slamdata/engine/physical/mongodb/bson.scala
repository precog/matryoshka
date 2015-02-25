package slamdata.engine.physical.mongodb

import org.specs2.mutable._
import org.specs2.ScalaCheck
import scala.collection.immutable.ListMap

import scalaz._
import Scalaz._

import org.threeten.bp._

import slamdata.engine._
import slamdata.engine.fp._
import slamdata.engine.javascript._

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
          "b" -> NA))
    }

    "preserve NA" in {
      val b = Doc(ListMap("a" -> NA))

      fromRepr(b.repr) must_== b
    }

    "handle completely unexpected object" in {
      // Simulating a type MongoDB uses that we know nothing about:
      class Foo

      val native = new com.mongodb.BasicDBObject()
      native.put("a", new Foo())

      Bson.fromRepr(native) must_== Doc(ListMap("a" -> NA))
    }

    import BsonGen._

    "be (fully) isomorphic for representable types" ! org.scalacheck.Arbitrary(simpleGen) { (bson: Bson) =>
      val representable = bson match {
        case JavaScript(_)         => false
        case JavaScriptScope(_, _) => false
        case `NA`                  => false
        case _ => true
      }

      val wrapped = Doc(ListMap("value" -> bson))

      if (representable)
        fromRepr(wrapped.repr) must_== wrapped
      else
        fromRepr(wrapped.repr) must_== Doc(ListMap("value" -> NA))
    }

    "be 'semi' isomorphic for all types" ! prop { (bson: Bson) =>
      val wrapped = Doc(ListMap("value" -> bson)).repr

      // (fromRepr >=> repr >=> fromRepr) == fromRepr
      fromRepr(fromRepr(wrapped).repr.asInstanceOf[com.mongodb.DBObject]) must_== fromRepr(wrapped)
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
    // const(Timestamp(Instant.now, 0)),  // FIXME: round to nearest second?
    const(Regex("a.*")),
    const(JavaScript(Js.Null)),
    const(JavaScriptScope(Js.Null, Doc(ListMap.empty))),
    resize(5, arbitrary[String]).map(Symbol.apply),
    const(MinKey),
    const(MaxKey),
    const(NA))

  val objGen = for {
    pairs <- listOf(for {
      n <- resize(5, alphaStr)
      v <- simpleGen
    } yield n -> v)
  } yield Doc(pairs.toListMap)

  val arrGen = listOf(simpleGen).map(Arr.apply)
}
