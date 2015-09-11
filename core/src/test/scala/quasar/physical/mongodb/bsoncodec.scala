package quasar.physical.mongodb

import quasar.Predef._

import org.specs2.mutable._
import org.specs2.scalaz._
import org.specs2.ScalaCheck

import scala.collection.immutable.ListMap

import scalaz._

import quasar._

class BsonCodecSpecs extends Specification with ScalaCheck with DisjunctionMatchers {
  import BsonCodec._

  import DataGen._
  import BsonGen._

  implicit val ShowData = new Show[Data] {
    override def show(v: Data) = Cord(v.toString)
  }
  implicit val ShowBson = new Show[Bson] {
    override def show(v: Bson) = Cord(v.toString)
  }

  "fromData" should {
    "fail with bad Id" in {
      fromData(Data.Id("invalid")) must beLeftDisjunction
    }

    "be isomorphic for preserved values" ! prop { (data: Data) =>
      // (fromData >=> toData) == identity, except for values that are known not to be preserved

      import Data._

      val preserved = data match {
        case Int(x)      => x.isValidLong
        case Interval(_) => false
        case Date(_)     => false
        case Time(_)     => false
        case Set(_)      => false
        case _ => true
      }

      preserved ==> {
        fromData(data).map(toData) must beRightDisjunction(data)
      }
    }

    "be 'semi'-isomorphic for all Bson values" ! prop { (bson: Bson) =>
      // (toData >=> fromData >=> toData) == toData

      val data = toData(bson)
      fromData(data).map(toData _) must beRightDisjunction(data)
    }
  }

  "toData" should {
    "convert MinKey to NA" in {
      toData(Bson.MinKey) must_== Data.NA
    }

    "be 'semi'-isomorphic for all Data values" ! prop { (data: Data) =>
      // (fromData >=> toData >=> fromData) == fromData
      // Which is to say, every Bson value that results from conversion
      // can be converted to Data and back to Bson, recovering the same
      // Bson value.
      fromData(data).fold(
        err => scala.sys.error(err.toString),
        bson => fromData(toData(bson)) must beRightDisjunction(bson))
    }
  }


  import DataGen._

  "round trip to repr (all Data types)" ! prop { (data: Data) =>
    BsonCodec.fromData(data).fold(
      err => scala.sys.error(err.message),
      bson => {
        val wrapped = Bson.Doc(ListMap("value" -> bson))
        Bson.fromRepr(wrapped.repr) must_== wrapped
      })
  }
}
