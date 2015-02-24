package slamdata.engine.physical.mongodb

import org.specs2.mutable._
import org.specs2.ScalaCheck
import scala.collection.immutable.ListMap

import slamdata.engine._
import slamdata.engine.fp._

class BsonCodecSpecs extends Specification with ScalaCheck with DisjunctionMatchers {
  "fromData" should {
    "fail with bad Id" in {
      BsonCodec.fromData(Data.Id("invalid")) must beAnyLeftDisj
    }
  }

  "toData" should {
    "fail with MinKey" in {
      BsonCodec.toData(Bson.MinKey) must beAnyLeftDisj
    }
  }


  import DataGen._

  "round trip to repr (all Data types)" ! prop { (data: Data) =>
    BsonCodec.fromData(data).fold(
      err => sys.error(err.message),
      bson => {
        val wrapped = Bson.Doc(ListMap("value" -> bson))
        Bson.fromRepr(wrapped.repr) must_== wrapped
      })
  }
}
