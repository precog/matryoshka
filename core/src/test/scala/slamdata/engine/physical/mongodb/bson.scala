package slamdata.engine.physical.mongodb

import org.specs2.mutable._
import org.specs2.ScalaCheck
import scala.collection.immutable.ListMap

import slamdata.engine._
import slamdata.engine.fp._

class BsonSpecs extends Specification with ScalaCheck with DisjunctionMatchers {
  "fromData" should {
    "fail with bad Id" in {
      Bson.fromData(Data.Id("invalid")) must beAnyLeftDisj
    }
  }
  
  "toData" should {
    "fail with MinKey" in {
      Bson.toData(Bson.MinKey) must beAnyLeftDisj
    }
  }


  import DataGen._

  "round trip to repr (all Data types)" ! prop { (data: Data) =>
    Bson.fromData(data).fold(
      err => sys.error(err.message),
      bson => {
        val wrapped = Bson.Doc(ListMap("value" -> bson))
        Bson.fromRepr(wrapped.repr.asInstanceOf[com.mongodb.DBObject]) must_== wrapped
      })
  }
}