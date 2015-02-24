package slamdata.engine.physical.mongodb

import scalaz._
import Scalaz._

import slamdata.engine._
import slamdata.engine.fp._

object BsonCodec {
  trait ConversionError extends Error
  case class InvalidObjectIdError(data: Data.Id) extends Error {
    def message = "Not a valid MongoDB ObjectId: " + data.value
  }
  case class BsonConversionError(bson: Bson) extends Error {
    def message = "BSON has no corresponding Data representation: " + bson
  }

  def fromData(data: Data): InvalidObjectIdError \/ Bson = {
    data match {
      case Data.Null => \/ right (Bson.Null)

      case Data.Str(value) => \/ right (Bson.Text(value))

      case Data.True => \/ right (Bson.Bool(true))
      case Data.False => \/ right (Bson.Bool(false))

      case Data.Dec(value) => \/ right (Bson.Dec(value.toDouble))
      case Data.Int(value) => \/ right (Bson.Int64(value.toLong))

      case Data.Obj(value) =>
        type MapF[X] = Map[String, X]
        type Right[X] = InvalidObjectIdError \/ X

        val map: MapF[InvalidObjectIdError \/ Bson] = value.mapValues(fromData _)

        Traverse[MapF].sequence[Right, Bson](map).map((x: MapF[Bson]) => Bson.Doc(x.toList.toListMap))

      case Data.Arr(value) => value.map(fromData _).sequenceU.map(Bson.Arr.apply _)

      case Data.Set(value) => value.map(fromData _).sequenceU.map(Bson.Arr.apply _)

      case Data.Timestamp(value) => \/ right (Bson.Date(value))

      case d @ Data.Date(_) => fromData(slamdata.engine.std.DateLib.startOfDay(d))

      case Data.Time(value) => {
        def pad2(x: Int) = if (x < 10) "0" + x else x.toString
        def pad3(x: Int) = if (x < 10) "00" + x else if (x < 100) "0" + x else x.toString
        \/ right (Bson.Text(pad2(value.getHour()) + ":" + pad2(value.getMinute()) + ":" + pad2(value.getSecond()) + "." + pad3(value.getNano()/1000000)))
      }
      case Data.Interval(value) => \/ right (Bson.Dec(value.getSeconds*1000 + value.getNano*1e-6))

      case Data.Binary(value) => \/ right (Bson.Binary(value.toArray[Byte]))

      case id @ Data.Id(value) => Bson.ObjectId(value) \/> InvalidObjectIdError(id)
    }
  }

  def toData(bson: Bson): BsonConversionError \/ Data = bson match {
    case Bson.Null              => \/-(Data.Null)
    case Bson.Text(str)         => \/-(Data.Str(str))
    case Bson.Bool(true)        => \/-(Data.True)
    case Bson.Bool(false)       => \/-(Data.False)
    case Bson.Dec(value)        => \/-(Data.Dec(value))
    case Bson.Int32(value)      => \/-(Data.Int(value))
    case Bson.Int64(value)      => \/-(Data.Int(value))
    case Bson.Doc(value)        => value.toList.map { case (k, v) => toData(v).map(k -> _) }.sequenceU.map(_.toListMap).map(Data.Obj.apply)
    case Bson.Arr(value)        => value.map(toData).sequenceU.map(Data.Arr.apply)
    case Bson.Date(value)       => \/-(Data.Timestamp(value))
    case Bson.Binary(value)     => \/-(Data.Binary(value))
    case oid @ Bson.ObjectId(_) => \/-(Data.Id(oid.str))

    // NB: several types have no corresponding Data representation, including
    // MinKey, MaxKey, Regex, Timestamp, JavaScript, and JavaScriptScope
    case _ => -\/(BsonConversionError(bson))
  }
}
