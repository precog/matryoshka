package slamdata.engine

import scala.collection.immutable.ListMap
import scalaz._, Scalaz._
import argonaut._, Argonaut._
import org.threeten.bp._

import slamdata.engine.fp._

trait DataEncodingError extends slamdata.engine.Error
object DataEncodingError {
  case class UnrepresentableDataError(data: Data) extends DataEncodingError {
    def message = "not representable: " + data
  }

  case class UnescapedKeyError(json: Json) extends DataEncodingError {
    def message = "un-escaped key: " + json
  }

  case class UnexpectedValueError(expected: String, json: Json) extends DataEncodingError {
    def message = "expected " + expected + ", found: " + json.pretty(minspace)
  }

  case class ParseError(cause: String) extends DataEncodingError {
    def message = cause
  }

  implicit val EncoderDataEncodingError: EncodeJson[DataEncodingError] =
    EncodeJson(err => Json.obj("error" := err.message))
}

trait DataCodec {
  def encode(data: Data): DataEncodingError \/ Json
  def decode(json: Json): DataEncodingError \/ Data
}
object DataCodec {
  import DataEncodingError._

  def parse(str: String)(implicit C: DataCodec): DataEncodingError \/ Data =
    Parse.parse(str).leftMap(ParseError.apply).flatMap(C.decode(_))

  def render(data: Data)(implicit C: DataCodec): DataEncodingError \/ String =
    C.encode(data).map(_.pretty(minspace))

  val Precise = new DataCodec {
    val DecKey = "$dec"
    val SetKey = "$set"
    val TimestampKey = "$timestamp"
    val DateKey = "$date"
    val TimeKey = "$time"
    val IntervalKey = "$interval"
    val BinaryKey = "$binary"
    val ObjKey = "$obj"
    val IdKey = "$oid"

    def encode(data: Data): DataEncodingError \/ Json = {
      import Data._
      // ({
      data match {
        case `Null`   => \/-(jNull)
        case `True`   => \/-(jTrue)
        case `False`  => \/-(jFalse)
        case Int(x)   => jNumber(x.doubleValue) \/> UnrepresentableDataError(Int(x))
        case Dec(x)   =>
          jNumber(x.doubleValue).map { json =>
            if (x.intValue == x) Json.obj(DecKey := json) else json
          } \/> UnrepresentableDataError(Dec(x))
        case Str(s)   => \/-(jString(s))

        case Obj(value) => for {
          obj <- value.toList.map { case (k, v) => encode(v).map(k -> _) }.sequenceU.map(Json.obj(_: _*))
        } yield value.keys.find(_.startsWith("$")).fold(obj)(κ(Json.obj(ObjKey -> obj)))

        case Arr(value) => value.map(encode).sequenceU.map(vs => Json.array(vs: _*))
        case Set(value) => value.map(encode).sequenceU.map(vs => Json.obj("$set" -> Json.array(vs: _*)))

        case Timestamp(value) => \/-(Json.obj(TimestampKey -> jString(value.toString)))
        case Date(value)      => \/-(Json.obj(DateKey      -> jString(value.toString)))
        case Time(value)      => \/-(Json.obj(TimeKey      -> jString(value.toString)))
        case Interval(value)  => \/-(Json.obj(IntervalKey  -> jString(value.toString)))

        case Binary(value)    => \/-(Json.obj(BinaryKey -> jString(new sun.misc.BASE64Encoder().encode(value.toArray))))

        case Id(value)        => \/-(Json.obj(IdKey -> jString(value)))
      }
      // }: PartialFunction[Data, DataEncodingError \/ Json]).lift.apply(data).getOrElse(-\/("unrecognized: " + data))
    }

    def decode(json: Json): DataEncodingError \/ Data =
      json.fold(
        \/-(Data.Null),
        bool => \/-(Data.Bool(bool)),
        num => {
          val ip = num.intValue
          if (ip == num.doubleValue) \/-(Data.Int(ip))
          else \/-(Data.Dec(num))
        },
        str => \/-(Data.Str(str)),
        arr => arr.map(decode).sequenceU.map(Data.Arr(_)),
        obj => {
          import std.DateLib._

          def unpack[A](a: Option[A], expected: String)(f: A => DataEncodingError \/ Data) =
            (a \/> UnexpectedValueError(expected, json)) flatMap f

          def decodeObj(obj: JsonObject): DataEncodingError \/ Data =
            obj.toList.map { case (k, v) => decode(v).map(k -> _) }.sequenceU.map(pairs => Data.Obj(ListMap(pairs: _*)))

          obj.toList match {
            case (`DecKey`, value) :: Nil       => unpack(value.number, "number value for $dec")(x => \/-(Data.Dec(x)))
            case (`TimestampKey`, value) :: Nil => unpack(value.string, "string value for $timestamp")(parseTimestamp(_).leftMap(err => ParseError(err.message)))
            case (`DateKey`, value) :: Nil      => unpack(value.string, "string value for $date")(parseDate(_).leftMap(err => ParseError(err.message)))
            case (`TimeKey`, value) :: Nil      => unpack(value.string, "string value for $time")(parseTime(_).leftMap(err => ParseError(err.message)))
            case (`IntervalKey`, value) :: Nil  => unpack(value.string, "string value for $interval")(parseInterval(_).leftMap(err => ParseError(err.message)))
            case (`ObjKey`, value) :: Nil       => unpack(value.obj,    "object value for $obj")(decodeObj)
            case (`SetKey`, value) :: Nil       => unpack(value.array,  "a list of values for $set")(_.map(decode).sequenceU.map(Data.Set(_)))
            case (`BinaryKey`, value) :: Nil    => unpack(value.string, "string value for $binary") { str =>
              \/.fromTryCatchNonFatal(Data.Binary(new sun.misc.BASE64Decoder().decodeBuffer(str).toList)).leftMap(_ => UnexpectedValueError("BASE64-encoded data", json))
            }
            case (`IdKey`, value) :: Nil        => unpack(value.string, "string value for $oid")(str => \/-(Data.Id(str)))
            case _ => obj.fields.find(_.startsWith("$")).fold(decodeObj(obj))(κ(-\/(UnescapedKeyError(json))))
          }
        })
  }

  object Readable extends DataCodec {
    /** True if this value can be parsed from JSON. */
    def representable(data: Data) = data match {
      case Data.Dec(x)    => x.intValue != x
      case Data.Set(_)    => false
      case Data.Binary(_) => false
      case Data.Id(_)     => false
      case _              => true
    }

    def encode(data: Data): DataEncodingError \/ Json = {
      import Data._
      // ({
      data match {
        case `Null`   => \/-(jNull)
        case `True`   => \/-(jTrue)
        case `False`  => \/-(jFalse)
        case Int(x)   => jNumber(x.doubleValue) \/> UnrepresentableDataError(Int(x))
        case Dec(x)   => jNumber(x.doubleValue) \/> UnrepresentableDataError(Dec(x))
        case Str(s)   => \/-(jString(s))

        case Obj(value) => value.toList.map { case (k, v) => encode(v).map(k -> _) }.sequenceU.map(Json.obj(_: _*))
        case Arr(value) => value.map(encode).sequenceU.map(vs => Json.array(vs: _*))
        case Set(value) => value.map(encode).sequenceU.map(vs => Json.array(vs: _*))

        case Timestamp(value) => \/-(jString(value.toString))
        case Date(value)      => \/-(jString(value.toString))
        case Time(value)      => \/-(jString(value.toString))
        case Interval(value)  => \/-(jString(value.toString))

        case Binary(value)    => \/-(jString(new sun.misc.BASE64Encoder().encode(value.toArray)))

        case Id(value)        => \/-(jString(value))
      }
      // }: PartialFunction[Data, String \/ Json]).lift.apply(data).getOrElse(-\/("unrecognized: " + data))
    }

    def decode(json: Json): DataEncodingError \/ Data =
      json.fold(
        \/-(Data.Null),
        bool => \/-(Data.Bool(bool)),
        num  => {
          val ip = num.intValue
          if (ip == num.doubleValue) \/-(Data.Int(ip))
          else \/-(Data.Dec(num))
        },
        str => {
          import std.DateLib._

          val converted = List(
              parseTimestamp(str),
              parseDate(str),
              parseTime(str),
              parseInterval(str))
          \/-(converted.flatMap(_.toList).headOption.getOrElse(Data.Str(str)))
        },
        arr => arr.map(decode).sequenceU.map(Data.Arr(_)),
        obj => obj.toList.map { case (k, v) => decode(v).map(k -> _) }.sequenceU.map(pairs => Data.Obj(ListMap(pairs: _*))))
  }
}