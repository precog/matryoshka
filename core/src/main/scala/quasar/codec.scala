/*
 * Copyright 2014 - 2015 SlamData Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package quasar

import quasar.Predef._
import quasar.fp._

import argonaut._, Argonaut._
import scalaz._, Scalaz._

trait DataEncodingError {
  def message: String
}
object DataEncodingError {
  final case class UnrepresentableDataError(data: Data) extends DataEncodingError {
    def message = "not representable: " + data
  }

  final case class UnescapedKeyError(json: Json) extends DataEncodingError {
    def message = "un-escaped key: " + json
  }

  final case class UnexpectedValueError(expected: String, json: Json) extends DataEncodingError {
    def message = "expected " + expected + ", found: " + json.pretty(minspace)
  }

  final case class ParseError(cause: String) extends DataEncodingError {
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
    val SetKey = "$set"
    val TimestampKey = "$timestamp"
    val DateKey = "$date"
    val TimeKey = "$time"
    val IntervalKey = "$interval"
    val BinaryKey = "$binary"
    val ObjKey = "$obj"
    val IdKey = "$oid"
    val NAKey = "$na"

    def encode(data: Data): DataEncodingError \/ Json = {
      import Data._
      data match {
        case Null => \/-(jNull)
        case Bool(true) => \/-(jTrue)
        case Bool(false) => \/-(jFalse)
        case Int(x) =>
          if (x.isValidLong) \/-(jNumber(JsonLong(x.longValue)))
          else \/-(jNumber(JsonBigDecimal(new java.math.BigDecimal(x.underlying))))
        case Dec(x)   => \/-(jNumber(JsonBigDecimal(x)))
        case Str(s)   => \/-(jString(s))

        case Obj(value) => for {
          obj <- value.toList.map { case (k, v) => encode(v).map(k -> _) }.sequenceU.map(Json.obj(_: _*))
        } yield value.keys.find(_.startsWith("$")).fold(obj)(κ(Json.obj(ObjKey -> obj)))

        case Arr(value) => value.map(encode).sequenceU.map(vs => Json.array(vs: _*))
        case Set(value) => value.map(encode).sequenceU.map(vs => Json.obj(SetKey -> Json.array(vs: _*)))

        case Timestamp(value) => \/-(Json.obj(TimestampKey -> jString(value.toString)))
        case Date(value)      => \/-(Json.obj(DateKey      -> jString(value.toString)))
        case Time(value)      => \/-(Json.obj(TimeKey      -> jString(value.toString)))
        case Interval(value)  => \/-(Json.obj(IntervalKey  -> jString(value.toString)))

        case bin @ Binary(_)  => \/-(Json.obj(BinaryKey    -> jString(bin.base64)))

        case Id(value)        => \/-(Json.obj(IdKey        -> jString(value)))

        case `NA`             => \/-(Json.obj(NAKey        -> jNull))
      }
    }

    def decode(json: Json): DataEncodingError \/ Data =
      json.fold(
        \/-(Data.Null),
        bool => \/-(Data.Bool(bool)),
        num => num match {
          case JsonLong(x) => \/-(Data.Int(x))
          case _           => \/-(Data.Dec(num.toBigDecimal))
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
            case (`TimestampKey`, value) :: Nil => unpack(value.string, "string value for $timestamp")(parseTimestamp(_).leftMap(err => ParseError(err.message)))
            case (`DateKey`, value) :: Nil      => unpack(value.string, "string value for $date")(parseDate(_).leftMap(err => ParseError(err.message)))
            case (`TimeKey`, value) :: Nil      => unpack(value.string, "string value for $time")(parseTime(_).leftMap(err => ParseError(err.message)))
            case (`IntervalKey`, value) :: Nil  => unpack(value.string, "string value for $interval")(parseInterval(_).leftMap(err => ParseError(err.message)))
            case (`ObjKey`, value) :: Nil       => unpack(value.obj,    "object value for $obj")(decodeObj)
            case (`SetKey`, value) :: Nil       => unpack(value.array,  "a list of values for $set")(_.map(decode).sequenceU.map(Data.Set(_)))
            case (`BinaryKey`, value) :: Nil    => unpack(value.string, "string value for $binary") { str =>
              \/.fromTryCatchNonFatal(Data.Binary(new sun.misc.BASE64Decoder().decodeBuffer(str))).leftMap(_ => UnexpectedValueError("BASE64-encoded data", json))
            }
            case (`IdKey`, value) :: Nil        => unpack(value.string, "string value for $oid")(str => \/-(Data.Id(str)))
            case (`NAKey`, _) :: Nil            => \/-(Data.NA)
            case _ => obj.fields.find(_.startsWith("$")).fold(decodeObj(obj))(κ(-\/(UnescapedKeyError(json))))
          }
        })
  }

  val Readable = new DataCodec {
    def encode(data: Data): DataEncodingError \/ Json = {
      import Data._
      data match {
        case Null => \/-(jNull)
        case Bool(true) => \/-(jTrue)
        case Bool(false) => \/-(jFalse)
        case Int(x)   =>
          if (x.isValidLong) \/-(jNumber(JsonLong(x.longValue)))
          else \/-(jNumber(JsonBigDecimal(new java.math.BigDecimal(x.underlying))))
        case Dec(x)   => \/-(jNumber(JsonBigDecimal(x)))
        case Str(s)   => \/-(jString(s))

        case Obj(value) => value.toList.map { case (k, v) => encode(v).map(k -> _) }.sequenceU.map(Json.obj(_: _*))
        case Arr(value) => value.map(encode).sequenceU.map(vs => Json.array(vs: _*))
        case Set(value) => value.map(encode).sequenceU.map(vs => Json.array(vs: _*))

        case Timestamp(value) => \/-(jString(value.toString))
        case Date(value)      => \/-(jString(value.toString))
        case Time(value)      => \/-(jString(value.toString))
        case Interval(value)  => \/-(jString(value.toString))

        case bin @ Binary(_)  => \/-(jString(bin.base64))

        case Id(value)        => \/-(jString(value))

        case `NA`             => \/-(jString("NA"))
      }
    }

    def decode(json: Json): DataEncodingError \/ Data =
      json.fold(
        \/-(Data.Null),
        bool => \/-(Data.Bool(bool)),
        num => num match {
          case JsonLong(x) => \/-(Data.Int(x))
          case _           => \/-(Data.Dec(num.toBigDecimal))
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
