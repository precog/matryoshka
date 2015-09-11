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

package quasar.physical.mongodb

import quasar.Predef._
import quasar.fp._
import quasar._; import Planner._

import scalaz._; import Scalaz._

object BsonCodec {
  def fromData(data: Data): PlannerError \/ Bson = {
    data match {
      case Data.Null => \/ right (Bson.Null)

      case Data.Str(value) => \/ right (Bson.Text(value))

      case Data.Bool(v) => \/ right (Bson.Bool(v))

      case Data.Dec(value) => \/ right (Bson.Dec(value.toDouble))
      case Data.Int(value) => \/ right (Bson.Int64(value.toLong))

      case Data.Obj(value) =>
        type MapF[X] = Map[String, X]
        type Right[X] = PlannerError \/ X

        val map: MapF[PlannerError \/ Bson] = value.mapValues(fromData _)

        Traverse[MapF].sequence[Right, Bson](map).map((x: MapF[Bson]) => Bson.Doc(x.toList.toListMap))

      case Data.Arr(value) => value.map(fromData _).sequenceU.map(Bson.Arr.apply _)

      case Data.Set(value) => value.map(fromData _).sequenceU.map(Bson.Arr.apply _)

      case Data.Timestamp(value) => \/ right (Bson.Date(value))

      case d @ Data.Date(_) => fromData(quasar.std.DateLib.startOfDay(d))

      case Data.Time(value) => {
        def pad2(x: Int) = if (x < 10) "0" + x else x.toString
        def pad3(x: Int) = if (x < 10) "00" + x else if (x < 100) "0" + x else x.toString
        \/ right (Bson.Text(
          pad2(value.getHour()) + ":" +
          pad2(value.getMinute()) + ":" +
          pad2(value.getSecond()) + "." +
          pad3(value.getNano()/1000000)))
      }
      case Data.Interval(value) => \/ right (Bson.Dec(value.getSeconds*1000 + value.getNano*1e-6))

      case Data.Binary(value) => \/ right (Bson.Binary(value.toArray[Byte]))

      case Data.Id(value) => Bson.ObjectId(value) \/> ObjectIdFormatError(value)

      case Data.NA => \/ right (Bson.Undefined)
    }
  }

  def toData(bson: Bson): Data = bson match {
    case Bson.Null              => Data.Null
    case Bson.Text(str)         => Data.Str(str)
    case Bson.Bool(true)        => Data.True
    case Bson.Bool(false)       => Data.False
    case Bson.Dec(value)        => Data.Dec(value)
    case Bson.Int32(value)      => Data.Int(value)
    case Bson.Int64(value)      => Data.Int(value)
    case Bson.Doc(value)        => Data.Obj(value ∘ toData)
    case Bson.Arr(value)        => Data.Arr(value ∘ toData)
    case Bson.Date(value)       => Data.Timestamp(value)
    case Bson.Binary(value)     => Data.Binary(value)
    case oid @ Bson.ObjectId(_) => Data.Id(oid.str)
    // NB: several types have no corresponding Data representation, including
    // MinKey, MaxKey, Regex, Symbol, Timestamp, JavaScript, and JavaScriptScope
    case _                     => Data.NA
  }
}
