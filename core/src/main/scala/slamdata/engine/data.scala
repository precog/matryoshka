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

package slamdata.engine

import scala.collection.immutable.ListMap
import scala.collection.immutable.Map

import scalaz._
import Scalaz._

import org.threeten.bp.{Instant, LocalDate, LocalTime, Duration}

import slamdata.engine.analysis.fixplate._
import slamdata.engine.fp._
import slamdata.engine.javascript.Js
import slamdata.engine.javascript.JsCore

sealed trait Data {
  def dataType: Type
  def toJs: Term[JsCore]
}

object Data {
  final case object Null extends Data {
    def dataType = Type.Null
    def toJs = JsCore.Literal(Js.Null).fix
  }

  final case class Str(value: String) extends Data {
    def dataType = Type.Str
    def toJs = JsCore.Literal(Js.Str(value)).fix
  }

  final case class Bool(value: Boolean) extends Data {
    def dataType = Type.Bool
    def toJs = JsCore.Literal(Js.Bool(value)).fix
  }
  val True = Bool(true)
  val False = Bool(false)

  sealed trait Number extends Data {
    override def equals(other: Any) = (this, other) match {
      case (Int(v1), Number(v2)) => BigDecimal(v1) == v2
      case (Dec(v1), Number(v2)) => v1 == v2
      case _                     => false
    }
  }
  object Number {
    def unapply(value: Data): Option[BigDecimal] = value match {
      case Int(value) => Some(BigDecimal(value))
      case Dec(value) => Some(value)
      case _ => None
    }
  }
  final case class Dec(value: BigDecimal) extends Number {
    def dataType = Type.Dec
    def toJs = JsCore.Literal(Js.Num(value.doubleValue, true)).fix
  }
  final case class Int(value: BigInt) extends Number {
    def dataType = Type.Int
    def toJs = JsCore.Literal(Js.Num(value.doubleValue, false)).fix
  }

  final case class Obj(value: Map[String, Data]) extends Data {
    def dataType = Type.Obj(value ∘ (Type.Const(_)), None)
    def toJs =
      JsCore.Obj(value.toList.map { case (k, v) => k -> v.toJs }.toListMap).fix
  }

  final case class Arr(value: List[Data]) extends Data {
    def dataType = Type.Arr(value ∘ (Type.Const(_)))
    def toJs = JsCore.Arr(value.map(_.toJs)).fix
  }

  final case class Set(value: List[Data]) extends Data {
    def dataType = (value.headOption.map { head =>
      value.drop(1).map(_.dataType).foldLeft(head.dataType)(Type.lub _)
    }).getOrElse(Type.Bottom) // TODO: ???
    def toJs = JsCore.Arr(value.map(_.toJs)).fix
  }

  final case class Timestamp(value: Instant) extends Data {
    def dataType = Type.Timestamp
    def toJs = JsCore.Call(JsCore.Ident("ISODate").fix, List(JsCore.Literal(Js.Str(value.toString)).fix)).fix
  }

  final case class Date(value: LocalDate) extends Data {
    def dataType = Type.Date
    def toJs = JsCore.Call(JsCore.Ident("ISODate").fix, List(JsCore.Literal(Js.Str(value.toString)).fix)).fix
  }

  final case class Time(value: LocalTime) extends Data {
    def dataType = Type.Time
    def toJs = JsCore.Literal(Js.Str(value.toString)).fix
  }

  final case class Interval(value: Duration) extends Data {
    def dataType = Type.Interval
    def toJs = JsCore.Literal(Js.Num(value.getSeconds*1000 + value.getNano*1e-6, true)).fix
  }

  final case class Binary(value: ImmutableArray[Byte]) extends Data {
    def dataType = Type.Binary
    def toJs = JsCore.Call(JsCore.Ident("BinData").fix, List(
      JsCore.Literal(Js.Num(0, false)).fix,
      JsCore.Literal(Js.Str(base64)).fix)).fix

    def base64: String = new sun.misc.BASE64Encoder().encode(value.toArray)

    override def toString = "Binary(Array[Byte](" + value.mkString(", ") + "))"

    override def equals(that: Any): Boolean = that match {
      case Binary(value2) => value === value2
      case _ => false
    }
    override def hashCode = java.util.Arrays.hashCode(value.toArray[Byte])
  }
  object Binary {
    def apply(array: Array[Byte]): Binary = Binary(ImmutableArray.fromArray(array))
  }

  final case class Id(value: String) extends Data {
    def dataType = Type.Id
    def toJs = JsCore.Call(JsCore.Ident("ObjectId").fix, List(JsCore.Literal(Js.Str(value)).fix)).fix
  }

  /**
   An object to represent any value that might come from a backend, but that
   we either don't know about or can't represent in this ADT. We represent it
   with JS's `undefined`, just because no other value will ever be translated
   that way.
   */
  final case object NA extends Data {
    def dataType = Type.Bottom
    def toJs = JsCore.Ident(Js.Undefined.ident).fix
  }
}
