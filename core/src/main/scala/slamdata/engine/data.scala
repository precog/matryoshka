package slamdata.engine

import scala.collection.immutable.ListMap
import scala.collection.immutable.Map

import scalaz._
import Scalaz._

import org.threeten.bp.{Instant, LocalDate, LocalTime, Duration}

import argonaut._

import slamdata.engine.analysis.fixplate._
import slamdata.engine.fp._
import slamdata.engine.javascript.Js
import slamdata.engine.javascript.JsCore

sealed trait Data {
  def dataType: Type
  def toJs: Term[JsCore]
}

object Data {
  case object Null extends Data {
    def dataType = Type.Null
    def toJs = JsCore.Literal(Js.Null).fix
  }

  case class Str(value: String) extends Data {
    def dataType = Type.Str
    def toJs = JsCore.Literal(Js.Str(value)).fix
  }

  sealed trait Bool extends Data {
    def dataType = Type.Bool
  }
  object Bool extends (Boolean => Bool) {
    def apply(value: Boolean): Bool = if (value) True else False
    def unapply(value: Bool): Option[Boolean] = value match {
      case True => Some(true)
      case False => Some(false)
    }
  }
  case object True extends Bool {
    def toJs = JsCore.Literal(Js.Bool(true)).fix
  }
  case object False extends Bool {
    override def toString = "Data.False"
    def toJs = JsCore.Literal(Js.Bool(false)).fix
  }

  sealed trait Number extends Data
  object Number {
    def unapply(value: Data): Option[BigDecimal] = value match {
      case Int(value) => Some(BigDecimal(value))
      case Dec(value) => Some(value)
      case _ => None
    }
  }
  case class Dec(value: BigDecimal) extends Number {
    def dataType = Type.Dec
    def toJs = JsCore.Literal(Js.Num(value.doubleValue, true)).fix
  }
  case class Int(value: BigInt) extends Number {
    def dataType = Type.Int
    def toJs = JsCore.Literal(Js.Num(value.doubleValue, false)).fix
  }

  case class Obj(value: Map[String, Data]) extends Data {
    def dataType = Type.Obj(value ∘ (Type.Const(_)), None)
    def toJs =
      JsCore.Obj(value.toList.map { case (k, v) => k -> v.toJs }.toListMap).fix
  }

  case class Arr(value: List[Data]) extends Data {
    def dataType = Type.Arr(value ∘ (Type.Const(_)))
    def toJs = JsCore.Arr(value.map(_.toJs)).fix
  }

  case class Set(value: List[Data]) extends Data {
    def dataType = (value.headOption.map { head =>
      value.drop(1).map(_.dataType).foldLeft(head.dataType)(Type.lub _)
    }).getOrElse(Type.Bottom) // TODO: ???
    def toJs = JsCore.Arr(value.map(_.toJs)).fix
  }

  case class Timestamp(value: Instant) extends Data {
    def dataType = Type.Timestamp
    def toJs = JsCore.New("Date", List(JsCore.Literal(Js.Str(value.toString)).fix)).fix
  }

  case class Date(value: LocalDate) extends Data {
    def dataType = Type.Date
    def toJs = JsCore.New("Date", List(JsCore.Literal(Js.Str(value.toString)).fix)).fix
  }

  case class Time(value: LocalTime) extends Data {
    def dataType = Type.Time
    def toJs = JsCore.Literal(Js.Str(value.toString)).fix
  }

  case class Interval(value: Duration) extends Data {
    def dataType = Type.Interval
    def toJs = JsCore.Literal(Js.Num(value.getSeconds*1000 + value.getNano*1e-6, true)).fix
  }

  case class Binary(value: ImmutableArray[Byte]) extends Data {
    def dataType = Type.Binary
    def toJs = JsCore.New("BinData", List(
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

  case class Id(value: String) extends Data {
    def dataType = Type.Id
    def toJs = JsCore.New("ObjectId", List(JsCore.Literal(Js.Str(value)).fix)).fix
  }

  /**
   An object to represent any value that might come from a backend, but that
   we either don't know about or can't represent in this ADT. We represent it
   with JS's `undefined`, just because no other value will ever be translated
   that way.
   */
  case object NA extends Data {
    def dataType = Type.Bottom
    def toJs = JsCore.Ident(Js.Undefined.ident).fix
  }
}
