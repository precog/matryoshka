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
import quasar.javascript._
import quasar.jscore, jscore.JsFn

import scala.Any
import scala.collection.JavaConverters._
import scala.Predef.{
  boolean2Boolean, double2Double, int2Integer, long2Long,
  Boolean2boolean, Double2double, Integer2int, Long2long}

import org.bson._
import org.bson.types
import org.threeten.bp.{Instant, ZoneOffset}
import org.threeten.bp.temporal.{ChronoUnit}
import scalaz._, Scalaz._

/**
 * A type-safe ADT for Mongo's native data format. Note that this representation
 * is not suitable for efficiently storing large quantities of data.
 */
sealed trait Bson {
  // TODO: Once Bson is fixpoint, this should be an algebra:
  //       BsonF[BsonValue] => BsonValue
  def repr: BsonValue
  def toJs: Js.Expr
}

object Bson {
  // TODO: Once Bson is fixpoint, this should be a coalgebra:
  //       BsonValue => BsonF[BsonValue]
  val fromRepr: BsonValue => Bson = {
    case arr:  BsonArray             => Arr(arr.getValues.asScala.toList ∘ fromRepr)
    case bin:  BsonBinary            => Binary(bin.getData)
    case bool: BsonBoolean           => Bool(bool.getValue)
    case dt:   BsonDateTime          => Date(Instant.ofEpochMilli(dt.getValue))
    case doc:  BsonDocument          => Doc(doc.asScala.toList.toListMap ∘ fromRepr)
    case dub:  BsonDouble            => Dec(dub.doubleValue)
    case i32:  BsonInt32             => Int32(i32.intValue)
    case i64:  BsonInt64             => Int64(i64.longValue)
    case _:    BsonMaxKey            => MaxKey
    case _:    BsonMinKey            => MinKey
    case _:    BsonNull              => Null
    case oid:  BsonObjectId          => ObjectId(oid.getValue.toByteArray)
    case rex:  BsonRegularExpression => Regex(rex.getPattern, rex.getOptions)
    case str:  BsonString            => Text(str.getValue)
    case sym:  BsonSymbol            => Symbol(sym.getSymbol)
    case tms:  BsonTimestamp         => Timestamp(Instant.ofEpochSecond(tms.getTime), tms.getInc)
    case _:    BsonUndefined         => Undefined
      // NB: These types we can’t currently translate back to Bson, but we don’t
      //     expect them to appear.
    case _: BsonDbPointer | _: BsonJavaScript | _: BsonJavaScriptWithScope => Undefined
  }

  final case class Dec(value: Double) extends Bson {
    def repr = new BsonDouble(value)

    def toJs = Js.Num(value, true)
  }
  final case class Text(value: String) extends Bson {
    def repr = new BsonString(value)

    def toJs = Js.Str(value)
  }
  final case class Binary(value: ImmutableArray[Byte]) extends Bson {
    def repr = new BsonBinary(value.toArray[Byte])
    def toJs = Js.Call(Js.Ident("BinData"), List(Js.Num(0, false), Js.Str(new sun.misc.BASE64Encoder().encode(value.toArray))))

    override def toString = "Binary(Array[Byte](" + value.mkString(", ") + "))"

    override def equals(that: Any): Boolean = that match {
      case Binary(value2) => value ≟ value2
      case _ => false
    }
    override def hashCode = java.util.Arrays.hashCode(value.toArray[Byte])
  }
  object Binary {
    def apply(array: Array[Byte]): Binary = Binary(ImmutableArray.fromArray(array))
  }
  final case class Doc(value: ListMap[String, Bson])
      extends Bson with org.bson.conversions.Bson {
    def repr: BsonDocument =
      new BsonDocument(value.toList.map { case (k, v) => new BsonElement(k, v.repr) }.asJava)
    def toJs = Js.AnonObjDecl((value ∘ (_.toJs)).toList)

    def toBsonDocument[TDocument](
      documentClass: java.lang.Class[TDocument],
      codecRegistry: org.bson.codecs.configuration.CodecRegistry) =
      repr
  }
  final case class Arr(value: List[Bson]) extends Bson {
    def repr = new BsonArray((value ∘ (_.repr)).asJava)
    def toJs = Js.AnonElem(value ∘ (_.toJs))
  }
  final case class ObjectId(value: ImmutableArray[Byte]) extends Bson {
    private def oid = new org.bson.types.ObjectId(value.toArray[Byte])
    def repr = new BsonObjectId(oid)

    def str = oid.toHexString

    def toJs = Js.Call(Js.Ident("ObjectId"), List(Js.Str(str)))

    override def toString = "ObjectId(" + str + ")"

    override def equals(that: Any): Boolean = that match {
      case ObjectId(value2) => value ≟ value2
      case _ => false
    }
    override def hashCode = java.util.Arrays.hashCode(value.toArray[Byte])
  }
  object ObjectId {
    def apply(array: Array[Byte]): ObjectId = ObjectId(ImmutableArray.fromArray(array))

    def apply(str: String): Option[ObjectId] = {
      \/.fromTryCatchNonFatal(new types.ObjectId(str)).toOption.map(oid => ObjectId(oid.toByteArray))
    }
  }
  final case class Bool(value: Boolean) extends Bson {
    def repr = new BsonBoolean(value)
    def toJs = Js.Bool(value)
  }
  final case class Date(value: Instant) extends Bson {
    def repr = new BsonDateTime(value.toEpochMilli)
    def toJs =
      Js.Call(Js.Ident("ISODate"), List(Js.Str(value.toString)))
  }
  final case object Null extends Bson {
    def repr = new BsonNull
    override def toJs = Js.Null
  }

  /** DEPRECATED in the spec, but the 3.0 Mongo driver returns it to us. */
  final case object Undefined extends Bson {
    def repr = new BsonUndefined
    override def toJs = Js.Undefined
  }
  final case class Regex(value: String, options: String) extends Bson {
    def repr = new BsonRegularExpression(value, options)
    def toJs = Js.New(Js.Call(Js.Ident("RegExp"), List(Js.Str(value), Js.Str(options))))
  }
  final case class JavaScript(value: Js.Expr) extends Bson {
    def repr = new BsonJavaScript(value.pprint(0))
    def toJs = value
  }
  final case class JavaScriptScope(code: Js.Expr, doc: ListMap[String, Bson])
      extends Bson {
    def repr =
      new BsonJavaScriptWithScope(
        code.pprint(0),
        new BsonDocument(doc.toList.map { case (k, v) => new BsonElement(k, v.repr) }.asJava))
    // FIXME: this loses scope, but I don’t know what it should look like
    def toJs = code
  }
  final case class Symbol(value: String) extends Bson {
    def repr = new BsonSymbol(value)
    def toJs = Js.Ident(value)
  }
  final case class Int32(value: Int) extends Bson {
    def repr = new BsonInt32(value)
    def toJs = Js.Call(Js.Ident("NumberInt"), List(Js.Num(value, false)))
  }
  final case class Int64(value: Long) extends Bson {
    def repr = new BsonInt64(value)
    def toJs = Js.Call(Js.Ident("NumberLong"), List(Js.Num(value, false)))
  }
  final case class Timestamp private (epochSecond: Int, ordinal: Int) extends Bson {
    def repr = new BsonTimestamp(epochSecond, ordinal)
    def toJs = Js.Call(Js.Ident("Timestamp"),
      List(Js.Num(epochSecond, false), Js.Num(ordinal, false)))
    override def toString = "Timestamp(" + Instant.ofEpochSecond(epochSecond) + ", " + ordinal + ")"
  }
  object Timestamp {
    def apply(instant: Instant, ordinal: Int): Timestamp =
      Timestamp((instant.toEpochMilli/1000).toInt, ordinal)
  }
  final case object MinKey extends Bson {
    def repr = new BsonMinKey()
    def toJs = Js.Ident("MinKey")
  }
  final case object MaxKey extends Bson {
    def repr = new BsonMaxKey()
    def toJs = Js.Ident("MaxKey")
  }
}

sealed trait BsonType {
  def ordinal: Int
}

object BsonType {
  private[BsonType] abstract class AbstractType(val ordinal: Int) extends BsonType
  final case object Dec extends AbstractType(1)
  final case object Text extends AbstractType(2)
  final case object Doc extends AbstractType(3)
  final case object Arr extends AbstractType(4)
  final case object Binary extends AbstractType(5)
  final case object Undefined extends AbstractType(6)
  final case object ObjectId extends AbstractType(7)
  final case object Bool extends AbstractType(8)
  final case object Date extends AbstractType(9)
  final case object Null extends AbstractType(10)
  final case object Regex extends AbstractType(11)
  final case object JavaScript extends AbstractType(13)
  final case object JavaScriptScope extends AbstractType(15)
  final case object Symbol extends AbstractType(14)
  final case object Int32 extends AbstractType(16)
  final case object Int64 extends AbstractType(18)
  final case object Timestamp extends AbstractType(17)
  final case object MinKey extends AbstractType(255)
  final case object MaxKey extends AbstractType(127)
}

sealed trait BsonField {
  def asText  : String
  def asField : String = "$" + asText
  def asVar   : String = "$$" + asText

  def bson      = Bson.Text(asText)
  def bsonField = Bson.Text(asField)
  def bsonVar   = Bson.Text(asVar)

  import BsonField._

  def \ (that: BsonField): BsonField = (this, that) match {
    case (Path(x),     Path(y))     => Path(x ⊹ y)
    case (Path(x),     y @ Name(_)) => Path(x ⊹ NonEmptyList(y))
    case (x @ Name(_), Path(y))     => Path(x <:: y)
    case (x @ Name(_), y @ Name(_)) => Path(NonEmptyList(x, y))
  }

  def \\ (tail: List[BsonField]): BsonField =
    if (tail.isEmpty) this
    else {
      val t = tail.flatMap(_.flatten.toList)
      this match {
        case Path(p)     => Path(p :::> t)
        case l @ Name(_) => Path(NonEmptyList.nel(l, t))
    }
  }

  def flatten: NonEmptyList[Name]

  def parent: Option[BsonField] =
    BsonField(flatten.toList.dropRight(1))

  def startsWith(that: BsonField) =
    this.flatten.toList.startsWith(that.flatten.toList)

  def relativeTo(that: BsonField): Option[BsonField] = {
    val f1 = flatten.toList
    val f2 = that.flatten.toList
    if (f1 startsWith f2) BsonField(f1.drop(f2.length))
    else None
  }

  def toJs: JsFn =
    this.flatten.foldLeft(JsFn.identity)((acc, leaf) =>
      leaf match {
        case Name(v)  => JsFn(JsFn.defaultName, jscore.Access(acc(jscore.Ident(JsFn.defaultName)), jscore.Literal(Js.Str(v))))
      })

  override def hashCode = this match {
    case Name(v) => v.hashCode
    case Path(v) if (v.tail.length ≟ 0) => v.head.hashCode
    case p @ Path(_) => p.flatten.hashCode
  }

  override def equals(that: Any): Boolean = (this, that) match {
    case (Name(v1),      Name(v2))      => v1 ≟ v2
    case (v1: BsonField, v2: BsonField) => v1.flatten.equals(v2.flatten)
    case _                              => false
  }
}

object BsonField {
  sealed trait Root
  final case object Root extends Root

  def apply(v: NonEmptyList[BsonField.Name]): BsonField = v match {
    case NonEmptyList(head) => head
    case _ => Path(v)
  }

  def apply(v: List[BsonField.Name]): Option[BsonField] = v.toNel.map(apply)

  final case class Name(value: String) extends BsonField {
    def asText = Path(NonEmptyList(this)).asText

    def flatten = NonEmptyList(this)

    override def toString = s"""BsonField.Name("$value")"""
  }

  final case class Path(values: NonEmptyList[Name]) extends BsonField {
    def flatten = values

    def asText = (values.list.zipWithIndex.map {
      case (Name(value), 0) => value
      case (Name(value), _) => "." + value
    }).mkString("")

    override def toString = values.list.mkString(" \\ ")
  }
}
