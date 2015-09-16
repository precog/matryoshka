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

import org.bson.types
import org.threeten.bp.{Instant, ZoneOffset}
import org.threeten.bp.temporal.{ChronoUnit}
import scalaz._, Scalaz._

/**
 * A type-safe ADT for Mongo's native data format. Note that this representation
 * is not suitable for efficiently storing large quantities of data.
 */
sealed trait Bson {
  def repr: java.lang.Object
  def toJs: Js.Expr
}

object Bson {
  def fromRepr(obj: java.lang.Object): Bson = {
    @SuppressWarnings(Array("org.brianmckenna.wartremover.warts.Null"))
    def loop(v: Any): Bson = v match {
      case null                       => Null
      case x: String                  => Text(x)
      case x: java.lang.Boolean       => Bool(x)
      case x: java.lang.Integer       => Int32(x)
      case x: java.lang.Long          => Int64(x)
      case x: java.lang.Double        => Dec(x)
      case list: java.util.ArrayList[_] => Arr(list.asScala.map(loop).toList)
      case obj: org.bson.Document     => Doc(obj.keySet.asScala.toList.map(k => k -> loop(obj.get(k))).toListMap)
      case x: java.util.Date          => Date(Instant.ofEpochMilli(x.getTime))
      case x: types.ObjectId          => ObjectId(x.toByteArray)
      case x: types.Binary            => Binary(x.getData)
      case _: types.MinKey            => MinKey
      case _: types.MaxKey            => MaxKey
      case x: types.Symbol            => Symbol(x.getSymbol)
      case x: types.BSONTimestamp     => Timestamp(Instant.ofEpochSecond(x.getTime), x.getInc)
      case x: java.util.regex.Pattern => Regex(x.pattern, "")
      case x: org.bson.BsonRegularExpression => Regex(x.getPattern, x.getOptions)
      case x: Array[Byte]             => Binary(x)
      case x: java.util.UUID          =>
        val bos = new java.io.ByteArrayOutputStream
        val dos = new java.io.DataOutputStream(bos)
        dos.writeLong(x.getLeastSignificantBits)
        dos.writeLong(x.getMostSignificantBits)
        Binary(bos.toByteArray.reverse)
      // NB: the remaining types are not easily translated back to Bson,
      //     and we don't expect them to appear anyway.
      //   • JavaScript/JavaScriptScope: would require parsing a string to our
      //     Js type.
      //   • Any other value that might be produced by MongoDB which is unknown
      //     to us.
      case _ => Undefined
    }

    loop(obj)
  }

  final case class Dec(value: Double) extends Bson {
    def repr = value: java.lang.Double
    def toJs = Js.Num(value, true)
  }
  final case class Text(value: String) extends Bson {
    def repr = value
    def toJs = Js.Str(value)
  }
  final case class Binary(value: ImmutableArray[Byte]) extends Bson {
    def repr = value.toArray[Byte]
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
  final case class Doc(value: ListMap[String, Bson]) extends Bson {
    def repr: org.bson.Document = new org.bson.Document((value ∘ (_.repr)).asJava)
    def toJs = Js.AnonObjDecl((value ∘ (_.toJs)).toList)
  }
  final case class Arr(value: List[Bson]) extends Bson {
    def repr = new java.util.ArrayList(value.map(_.repr).asJava)
    def toJs = Js.AnonElem(value ∘ (_.toJs))
  }
  final case class ObjectId(value: ImmutableArray[Byte]) extends Bson {
    def repr = new types.ObjectId(value.toArray[Byte])

    def str = repr.toHexString

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
    def repr = value: java.lang.Boolean
    def toJs = Js.Bool(value)
  }
  final case class Date(value: Instant) extends Bson {
    def repr = new java.util.Date(value.toEpochMilli)
    def toJs =
      Js.Call(Js.Ident("ISODate"), List(Js.Str(value.toString)))
  }
  final case object Null extends Bson {
    @SuppressWarnings(Array("org.brianmckenna.wartremover.warts.Null"))
    def repr = null
    override def toJs = Js.Null
  }

  /** DEPRECATED in the spec, but the 3.0 Mongo driver returns it to us. */
  final case object Undefined extends Bson {
    def repr = new org.bson.BsonUndefined
    override def toJs = Js.Undefined
  }
  final case class Regex(value: String, options: String) extends Bson {
    def repr = new org.bson.BsonRegularExpression(value, options)
    def toJs = Js.New(Js.Call(Js.Ident("RegExp"), List(Js.Str(value), Js.Str(options))))
  }
  final case class JavaScript(value: Js.Expr) extends Bson {
    def repr = new types.Code(value.pprint(2))
    def toJs = value
  }
  final case class JavaScriptScope(code: Js.Expr, doc: Doc) extends Bson {
    def repr = new types.CodeWithScope(code.pprint(2), doc.repr)
    // FIXME: this loses scope, but I don’t know what it should look like
    def toJs = code
  }
  final case class Symbol(value: String) extends Bson {
    def repr = new types.Symbol(value)
    def toJs = Js.Ident(value)
  }
  final case class Int32(value: Int) extends Bson {
    def repr = value: java.lang.Integer
    def toJs = Js.Call(Js.Ident("NumberInt"), List(Js.Num(value, false)))
  }
  final case class Int64(value: Long) extends Bson {
    def repr = value: java.lang.Long
    def toJs = Js.Call(Js.Ident("NumberLong"), List(Js.Num(value, false)))
  }
  final case class Timestamp private (epochSecond: Int, ordinal: Int) extends Bson {
    def repr = new types.BSONTimestamp(epochSecond, ordinal)
    def toJs = Js.Call(Js.Ident("Timestamp"),
      List(Js.Num(epochSecond, false), Js.Num(ordinal, false)))
    override def toString = "Timestamp(" + Instant.ofEpochSecond(epochSecond) + ", " + ordinal + ")"
  }
  object Timestamp {
    def apply(instant: Instant, ordinal: Int): Timestamp =
      Timestamp((instant.toEpochMilli/1000).toInt, ordinal)
  }
  final case object MinKey extends Bson {
    def repr = new types.MinKey
    def toJs = Js.Ident("MinKey")
  }
  final case object MaxKey extends Bson {
    def repr = new types.MaxKey
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
    case (Path(x), Path(y)) => Path(NonEmptyList.nel(x.head, x.tail ++ y.list))
    case (Path(x), y: Leaf) => Path(NonEmptyList.nel(x.head, x.tail :+ y))
    case (y: Leaf, Path(x)) => Path(NonEmptyList.nel(y, x.list))
    case (x: Leaf, y: Leaf) => Path(NonEmptyList.nels(x, y))
  }

  def \\ (tail: List[BsonField]): BsonField = if (tail.isEmpty) this else this match {
    case Path(p) => Path(NonEmptyList.nel(p.head, p.tail ::: tail.flatMap(_.flatten.toList)))
    case l: Leaf => Path(NonEmptyList.nel(l, tail.flatMap(_.flatten.toList)))
  }

  def flatten: NonEmptyList[Leaf]

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
        case Index(v) => JsFn(JsFn.defaultName, jscore.Access(acc(jscore.Ident(JsFn.defaultName)), jscore.Literal(Js.Num(v, false))))
      })

  override def hashCode = this match {
    case Name(v) => v.hashCode
    case Index(v) => v.hashCode
    case Path(v) if (v.tail.length ≟ 0) => v.head.hashCode
    case p @ Path(_) => p.flatten.hashCode
  }

  override def equals(that: Any): Boolean = (this, that) match {
    case (Name(v1),      Name(v2))      => v1 ≟ v2
    case (Name(_),       Index(_))      => false
    case (Index(v1),     Index(v2))     => v1 ≟ v2
    case (Index(_),      Name(_))       => false
    case (v1: BsonField, v2: BsonField) => v1.flatten.equals(v2.flatten)
    case _                              => false
  }
}

object BsonField {
  sealed trait Root
  final case object Root extends Root

  def apply(v: NonEmptyList[BsonField.Leaf]): BsonField = v match {
    case NonEmptyList(head) => head
    case _ => Path(v)
  }

  def apply(v: List[BsonField.Leaf]): Option[BsonField] = v.toNel.map(apply)

  sealed trait Leaf extends BsonField {
    def asText = Path(NonEmptyList(this)).asText

    def flatten = NonEmptyList(this)

    // Distinction between these is artificial as far as BSON concerned so you
    // can always translate a leaf to a Name (but not an Index since the key might
    // not be numeric).
    def toName: Name = this match {
      case n @ Name(_) => n
      case Index(idx) => Name(idx.toString)
    }
  }

  final case class Name(value: String) extends Leaf {
    override def toString = s"""BsonField.Name("$value")"""
  }
  final case class Index(value: Int) extends Leaf {
    override def toString = s"BsonField.Index($value)"
  }

  private final case class Path(values: NonEmptyList[Leaf]) extends BsonField {
    def flatten = values

    def asText = (values.list.zipWithIndex.map {
      case (Name(value), 0) => value
      case (Name(value), _) => "." + value
      case (Index(value), 0) => value.toString
      case (Index(value), _) => "." + value.toString
    }).mkString("")

    override def toString = values.list.mkString(" \\ ")
  }
}
