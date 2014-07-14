package slamdata.engine.physical.mongodb

import slamdata.engine.Data

import org.threeten.bp.Instant

import com.mongodb._
import org.bson.types

import scalaz._
import scalaz.syntax.traverse._
import scalaz.std.list._
import scalaz.std.map._

/**
 * A type-safe ADT for Mongo's native data format. Note that this representation
 * is not suitable for efficiently storing large quantities of data.
 */
sealed trait Bson {
  def bsonType: BsonType

  def repr: AnyRef
}

object Bson {
  case class ConversionError(data: Data)

  def fromData(data: Data): ConversionError \/ Bson = {
    data match {
      case Data.Null => \/ right (Bson.Null)
      
      case Data.Str(value) => \/ right (Bson.Text(value))

      case Data.True => \/ right (Bson.Bool(true))
      case Data.False => \/ right (Bson.Bool(false))

      case Data.Dec(value) => \/ right (Bson.Dec(value.toDouble))
      case Data.Int(value) => \/ right (Bson.Int64(value.toLong))

      case Data.Obj(value) => 
        type MapF[X] = Map[String, X] 
        type Right[X] = ConversionError \/ X

        val map: Map[String, ConversionError \/ Bson] = value.mapValues(fromData _)

        Traverse[MapF].sequence[Right, Bson](map).map(Bson.Doc.apply _)

      case Data.Arr(value) => value.map(fromData _).sequenceU.map(Bson.Arr.apply _)

      case Data.Set(value) => value.map(fromData _).sequenceU.map(Bson.Arr.apply _)

      case Data.DateTime(value) => \/ right (Bson.Date(value))

      case Data.Interval(value) => \/ left (ConversionError(data))

      case Data.Binary(value) => \/ right (Bson.Binary(value))
    }
  }

  case class Dec(value: Double) extends Bson {
    def bsonType = BsonType.Dec

    def repr = value: java.lang.Double

    override def toString = s"Bson.Dec($value)"
  }
  case class Text(value: String) extends Bson {
    def bsonType = BsonType.Text

    def repr = value

    override def toString = s"""Bson.Text("$value")"""
  }
  case class Binary(value: Array[Byte]) extends Bson {
    def bsonType = BsonType.Binary

    def repr = value

    override def toString = s"Bson.Binary($value)"
  }
  case class Doc(value: Map[String, Bson]) extends Bson {
    def bsonType = BsonType.Doc

    def repr: DBObject = value.foldLeft(new BasicDBObject) {
      case (obj, (name, value)) =>
        obj.put(name, value.repr)

        obj
    }

    override def toString = s"Bson.Doc($value)"
  }
  case class Arr(value: Seq[Bson]) extends Bson {
    def bsonType = BsonType.Arr

    def repr = value.foldLeft(new BasicDBList) {
      case (array, value) =>
        array.add(value.repr)

        array
    }

    override def toString = s"Bson.Arr($value)"
  }
  case class ObjectId(value: Array[Byte]) extends Bson {
    def bsonType = BsonType.ObjectId

    def repr = new types.ObjectId(value)

    override def toString = s"Bson.ObjectId($value)"
  }
  case class Bool(value: Boolean) extends Bson {
    def bsonType = BsonType.Bool

    def repr = value: java.lang.Boolean

    override def toString = s"Bson.Bool($value)"
  }
  case class Date(value: Instant) extends Bson {
    def bsonType = BsonType.Date

    def repr = new java.util.Date(value.toEpochMilli)

    override def toString = s"Bson.Date($value)"
  }
  case object Null extends Bson {
    def bsonType = BsonType.Null

    def repr = null

    override def toString = s"Bson.Null"
  }
  case class Regex(value: String) extends Bson {
    def bsonType = BsonType.Regex

    def repr = java.util.regex.Pattern.compile(value)

    override def toString = s"""Bson.Regex("$value")"""
  }
  case class JavaScript(value: Js) extends Bson {
    def bsonType = BsonType.JavaScript

    def repr = new types.Code(value.render(2))

    override def toString = s"Bson.JavaScript($value)"
  }
  case class JavaScriptScope(code: Js, doc: Doc) extends Bson {
    def bsonType = BsonType.JavaScriptScope

    def repr = new types.CodeWScope(code.render(2), doc.repr)

    override def toString = s"Bson.JavaScriptScope($code, $doc)"
  }
  case class Symbol(value: String) extends Bson {
    def bsonType = BsonType.Symbol

    def repr = new types.Symbol(value)

    override def toString = s"Bson.Symbol($value)"
  }
  case class Int32(value: Int) extends Bson {
    def bsonType = BsonType.Int32

    def repr = value: java.lang.Integer

    override def toString = s"Bson.Int32($value)"
  }
  case class Int64(value: Long) extends Bson {
    def bsonType = BsonType.Int64

    def repr = value: java.lang.Long

    override def toString = s"Bson.Int64($value)"
  }
  case class Timestamp(instant: Instant, ordinal: Int) extends Bson {
    def bsonType = BsonType.Timestamp

    def repr = new types.BSONTimestamp((instant.toEpochMilli / 1000).toInt, ordinal)

    override def toString = s"Bson.Timestamp($instant, $ordinal)"
  }
  case object MinKey extends Bson {
    def bsonType = BsonType.MinKey

    def repr = new types.MinKey

    override def toString = s"Bson.MinKey"
  }
  case object MaxKey extends Bson {
    def bsonType = BsonType.MaxKey

    def repr = new types.MaxKey

    override def toString = s"Bson.MaxKey"
  }
}

sealed trait BsonType {
  def ordinal: Int
}

object BsonType {
  private[BsonType] abstract class AbstractType(val ordinal: Int) extends BsonType
  case object Dec extends AbstractType(1)
  case object Text extends AbstractType(2)
  case object Doc extends AbstractType(3)
  case object Arr extends AbstractType(4)
  case object Binary extends AbstractType(5)
  case object ObjectId extends AbstractType(7)
  case object Bool extends AbstractType(8)
  case object Date extends AbstractType(9)
  case object Null extends AbstractType(10)
  case object Regex extends AbstractType(11)
  case object JavaScript extends AbstractType(13)
  case object JavaScriptScope extends AbstractType(15)
  case object Symbol extends AbstractType(14)
  case object Int32 extends AbstractType(16)
  case object Int64 extends AbstractType(18)
  case object Timestamp extends AbstractType(17)
  case object MinKey extends AbstractType(255)
  case object MaxKey extends AbstractType(127)
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
    case (x : Leaf, y : Leaf) => Path(NonEmptyList.nels(x, y))
    case (x : Path, y : Leaf) => Path(NonEmptyList.nel(x.values.head, x.values.tail :+ y))
    case (y : Leaf, x : Path) => Path(NonEmptyList.nel(y, x.values.list))
    case (x : Path, y : Path) => Path(NonEmptyList.nel(x.values.head, x.values.tail ++ y.values.list))
  }

  def \\ (tail: List[BsonField]): BsonField = this match {
    case l : Leaf => Path(NonEmptyList.nel(l, tail.flatMap(_.flatten)))
    case p : Path => Path(NonEmptyList.nel(p.values.head, p.values.tail ::: tail.flatMap(_.flatten)))
  }

  def flatten: List[Leaf]

  override def hashCode = this match {
    case Name(v) => v.hashCode
    case Index(v) => v.hashCode
    case Path(v) if (v.tail.length == 0) => v.head.hashCode
    case p @ Path(_) => p.flatten.hashCode
  }

  override def equals(that: Any): Boolean = (this, that) match {
    case (Name(v1), Name(v2)) => v1 == v2
    case (Name(_), Index(_)) => false
    case (Index(v1), Index(v2)) => v1 == v2
    case (Index(_), Name(_)) => false
    case (v1 : BsonField, v2 : BsonField) => v1.flatten.equals(v2.flatten)

    case _ => false
  }
}

object BsonField {
  def apply(v: List[BsonField.Leaf]): Option[BsonField] = v match {
    case Nil => None
    case head :: Nil => Some(head)
    case head :: tail => Some(Path(NonEmptyList.nel(head, tail)))
  }

  sealed trait Leaf extends BsonField {
    def asText = Path(NonEmptyList(this)).asText

    def flatten: List[Leaf] = this :: Nil

    // Distinction between these is artificial as far as BSON concerned so you 
    // can always translate a leaf to a Name (but not an Index since the key might
    // not be numeric).
    def toName: Name = this match {
      case n @ Name(_) => n
      case Index(idx) => Name(idx.toString)
    }
  }

  case class Name(value: String) extends Leaf {
    override def toString = s"""BsonField.Name("$value")"""
  }
  case class Index(value: Int) extends Leaf {
    override def toString = s"BsonField.Index($value)"
  }

  private case class Path(values: NonEmptyList[Leaf]) extends BsonField {
    def flatten: List[Leaf] = values.list

    def asText = (values.list.zipWithIndex.map { 
      case (Name(value), 0) => value
      case (Name(value), _) => "." + value
      case (Index(value), 0) => value.toString
      case (Index(value), _) => "." + value.toString
    }).mkString("")
    
    override def toString = values.list.mkString(" \\ ")
  }

  private lazy val TempNames:   EphemeralStream[BsonField.Name]  = EphemeralStream.iterate(0)(_ + 1).map(i => BsonField.Name("__sd_tmp_" + i.toString))
  private lazy val TempIndices: EphemeralStream[BsonField.Index] = EphemeralStream.iterate(0)(_ + 1).map(i => BsonField.Index(i))

  def genUniqName(v: Iterable[BsonField.Name]): BsonField.Name = genUniqNames(1, v).head

  def genUniqNames(n: Int, v: Iterable[BsonField.Name]): List[BsonField.Name] = {
    val s = v.toSet

    TempNames.filter(n => !s.contains(n)).take(n).toList
  }

  def genUniqIndex(v: Iterable[BsonField.Index]): BsonField.Index = genUniqIndices(1, v).head

  def genUniqIndices(n: Int, v: Iterable[BsonField.Index]): List[BsonField.Index] = {
    val s = v.toSet

    TempIndices.filter(n => !s.contains(n)).take(n).toList
  }
}