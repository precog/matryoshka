package slamdata.engine.physical.mongodb

import org.threeten.bp.Instant

import org.bson._

import scalaz.NonEmptyList

/**
 * A type-safe ADT for Mongo's native data format. Note that this representation
 * is not suitable for efficiently storing large quantities of data.
 */
sealed trait Bson {
  def bsonType: BsonType

  def repr: AnyRef
}

object Bson {
  case class Dec(value: Double) extends Bson {
    def bsonType = BsonType.Dec

    def repr = value: java.lang.Double
  }
  case class Text(value: String) extends Bson {
    def bsonType = BsonType.Text

    def repr = value
  }
  case class Doc(value: Map[String, Bson]) extends Bson {
    def bsonType = BsonType.Doc

    def repr = value.foldLeft(new BasicBSONObject) {
      case (obj, (name, value)) =>
        obj.put(name, value.repr)

        obj
    }
  }
  case class Arr(value: Seq[Bson]) extends Bson {
    def bsonType = BsonType.Arr

    def repr = value.foldLeft(new types.BasicBSONList) {
      case (array, value) =>
        array.add(value.repr)

        array
    }
  }
  case class ObjectId(value: Array[Byte]) extends Bson {
    def bsonType = BsonType.ObjectId

    def repr = new types.ObjectId(value)
  }
  case class Bool(value: Boolean) extends Bson {
    def bsonType = BsonType.Bool

    def repr = value: java.lang.Boolean
  }
  case class Date(value: Instant) extends Bson {
    def bsonType = BsonType.Date

    def repr = new java.util.Date(value.toEpochMilli)
  }
  case object Null extends Bson {
    def bsonType = BsonType.Null

    def repr = null
  }
  case class Regex(value: String) extends Bson {
    def bsonType = BsonType.Regex

    def repr = java.util.regex.Pattern.compile(value)
  }
  case class JavaScript(value: Js) extends Bson {
    def bsonType = BsonType.JavaScript

    def repr = new types.Code(value.render(2))
  }
  case class JavaScriptScope(code: Js, doc: Doc) extends Bson {
    def bsonType = BsonType.JavaScriptScope

    def repr = new types.CodeWScope(code.render(2), doc.repr)
  }
  case class Symbol(value: String) extends Bson {
    def bsonType = BsonType.Symbol

    def repr = new types.Symbol(value)
  }
  case class Int32(value: Int) extends Bson {
    def bsonType = BsonType.Int32

    def repr = value: java.lang.Integer
  }
  case class Int64(value: Long) extends Bson {
    def bsonType = BsonType.Int64

    def repr = value: java.lang.Long
  }
  case class Timestamp(instant: Instant, ordinal: Int) extends Bson {
    def bsonType = BsonType.Timestamp

    def repr = new types.BSONTimestamp((instant.toEpochMilli / 1000).toInt, ordinal)
  }
  case object MinKey extends Bson {
    def bsonType = BsonType.MinKey

    def repr = new types.MinKey
  }
  case object MaxKey extends Bson {
    def bsonType = BsonType.MaxKey

    def repr = new types.MaxKey
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
  def bsonText: String

  def bson = Bson.Text(bsonText)
}

object BsonField {
  sealed trait Leaf extends BsonField {
    def bsonText = Path(NonEmptyList(this)).bsonText
  }

  case class Name(value: String) extends Leaf
  case class Index(value: Int) extends Leaf

  case class Path(values: NonEmptyList[Leaf]) extends BsonField {
    def bsonText = (values.list.zipWithIndex.map { 
      case (Name(value), 0) => value
      case (Name(value), _) => "." + value
      case (Index(value), 0) => value.toString
      case (Index(value), _) => "." + value.toString
    }).mkString("")
  }
}