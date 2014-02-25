package slamdata.engine

import org.threeten.bp.{Instant, Duration}

sealed trait Data {
  def dataType: Type
}

object Data {
  case object Null extends Data {
    def dataType = Type.Null
  }
  
  case class Str(value: String) extends Data {
    def dataType = Type.Str
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
  case object True extends Bool
  case object False extends Bool

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
  }
  case class Int(value: BigInt) extends Number {
    def dataType = Type.Int
  }

  case class Obj(value: Map[String, Data]) extends Data {
    def dataType = (value.map {
      case (name, data) => Type.ObjField(name, data.dataType)
    }).foldLeft[Type](Type.Top)(_ & _)
  }

  case class Arr(value: Seq[Data]) extends Data {
    def dataType = (value.zipWithIndex.map {
      case (data, index) => Type.ArrayElem(index, data.dataType)
    }).foldLeft[Type](Type.Top)(_ & _)
  }

  case class DateTime(value: Instant) extends Data {
    def dataType = Type.DateTime
  }

  case class Interval(value: Duration) extends Data {
    def dataType = Type.Interval
  }

  case class Binary(value: Array[Byte]) extends Data {
    def dataType = Type.Binary
  }
}
