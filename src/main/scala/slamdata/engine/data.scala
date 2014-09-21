package slamdata.engine

import org.threeten.bp.{Instant, Duration}

sealed trait Data {
  def dataType: Type
}

object Data {
  case object Null extends Data {
    def dataType = Type.Null
    
    override def toString = "Data.Null"
  }
  
  case class Str(value: String) extends Data {
    def dataType = Type.Str
    
    override def toString = s"""Data.Str("$value")"""
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
    override def toString = "Data.True"
  }
  case object False extends Bool {
    override def toString = "Data.False"
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
    
    override def toString = s"Data.Dec($value)"
  }
  case class Int(value: BigInt) extends Number {
    def dataType = Type.Int
    
    override def toString = s"Data.Int($value)"
  }

  case class Obj(value: Map[String, Data]) extends Data {
    def dataType = (value.map {
      case (name, data) => Type.NamedField(name, data.dataType)
    }).foldLeft[Type](Type.Top)(_ & _)
  }

  case class Arr(value: List[Data]) extends Data {
    def dataType = (value.zipWithIndex.map {
      case (data, index) => Type.IndexedElem(index, data.dataType)
    }).foldLeft[Type](Type.Top)(_ & _)
  }

  case class Set(value: List[Data]) extends Data {
    def dataType = (value.headOption.map { head => 
      value.drop(1).map(_.dataType).foldLeft(head.dataType)(Type.lub _)
    }).getOrElse(Type.Bottom) // TODO: ???
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
