package slamdata.engine

import org.threeten.bp.{Instant, Duration}

sealed trait Data

object Data {
  case object Null extends Data
  
  case class Str(value: String)

  sealed trait Bool extends Data
  case object True extends Bool
  case object False extends Bool

  sealed trait Number extends Data
  case class Dec(value: BigDecimal) extends Number
  case class Int(value: BigInt) extends Number

  case class Obj(value: Map[String, Data]) extends Data

  case class Arr(value: Seq[Data]) extends Data

  case class DateTime(value: Instant) extends Data

  case class Interval(value: Duration) extends Data

  case class Bytes(value: Array[Byte]) extends Data
}
