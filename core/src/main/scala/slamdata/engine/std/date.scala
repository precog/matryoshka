package slamdata.engine.std

import scalaz._
import Validation.{success, failure}
import NonEmptyList.nel

import org.threeten.bp.{Duration, Instant, LocalDate, LocalTime, Period, ZoneOffset}

import slamdata.engine.{Data, Func, Type, Mapping, SemanticError}
import slamdata.engine.fp._
import SemanticError._

trait DateLib extends Library {
  def parseTimestamp(str: String): SemanticError \/ Data.Timestamp =
    \/.fromTryCatchNonFatal(Instant.parse(str)).bimap(
      κ(DateFormatError(ToTimestamp, str)),
      Data.Timestamp.apply)

  def parseDate(str: String): SemanticError \/ Data.Date =
    \/.fromTryCatchNonFatal(LocalDate.parse(str)).bimap(
      κ(DateFormatError(ToDate, str)),
      Data.Date.apply)

  def parseTime(str: String): SemanticError \/ Data.Time =
    \/.fromTryCatchNonFatal(LocalTime.parse(str)).bimap(
      κ(DateFormatError(ToTime, str)),
      Data.Time.apply)

  def parseInterval(str: String): SemanticError \/ Data.Interval =
    \/.fromTryCatchNonFatal(Duration.parse(str)).bimap(
      κ(DateFormatError(ToInterval, str)),
      Data.Interval.apply)

  private def startOfDayInstant(date: LocalDate): Instant =
    date.atStartOfDay.atZone(ZoneOffset.UTC).toInstant

  def startOfDay(date: Data.Date): Data.Timestamp =
    Data.Timestamp(startOfDayInstant(date.value))

  def startOfNextDay(date: Data.Date): Data.Timestamp =
    Data.Timestamp(startOfDayInstant(date.value.plus(Period.ofDays(1))))

  // NB: SQL specifies a function called `extract`, but that doesn't have comma-
  //     separated arguments. `date_part` is Postgres’ name for the same thing
  //     with commas.
  val Extract = Mapping(
    "date_part",
    "Pulls out a part of the date.",
    Type.Str :: Type.Temporal :: Nil,
    partialTyper {
      case Type.Const(Data.Str(_)) :: Type.Temporal :: Nil => Type.Numeric
    },
    Type.typecheck(_, Type.Numeric) map κ(Type.Str :: Type.Temporal :: Nil)
  )

  val ToDate = Mapping(
    "date",
    "Converts a string literal (YYYY-MM-DD) to a date constant.",
    Type.Str :: Nil,
    partialTyperV {
      case Type.Const(Data.Str(str)) :: Nil => parseDate(str).map(Type.Const(_)).validation.toValidationNel
    },
    Type.typecheck(_, Type.Date) map κ(Type.Str :: Nil)
  )

  val ToTime = Mapping(
    "time",
    "Converts a string literal (HH:MM:SS[.SSS]) to a time constant.",
    Type.Str :: Nil,
    partialTyperV {
      case Type.Const(Data.Str(str)) :: Nil => parseTime(str).map(Type.Const(_)).validation.toValidationNel
    },
    Type.typecheck(_, Type.Time) map κ(Type.Str :: Nil)
  )

  val ToTimestamp = Mapping(
    "timestamp",
    "Converts a string literal (ISO 8601, UTC) to a timestamp constant.",
    Type.Str :: Nil,
    partialTyperV {
      case Type.Const(Data.Str(str)) :: Nil => parseTimestamp(str).map(Type.Const(_)).validation.toValidationNel
    },
    Type.typecheck(_, Type.Timestamp) map κ(Type.Str :: Nil)
  )

  val ToInterval = Mapping(
    "interval",
    "Converts a string literal (ISO 8601) to an interval constant.",
    Type.Str :: Nil,
    partialTyperV {
      case Type.Const(Data.Str(str)) :: Nil => parseInterval(str).map(Type.Const(_)).validation.toValidationNel
    },
    Type.typecheck(_, Type.Interval) map κ(Type.Str :: Nil)
  )

  val TimeOfDay = Mapping(
    "time_of_day",
    "Extracts the time of day from a (UTC) timestamp value.",
    Type.Timestamp :: Nil,
    partialTyper {
      case Type.Const(Data.Timestamp(value)) :: Nil => Type.Const(Data.Time(value.atZone(ZoneOffset.UTC).toLocalTime))
      case Type.Timestamp :: Nil => Type.Time
    },
    Type.typecheck(_, Type.Time) map κ(Type.Timestamp :: Nil)
  )

  def functions = Extract :: ToDate :: ToTime :: ToTimestamp :: ToInterval :: TimeOfDay :: Nil
}
object DateLib extends DateLib
