package slamdata.engine.std

import scalaz._

import slamdata.engine.{Data, Func, Type, Mapping, SemanticError}

import SemanticError._
import Validation.{success, failure}
import NonEmptyList.nel

trait DateLib extends Library {
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
    Type.typecheck(_, Type.Numeric) map { _ => Type.Str :: Type.Temporal :: Nil }
  )

  val ToDate = Mapping(
    "date",
    "Converts a string literal (YYYY-MM-DD) to a date constant.",
    Type.Str :: Nil,
    partialTyper {
      case Type.Const(Data.Str(_)) :: Nil => Type.DateTime
    },
    Type.typecheck(_, Type.DateTime) map { _ => Type.Str :: Nil }
  )

  val ToTime = Mapping(
    "time",
    "Converts a string literal (HH:MM:SS[.SSS]) to a time constant.",
    Type.Str :: Nil,
    partialTyper {
      case Type.Const(Data.Str(_)) :: Nil => Type.DateTime
    },
    Type.typecheck(_, Type.DateTime) map { _ => Type.Str :: Nil }
  )

  val ToTimestamp = Mapping(
    "timestamp",
    "Converts a string literal (ISO 8601) to a timestamp constant.",
    Type.Str :: Nil,
    partialTyper {
      case Type.Const(Data.Str(_)) :: Nil => Type.DateTime
    },
    Type.typecheck(_, Type.DateTime) map { _ => Type.Str :: Nil }
  )

  val ToInterval = Mapping(
    "interval",
    "Converts a string literal (ISO 8601) to an interval constant.",
    Type.Str :: Nil,
    partialTyper {
      case Type.Const(Data.Str(_)) :: Nil => Type.Interval
    },
    Type.typecheck(_, Type.Interval) map { _ => Type.Str :: Nil }
  )

  def functions = Extract :: ToDate :: ToTime :: ToTimestamp :: ToInterval :: Nil
}
object DateLib extends DateLib
