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
    _ match {
      case Type.Numeric => success(Type.Str :: Type.Temporal :: Nil)
      case t => failure(nel(TypeError(Type.Numeric, t, Some("numeric function where non-numeric expression is expected")), Nil))
    }
  )

  def functions = Extract :: Nil
}
object DateLib extends DateLib
