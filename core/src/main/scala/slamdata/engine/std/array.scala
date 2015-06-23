package slamdata.engine.std

import scalaz._

import slamdata.engine.{Data, Func, Type, Mapping, SemanticError}
import slamdata.engine.fp._

import SemanticError._
import Validation.{success, failure}
import NonEmptyList.nel

trait ArrayLib extends Library {
  val ArrayLength = Mapping(
    "array_length",
    "Gets the length of a given dimension of an array.",
    Type.AnyArray :: Type.Int :: Nil,
    noSimplification,
    partialTyperV {
      case _ :: Type.Const(Data.Int(dim)) :: Nil if (dim < 1) =>
        failure(nel(GenericError("array dimension out of range"), Nil))
      case Type.Const(Data.Arr(arr)) :: Type.Const(Data.Int(i)) :: Nil
          if (i == 1) =>
        // TODO: we should support dims other than 1, but it's work
        success(Type.Const(Data.Int(arr.length)))
      case Type.AnyArray :: Type.Const(Data.Int(_)) :: Nil =>
        success(Type.Int)
    },
    Type.typecheck(_, Type.Int) map κ(Type.AnyArray :: Type.Int :: Nil)
  )

  val In = Mapping(
    "(in)",
    "Determines whether a value is in a given array.",
    Type.Top :: Type.AnyArray :: Nil,
    noSimplification,
    partialTyper {
      case List(Type.Const(x), Type.Const(Data.Arr(arr))) =>
        Type.Const(Data.Bool(arr.contains(x)))
      case List(_,             Type.Const(Data.Arr(_)))   => Type.Bool
      case List(_,             _)                         => Type.Bool
    },
    Type.typecheck(_, Type.Bool) map κ(Type.Top :: Type.AnyArray :: Nil))

  def functions = ArrayLength :: In :: Nil
}
object ArrayLib extends ArrayLib
