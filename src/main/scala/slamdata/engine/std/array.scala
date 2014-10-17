package slamdata.engine.std

import scalaz._

import slamdata.engine.{Data, Func, Type, Mapping, SemanticError}

import SemanticError._
import Validation.{success, failure}
import NonEmptyList.nel

trait ArrayLib extends Library {
  val ArrayLength = Mapping(
    "array_length",
    "Gets the length of a given dimension of an array.",
    Type.AnyArray :: Type.Int :: Nil,
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
    Type.typecheck(_, Type.Int) map { _ => Type.AnyArray :: Type.Int :: Nil }
  )

  val In = Mapping(
    "(in)",
    "Determines whether a value is in a given array.",
    Type.Top :: Type.AnyArray :: Nil,
    Function.const(success(Type.Bool)),
    Type.typecheck(_, Type.Bool) map { _ => Type.Top :: Type.AnyArray :: Nil })

  def functions = ArrayLength :: In :: Nil
}
object ArrayLib extends ArrayLib
