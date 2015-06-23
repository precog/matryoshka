package slamdata.engine.std

import scalaz._

import slamdata.engine.{Data, Func, LogicalPlan, Type, Mapping, SemanticError}; import LogicalPlan._
import slamdata.engine.analysis.fixplate._
import slamdata.engine.fp._

import SemanticError._
import Validation.{success, failure}
import NonEmptyList.nel

trait ArrayLib extends Library {
  val ArrayLength = Mapping(
    "array_length",
    "Gets the length of a given dimension of an array.",
    Type.AnyArray :: Type.Int :: Nil,
    partialSimplifier {
      case List(Term(ConstantF(Data.Arr(arr))), Term(ConstantF(Data.Int(x)))) if x == 1 => Constant(Data.Int(arr.length))
    },
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
    partialSimplifier {
      case List(Term(ConstantF(x)), Term(ConstantF(Data.Arr(arr)))) => Constant(Data.Bool(arr.contains(x)))
    },
    κ(success(Type.Bool)),
    Type.typecheck(_, Type.Bool) map κ(Type.Top :: Type.AnyArray :: Nil))

  def functions = ArrayLength :: In :: Nil
}
object ArrayLib extends ArrayLib
