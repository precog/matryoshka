package slamdata.engine.std

import slamdata.engine.{Data, Func, Type, Mapping, SemanticError}

import scalaz._

import SemanticError._
import Validation.{success, failure}
import NonEmptyList.nel

// TODO: Cleanup!
trait RelationsLib extends Library {
  private val BinaryAny: Func.Untyper = {
    case Type.Const(Data.Bool(_)) => success(Type.Top :: Type.Top :: Nil)
    case Type.Bool => success(Type.Top :: Type.Top :: Nil)
    case t => failure(nel(TypeError(Type.Bool, t), Nil))
  }
  private val BinaryBool: Func.Untyper = {
    case Type.Bool => success(Type.Bool :: Type.Bool :: Nil)
    case t => failure(nel(TypeError(Type.Bool, t), Nil))
  }
  private val UnaryBool: Func.Untyper = {
    case Type.Bool => success(Type.Bool :: Nil)
    case t => failure(nel(TypeError(Type.Bool, t), Nil))
  }

  val Eq = Mapping("(=)", "Determines if two values are equal", Type.Top :: Type.Top :: Nil,
    (partialTyper {
      case Type.Const(Data.Number(v1)) :: Type.Const(Data.Number(v2)) :: Nil => Type.Const(Data.Bool(v1 == v2))
      case Type.Const(data1) :: Type.Const(data2) :: Nil => Type.Const(Data.Bool(data1 == data2))
      case type1 :: type2 :: Nil if Type.lub(type1, type2) == Type.Top && type1 != Type.Top => Type.Const(Data.Bool(false))
      case _ => Type.Bool
    }),
    BinaryAny
  )

  val Neq = Mapping("(<>)", "Determines if two values are not equal", Type.Top :: Type.Top :: Nil,
    (partialTyper {
      case Type.Const(Data.Number(v1)) :: Type.Const(Data.Number(v2)) :: Nil => Type.Const(Data.Bool(v1 != v2))
      case Type.Const(data1) :: Type.Const(data2) :: Nil => Type.Const(Data.Bool(data1 != data2))
      case type1 :: type2 :: Nil if Type.lub(type1, type2) == Type.Top && type1 != Type.Top => Type.Const(Data.Bool(true))
      case _ => Type.Bool
    }),
    BinaryAny
  )

  val Lt = Mapping("(<)", "Determines if one value is less than another value of the same type", Type.Top :: Type.Top :: Nil,
    (partialTyper {
      case Type.Const(Data.Bool(v1)) :: Type.Const(Data.Bool(v2)) :: Nil => Type.Const(Data.Bool(v1 < v2))
      case Type.Const(Data.Number(v1)) :: Type.Const(Data.Number(v2)) :: Nil => Type.Const(Data.Bool(v1 < v2))
      case Type.Const(Data.Str(v1)) :: Type.Const(Data.Str(v2)) :: Nil => Type.Const(Data.Bool(v1 < v2))
      case Type.Const(Data.DateTime(v1)) :: Type.Const(Data.DateTime(v2)) :: Nil => Type.Const(Data.Bool(v1.compareTo(v2) < 0))
      case Type.Const(Data.Interval(v1)) :: Type.Const(Data.Interval(v2)) :: Nil => Type.Const(Data.Bool(v1.compareTo(v2) < 0))
      case _ => Type.Bool
    }),
    BinaryAny
  )

  val Lte = Mapping("(<=)", "Determines if one value is less than or equal to another value of the same type", Type.Top :: Type.Top :: Nil,
    (partialTyper {
      case Type.Const(Data.Bool(v1)) :: Type.Const(Data.Bool(v2)) :: Nil => Type.Const(Data.Bool(v1 <= v2))
      case Type.Const(Data.Number(v1)) :: Type.Const(Data.Number(v2)) :: Nil => Type.Const(Data.Bool(v1 <= v2))
      case Type.Const(Data.Str(v1)) :: Type.Const(Data.Str(v2)) :: Nil => Type.Const(Data.Bool(v1 <= v2))
      case Type.Const(Data.DateTime(v1)) :: Type.Const(Data.DateTime(v2)) :: Nil => Type.Const(Data.Bool(v1.compareTo(v2) <= 0))
      case Type.Const(Data.Interval(v1)) :: Type.Const(Data.Interval(v2)) :: Nil => Type.Const(Data.Bool(v1.compareTo(v2) <= 0))
      case _ => Type.Bool
    }),
    BinaryAny
  )

  val Gt = Mapping("(>)", "Determines if one value is greater than another value of the same type", Type.Top :: Type.Top :: Nil,
    (partialTyper {
      case Type.Const(Data.Bool(v1)) :: Type.Const(Data.Bool(v2)) :: Nil => Type.Const(Data.Bool(v1 > v2))
      case Type.Const(Data.Number(v1)) :: Type.Const(Data.Number(v2)) :: Nil => Type.Const(Data.Bool(v1 > v2))
      case Type.Const(Data.Str(v1)) :: Type.Const(Data.Str(v2)) :: Nil => Type.Const(Data.Bool(v1 > v2))
      case Type.Const(Data.DateTime(v1)) :: Type.Const(Data.DateTime(v2)) :: Nil => Type.Const(Data.Bool(v1.compareTo(v2) > 0))
      case Type.Const(Data.Interval(v1)) :: Type.Const(Data.Interval(v2)) :: Nil => Type.Const(Data.Bool(v1.compareTo(v2) > 0))
      case _ => Type.Bool
    }),
    BinaryAny
  )

  val Gte = Mapping("(>=)", "Determines if one value is greater than or equal to another value of the same type", Type.Top :: Type.Top :: Nil,
    (partialTyper {
      case Type.Const(Data.Bool(v1)) :: Type.Const(Data.Bool(v2)) :: Nil => Type.Const(Data.Bool(v1 >= v2))
      case Type.Const(Data.Number(v1)) :: Type.Const(Data.Number(v2)) :: Nil => Type.Const(Data.Bool(v1 >= v2))
      case Type.Const(Data.Str(v1)) :: Type.Const(Data.Str(v2)) :: Nil => Type.Const(Data.Bool(v1 >= v2))
      case Type.Const(Data.DateTime(v1)) :: Type.Const(Data.DateTime(v2)) :: Nil => Type.Const(Data.Bool(v1.compareTo(v2) >= 0))
      case Type.Const(Data.Interval(v1)) :: Type.Const(Data.Interval(v2)) :: Nil => Type.Const(Data.Bool(v1.compareTo(v2) >= 0))
      case _ => Type.Bool
    }),
    BinaryAny
  )
  
  val Between = Mapping("(BETWEEN)", "Determines if a value is between two other values of the same type, inclusive", Type.Top :: Type.Top :: Nil,
    (partialTyper {
      // TODO: partial evaluation for Int and Dec and possibly other constants
      case _ :: _ :: Nil => Type.Bool
      case _ => Type.Bool
    }),
    BinaryAny
  )

  val Range = Mapping("RANGE", "Used with BETWEEN", Type.Top :: Type.Top :: Nil,
    (partialTyper {
      // TODO: partial evaluation for Int and Dec and possibly other constants
      case _ :: _ :: Nil => Type.Top  // HACK
    }),
    t => t match {
      case _ => success(Type.Top :: Type.Top :: Nil)
    }
  )
  
  val And = Mapping("(AND)", "Performs a logical AND of two boolean values", Type.Bool :: Type.Bool :: Nil,
    partialTyper {
      case Type.Const(Data.Bool(v1)) :: Type.Const(Data.Bool(v2)) :: Nil => Type.Const(Data.Bool(v1 && v2))
      case Type.Const(Data.Bool(false)) :: _ :: Nil => Type.Const(Data.Bool(false))
      case _ :: Type.Const(Data.Bool(false)) :: Nil => Type.Const(Data.Bool(false))
      case Type.Const(Data.Bool(true)) :: x :: Nil => x
      case x :: Type.Const(Data.Bool(true)) :: Nil => x
      case _ => Type.Bool
    },
    BinaryBool
  )

  val Or = Mapping("(OR)", "Performs a logical OR of two boolean values", Type.Bool :: Type.Bool :: Nil,
    partialTyper {
      case Type.Const(Data.Bool(v1)) :: Type.Const(Data.Bool(v2)) :: Nil => Type.Const(Data.Bool(v1 || v2))
      case Type.Const(Data.Bool(true)) :: _ :: Nil => Type.Const(Data.Bool(true))
      case _ :: Type.Const(Data.Bool(true)) :: Nil => Type.Const(Data.Bool(false))
      case Type.Const(Data.Bool(false)) :: x :: Nil => x
      case x :: Type.Const(Data.Bool(false)) :: Nil => x
      case _ => Type.Bool
    },
    BinaryBool
  )

  val Not = Mapping("(NOT)", "Performs a logical negation of a boolean value", Type.Bool :: Nil,
    partialTyper {
      case Type.Const(Data.Bool(v)) :: Nil => Type.Const(Data.Bool(!v))
      case _ => Type.Bool
    },
    UnaryBool
  )

  val Cond = Mapping("(IF_THEN_ELSE)", "Chooses between one of two cases based on the value of a boolean expression", Type.Bool :: Type.Top :: Type.Top :: Nil,
    partialTyper {
      case Type.Const(Data.Bool(true)) :: ifTrue :: ifFalse :: Nil => ifTrue
      case Type.Const(Data.Bool(false)) :: ifTrue :: ifFalse :: Nil => ifFalse
    }, _ => success(Type.Top :: Type.Top :: Nil)
  )

  def functions = Eq :: Neq :: Lt :: Lte :: Gt :: Gte :: Between :: Range :: And :: Or :: Not :: Cond :: Nil
}
object RelationsLib extends RelationsLib