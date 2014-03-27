package slamdata.engine.std

import slamdata.engine.{Data, Func, Type, Mapping, SemanticError}

import scalaz._

import SemanticError._
import Validation.{success, failure}
import NonEmptyList.nel

// TODO: Cleanup!
trait RelationsLib extends Library {
  private val AnyAny: Func.Untyper = {
    case Type.Const(Data.Bool(_)) => success(Type.Top :: Type.Top :: Nil)
    case Type.Bool => success(Type.Top :: Type.Top :: Nil)
    case t => failure(nel(TypeError(Type.Bool, t), Nil))
  }

  val Eq = Mapping("(=)", "Determines if two values are equal", Type.Top :: Nil,
    (partialTyper {
      case Type.Const(Data.Number(v1)) :: Type.Const(Data.Number(v2)) :: Nil => Type.Const(Data.Bool(v1 == v2))
      case Type.Const(data1) :: Type.Const(data2) :: Nil => Type.Const(Data.Bool(data1 == data2))
      case type1 :: type2 :: Nil if Type.lub(type1, type2) == Type.Top && type1 != Type.Top => Type.Const(Data.Bool(false))
      case _ => Type.Bool
    }),
    AnyAny
  )

  val Neq = Mapping("(<>)", "Determines if two values are not equal", Type.Top :: Nil,
    (partialTyper {
      case Type.Const(Data.Number(v1)) :: Type.Const(Data.Number(v2)) :: Nil => Type.Const(Data.Bool(v1 != v2))
      case Type.Const(data1) :: Type.Const(data2) :: Nil => Type.Const(Data.Bool(data1 != data2))
      case type1 :: type2 :: Nil if Type.lub(type1, type2) == Type.Top && type1 != Type.Top => Type.Const(Data.Bool(true))
      case _ => Type.Bool
    }),
    AnyAny
  )

  val Lt = Mapping("(<)", "Determines if one value is less than another value of the same type", Type.Top :: Nil,
    (partialTyper {
      case Type.Const(Data.Bool(v1)) :: Type.Const(Data.Bool(v2)) :: Nil => Type.Const(Data.Bool(v1 < v2))
      case Type.Const(Data.Number(v1)) :: Type.Const(Data.Number(v2)) :: Nil => Type.Const(Data.Bool(v1 < v2))
      case Type.Const(Data.Str(v1)) :: Type.Const(Data.Str(v2)) :: Nil => Type.Const(Data.Bool(v1 < v2))
      case Type.Const(Data.DateTime(v1)) :: Type.Const(Data.DateTime(v2)) :: Nil => Type.Const(Data.Bool(v1.compareTo(v2) < 0))
      case Type.Const(Data.Interval(v1)) :: Type.Const(Data.Interval(v2)) :: Nil => Type.Const(Data.Bool(v1.compareTo(v2) < 0))
      case _ => Type.Bool
    }),
    AnyAny
  )

  val Lte = Mapping("(<=)", "Determines if one value is less than or equal to another value of the same type", Type.Top :: Nil,
    (partialTyper {
      case Type.Const(Data.Bool(v1)) :: Type.Const(Data.Bool(v2)) :: Nil => Type.Const(Data.Bool(v1 <= v2))
      case Type.Const(Data.Number(v1)) :: Type.Const(Data.Number(v2)) :: Nil => Type.Const(Data.Bool(v1 <= v2))
      case Type.Const(Data.Str(v1)) :: Type.Const(Data.Str(v2)) :: Nil => Type.Const(Data.Bool(v1 <= v2))
      case Type.Const(Data.DateTime(v1)) :: Type.Const(Data.DateTime(v2)) :: Nil => Type.Const(Data.Bool(v1.compareTo(v2) <= 0))
      case Type.Const(Data.Interval(v1)) :: Type.Const(Data.Interval(v2)) :: Nil => Type.Const(Data.Bool(v1.compareTo(v2) <= 0))
      case _ => Type.Bool
    }),
    AnyAny
  )

  val Gt = Mapping("(>)", "Determines if one value is greater than another value of the same type", Type.Top :: Nil,
    (partialTyper {
      case Type.Const(Data.Bool(v1)) :: Type.Const(Data.Bool(v2)) :: Nil => Type.Const(Data.Bool(v1 > v2))
      case Type.Const(Data.Number(v1)) :: Type.Const(Data.Number(v2)) :: Nil => Type.Const(Data.Bool(v1 > v2))
      case Type.Const(Data.Str(v1)) :: Type.Const(Data.Str(v2)) :: Nil => Type.Const(Data.Bool(v1 > v2))
      case Type.Const(Data.DateTime(v1)) :: Type.Const(Data.DateTime(v2)) :: Nil => Type.Const(Data.Bool(v1.compareTo(v2) > 0))
      case Type.Const(Data.Interval(v1)) :: Type.Const(Data.Interval(v2)) :: Nil => Type.Const(Data.Bool(v1.compareTo(v2) > 0))
      case _ => Type.Bool
    }),
    AnyAny
  )

  val Gte = Mapping("(>=)", "Determines if one value is greater than or equal to another value of the same type", Type.Top :: Nil,
    (partialTyper {
      case Type.Const(Data.Bool(v1)) :: Type.Const(Data.Bool(v2)) :: Nil => Type.Const(Data.Bool(v1 >= v2))
      case Type.Const(Data.Number(v1)) :: Type.Const(Data.Number(v2)) :: Nil => Type.Const(Data.Bool(v1 >= v2))
      case Type.Const(Data.Str(v1)) :: Type.Const(Data.Str(v2)) :: Nil => Type.Const(Data.Bool(v1 >= v2))
      case Type.Const(Data.DateTime(v1)) :: Type.Const(Data.DateTime(v2)) :: Nil => Type.Const(Data.Bool(v1.compareTo(v2) >= 0))
      case Type.Const(Data.Interval(v1)) :: Type.Const(Data.Interval(v2)) :: Nil => Type.Const(Data.Bool(v1.compareTo(v2) >= 0))
      case _ => Type.Bool
    }),
    AnyAny
  )

  val Cond = Mapping("IF_THEN_ELSE", "Chooses between one of two cases based on the value of a boolean expression", Type.Bool :: Type.Top :: Type.Top :: Nil,
    partialTyper {
      case Type.Const(Data.Bool(true)) :: ifTrue :: ifFalse :: Nil => ifTrue
      case Type.Const(Data.Bool(false)) :: ifTrue :: ifFalse :: Nil => ifFalse
    }, _ => success(Type.Top :: Type.Top :: Nil)
  )

  def functions = Eq :: Neq :: Lt :: Lte :: Gt :: Gte :: Cond :: Nil
}
object RelationsLib extends RelationsLib