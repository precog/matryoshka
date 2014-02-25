package slamdata.engine.std

import slamdata.engine.{Data, Func, Type, Mapping, SemanticError}

import SemanticError._

// TODO: Cleanup!
trait RelationsLib extends Library {
  val Eq = Mapping("(=)", "Determines if two values are equal", Type.Top :: Nil,
    (partialRefiner {
      case Type.Const(Data.Number(v1)) :: Type.Const(Data.Number(v2)) :: Nil => Type.Const(Data.Bool(v1 == v2))
      case Type.Const(data1) :: Type.Const(data2) :: Nil => Type.Const(Data.Bool(data1 == data2))
      case type1 :: type2 :: Nil if Type.lub(type1, type2) == Type.Top && type1 != Type.Top => Type.Const(Data.Bool(false))
      case _ => Type.Bool
    })
  )

  val Neq = Mapping("(<>)", "Determines if two values are equal", Type.Top :: Nil,
    (partialRefiner {
      case Type.Const(Data.Number(v1)) :: Type.Const(Data.Number(v2)) :: Nil => Type.Const(Data.Bool(v1 != v2))
      case Type.Const(data1) :: Type.Const(data2) :: Nil => Type.Const(Data.Bool(data1 != data2))
      case type1 :: type2 :: Nil if Type.lub(type1, type2) == Type.Top && type1 != Type.Top => Type.Const(Data.Bool(true))
      case _ => Type.Bool
    })
  )

  val Lt = Mapping("(<)", "Determines if one value is less than another value of the same type", Type.Top :: Nil,
    (partialRefiner {
      case Type.Const(Data.Bool(v1)) :: Type.Const(Data.Bool(v2)) :: Nil => Type.Const(Data.Bool(v1 < v2))
      case Type.Const(Data.Number(v1)) :: Type.Const(Data.Number(v2)) :: Nil => Type.Const(Data.Bool(v1 < v2))
      case Type.Const(Data.Str(v1)) :: Type.Const(Data.Str(v2)) :: Nil => Type.Const(Data.Bool(v1 < v2))
      case Type.Const(Data.DateTime(v1)) :: Type.Const(Data.DateTime(v2)) :: Nil => Type.Const(Data.Bool(v1.compareTo(v2) < 0))
      case Type.Const(Data.Interval(v1)) :: Type.Const(Data.Interval(v2)) :: Nil => Type.Const(Data.Bool(v1.compareTo(v2) < 0))
      case _ => Type.Bool
    })
  )

  val Lte = Mapping("(<=)", "Determines if one value is less than another value of the same type", Type.Top :: Nil,
    (partialRefiner {
      case Type.Const(Data.Bool(v1)) :: Type.Const(Data.Bool(v2)) :: Nil => Type.Const(Data.Bool(v1 <= v2))
      case Type.Const(Data.Number(v1)) :: Type.Const(Data.Number(v2)) :: Nil => Type.Const(Data.Bool(v1 <= v2))
      case Type.Const(Data.Str(v1)) :: Type.Const(Data.Str(v2)) :: Nil => Type.Const(Data.Bool(v1 <= v2))
      case Type.Const(Data.DateTime(v1)) :: Type.Const(Data.DateTime(v2)) :: Nil => Type.Const(Data.Bool(v1.compareTo(v2) <= 0))
      case Type.Const(Data.Interval(v1)) :: Type.Const(Data.Interval(v2)) :: Nil => Type.Const(Data.Bool(v1.compareTo(v2) <= 0))
      case _ => Type.Bool
    })
  )

  val Gt = Mapping("(>)", "Determines if one value is greater than another value of the same type", Type.Top :: Nil,
    (partialRefiner {
      case Type.Const(Data.Bool(v1)) :: Type.Const(Data.Bool(v2)) :: Nil => Type.Const(Data.Bool(v1 > v2))
      case Type.Const(Data.Number(v1)) :: Type.Const(Data.Number(v2)) :: Nil => Type.Const(Data.Bool(v1 > v2))
      case Type.Const(Data.Str(v1)) :: Type.Const(Data.Str(v2)) :: Nil => Type.Const(Data.Bool(v1 > v2))
      case Type.Const(Data.DateTime(v1)) :: Type.Const(Data.DateTime(v2)) :: Nil => Type.Const(Data.Bool(v1.compareTo(v2) > 0))
      case Type.Const(Data.Interval(v1)) :: Type.Const(Data.Interval(v2)) :: Nil => Type.Const(Data.Bool(v1.compareTo(v2) > 0))
      case _ => Type.Bool
    })
  )

  val Gte = Mapping("(>=)", "Determines if one value is greater than or equal to another value of the same type", Type.Top :: Nil,
    (partialRefiner {
      case Type.Const(Data.Bool(v1)) :: Type.Const(Data.Bool(v2)) :: Nil => Type.Const(Data.Bool(v1 >= v2))
      case Type.Const(Data.Number(v1)) :: Type.Const(Data.Number(v2)) :: Nil => Type.Const(Data.Bool(v1 >= v2))
      case Type.Const(Data.Str(v1)) :: Type.Const(Data.Str(v2)) :: Nil => Type.Const(Data.Bool(v1 >= v2))
      case Type.Const(Data.DateTime(v1)) :: Type.Const(Data.DateTime(v2)) :: Nil => Type.Const(Data.Bool(v1.compareTo(v2) >= 0))
      case Type.Const(Data.Interval(v1)) :: Type.Const(Data.Interval(v2)) :: Nil => Type.Const(Data.Bool(v1.compareTo(v2) >= 0))
      case _ => Type.Bool
    })
  )

  def functions = Eq :: Neq :: Lt :: Lte :: Gt :: Gte :: Nil
}
object RelationsLib extends RelationsLib