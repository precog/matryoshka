package slamdata.engine.std

import scalaz._

import slamdata.engine.{Data, Func, Type, Mapping, SemanticError}

import SemanticError._
import Validation.{success, failure}
import NonEmptyList.nel

trait MathLib extends Library {
  private val NumericDomain = Type.Numeric :: Type.Numeric :: Nil

  private val NumericUnapply: Func.Untyper = {
    case Type.Const(Data.Int(_)) => success(Type.Int :: Type.Int :: Nil)
    case Type.Int                => success(Type.Int :: Type.Int :: Nil)

    case t => Type.typecheck(t, Type.Numeric).map(_ => Type.Numeric :: Type.Numeric :: Nil)
  }

  private val UnaryNumericUnapply: Func.Untyper = {
    case Type.Const(Data.Int(_)) => success(Type.Int :: Nil)
    case Type.Int                => success(Type.Int :: Nil)

    case t => Type.typecheck(t, Type.Numeric).map(_ => Type.Numeric :: Nil)
  }

  /**
   * Adds two numeric values, promoting to decimal if either operand is decimal.
   */
  val Add = Mapping("(+)", "Adds two numeric values", NumericDomain,
    (partialTyper {
      case Type.Const(Data.Number(v1)) :: v2 :: Nil if (v1.signum == 0) => v2
      case v1 :: Type.Const(Data.Number(v2)) :: Nil if (v2.signum == 0) => v1
      case Type.Const(Data.Int(v1)) :: Type.Const(Data.Int(v2)) :: Nil => Type.Const(Data.Int(v1 + v2))
      case Type.Const(Data.Number(v1)) :: Type.Const(Data.Number(v2)) :: Nil => Type.Const(Data.Dec(v1 + v2))
    }) ||| numericWidening,
    NumericUnapply
  )

  /**
   * Multiplies two numeric values, promoting to decimal if either operand is decimal.
   */
  val Multiply = Mapping("(*)", "Multiplies two numeric values", NumericDomain,
    (partialTyper {
      case (zero @ Type.Const(Data.Number(v1))) :: v2 :: Nil if (v1.signum == 0) => zero
      case v1 :: (zero @ Type.Const(Data.Number(v2))) :: Nil if (v2.signum == 0) => zero
      case Type.Const(Data.Int(v1)) :: Type.Const(Data.Int(v2)) :: Nil => Type.Const(Data.Int(v1 * v2))
      case Type.Const(Data.Number(v1)) :: Type.Const(Data.Number(v2)) :: Nil => Type.Const(Data.Dec(v1 * v2))
    }) ||| numericWidening,
    NumericUnapply
  )

  /**
   * Subtracts one value from another, promoting to decimal if either operand is decimal.
   */
  val Subtract = Mapping("(-)", "Subtracts two numeric values", NumericDomain,
    (partialTyper {
      case v1 :: Type.Const(Data.Number(v2)) :: Nil if (v2.signum == 0) => v1
      case Type.Const(Data.Int(v1)) :: Type.Const(Data.Int(v2)) :: Nil => Type.Const(Data.Int(v1 - v2))
      case Type.Const(Data.Number(v1)) :: Type.Const(Data.Number(v2)) :: Nil => Type.Const(Data.Dec(v1 - v2))
    }) ||| numericWidening,
    NumericUnapply
  )

  /**
   * Divides one value by another, promoting to decimal if either operand is decimal.
   */
  val Divide = Mapping("(/)", "Divides one numeric value by another (non-zero) numeric value", NumericDomain,
    (partialTyperV {
      case v1 :: Type.Const(Data.Number(v2)) :: Nil if (v2.doubleValue == 1.0) => success(v1)
      case v1 :: Type.Const(Data.Number(v2)) :: Nil if (v2.doubleValue == 0.0) => failure(NonEmptyList(GenericError("Division by zero")))
      case Type.Const(Data.Int(v1)) :: Type.Const(Data.Int(v2)) :: Nil => success(Type.Const(Data.Int(v1 / v2)))
      case Type.Const(Data.Number(v1)) :: Type.Const(Data.Number(v2)) :: Nil => success(Type.Const(Data.Dec(v1 / v2)))
    }) ||| numericWidening,
    NumericUnapply
  )

  /**
   * Aka "unary minus".
   */
  val Negate = Mapping("-", "Reverses the sign of a numeric value", Type.Numeric :: Nil,
    (partialTyperV {
      case Type.Const(Data.Int(v)) :: Nil    => success(Type.Const(Data.Int(-v)))
      case Type.Const(Data.Number(v)) :: Nil => success(Type.Const(Data.Dec(-v)))
    }) ||| numericWidening,
    UnaryNumericUnapply
  )

  val Modulo = Mapping(
    "(%)",
    "Finds the remainder of one number divided by another",
    NumericDomain,
    (partialTyperV {
      case v1 :: Type.Const(Data.Number(v2)) :: Nil if (v2.doubleValue == 1.0) =>
        success(v1)
      case v1 :: Type.Const(Data.Number(v2)) :: Nil if (v2.doubleValue == 0.0) =>
        failure(NonEmptyList(GenericError("Division by zero")))
      case Type.Const(Data.Int(v1)) :: Type.Const(Data.Int(v2)) :: Nil =>
        success(Type.Const(Data.Int(v1 % v2)))
      case Type.Const(Data.Number(v1)) :: Type.Const(Data.Number(v2)) :: Nil =>
        success(Type.Const(Data.Dec(v1 % v2)))
    }) ||| numericWidening,
    NumericUnapply
  )

  def functions = Add :: Multiply :: Subtract :: Divide :: Negate :: Modulo :: Nil
}
object MathLib extends MathLib
