package slamdata.engine.std

import scalaz._

import slamdata.engine.{Data, Func, Type, Mapping, SemanticError}

import SemanticError._

trait MathLib extends Library {
  private val NumericDomain = Type.Numeric :: Type.Numeric :: Nil

  /**
   * Adds two numeric values, promoting to decimal if either operand is decimal.
   */
  val Add = Mapping("(+)", "Adds two numeric values", NumericDomain,
    (partialRefiner {
      case Type.Const(Data.Number(v1)) :: v2 :: Nil if (v1.signum == 0) => v2
      case v1 :: Type.Const(Data.Number(v2)) :: Nil if (v2.signum == 0) => v1
      case Type.Const(Data.Int(v1)) :: Type.Const(Data.Int(v2)) :: Nil => Type.Const(Data.Int(v1 + v2))
      case Type.Const(Data.Number(v1)) :: Type.Const(Data.Number(v2)) :: Nil => Type.Const(Data.Dec(v1 + v2))
    }) ||| numericWidening
  )

  /**
   * Multiplies two numeric values, promoting to decimal if either operand is decimal.
   */
  val Multiply = Mapping("(*)", "Multiplies two numeric values", NumericDomain,
    (partialRefiner {
      case (zero @ Type.Const(Data.Number(v1))) :: v2 :: Nil if (v1.signum == 0) => zero
      case v1 :: (zero @ Type.Const(Data.Number(v2))) :: Nil if (v2.signum == 0) => zero
      case Type.Const(Data.Int(v1)) :: Type.Const(Data.Int(v2)) :: Nil => Type.Const(Data.Int(v1 * v2))
      case Type.Const(Data.Number(v1)) :: Type.Const(Data.Number(v2)) :: Nil => Type.Const(Data.Dec(v1 * v2))
    }) ||| numericWidening
  )

  /**
   * Subtracts one value from another, promoting to decimal if either operand is decimal.
   */
  val Subtract = Mapping("(-)", "Subtracts two numeric values", NumericDomain,
    (partialRefiner {
      case v1 :: Type.Const(Data.Number(v2)) :: Nil if (v2.signum == 0) => v1
      case Type.Const(Data.Int(v1)) :: Type.Const(Data.Int(v2)) :: Nil => Type.Const(Data.Int(v1 - v2))
      case Type.Const(Data.Number(v1)) :: Type.Const(Data.Number(v2)) :: Nil => Type.Const(Data.Dec(v1 - v2))
    }) ||| numericWidening
  )

  /**
   * Divides one value by another, promoting to decimal if either operand is decimal.
   */
  val Divide = Mapping("(/)", "Divides one numeric value by another (non-zero) numeric value", NumericDomain,
    (partialRefinerV {
      case v1 :: Type.Const(Data.Number(v2)) :: Nil if (v2.doubleValue == 1.0) => Validation.success(v1)
      case v1 :: Type.Const(Data.Number(v2)) :: Nil if (v2.doubleValue == 0.0) => Validation.failure(NonEmptyList(GenericError("Division by zero")))
      case Type.Const(Data.Int(v1)) :: Type.Const(Data.Int(v2)) :: Nil => Validation.success(Type.Const(Data.Int(v1 / v2)))
      case Type.Const(Data.Number(v1)) :: Type.Const(Data.Number(v2)) :: Nil => Validation.success(Type.Const(Data.Dec(v1 / v2)))
    }) ||| numericWidening
  )

  def functions = Add :: Multiply :: Subtract :: Divide :: Nil
}
object MathLib extends MathLib