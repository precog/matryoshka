package slamdata.engine.std

import scalaz._

import slamdata.engine.{Data, Func, Type, Mapping, SemanticError}
import slamdata.engine.fp._

import SemanticError._
import Validation.{success, failure}
import NonEmptyList.nel

trait MathLib extends Library {
  private val UnaryNumericDomain = (Type.Numeric | Type.Timestamp | Type.Interval) :: Nil
  private val NumericDomain = UnaryNumericDomain ++ UnaryNumericDomain

  private val NumericUnapply: Func.Untyper = {
    case Type.Const(Data.Int(_)) => success(Type.Int :: Type.Int :: Nil)
    case Type.Int                => success(Type.Int :: Type.Int :: Nil)

    case t => Type.typecheck(t, Type.Numeric) map κ(Type.Numeric :: Type.Numeric :: Nil)
  }

  /**
   * Adds two numeric values, promoting to decimal if either operand is decimal.
   */
  val Add = Mapping("(+)", "Adds two numeric or temporal values", NumericDomain,
    (partialTyper {
      case Type.Const(Data.Number(v1)) :: t2 :: Nil if (v1.signum == 0) && (Type.Numeric contains t2) => t2
      case t1 :: Type.Const(Data.Number(v2)) :: Nil if (Type.Numeric contains t1) && (v2.signum == 0) => t1
      case Type.Const(Data.Int(v1)) :: Type.Const(Data.Int(v2)) :: Nil => Type.Const(Data.Int(v1 + v2))
      case Type.Const(Data.Number(v1)) :: Type.Const(Data.Number(v2)) :: Nil => Type.Const(Data.Dec(v1 + v2))

      case Type.Const(Data.Timestamp(v1)) :: Type.Const(Data.Interval(v2)) :: Nil => Type.Const(Data.Timestamp(v1.plus(v2)))
      case Type.Timestamp :: Type.Interval :: Nil => Type.Timestamp
      case Type.Const(Data.Interval(v1)) :: Type.Const(Data.Timestamp(v2)) :: Nil => Type.Const(Data.Timestamp(v2.plus(v1)))
      case Type.Interval :: Type.Timestamp :: Nil => Type.Timestamp

      case Type.Const(Data.Timestamp(_)) :: t2 :: Nil if t2 contains Type.Interval => Type.Timestamp
      case t1 :: Type.Const(Data.Timestamp(_)) :: Nil if t1 contains Type.Interval => Type.Timestamp
    }) ||| numericWidening,
    NumericUnapply
  )

  /**
   * Multiplies two numeric values, promoting to decimal if either operand is decimal.
   */
  val Multiply = Mapping("(*)", "Multiplies two numeric values or one interval and one numeric value", (Type.Numeric | Type.Interval) :: (Type.Numeric | Type.Interval) :: Nil,
    (partialTyper {
      case (zero @ Type.Const(Data.Number(v1))) :: v2 :: Nil if (v1.signum == 0) => zero
      case v1 :: (zero @ Type.Const(Data.Number(v2))) :: Nil if (v2.signum == 0) => zero
      case Type.Const(Data.Int(v1)) :: Type.Const(Data.Int(v2)) :: Nil => Type.Const(Data.Int(v1 * v2))
      case Type.Const(Data.Number(v1)) :: Type.Const(Data.Number(v2)) :: Nil => Type.Const(Data.Dec(v1 * v2))

      // TODO: handle interval multiplied by Dec (not provided by threeten). See #580.
      case Type.Const(Data.Interval(v1)) :: Type.Const(Data.Int(v2)) :: Nil => Type.Const(Data.Interval(v1.multipliedBy(v2.longValue)))
      case Type.Const(Data.Int(v1)) :: Type.Const(Data.Interval(v2)) :: Nil => Type.Const(Data.Interval(v2.multipliedBy(v1.longValue)))
      case Type.Const(Data.Interval(v1)) :: t :: Nil if t contains Type.Int => Type.Interval
    }) ||| numericWidening,
    NumericUnapply
  )

  /**
   * Subtracts one value from another, promoting to decimal if either operand is decimal.
   */
  val Subtract = Mapping("(-)", "Subtracts two numeric or temporal values", NumericDomain,
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
  val Divide = Mapping("(/)", "Divides one numeric or interval value by another (non-zero) numeric value", (Type.Numeric | Type.Interval) :: (Type.Numeric | Type.Interval) :: Nil,
    (partialTyperV {
      case v1 :: Type.Const(Data.Number(v2)) :: Nil if (v2.doubleValue == 1.0) => success(v1)
      case v1 :: Type.Const(Data.Number(v2)) :: Nil if (v2.doubleValue == 0.0) => failure(NonEmptyList(GenericError("Division by zero")))
      case Type.Const(Data.Int(v1)) :: Type.Const(Data.Int(v2)) :: Nil => success(Type.Const(Data.Int(v1 / v2)))
      case Type.Const(Data.Number(v1)) :: Type.Const(Data.Number(v2)) :: Nil => success(Type.Const(Data.Dec(v1 / v2)))

      // TODO: handle interval divided by Dec (not provided by threeten). See #580.
      case Type.Const(Data.Interval(v1)) :: Type.Const(Data.Int(v2)) :: Nil => success(Type.Const(Data.Interval(v1.dividedBy(v2.longValue))))
      case Type.Const(Data.Interval(v1)) :: t :: Nil if t contains Type.Int => success(Type.Interval)
    }) ||| numericWidening,
    NumericUnapply
  )

  /**
   * Aka "unary minus".
   */
  val Negate = Mapping("-", "Reverses the sign of a numeric or interval value", (Type.Numeric | Type.Interval) :: Nil,
    (partialTyperV {
      case Type.Const(Data.Int(v)) :: Nil      => success(Type.Const(Data.Int(-v)))
      case Type.Const(Data.Number(v)) :: Nil   => success(Type.Const(Data.Dec(-v)))
      case Type.Const(Data.Interval(v)) :: Nil => success(Type.Const(Data.Interval(v.negated)))
      case t :: Nil if (Type.Numeric | Type.Interval) contains t => success(t)
    }),
    {
      case Type.Const(d) => success(d.dataType :: Nil)
      case Type.Int      => success(Type.Int :: Nil)

      case t => Type.typecheck(t, Type.Numeric) map κ(Type.Numeric :: Nil)
    }
  )

  val Modulo = Mapping(
    "(%)",
    "Finds the remainder of one number divided by another",
    (Type.Numeric | Type.Interval) :: (Type.Numeric | Type.Interval) :: Nil,
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
