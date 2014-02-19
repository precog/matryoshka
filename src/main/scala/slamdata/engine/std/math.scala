package slamdata.engine.std

import scalaz._

import slamdata.engine.{Data, Func, Type, Mapping, SemanticError}

import SemanticError._

trait MathLib extends Library {
  private val NumericDomain = Type.Numeric :: Type.Numeric :: Nil

  private val numericWidening: Func.CodomainRefiner = wideningRefiner(new Order[Type] {
    def order(v1: Type, v2: Type) = (v1, v2) match {
      case (Type.Dec, Type.Dec) => Ordering.EQ
      case (Type.Dec, _) => Ordering.LT
      case (_, Type.Dec) => Ordering.GT
      case _ => Ordering.EQ
    }
  })

  val Add = Mapping("(+)", "Adds two numeric values", NumericDomain,
    (partialRefiner {
      case \/-(Data.Dec(v1)) :: v2 :: Nil if (v1.signum == 0) => v2
      case v1 :: \/-(Data.Dec(v2)) :: Nil if (v2.signum == 0) => v1
      case \/-(Data.Int(v1)) :: v2 :: Nil if (v1.signum == 0) => v2
      case v1 :: \/-(Data.Int(v2)) :: Nil if (v2.signum == 0) => v1
      case \/-(Data.Int(v1)) :: \/-(Data.Int(v2)) :: Nil => \/-(Data.Int(v1 + v2))
      case \/-(Data.Dec(v1)) :: \/-(Data.Int(v2)) :: Nil => \/-(Data.Dec(v1 + BigDecimal(v2)))
      case \/-(Data.Int(v1)) :: \/-(Data.Dec(v2)) :: Nil => \/-(Data.Dec(BigDecimal(v1) + v2))
      case \/-(Data.Dec(v1)) :: \/-(Data.Dec(v2)) :: Nil => \/-(Data.Dec(v1 + v2))
    }) ||| numericWidening
  )
  val Multiply = Mapping("(*)", "Multiplies two numeric values", NumericDomain,
    (partialRefiner {
      case (zero @ \/-(Data.Dec(v1))) :: v2 :: Nil if (v1.signum == 0) => zero
      case v1 :: (zero @ \/-(Data.Dec(v2))) :: Nil if (v2.signum == 0) => zero
      case (zero @ \/-(Data.Int(v1))) :: v2 :: Nil if (v1.signum == 0) => zero
      case v1 :: (zero @ \/-(Data.Int(v2))) :: Nil if (v2.signum == 0) => zero
      case \/-(Data.Int(v1)) :: \/-(Data.Int(v2)) :: Nil => \/-(Data.Int(v1 * v2))
      case \/-(Data.Dec(v1)) :: \/-(Data.Int(v2)) :: Nil => \/-(Data.Dec(v1 * BigDecimal(v2)))
      case \/-(Data.Int(v1)) :: \/-(Data.Dec(v2)) :: Nil => \/-(Data.Dec(BigDecimal(v1) * v2))
      case \/-(Data.Dec(v1)) :: \/-(Data.Dec(v2)) :: Nil => \/-(Data.Dec(v1 * v2))
    }) ||| numericWidening
  )
  val Subtract = Mapping("(-)", "Subtracts two numeric values", NumericDomain,
    (partialRefiner {
      case v1 :: \/-(Data.Dec(v2)) :: Nil if (v2.signum == 0) => v1
      case v1 :: \/-(Data.Int(v2)) :: Nil if (v2.signum == 0) => v1
      case \/-(Data.Int(v1)) :: \/-(Data.Int(v2)) :: Nil => \/-(Data.Int(v1 - v2))
      case \/-(Data.Dec(v1)) :: \/-(Data.Int(v2)) :: Nil => \/-(Data.Dec(v1 - BigDecimal(v2)))
      case \/-(Data.Int(v1)) :: \/-(Data.Dec(v2)) :: Nil => \/-(Data.Dec(BigDecimal(v1) - v2))
      case \/-(Data.Dec(v1)) :: \/-(Data.Dec(v2)) :: Nil => \/-(Data.Dec(v1 - v2))
    }) ||| numericWidening
  )

  val Divide = Mapping("(/)", "Divides one numeric value by another (non-zero) numeric value", NumericDomain,
    (partialRefinerV {
      case v1 :: \/-(Data.Dec(v2)) :: Nil if (v2.doubleValue == 1.0) => Validation.success(v1)
      case v1 :: \/-(Data.Int(v2)) :: Nil if (v2.longValue == 1L) => Validation.success(v1)
      case v1 :: \/-(Data.Dec(v2)) :: Nil if (v2.doubleValue == 0.0) => Validation.failure(NonEmptyList(GenericError("Division by zero")))
      case v1 :: \/-(Data.Int(v2)) :: Nil if (v2.longValue == 0L) => Validation.failure(NonEmptyList(GenericError("Division by zero")))
      case \/-(Data.Int(v1)) :: \/-(Data.Int(v2)) :: Nil => Validation.success(\/-(Data.Int(v1 / v2)))
      case \/-(Data.Dec(v1)) :: \/-(Data.Int(v2)) :: Nil => Validation.success(\/-(Data.Dec(v1 / BigDecimal(v2))))
      case \/-(Data.Int(v1)) :: \/-(Data.Dec(v2)) :: Nil => Validation.success(\/-(Data.Dec(BigDecimal(v1) / v2)))
      case \/-(Data.Dec(v1)) :: \/-(Data.Dec(v2)) :: Nil => Validation.success(\/-(Data.Dec(v1 / v2)))
    }) ||| numericWidening
  )

  def functions = Add :: Multiply :: Subtract :: Divide :: Nil
}
object MathLib extends MathLib