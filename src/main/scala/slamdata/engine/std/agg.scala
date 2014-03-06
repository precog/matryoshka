package slamdata.engine.std

import scalaz._

import slamdata.engine._

import Validation.{success, failure}
import NonEmptyList.nel
import SemanticError._

trait AggLib extends Library {
  private val NumericUnary: Func.Untyper = {
    case Type.Dec => success(Type.Dec :: Nil)
    case t => failure(nel(TypeError(Type.Dec, t), Nil))
  }

  val Count = Reduction("COUNT", "Counts the values in a set", Type.Top :: Nil, constTyper(Type.Int), Function.const(success(Type.Top :: Nil)))

  val Sum = Reduction("SUM", "Sums the values in a set", Type.Numeric :: Nil, reflexiveTyper, NumericUnary)

  val Min = Reduction("MIN", "Finds the minimum in a set of values", Type.Comparable :: Nil, reflexiveTyper, NumericUnary)

  val Max = Reduction("MAX", "Finds the maximum in a set of values", Type.Comparable :: Nil, reflexiveTyper, NumericUnary)

  val Avg = Reduction("AVG", "Finds the average in a set of numeric values", Type.Numeric :: Nil, constTyper(Type.Dec), NumericUnary)

  def functions = Count :: Sum :: Min :: Max :: Avg :: Nil
}
object AggLib extends AggLib