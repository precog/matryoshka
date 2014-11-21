package slamdata.engine.std

import scalaz._

import slamdata.engine._

import Validation.{success, failure}
import NonEmptyList.nel
import SemanticError._

trait AggLib extends Library {
  private def reflexiveUnary(exp: Type): Func.Untyper = x => Type.typecheck(exp, x) map { _ => x :: Nil }
  private val NumericUnary: Func.Untyper = reflexiveUnary(Type.Numeric)

  val Count = Reduction("COUNT", "Counts the values in a set", Type.Top :: Nil, constTyper(Type.Int), Function.const(success(Type.Top :: Nil)))

  val Sum = Reduction("SUM", "Sums the values in a set", Type.Numeric :: Nil, reflexiveTyper, NumericUnary)

  val Min = Reduction("MIN", "Finds the minimum in a set of values", Type.Comparable :: Nil, 
    reflexiveTyper, 
    reflexiveUnary(Type.Comparable))

  val Max = Reduction("MAX", "Finds the maximum in a set of values", Type.Comparable :: Nil,
    reflexiveTyper, 
    reflexiveUnary(Type.Comparable))

  val Avg = Reduction("AVG", "Finds the average in a set of numeric values", Type.Numeric :: Nil, constTyper(Type.Dec), NumericUnary)

  def functions = Count :: Sum :: Min :: Max :: Avg :: Nil
}
object AggLib extends AggLib
