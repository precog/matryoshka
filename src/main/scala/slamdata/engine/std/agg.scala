package slamdata.engine.std

import scalaz._

import slamdata.engine.{Type, Reduction}

trait AggLib extends Library {
  private val NumericSet = Type.Int | Type.Dec

  val Count = Reduction("COUNT", "Counts the values in a set", Type.Top :: Nil, constTyper(Type.Int))

  val Sum = Reduction("SUM", "Sums the values in a set", Type.Numeric :: Nil, numericWidening)

  val Min = Reduction("MIN", "Finds the minimum in a set of values", Type.Comparable :: Nil, reflexiveTyper)

  val Max = Reduction("MAX", "Finds the maximum in a set of values", Type.Comparable :: Nil, reflexiveTyper)

  val Avg = Reduction("AVG", "Finds the average in a set of numeric values", Type.Numeric :: Nil, numericWidening)

  def functions = Count :: Sum :: Min :: Max :: Avg :: Nil
}
object AggLib extends AggLib