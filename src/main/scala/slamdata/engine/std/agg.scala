package slamdata.engine.std

import scalaz._

import slamdata.engine.{Type, Reduction}

trait AggLib extends Library {
  private val NumericSet = Type.Int | Type.Dec

  val Count = Reduction("COUNT", "Counts the values in a set", Type.Top :: Nil, constRefiner(Type.Int))

  val Sum = Reduction("SUM", "Counts the values in a set", Type.Numeric :: Nil, numericWidening)

  val Min = Reduction("MIN", "Counts the values in a set", Type.Numeric :: Nil, numericWidening)

  val Max = Reduction("MAX", "Counts the values in a set", Type.Numeric :: Nil, numericWidening)

  val Avg = Reduction("AVG", "Counts the values in a set", Type.Numeric :: Nil, numericWidening)

  def functions = Count :: Sum :: Min :: Max :: Avg :: Nil
}
object AggLib extends AggLib