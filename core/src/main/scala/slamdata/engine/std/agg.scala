package slamdata.engine.std

import scalaz._

import slamdata.engine._
import slamdata.engine.fp._

import Validation.{success, failure}
import NonEmptyList.nel

trait AggLib extends Library {
  private def reflexiveUnary(exp: Type): Func.Untyper = x => Type.typecheck(exp, x) map κ(x :: Nil)
  private val NumericUnary: Func.Untyper = reflexiveUnary(Type.Numeric)

  val Count = Reduction("COUNT", "Counts the values in a set", Type.Top :: Nil,
    noSimplification,
    partialTyper {
      case List(Type.Const(Data.Set(Nil))) => Type.Const(Data.Int(0))
      case List(_)                         => Type.Int
    },
    κ(success(Type.Top :: Nil)))

  val Sum = Reduction("SUM", "Sums the values in a set", Type.Numeric :: Nil,
    noSimplification,
    partialTyper {
      case List(Type.Const(Data.Set(Nil)))  => Type.Const(Data.Int(0))
      case List(Type.Set(t))                => t
      case List(t)                          => t
    },
    NumericUnary)

  val Min = Reduction("MIN", "Finds the minimum in a set of values", Type.Comparable :: Nil,
    noSimplification,
    reflexiveTyper,
    reflexiveUnary(Type.Comparable))

  val Max = Reduction("MAX", "Finds the maximum in a set of values", Type.Comparable :: Nil,
    noSimplification,
    reflexiveTyper,
    reflexiveUnary(Type.Comparable))

  val Avg = Reduction("AVG", "Finds the average in a set of numeric values", Type.Numeric :: Nil,
    noSimplification,
    constTyper(Type.Dec),
    NumericUnary)

  def functions = Count :: Sum :: Min :: Max :: Avg :: Nil
}
object AggLib extends AggLib
