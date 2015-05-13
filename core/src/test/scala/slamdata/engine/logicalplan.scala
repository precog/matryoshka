package slamdata.engine

import org.specs2.mutable._
import org.specs2.ScalaCheck
import org.scalacheck._

class LogicalPlanSpecs extends Specification with ScalaCheck {
  import LogicalPlan._
  import analysis.fixplate._
  import std.MathLib.{Add}

  implicit def arbitraryExpr: Arbitrary[Term[LogicalPlan]] =
    Arbitrary { Gen.sized(size => exprGen(size/30, Nil, Nil, true)) }

  def exprGen(size: Int, in: List[Term[LogicalPlan]], out: List[Term[LogicalPlan]], free: Boolean): Gen[Term[LogicalPlan]] = {
    val simple = if (free) Gen.oneOf(constGen, Gen.oneOf(in)) else constGen
    if (size == 0)
      simple
    else
      Gen.oneOf(simple, addGen(size, in, out), letGen(size, in, out))
  }

  def addGen(size: Int, in: List[Term[LogicalPlan]], out: List[Term[LogicalPlan]]): Gen[Term[LogicalPlan]] = for {
    l <- exprGen(size-1, in, out, true)
    r <- exprGen(size-1, in, out, true)
  } yield Invoke(std.MathLib.Add, l :: r :: Nil)

  def letGen(size: Int, in: List[Term[LogicalPlan]], out: List[Term[LogicalPlan]]): Gen[Term[LogicalPlan]] = {
    val n = Symbol("tmp" + ((in.length + out.length)+1))
    for {
      expr <- exprGen(size-1, in, Free(n) :: out, false) // don't generate Let(_, Free(), _) forms that break lpBoundPhase
      body <- exprGen(size-1, Free(n) :: in, out, true)
    } yield Let(n, expr, body)
  }

  def constGen: Gen[Term[LogicalPlan]] = for {
    n <- Gen.choose(0, 100)
  } yield Constant(Data.Int(n))
}
