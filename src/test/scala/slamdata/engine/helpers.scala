package slamdata.engine

import slamdata.engine.analysis.fixplate._
import slamdata.engine.analysis._
import slamdata.engine.sql.SQLParser
import slamdata.engine.std._

import scalaz._

import org.specs2.mutable._
import org.specs2.matcher.{Matcher, Expectable}

trait CompilerHelpers extends Specification {
  import StdLib._
  import structural._
  import math._
  import LogicalPlan._
  import SemanticAnalysis._

  val compile: String => Option[Term[LogicalPlan]] = query => {
    for {
      ast   <- (new SQLParser().parse(query)).toOption
      attr  <- SemanticAnalysis.AllPhases(tree(ast)).toOption
      cld   <- Compiler.compile(attr).toOption
    } yield cld
  }

  case class equalToPlan(expected: Term[LogicalPlan]) extends Matcher[Term[LogicalPlan]] {
    val equal = Equal[Term[LogicalPlan]].equal _

    def apply[S <: Term[LogicalPlan]](s: Expectable[S]) = {
      result(equal(expected, s.value),
             s.description + " is equal to " + expected,
             s.description + " is not equal to " + expected,
             s)
    }
  }

  def testLogicalPlanCompile(query: String, expected: Term[LogicalPlan]) = {
    compile(query) must beSome(equalToPlan(expected))
  }
}