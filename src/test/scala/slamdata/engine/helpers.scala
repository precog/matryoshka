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
             "\n" + Show[Term[LogicalPlan]].show(s.value) + "\n is equal to\n" + Show[Term[LogicalPlan]].show(expected),
             "\n" + Show[Term[LogicalPlan]].show(s.value) + "\n is not equal to\n" + Show[Term[LogicalPlan]].show(expected),
             s)
    }
  }

  def testLogicalPlanCompile(query: String, expected: Term[LogicalPlan]) = {
//    println("query: " + query)
//    val cq = compile(query)
//    println("compiled: " + cq.map(q => Show[Term[LogicalPlan]].show(q).toString).getOrElse("<error>"))
//    println("expected: " + Show[Term[LogicalPlan]].show(expected).toString)
//    cq must beSome(equalToPlan(expected))

    compile(query) must beSome(equalToPlan(expected))
  }
}