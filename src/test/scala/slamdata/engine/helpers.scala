package slamdata.engine

import slamdata.engine.analysis.fixplate._
import slamdata.engine.analysis._
import slamdata.engine.sql.{SQLParser, Query}
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

  val compile: String => String \/ Term[LogicalPlan] = query => {
    for {
      ast   <- (new SQLParser().parse(Query(query))).leftMap(e => e.toString())
      attr  <- SemanticAnalysis.AllPhases(tree(ast)).leftMap(e => e.toString()).disjunction
      cld   <- Compiler.compile(attr).leftMap(e => e.toString())
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
//    println(compile(query))
    
    compile(query).toEither must beRight(equalToPlan(expected))
  }
}