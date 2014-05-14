package slamdata.engine.physical.mongodb

import slamdata.engine._
import slamdata.engine.analysis.fixplate._
import slamdata.engine.analysis._
import slamdata.engine.sql.SQLParser
import slamdata.engine.std._

import scalaz._

import org.specs2.mutable._
import org.specs2.matcher.{Matcher, Expectable}

class PlannerSpec extends Specification with CompilerHelpers {
  import StdLib._
  import structural._
  import math._
  import LogicalPlan._
  import SemanticAnalysis._

  case class equalToWorkflow(expected: Workflow) extends Matcher[Workflow] {
    def apply[S <: Workflow](s: Expectable[S]) = {
      result(expected == s.value,
             s.description + " is not equal to " + expected,
             s.description + " is unexpectedly equal to " + expected,
             s)
    }
  }

  def testPhysicalPlanCompile(query: String, expected: Workflow) = {
    compile(query).flatMap(MongoDbPlanner.plan(_, "out").fold(e => sys.error(e.toString), s => Some(s))) must beSome(equalToWorkflow(expected))
  }

  "planner" should {
    "plan simple select *" in {
      testPhysicalPlanCompile(
        "select * from foo", 
        null
      )
    }.pendingUntilFixed
  }
}