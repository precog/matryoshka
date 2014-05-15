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
  import WorkflowTask._
  import PipelineOp._
  import ExprOp._

  case class equalToWorkflow(expected: Workflow) extends Matcher[Workflow] {
    def apply[S <: Workflow](s: Expectable[S]) = {
      result(expected == s.value,
             s.description + " is equal to " + expected,
             s.description + " is not equal to " + expected,
             s)
    }
  }

  def testPhysicalPlanCompile(query: String, expected: Workflow) = {
    compile(query).flatMap(MongoDbPlanner.plan(_, "out").fold(e => sys.error(e.toString), s => Some(s))) must beSome(equalToWorkflow(expected))
  }

  "planner" should {
    "plan simple select *" in {
      // FIXME: This isn't quite right because "select *" is not compiling to the right logical plan
      testPhysicalPlanCompile(
        "select * from foo", 
        Workflow(
          PipelineTask(
            ReadTask(Collection("foo")),
            Pipeline(List(Project(Reshape(Map("0" -> -\/ (DocVar(BsonField.Name("ROOT")))))), Out(Collection("out"))))
          ),
          Collection("out")
        )
      )
    }
  }
}