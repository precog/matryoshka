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
             "\n" + Show[Workflow].show(s.value) + "\n is equal to \n" + Show[Workflow].show(expected),
             "\n" + Show[Workflow].show(s.value) + "\n is not equal to \n" + Show[Workflow].show(expected),
             s)
    }
  }

  def testPhysicalPlanCompile(query: String, expected: Workflow) = {
    val phys = for {
      logical <- compile(query)
      phys <- MongoDbPlanner.plan(logical)
    } yield phys
    phys.toEither must beRight(equalToWorkflow(expected))
  }

  "planner" should {
    "plan simple select *" in {
      testPhysicalPlanCompile(
        "select * from foo", 
        Workflow(
          ReadTask(Collection("foo"))
        )
      )
    }

    "plan simple field projection on single set" in {
      testPhysicalPlanCompile(
        "select foo.bar from foo",
        Workflow(
          PipelineTask(
            ReadTask(Collection("foo")),
            Pipeline(List(
              Project(Reshape(Map("bar" -> -\/(DocField(BsonField.Name("bar"))))))
            ))
          )
        )
      )
    }

    "plan simple field projection on single set when table name is inferred" in {
      testPhysicalPlanCompile(
        "select bar from foo",
        Workflow(
          PipelineTask(
            ReadTask(Collection("foo")),
            Pipeline(List(
              Project(Reshape(Map("bar" -> -\/(DocField(BsonField.Name("bar"))))))
            ))
          )
        )
      )
    }

    "plan simple addition on two fields" in {
      testPhysicalPlanCompile(
        "select foo + bar from baz",
        Workflow(
          PipelineTask(
            ReadTask(Collection("baz")),
            Pipeline(List(
              Project(Reshape(Map("0" -> -\/ (ExprOp.Add(DocField(BsonField.Name("foo")), DocField(BsonField.Name("bar")))))))
            ))
          )
        )
      )
    }
  }
}