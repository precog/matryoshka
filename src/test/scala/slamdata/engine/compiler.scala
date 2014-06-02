package slamdata.engine

import slamdata.engine.analysis.fixplate._
import slamdata.engine.analysis._
import slamdata.engine.sql.SQLParser
import slamdata.engine.std._

import scalaz._

import org.specs2.mutable._
import org.specs2.matcher.{Matcher, Expectable}

class CompilerSpec extends Specification with CompilerHelpers {
  import StdLib._
  import structural._
  import math._
  import LogicalPlan._
  import SemanticAnalysis._

  "compiler" should {
    "compile simple constant example 1" in {
      testLogicalPlanCompile(
        "select 1", 
        MakeObject(constant(Data.Str("0")), constant(Data.Int(1)))
      )
    }

    "compile simple constant example 2" in {
      testLogicalPlanCompile(
        "select 1 * 1",
        MakeObject(
          constant(Data.Str("0")), 
          Multiply(constant(Data.Int(1)), constant(Data.Int(1)))
        )
      )
    }

    "compile simple select *" in {
      testLogicalPlanCompile(
        "select * from foo",
        read("foo")
      )
    }

    "compile simple 1-table projection when root identifier is also a projection" in {
      // 'foo' must be interpreted as a projection because only this interpretation is possible
      testLogicalPlanCompile(
        "select foo.bar from baz",
        let(
          Map('tmp0 -> read("baz")),
          MakeObject(
            constant(Data.Str("0")), 
            ObjectProject(
              ObjectProject(free('tmp0), constant(Data.Str("foo"))), 
              constant(Data.Str("bar"))
            )
          )
        )
      )
    }.pendingUntilFixed

    "compile simple 1-table projection when root identifier is also a table ref" in {
      // 'foo' must be interpreted as a table reference because this interpretation is possible
      // and consistent with ANSI SQL.
      testLogicalPlanCompile(
        "select foo.bar from foo",
        let(
          Map('tmp0 -> read("foo")),
          MakeObject(
            constant(Data.Str("0")), 
            ObjectProject(free('tmp0), constant(Data.Str("bar")))
          )
        )
      )
    }.pendingUntilFixed

    "compile two term addition from one table" in {
      testLogicalPlanCompile(
        "select foo + bar from baz",
        let(
          Map('tmp0 -> read("baz")),
          MakeObject(
            constant(Data.Str("0")), 
            Add(
              ObjectProject(free('tmp0), constant(Data.Str("foo"))),
              ObjectProject(free('tmp0), constant(Data.Str("bar")))
            )
          )
        )
      )
    }
  }
}