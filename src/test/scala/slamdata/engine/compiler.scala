package slamdata.engine

import slamdata.engine.analysis.fixplate._
import slamdata.engine.analysis._
import slamdata.engine.sql.SQLParser
import slamdata.engine.std._

import scalaz._

import org.specs2.mutable._
import org.specs2.matcher.{Matcher, Expectable}

@org.junit.runner.RunWith(classOf[org.specs2.runner.JUnitRunner])
class CompilerSpec extends Specification with CompilerHelpers {
  import StdLib._
  import structural._
  import math._
  import set._
  import relations._
  import agg._
  import LogicalPlan._
  import SemanticAnalysis._

  "compiler" should {
    "compile simple constant example 1" in {
      testLogicalPlanCompile(
        "select 1", 
        MakeObject(
          constant(Data.Str("0")), 
          constant(Data.Int(1))
        )
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

    "compile simple constant with multiple named projections" in {
      testLogicalPlanCompile(
        "select 1.0 as a, 'abc' as b", 
        ObjectConcat(
          MakeObject(
            constant(Data.Str("a")), 
            constant(Data.Dec(1.0))
          ),
          MakeObject(
            constant(Data.Str("b")),
            constant(Data.Str("abc"))
          )
        )
      )
    }
    

    "compile simple select *" in {
      testLogicalPlanCompile(
        "select * from foo",
        let(
          Map('tmp0 -> read("foo")),
          free('tmp0)
        )
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
    }

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
    }

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
    
    "compile two term multiplication from two tables" in {
      testLogicalPlanCompile(
        "select person.age * car.modelYear from person, car",
        let(
          Map('tmp0 -> 
            Cross(
              read("person"),
              read("car"))),
          MakeObject(
            constant(Data.Str("0")), 
            Multiply(
              ObjectProject(
                ObjectProject(
                  free('tmp0), 
                  constant(Data.Str("left"))
                ), 
                constant(Data.Str("age"))
              ),
              ObjectProject(
                ObjectProject(
                  free('tmp0), 
                  constant(Data.Str("right"))
                ), 
                constant(Data.Str("modelYear"))
              )
            )
          )
        )
      )
    }
    
    "compile simple group by" in {
      testLogicalPlanCompile(
        "select count(*) from person group by name",
        let(
          Map('tmp0 -> read("person")),
          GroupBy(
            MakeObject(
              constant(Data.Str("0")),
              ObjectProject(free('tmp0), constant(Data.Str("name")))  // TODO: count(*)
            ),
            ObjectProject(free('tmp0), constant(Data.Str("name")))
          )
        )
      )
    }.pendingUntilFixed  // needs some work in compiler.scala 
    
    "compile simple order by" in {
      testLogicalPlanCompile(
        "select name from person order by height",
        let(
          Map('tmp0 -> read("person")),
          OrderBy(
            MakeObject(
              constant(Data.Str("0")),
              ObjectProject(free('tmp0), constant(Data.Str("name")))
            ),
            ObjectProject(free('tmp0), constant(Data.Str("height")))
   		  )
        )
      )
    }.pendingUntilFixed  // needs some work in compiler.scala
    
    "compile simple where (with just a constant)" in {
      testLogicalPlanCompile(
        "select name from person where 1",
        let(
          Map('tmp0 -> 
            Filter(
              read("person"), 
              constant(Data.Int(1))
            )
          ),
          MakeObject(
            constant(Data.Str("0")),
            ObjectProject(free('tmp0), constant(Data.Str("name")))
          )
        )
      )
    }
    
    "compile simple where" in {
      testLogicalPlanCompile(
        "select name from person where age > 18",  // TODO: any real expression gives type error
        let(
          Map('tmp0 -> 
            Filter(
              read("person"),
              Gt(
                ObjectProject(free('tmp0), constant(Data.Str("name"))),
                constant(Data.Int(18))
              )
            )
          ),
          MakeObject(
            constant(Data.Str("0")),
            ObjectProject(free('tmp0), constant(Data.Str("name")))
          )
        )
      )
    }.pendingUntilFixed
    
    "compile simple sum" in {
      testLogicalPlanCompile(
        "select sum(height) from person",  // TODO: type error
        let(
          Map('tmp0 -> read("person")),
          MakeObject(
            constant(Data.Str("0")),
            Sum(ObjectProject(free('tmp0), constant(Data.Str("height"))))
          )
        )
      )
    }.pendingUntilFixed

  }
}