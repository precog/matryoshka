package slamdata.engine

import slamdata.engine.analysis.fixplate._
import slamdata.engine.analysis._
import slamdata.engine.sql.SQLParser
import slamdata.engine.std._
import scalaz._
import org.specs2.mutable._
import org.specs2.matcher.{Matcher, Expectable}
import org.specs2.execute.PendingUntilFixed

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

  def read(name: String): Term[LogicalPlan] = LogicalPlan.read(fs.Path(name))

  def letOne(s: Symbol, t: Term[LogicalPlan], expr: Term[LogicalPlan]) = 
    let(Map(s -> t), expr)
    
  def makeObj(ts: (String, Term[LogicalPlan])*): Term[LogicalPlan] = {
    val objs = ts.map { case (label, term) => MakeObject(constant(Data.Str(label)), term) }
    if (objs.length == 1) objs(0) else ObjectConcat(objs: _*)
  }

  "compiler" should {
    "compile simple constant example 1" in {
      testLogicalPlanCompile(
        "select 1",
        makeObj(
          "0" -> constant(Data.Int(1))
        )
      )
    }

    "compile simple constant example 2" in {
      testLogicalPlanCompile(
        "select 1 * 1",
        makeObj(
          "0" -> Multiply(constant(Data.Int(1)), constant(Data.Int(1)))
        )
      )
    }

    "compile simple constant with multiple named projections" in {
      testLogicalPlanCompile(
        "select 1.0 as a, 'abc' as b", 
        makeObj(
          "a" -> constant(Data.Dec(1.0)),
          "b" -> constant(Data.Str("abc"))
        )
      )
    }
    

    "compile simple select *" in {
      testLogicalPlanCompile(
        "select * from foo",
        read("foo")
      )
    }
    
    "compile simple select with unnamed projection which is just an identifier" in {
      testLogicalPlanCompile(
        "select name from city",
        makeObj(
          "name" ->
          ObjectProject(
            read("city"),
            constant(Data.Str("name"))
          )
        )
      )
    }

    "compile simple 1-table projection when root identifier is also a projection" in {
      // 'foo' must be interpreted as a projection because only this interpretation is possible
      testLogicalPlanCompile(
        "select foo.bar from baz",        
        makeObj(
          "bar" -> 
          ObjectProject(
            ObjectProject(read("baz"), constant(Data.Str("foo"))), 
            constant(Data.Str("bar"))
          )
        )
      )
    }

    "compile simple 1-table projection when root identifier is also a table ref" in {
      // 'foo' must be interpreted as a table reference because this interpretation is possible
      // and consistent with ANSI SQL.
      testLogicalPlanCompile(
        "select foo.bar from foo",        
        makeObj(
          "bar" ->
          ObjectProject(read("foo"), constant(Data.Str("bar")))
        )
      )
    }

    "compile two term addition from one table" in {
      testLogicalPlanCompile(
        "select foo + bar from baz",
        letOne('tmp0,
          read("baz"),
          makeObj(
            "0" ->  
            Add(
              ObjectProject(free('tmp0), constant(Data.Str("foo"))),
              ObjectProject(free('tmp0), constant(Data.Str("bar")))
            )
          )
        )
      )
    }
    
    "compile complex expression" in {
      testLogicalPlanCompile(
        "select avgTemp*9/5 + 32 from cities",
        makeObj(
          "0" -> 
          Add(
            Divide(
              Multiply(
                ObjectProject(read("cities"), constant(Data.Str("avgTemp"))),
                constant(Data.Int(9))
              ),
              constant(Data.Int(5))
            ),
            constant(Data.Int(32))
          )
        )
      )
    }
    
    "compile two term multiplication from two tables" in {
      testLogicalPlanCompile(
        "select person.age * car.modelYear from person, car",
        letOne('tmp0,
          Cross(
            read("person"),
            read("car")
          ),
          makeObj(
            "0" -> 
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
    
    "compile simple where (with just a constant)" in {
      testLogicalPlanCompile(
        "select name from person where 1",
        makeObj(
          "name" -> 
          ObjectProject(
            Filter(
              read("person"), 
              constant(Data.Int(1))
            ), 
            constant(Data.Str("name"))
          )
        )
      )
    }
    
    "compile simple where" in {
      testLogicalPlanCompile(
        "select name from person where age > 18",
        letOne('tmp0,
          read("person"),
          makeObj(
            "name" ->
            ObjectProject(
              Filter(
                free('tmp0),
                Gt(
                  ObjectProject(free('tmp0), constant(Data.Str("age"))),
                  constant(Data.Int(18))
                )
              ),
              constant(Data.Str("name"))
            )
          )
        )
      )
    }
    
    "compile simple group by" in {
      testLogicalPlanCompile(
        "select count(*) from person group by name",
        letOne('tmp0,
          read("person"),
          makeObj(
            "0" ->
            Count(
              GroupBy(
                free('tmp0),
                MakeArray(  // TODO: strip this unneeded array?
                  ObjectProject(free('tmp0), constant(Data.Str("name")))
                )
              )
            )
          )
        )
      )
    }
    
    "compile simple order by" in {
      testLogicalPlanCompile(
        "select name from person order by height",
        letOne('tmp0,
          read("person"),
          letOne('tmp1,
            makeObj(
              "0" -> ObjectProject(free('tmp0), constant(Data.Str("name"))),
              "$order0" -> ObjectProject(free('tmp0), constant(Data.Str("height")))
            ),
            letOne('tmp2,
              OrderBy(
                free('tmp1),
                ObjectProject(free('tmp1), constant(Data.Str("$order0")))
              ),
              makeObj(
                "0" -> ObjectProject(free('tmp2), constant(Data.Str("0")))
              )
            )
          )
        )
      )
    }.pendingUntilFixed
    
    "compile multiple stages" in {
      testLogicalPlanCompile(
        "select height*2.54 as cm" +
          " from person" +
          " where height > 60" +
          " group by gender, height" + 
          " having count(*) > 10" +
          " order by cm" +
          " limit 5" +
          " offset 10",
      letOne('tmp0,    // from person
        read("person"),
        letOne('tmp1,    // where height > 60
          Filter(
            free('tmp0),
            Gt(
                ObjectProject(free('tmp0), constant(Data.Str("height"))),
                constant(Data.Int(60))
            )
          ),
          letOne('tmp2,    // group by gender, height
            GroupBy(
              free('tmp1),
              ArrayConcat(
                MakeArray(ObjectProject(free('tmp1), constant(Data.Str("gender")))),
                MakeArray(ObjectProject(free('tmp1), constant(Data.Str("height"))))
                )
              ),
              letOne('tmp4,    // select height*2.54 as cm
                makeObj(
                  "cm" ->
                  Multiply(
                    ObjectProject(
                      Filter(  // having count(*) > 10
                        free('tmp2),
                        Gt(
                          Count(free('tmp2)),
                          constant(Data.Int(10))
                        )
                      ), 
                      constant(Data.Str("height"))),
                    constant(Data.Dec(2.54))
                  )
                ),
                Take(  // limit 5
                  Drop(    // offset 10
                    OrderBy(  // order by cm
                      free('tmp4),
                      MakeArray(  // TODO: should this be elminated?
                        ObjectProject(free('tmp4), constant(Data.Str("cm")))
                      )
                    ), 
                    constant(Data.Int(10))
                  ), 
                  constant(Data.Int(5))
                )
              )
            )   
          )
        )
      )
    }//.pendingUntilFixed

    "compile simple sum" in {
      testLogicalPlanCompile(
        "select sum(height) from person",
        makeObj(
          "0" ->
          Sum(
            ObjectProject(
              read("person"), 
              constant(Data.Str("height"))
            )
          )
        )
      )
    }

  }
}