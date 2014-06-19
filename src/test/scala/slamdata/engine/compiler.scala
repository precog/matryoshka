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
        letOne('tmp0,
          read("foo"),
          letOne('tmp1,  // OK, this one is pretty silly 
            free('tmp0),
            free('tmp1)
      )
        )
      )
    }
    
    "compile qualified select *" in {
      testLogicalPlanCompile(
        "select foo.* from foo",
        letOne('tmp0,
          read("foo"),
          letOne('tmp1,  // OK, this one is pretty silly 
            free('tmp0),
            free('tmp1)
          )
        )
      )
    }.pendingUntilFixed  // parser bug?
    
    "compile simple select with unnamed projection which is just an identifier" in {
      testLogicalPlanCompile(
        "select name from city",
        letOne('tmp0,
          read("city"),
          letOne('tmp1,
        makeObj(
          "name" ->
          ObjectProject(
                free('tmp0),
            constant(Data.Str("name"))
          )
            ),
            free('tmp1)
        )
      )
      )
    }

    "compile simple 1-table projection when root identifier is also a projection" in {
      // 'foo' must be interpreted as a projection because only this interpretation is possible
      testLogicalPlanCompile(
        "select foo.bar from baz",        
        letOne('tmp0,
          read("baz"),
          letOne('tmp1,
        makeObj(
          "bar" -> 
          ObjectProject(
                ObjectProject(free('tmp0), constant(Data.Str("foo"))), 
            constant(Data.Str("bar"))
          )
            ),
            free('tmp1)
        )
      )
      )
    }

    "compile simple 1-table projection when root identifier is also a table ref" in {
      // 'foo' must be interpreted as a table reference because this interpretation is possible
      // and consistent with ANSI SQL.
      testLogicalPlanCompile(
        "select foo.bar from foo",        
        letOne('tmp0,
          read("foo"),
          letOne('tmp1,
        makeObj(
          "bar" ->
              ObjectProject(free('tmp0), constant(Data.Str("bar")))
            ),
            free('tmp1)
        )
      )
      )
    }

    "compile two term addition from one table" in {
      testLogicalPlanCompile(
        "select foo + bar from baz",
        letOne('tmp0,
          read("baz"),
          letOne('tmp1,
          makeObj(
            "0" ->  
            Add(
              ObjectProject(free('tmp0), constant(Data.Str("foo"))),
              ObjectProject(free('tmp0), constant(Data.Str("bar")))
            )
            ),
            free('tmp1)
          )
        )
      )
    }
    
    "compile complex expression" in {
      testLogicalPlanCompile(
        "select avgTemp*9/5 + 32 from cities",
        letOne('tmp0,
          read("cities"),
          letOne('tmp1,
        makeObj(
          "0" -> 
          Add(
            Divide(
              Multiply(
                    ObjectProject(free('tmp0), constant(Data.Str("avgTemp"))),
                constant(Data.Int(9))
              ),
              constant(Data.Int(5))
            ),
            constant(Data.Int(32))
          )
            ),
            free('tmp1)
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
          letOne('tmp1,
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
            ),
            free('tmp1)
          )
        )
      )
    }
    
    "compile simple where (with just a constant)" in {
      testLogicalPlanCompile(
        "select name from person where 1",
        letOne('tmp0,
          read("person"), 
          letOne('tmp1,
            Filter(
              free('tmp0),
              constant(Data.Int(1))
            ),
            letOne('tmp2,
        makeObj(
          "name" -> 
          ObjectProject(
                  free('tmp1), 
            constant(Data.Str("name"))
          )
              ),
              free('tmp2)
        )
      )
        )
      )
    }
    
    "compile simple where" in {
      testLogicalPlanCompile(
        "select name from person where age > 18",
        letOne('tmp0,
          read("person"),
          letOne('tmp1,
              Filter(
                free('tmp0),
                Gt(
                  ObjectProject(free('tmp0), constant(Data.Str("age"))),
                  constant(Data.Int(18))
                )
              ),
            letOne('tmp2,
              makeObj(
                "name" -> ObjectProject(free('tmp1), constant(Data.Str("name")))
              ),
              free('tmp2)
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
          letOne('tmp1,
              GroupBy(
                free('tmp0),
              MakeArray(
                  ObjectProject(free('tmp0), constant(Data.Str("name")))
                )
            ),
            letOne('tmp2,
              makeObj(
                "0" ->
                Count(
                  free('tmp1)
              )
              ),
              free('tmp2)
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
              "name" -> ObjectProject(free('tmp0), constant(Data.Str("name"))),
              "__sd__0" -> ObjectProject(free('tmp0), constant(Data.Str("height")))
            ),
            letOne('tmp2,
              OrderBy(
                free('tmp1),
                MakeArray(
                  ObjectProject(free('tmp1), constant(Data.Str("__sd__0")))
                )
              ),
              letOne('tmp3,
              makeObj(
                  "name" -> 
                  ObjectProject(free('tmp2), constant(Data.Str("name")))
                ),
                free('tmp3)
              )
            )
          )
        )
      )
    }
    
    "compile simple order by with field in projections" in {
      testLogicalPlanCompile(
        "select name from person order by name",
        letOne('tmp0,
          read("person"),
          letOne('tmp1,
            makeObj(
              "name" -> ObjectProject(free('tmp0), constant(Data.Str("name")))
            ),
            letOne('tmp2,
              OrderBy(
                free('tmp1),
                MakeArray(
                  ObjectProject(free('tmp1), constant(Data.Str("name")))
                )
              ),
              free('tmp2)
            )
          )
        )
      )
    }
    
    "compile simple order by with wildcard" in {
      testLogicalPlanCompile(
        "select * from person order by height",
        letOne('tmp0,
          read("person"),
          letOne('tmp1,  // Another silly temporary var here
            free('tmp0),
            letOne('tmp2,
              OrderBy(
                free('tmp1),
                MakeArray(
                  ObjectProject(free('tmp1), constant(Data.Str("height")))
                )
              ),
              free('tmp2)
            )
          )
        )
      )
    }
    
    "compile simple order by with expression" in {
      testLogicalPlanCompile(
        "select * from person order by height*2.54",
        letOne('tmp0,
          read("person"),
          ???  // Need to add synthetic projection for the expression, but also deal with the wildcard
        )
      )
    }.pendingUntilFixed
    
    "compile simple order by with expression in synthetic field" in {
      testLogicalPlanCompile(
        "select name from person order by height*2.54",
        letOne('tmp0,
          read("person"),
          letOne('tmp1,  // Another silly temporary var here
            makeObj(
              "name" -> ObjectProject(free('tmp0), constant(Data.Str("name"))),
              "__sd__0" -> 
                Multiply(
                  ObjectProject(free('tmp0), constant(Data.Str("height"))),
                  constant(Data.Dec(2.54))
                )
            ),
            letOne('tmp2,
              OrderBy(
                free('tmp1),
                MakeArray(
                  ObjectProject(free('tmp1), constant(Data.Str("__sd__0")))
                )
              ),
              letOne('tmp3,
                makeObj(
                  "name" -> ObjectProject(free('tmp2), constant(Data.Str("name")))
                ),
                free('tmp3)
              )
            )
          )
        )
      )
    }
    
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
              letOne('tmp3,
                      Filter(  // having count(*) > 10
                        free('tmp2),
                        Gt(
                          Count(free('tmp2)),
                          constant(Data.Int(10))
                        )
                      ), 
                letOne('tmp4,    // select height*2.54 as cm
                  makeObj(
                    "cm" ->
                    Multiply(
                      ObjectProject(
                        free('tmp3), 
                      constant(Data.Str("height"))),
                    constant(Data.Dec(2.54))
                  )
                ),
                  letOne('tmp5,
                    OrderBy(  // order by cm
                      free('tmp4),
                      MakeArray(  // TODO: should this be elminated?
                        ObjectProject(free('tmp4), constant(Data.Str("cm")))
                      )
                    ), 
                    letOne('tmp6,
                      Drop(    // offset 10
                        free('tmp5), 
                    constant(Data.Int(10))
                  ), 
                      Take(  // limit 5
                        free('tmp6), 
                  constant(Data.Int(5))
                )
              )
            )   
          )
        )
      )
          )
        )
      )
    }

    "compile simple sum" in {
      testLogicalPlanCompile(
        "select sum(height) from person",
        letOne('tmp0,
          read("person"), 
          letOne('tmp1,
        makeObj(
          "0" ->
          Sum(
                ObjectProject(free('tmp0), constant(Data.Str("height")))
            )
            ),
            free('tmp1)
          )
        )
      )
    }

    "compile simple inner equi-join" in {
      testLogicalPlanCompile(
        "select foo.name, bar.address from foo join bar on foo.id = bar.foo_id",
        letOne('tmp2,
               let(Map('left0 -> read("foo"),
                       'right1 -> read("bar")),
                 join(free('left0), free('right1),
                      JoinType.Inner, JoinRel.Eq,
                      ObjectProject(free('left0), constant(Data.Str("id"))),
                      ObjectProject(free('right1), constant(Data.Str("foo_id"))))),
          letOne('tmp3,
                 makeObj(
                   "name" ->
                     ObjectProject(
                       ObjectProject(free('tmp2), constant(Data.Str("left"))),
                       constant(Data.Str("name"))),
                   "address" ->
                     ObjectProject(
                       ObjectProject(free('tmp2), constant(Data.Str("right"))),
                       constant(Data.Str("address")))),
            free('tmp3))))
    }

  }
}
