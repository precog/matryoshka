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
  import agg._
  import array._
  import date._
  import math._
  import relations._
  import set._
  import string._
  import LogicalPlan._
  import SemanticAnalysis._

  "compiler" should {
    "compile simple constant example 1" in {
      testLogicalPlanCompile(
        "select 1",
        makeObj(
          "0" -> Constant(Data.Int(1))
        )
      )
    }

    "compile simple constant example 2" in {
      testLogicalPlanCompile(
        "select 1 * 1",
        makeObj(
          "0" -> Multiply(Constant(Data.Int(1)), Constant(Data.Int(1)))
        )
      )
    }

    "compile simple constant with multiple named projections" in {
      testLogicalPlanCompile(
        "select 1.0 as a, 'abc' as b",
        makeObj(
          "a" -> Constant(Data.Dec(1.0)),
          "b" -> Constant(Data.Str("abc"))
        )
      )
    }

    "compile select substring" in {
      testLogicalPlanCompile(
        "select substring(bar, 2, 3) from foo",
        Let('tmp0, read("foo"),
          Let('tmp1,
            makeObj("0" ->
              Substring(
                ObjectProject(Free('tmp0), Constant(Data.Str("bar"))),
                Constant(Data.Int(2)),
                Constant(Data.Int(3)))),
            Free('tmp1))))
    }

    "compile select length" in {
      testLogicalPlanCompile(
        "select length(bar) from foo",
        Let('tmp0, read("foo"),
          Let('tmp1,
            makeObj("0" ->
              Length(ObjectProject(Free('tmp0), Constant(Data.Str("bar"))))),
            Free('tmp1))))
    }

    "compile simple select *" in {
      testLogicalPlanCompile(
        "select * from foo",
        Let('tmp0, read("foo"),
          Let('tmp1, Free('tmp0), // OK, this one is pretty silly
            Free('tmp1))))
    }
    
    "compile qualified select *" in {
      testLogicalPlanCompile(
        "select foo.* from foo",
        Let('tmp0, read("foo"),
          Let('tmp1, Free('tmp0), // OK, this one is pretty silly
            Free('tmp1))))
    }

    "compile qualified select * with additional fields" in {
      testLogicalPlanCompile(
        "select foo.*, bar.address from foo, bar",
        Let('tmp0, Cross(read("foo"), read("bar")),
          Let('tmp1,
            ObjectConcat(
              ObjectProject(Free('tmp0), Constant(Data.Str("left"))),
              makeObj(
                "address" ->
                  ObjectProject(
                    ObjectProject(Free('tmp0), Constant(Data.Str("right"))),
                    Constant(Data.Str("address"))))),
            Free('tmp1))))
    }

    "compile deeply-nested qualified select *" in {
      testLogicalPlanCompile(
        "select foo.bar.baz.*, bar.address from foo, bar",
        Let('tmp0, Cross(read("foo"), read("bar")),
          Let('tmp1,
            ObjectConcat(
              ObjectProject(
                ObjectProject(
                  ObjectProject(Free('tmp0), Constant(Data.Str("left"))),
                  Constant(Data.Str("bar"))),
                Constant(Data.Str("baz"))),
              makeObj(
                "address" ->
                  ObjectProject(
                    ObjectProject(Free('tmp0), Constant(Data.Str("right"))),
                    Constant(Data.Str("address"))))),
            Free('tmp1))))
    }

    "compile simple select with unnamed projection which is just an identifier" in {
      testLogicalPlanCompile(
        "select name from city",
        Let('tmp0, read("city"),
          Let('tmp1,
            makeObj(
              "name" ->
                ObjectProject(
                  Free('tmp0),
                  Constant(Data.Str("name")))),
            Free('tmp1))))
    }

    "compile simple 1-table projection when root identifier is also a projection" in {
      // 'foo' must be interpreted as a projection because only this interpretation is possible
      testLogicalPlanCompile(
        "select foo.bar from baz",        
        Let('tmp0, read("baz"),
          Let('tmp1,
            makeObj(
              "bar" ->
                ObjectProject(
                  ObjectProject(Free('tmp0), Constant(Data.Str("foo"))),
                  Constant(Data.Str("bar")))),
            Free('tmp1))))
    }

    "compile simple 1-table projection when root identifier is also a table ref" in {
      // 'foo' must be interpreted as a table reference because this
      // interpretation is possible and consistent with ANSI SQL.
      testLogicalPlanCompile(
        "select foo.bar from foo",
        Let('tmp0, read("foo"),
          Let('tmp1,
            makeObj(
              "bar" -> ObjectProject(Free('tmp0), Constant(Data.Str("bar")))),
            Free('tmp1))))
    }

    "compile two term addition from one table" in {
      testLogicalPlanCompile(
        "select foo + bar from baz",
        Let('tmp0, read("baz"),
          Let('tmp1,
            makeObj(
              "0" ->
                Add(
                  ObjectProject(Free('tmp0), Constant(Data.Str("foo"))),
                  ObjectProject(Free('tmp0), Constant(Data.Str("bar"))))),
            Free('tmp1))))
    }
    
    "compile negate" in {
      testLogicalPlanCompile(
        "select -foo from bar",
        Let('tmp0, read("bar"),
          Let('tmp1,
            makeObj(
              "0" ->
                Multiply(
                  Constant(Data.Int(-1)),
                  ObjectProject(Free('tmp0), Constant(Data.Str("foo"))))),
            Free('tmp1))))
    }

    "compile modulo" in {
      testLogicalPlanCompile(
        "select foo % baz from bar",
        Let('tmp0, read("bar"),
          Let('tmp1,
            makeObj(
              "0" ->
                Modulo(
                  ObjectProject(Free('tmp0), Constant(Data.Str("foo"))),
                  ObjectProject(Free('tmp0), Constant(Data.Str("baz"))))),
            Free('tmp1))))
    }

    "compile coalesce" in {
      testLogicalPlanCompile(
        "select coalesce(bar, baz) from foo",
        Let('tmp0, read("foo"),
          Let('tmp1,
            makeObj(
              "0" ->
                Coalesce(
                  ObjectProject(Free('tmp0), Constant(Data.Str("bar"))),
                  ObjectProject(Free('tmp0), Constant(Data.Str("baz"))))),
            Free('tmp1)))
      )
    }

    "compile date field extraction" in {
      testLogicalPlanCompile(
        "select date_part('day', baz) from foo",
        Let('tmp0, read("foo"),
          Let('tmp1,
            makeObj(
              "0" ->
                Extract(
                  Constant(Data.Str("day")),
                  ObjectProject(Free('tmp0), Constant(Data.Str("baz"))))),
            Free('tmp1)))
      )
    }

    "compile date field extraction" in {
      testLogicalPlanCompile(
        "select date_part('day', baz) from foo",
        Let('tmp0, read("foo"),
          Let('tmp1,
            makeObj(
              "0" ->
                Extract(
                  Constant(Data.Str("day")),
                  ObjectProject(Free('tmp0), Constant(Data.Str("baz"))))),
            Free('tmp1)))
      )
    }

    "compile conditional" in {
      testLogicalPlanCompile(
        "select case when pop < 10000 then city else loc end from zips",
        Let('tmp0, read("zips"),
          Let('tmp1,
            makeObj(
              "0" ->
                Cond(
                  Lt(
                    ObjectProject(Free('tmp0), Constant(Data.Str("pop"))),
                    Constant(Data.Int(10000))),
                  ObjectProject(Free('tmp0), Constant(Data.Str("city"))),
                  ObjectProject(Free('tmp0), Constant(Data.Str("loc"))))),
            Free('tmp1))))
    }

    "compile array length" in {
      testLogicalPlanCompile(
        "select array_length(bar, 1) from foo",
        Let('tmp0, read("foo"),
          Let('tmp1,
            makeObj(
              "0" ->
                ArrayLength(
                  ObjectProject(Free('tmp0), Constant(Data.Str("bar"))),
                  Constant(Data.Int(1)))),
            Free('tmp1))))
    }

    "compile concat" in {
      testLogicalPlanCompile(
        "select concat(foo, concat(' ', bar)) from baz",
        Let('tmp0, read("baz"),
          Let('tmp1,
            makeObj(
              "0" ->
                Concat(
                  ObjectProject(Free('tmp0), Constant(Data.Str("foo"))),
                  Concat(
                    Constant(Data.Str(" ")),
                    ObjectProject(Free('tmp0), Constant(Data.Str("bar")))))),
            Free('tmp1))))
    }
    
    "compile between" in {
      testLogicalPlanCompile(
        "select * from foo where bar between 1 and 10",
        Let('tmp0, read("foo"),
          Let('tmp1,
            Filter(
              Free('tmp0),
              Between(
                ObjectProject(Free('tmp0), Constant(Data.Str("bar"))),
                Constant(Data.Int(1)),
                Constant(Data.Int(10)))),
            Let('tmp2, Free('tmp1),
              Free('tmp2)))))
    }
    
    "compile like" in {
      testLogicalPlanCompile(
        "select * from foo where bar like 'a%'",
        Let('tmp0, read("foo"),
          Let('tmp1,
            Filter(
              Free('tmp0),
              Like(
                ObjectProject(Free('tmp0), Constant(Data.Str("bar"))),
                Constant(Data.Str("a%")))),
            Let('tmp2, Free('tmp1),
              Free('tmp2)))))
    }
    
    "compile complex expression" in {
      testLogicalPlanCompile(
        "select avgTemp*9/5 + 32 from cities",
        Let('tmp0, read("cities"),
          Let('tmp1,
            makeObj(
              "0" ->
                Add(
                  Divide(
                    Multiply(
                      ObjectProject(Free('tmp0), Constant(Data.Str("avgTemp"))),
                      Constant(Data.Int(9))),
                    Constant(Data.Int(5))),
                  Constant(Data.Int(32)))),
            Free('tmp1))))
    }

    "compile cross select *" in {
      testLogicalPlanCompile(
        "select * from person, car",
        Let('tmp0, Cross(read("person"), read("car")),
          Let('tmp1,
            ObjectConcat(
              ObjectProject(Free('tmp0), Constant(Data.Str("left"))),
              ObjectProject(Free('tmp0), Constant(Data.Str("right")))),
            Free('tmp1))))
    }

    "compile two term multiplication from two tables" in {
      testLogicalPlanCompile(
        "select person.age * car.modelYear from person, car",
        Let('tmp0, Cross(read("person"), read("car")),
          Let('tmp1,
            makeObj(
              "0" ->
                Multiply(
                  ObjectProject(
                    ObjectProject(
                      Free('tmp0),
                      Constant(Data.Str("left"))),
                    Constant(Data.Str("age"))),
                  ObjectProject(
                    ObjectProject(
                      Free('tmp0),
                      Constant(Data.Str("right"))),
                    Constant(Data.Str("modelYear"))))),
            Free('tmp1))))
    }
    
    "compile simple where (with just a constant)" in {
      testLogicalPlanCompile(
        "select name from person where 1",
        Let('tmp0, read("person"),
          Let('tmp1, Filter(Free('tmp0), Constant(Data.Int(1))),
            Let('tmp2,
              makeObj(
                "name" ->
                  ObjectProject(
                    Free('tmp1),
                    Constant(Data.Str("name")))),
              Free('tmp2)))))
    }
    
    "compile simple where" in {
      testLogicalPlanCompile(
        "select name from person where age > 18",
        Let('tmp0, read("person"),
          Let('tmp1,
            Filter(
              Free('tmp0),
              Gt(
                ObjectProject(Free('tmp0), Constant(Data.Str("age"))),
                Constant(Data.Int(18)))),
            Let('tmp2,
              makeObj(
                "name" ->
                  ObjectProject(Free('tmp1), Constant(Data.Str("name")))),
              Free('tmp2)))))
    }
    
    "compile simple group by" in {
      testLogicalPlanCompile(
        "select count(*) from person group by name",
        Let('tmp0, read("person"),
          Let('tmp1,
            GroupBy(
              Free('tmp0),
              MakeArrayN(ObjectProject(
                Free('tmp0),
                Constant(Data.Str("name"))))),
            Let('tmp2, makeObj("0" -> Count(Free('tmp1))),
              Free('tmp2)))))
    }
    
    "compile simple order by" in {
      testLogicalPlanCompile(
        "select name from person order by height",
        Let('tmp0, read("person"),
          Let('tmp1,
            makeObj(
              "name" -> ObjectProject(Free('tmp0), Constant(Data.Str("name"))),
              "__sd__0" -> ObjectProject(Free('tmp0), Constant(Data.Str("height")))),
            Let('tmp2,
              OrderBy(
                Free('tmp1),
                MakeArrayN(
                  makeObj(
                    "key" -> ObjectProject(Free('tmp1), Constant(Data.Str("__sd__0"))),
                    "order" -> Constant(Data.Str("ASC"))))),
              Let('tmp3,
                makeObj(
                  "name" ->
                    ObjectProject(Free('tmp2), Constant(Data.Str("name")))),
                Free('tmp3))))))
    }

    "compile simple order by with filter" in {
      testLogicalPlanCompile(
        "select name from person where gender = 'male' order by name, height",
        Let('tmp0, read("person"),
          Let('tmp1,
            Filter(
              Free('tmp0),
              Eq(
                ObjectProject(Free('tmp0), Constant(Data.Str("gender"))),
                Constant(Data.Str("male")))),
            Let('tmp2,
              makeObj(
                "name"    -> ObjectProject(Free('tmp1), Constant(Data.Str("name"))),
                "__sd__0" -> ObjectProject(Free('tmp1), Constant(Data.Str("height")))),
              Let('tmp3,
                OrderBy(
                  Free('tmp2),
                  MakeArrayN(
                    makeObj(
                      "key"   -> ObjectProject(Free('tmp2), Constant(Data.Str("name"))),
                      "order" -> Constant(Data.Str("ASC"))),
                    makeObj(
                      "key"   -> ObjectProject(Free('tmp2), Constant(Data.Str("__sd__0"))),
                      "order" -> Constant(Data.Str("ASC"))))),
                Let('tmp4,
                  makeObj(
                    "name" -> ObjectProject(Free('tmp3), Constant(Data.Str("name")))),
                  Free('tmp4)))))))
    }
    
    "compile simple order by with wildcard" in {
      testLogicalPlanCompile(
        "select * from person order by height",
        Let('tmp0, read("person"),
          Let('tmp1, Free('tmp0), // Another silly temporary var here
            Let('tmp2,
              OrderBy(
                Free('tmp1),
                MakeArrayN(
                  makeObj(
                    "key" -> ObjectProject(Free('tmp1), Constant(Data.Str("height"))),
                    "order" -> Constant(Data.Str("ASC"))))),
              Free('tmp2)))))
    }
    
    "compile simple order by with ascending and descending" in {
      testLogicalPlanCompile(
        "select * from person order by height desc, name",
        Let('tmp0, read("person"),
          Let('tmp1, Free('tmp0), // Another silly temporary var here
            Let('tmp2,
              OrderBy(
                Free('tmp1),
                  MakeArrayN(
                    makeObj(
                      "key" -> ObjectProject(Free('tmp1), Constant(Data.Str("height"))),
                      "order" -> Constant(Data.Str("DESC"))),
                    makeObj(
                      "key" -> ObjectProject(Free('tmp1), Constant(Data.Str("name"))),
                      "order" -> Constant(Data.Str("ASC"))))),
              Free('tmp2)))))
    }

    "compile simple order by with expression" in {
      testLogicalPlanCompile(
        "select * from person order by height*2.54",
        Let('tmp0, read("person"),
          Let('tmp1,
            ObjectConcat(
              Free('tmp0),
              makeObj(
                "__sd__0" -> Multiply(
                              ObjectProject(Free('tmp0), Constant(Data.Str("height"))),
                              Constant(Data.Dec(2.54)))
              )
            ),
            Let('tmp2,
              OrderBy(
                Free('tmp1),
                MakeArrayN(
                  makeObj(
                    "key" -> ObjectProject(Free('tmp1), Constant(Data.Str("__sd__0"))),
                    "order" -> Constant(Data.Str("ASC"))))),
              Let('tmp3,
                Free('tmp2),
                Free('tmp3)
              )
            )
          )
        )
      )
    }

    "compile order by with alias" in {
      testLogicalPlanCompile(
        "select firstName as name from person order by name",
        Let('tmp0, read("person"),
          Let('tmp1,
            makeObj(
              "name" -> ObjectProject(Free('tmp0), Constant(Data.Str("firstName")))),
            Let('tmp2,
              OrderBy(
                Free('tmp1),
                MakeArrayN(
                  makeObj(
                    "key" -> ObjectProject(Free('tmp1), Constant(Data.Str("name"))),
                    "order" -> Constant(Data.Str("ASC"))))),
              Free('tmp2)))))
    }

    "compile simple order by with expression in synthetic field" in {
      testLogicalPlanCompile(
        "select name from person order by height*2.54",
        Let('tmp0, read("person"),
          Let('tmp1,
            makeObj(
              "name" -> ObjectProject(Free('tmp0), Constant(Data.Str("name"))),
              "__sd__0" ->
                Multiply(
                  ObjectProject(Free('tmp0), Constant(Data.Str("height"))),
                  Constant(Data.Dec(2.54)))),
            Let('tmp2,
              OrderBy(
                Free('tmp1),
                MakeArrayN(
                  makeObj(
                    "key" -> ObjectProject(Free('tmp1), Constant(Data.Str("__sd__0"))),
                    "order" -> Constant(Data.Str("ASC"))))),
              Let('tmp3,
                makeObj(
                  "name" ->
                    ObjectProject(Free('tmp2), Constant(Data.Str("name")))),
                Free('tmp3))))))
    }

    "compile order by with nested projection" in {
      compile("select bar from foo order by foo.bar.baz.quux/3").toEither must
        beRight(equalToPlan(
          Let('tmp0, read("foo"),
            Let('tmp1,
              makeObj(
                "bar" -> ObjectProject(Free('tmp0), Constant(Data.Str("bar"))),
                "__sd__0" -> Divide(
                              ObjectProject(
                                ObjectProject(
                                  ObjectProject(Free('tmp0),
                                    Constant(Data.Str("bar"))),
                                  Constant(Data.Str("baz"))),
                                Constant(Data.Str("quux"))),
                              Constant(Data.Int(3)))),
              Let('tmp2,
                OrderBy(
                  Free('tmp1),
                  MakeArrayN(
                    makeObj(
                      "key" -> ObjectProject(Free('tmp1), Constant(Data.Str("__sd__0"))),
                      "order" -> Constant(Data.Str("ASC"))))),
                Let('tmp3,
                  makeObj(
                    "bar" ->
                      ObjectProject(Free('tmp2), Constant(Data.Str("bar")))),
                  Free('tmp3)))))))
    }

    "compile order by with root projection a table ref" in {
      // Note: not using wildcard here because the simple case is optimized differently
      val lp = compile(    "select foo from bar order by bar.baz")
      val exp = compileExp("select foo from bar order by baz")
      lp.toEither must beRight(equalToPlan(exp))
    }

    "compile order by with root projection a table ref with alias" in {
      // Note: not using wildcard here because the simple case is optimized differently
      val lp = compile(    "select foo from bar b order by b.baz")
      val exp = compileExp("select foo from bar b order by baz")
      lp.toEither must beRight(equalToPlan(exp))
    }

    "compile order by with root projection a table ref with alias, mismatched" in {
      val lp = compile(    "select * from bar b order by bar.baz")
      val exp = compileExp("select * from bar b order by b.bar.baz")
      lp.toEither must beRight(equalToPlan(exp))
    }

    "compile order by with root projection a table ref, embedded in expr" in {
      val lp = compile(    "select * from bar order by bar.baz/10")
      val exp = compileExp("select * from bar order by baz/10")
      lp.toEither must beRight(equalToPlan(exp))
    }

    "compile order by with root projection a table ref, embedded in complex expr" in {
      val lp = compile(    "select * from bar order by bar.baz/10 - 3*bar.quux")
      val exp = compileExp("select * from bar order by baz/10 - 3*quux")
      lp.toEither must beRight(equalToPlan(exp))
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
        Let('tmp0, read("person"), // from person
          Let('tmp1,    // where height > 60
            Filter(
              Free('tmp0),
              Gt(
                ObjectProject(Free('tmp0), Constant(Data.Str("height"))),
                Constant(Data.Int(60)))),
            Let('tmp2,    // group by gender, height
              GroupBy(
                Free('tmp1),
                MakeArrayN(
                  ObjectProject(Free('tmp1), Constant(Data.Str("gender"))),
                  ObjectProject(Free('tmp1), Constant(Data.Str("height"))))),
              Let('tmp3,
                Filter(  // having count(*) > 10
                  Free('tmp2),
                  Gt(Count(Free('tmp2)), Constant(Data.Int(10)))),
                Let('tmp4,    // select height*2.54 as cm
                  makeObj(
                    "cm" ->
                      Multiply(
                        ObjectProject(
                          Free('tmp3),
                          Constant(Data.Str("height"))),
                        Constant(Data.Dec(2.54)))),
                  Let('tmp5,
                    OrderBy(  // order by cm
                      Free('tmp4),
                      MakeArrayN(
                        makeObj(
                          "key" -> ObjectProject(Free('tmp4), Constant(Data.Str("cm"))),
                          "order" -> Constant(Data.Str("ASC"))))),
                    Let('tmp6,
                      Drop(    // offset 10
                        Free('tmp5),
                        Constant(Data.Int(10))),
                      Take(  // limit 5
                        Free('tmp6),
                        Constant(Data.Int(5)))))))))))
    }

    "compile simple sum" in {
      testLogicalPlanCompile(
        "select sum(height) from person",
        Let('tmp0, read("person"), 
          Let('tmp1,
            makeObj(
              "0" ->
                Sum(ObjectProject(Free('tmp0), Constant(Data.Str("height"))))),
            Free('tmp1))))
    }

    "compile simple inner equi-join" in {
      testLogicalPlanCompile(
        "select foo.name, bar.address from foo join bar on foo.id = bar.foo_id",
        Let('tmp0,
          Let('left1, read("foo"),
            Let('right2, read("bar"),
              Join(Free('left1), Free('right2),
                JoinType.Inner, JoinRel.Eq,
                ObjectProject(Free('left1), Constant(Data.Str("id"))),
                ObjectProject(Free('right2), Constant(Data.Str("foo_id")))))),
          Let('tmp3,
            makeObj(
              "name" ->
                ObjectProject(
                  ObjectProject(Free('tmp0), Constant(Data.Str("left"))),
                  Constant(Data.Str("name"))),
              "address" ->
                ObjectProject(
                  ObjectProject(Free('tmp0), Constant(Data.Str("right"))),
                  Constant(Data.Str("address")))),
            Free('tmp3))))
    }

    "compile simple left ineq-join" in {
      testLogicalPlanCompile(
        "select foo.name, bar.address " +
          "from foo left join bar on foo.id < bar.foo_id",
        Let('tmp0,
          Let('left1, read("foo"),
            Let('right2, read("bar"),
              Join(Free('left1), Free('right2),
                JoinType.LeftOuter, JoinRel.Lt,
                ObjectProject(Free('left1), Constant(Data.Str("id"))),
                ObjectProject(Free('right2), Constant(Data.Str("foo_id")))))),
          Let('tmp3,
            makeObj(
              "name" ->
                ObjectProject(
                  ObjectProject(Free('tmp0), Constant(Data.Str("left"))),
                  Constant(Data.Str("name"))),
              "address" ->
                ObjectProject(
                  ObjectProject(Free('tmp0), Constant(Data.Str("right"))),
                  Constant(Data.Str("address")))),
            Free('tmp3))))
    }
 
    "compile complex equi-join" in {
      testLogicalPlanCompile(
        "select foo.name, bar.address " +
          "from foo join bar on foo.id = bar.foo_id " +
          "join baz on baz.bar_id = bar.id",
        Let('tmp0,
          Let('left1,
            Let('left3, read("foo"),
              Let('right4, read("bar"),
                Join(Free('left3), Free('right4),
                  JoinType.Inner, JoinRel.Eq,
                  ObjectProject(
                    Free('left3),
                    Constant(Data.Str("id"))),
                  ObjectProject(
                    Free('right4),
                    Constant(Data.Str("foo_id")))))),
            Let('right2, read("baz"),
              Join(Free('left1), Free('right2),
                JoinType.Inner, JoinRel.Eq,
                ObjectProject(Free('right2),
                  Constant(Data.Str("bar_id"))),
                ObjectProject(
                  ObjectProject(Free('left1),
                    Constant(Data.Str("right"))),
                  Constant(Data.Str("id")))))),
          Let('tmp5,
            makeObj(
              "name" ->
                ObjectProject(
                  ObjectProject(
                    ObjectProject(Free('tmp0), Constant(Data.Str("left"))),
                    Constant(Data.Str("left"))),
                  Constant(Data.Str("name"))),
              "address" ->
                ObjectProject(
                  ObjectProject(
                    ObjectProject(Free('tmp0), Constant(Data.Str("left"))),
                    Constant(Data.Str("right"))),
                  Constant(Data.Str("address")))),
            Free('tmp5))))
    }
 
  }
}
