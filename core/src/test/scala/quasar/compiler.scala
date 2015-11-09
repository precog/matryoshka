package quasar

import quasar.Predef._

import quasar.sql.SQLParser
import quasar.std._
import org.specs2.mutable._
import org.specs2.matcher.{Matcher, Expectable}
import org.specs2.scalaz._
import quasar.specs2.PendingWithAccurateCoverage

class CompilerSpec extends Specification with CompilerHelpers with PendingWithAccurateCoverage with DisjunctionMatchers {
  import StdLib._
  import agg._
  import array._
  import date._
  import identity._
  import math._
  import relations._
  import set._
  import string._
  import structural._

  import LogicalPlan._

  "compiler" should {
    "compile simple constant example 1" in {
      testLogicalPlanCompile(
        "select 1",
        makeObj("0" -> Constant(Data.Int(1))))
    }

    "compile simple boolean literal (true)" in {
      testLogicalPlanCompile(
        "select true",
        makeObj("0" -> Constant(Data.Bool(true))))
    }

    "compile simple boolean literal (false)" in {
      testLogicalPlanCompile(
        "select false",
        makeObj("0" -> Constant(Data.Bool(false))))
    }

    "compile simple constant with multiple named projections" in {
      testLogicalPlanCompile(
        "select 1.0 as a, 'abc' as b",
        makeObj(
          "a" -> Constant(Data.Dec(1.0)),
          "b" -> Constant(Data.Str("abc"))))
    }

    "compile select substring" in {
      testLogicalPlanCompile(
        "select substring(bar, 2, 3) from foo",
        Squash(
          makeObj(
            "0" ->
              Substring(
                ObjectProject(read("foo"), Constant(Data.Str("bar"))),
                Constant(Data.Int(2)),
                Constant(Data.Int(3))))))
    }

    "compile select length" in {
      testLogicalPlanCompile(
        "select length(bar) from foo",
        Squash(
          makeObj(
            "0" -> Length(ObjectProject(read("foo"), Constant(Data.Str("bar")))))))
    }

    "compile simple select *" in {
      testLogicalPlanCompile("select * from foo", Squash(read("foo")))
    }

    "compile qualified select *" in {
      testLogicalPlanCompile("select foo.* from foo", Squash(read("foo")))
    }

    "compile qualified select * with additional fields" in {
      testLogicalPlanCompile(
        "select foo.*, bar.address from foo, bar",
        Let('tmp0,
          InnerJoin(read("foo"), read("bar"), Constant(Data.Bool(true))),
          Squash(
            ObjectConcat(
              ObjectProject(Free('tmp0), Constant(Data.Str("left"))),
              makeObj(
                "address" ->
                  ObjectProject(
                    ObjectProject(Free('tmp0), Constant(Data.Str("right"))),
                    Constant(Data.Str("address"))))))))
    }

    "compile deeply-nested qualified select *" in {
      testLogicalPlanCompile(
        "select foo.bar.baz.*, bar.address from foo, bar",
        Let('tmp0,
          InnerJoin(read("foo"), read("bar"), Constant(Data.Bool(true))),
          Squash(
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
                    Constant(Data.Str("address"))))))))
    }

    "compile simple select with unnamed projection which is just an identifier" in {
      testLogicalPlanCompile(
        "select name from city",
        Squash(
          makeObj(
            "name" -> ObjectProject(read("city"), Constant(Data.Str("name"))))))
    }

    "compile simple 1-table projection when root identifier is also a projection" in {
      // 'foo' must be interpreted as a projection because only this interpretation is possible
      testLogicalPlanCompile(
        "select foo.bar from baz",
        Squash(
          makeObj(
            "bar" ->
              ObjectProject(
                ObjectProject(read("baz"), Constant(Data.Str("foo"))),
                Constant(Data.Str("bar"))))))
    }

    "compile simple 1-table projection when root identifier is also a table ref" in {
      // 'foo' must be interpreted as a table reference because this
      // interpretation is possible and consistent with ANSI SQL.
      testLogicalPlanCompile(
        "select foo.bar from foo",
        Squash(
          makeObj(
            "bar" -> ObjectProject(read("foo"), Constant(Data.Str("bar"))))))
    }

    "compile two term addition from one table" in {
      testLogicalPlanCompile(
        "select foo + bar from baz",
        Let('tmp0, read("baz"),
          Squash(
            makeObj(
              "0" ->
                Add(
                  ObjectProject(Free('tmp0), Constant(Data.Str("foo"))),
                  ObjectProject(Free('tmp0), Constant(Data.Str("bar"))))))))
    }

    "compile negate" in {
      testLogicalPlanCompile(
        "select -foo from bar",
        Squash(
          makeObj(
            "0" ->
              Negate(ObjectProject(read("bar"), Constant(Data.Str("foo")))))))
    }

    "compile modulo" in {
      testLogicalPlanCompile(
        "select foo % baz from bar",
        Let('tmp0, read("bar"),
          Squash(
            makeObj(
              "0" ->
                Modulo(
                  ObjectProject(Free('tmp0), Constant(Data.Str("foo"))),
                  ObjectProject(Free('tmp0), Constant(Data.Str("baz"))))))))
    }

    "compile coalesce" in {
      testLogicalPlanCompile(
        "select coalesce(bar, baz) from foo",
        Let('tmp0, read("foo"),
          Squash(
            makeObj(
              "0" ->
                Coalesce(
                  ObjectProject(Free('tmp0), Constant(Data.Str("bar"))),
                  ObjectProject(Free('tmp0), Constant(Data.Str("baz"))))))))
    }

    "compile date field extraction" in {
      testLogicalPlanCompile(
        "select date_part('day', baz) from foo",
        Squash(
          makeObj(
            "0" ->
              Extract(
                Constant(Data.Str("day")),
                ObjectProject(read("foo"), Constant(Data.Str("baz")))))))
    }

    "compile conditional" in {
      testLogicalPlanCompile(
        "select case when pop < 10000 then city else loc end from zips",
        Let('tmp0, read("zips"),
          Squash(makeObj(
            "0" ->
              Cond(
                Lt(
                  ObjectProject(Free('tmp0), Constant(Data.Str("pop"))),
                  Constant(Data.Int(10000))),
                ObjectProject(Free('tmp0), Constant(Data.Str("city"))),
                ObjectProject(Free('tmp0), Constant(Data.Str("loc"))))))))
    }

    "compile conditional (match) without else" in {
      testLogicalPlanCompile(
                   "select case when pop = 0 then 'nobody' end from zips",
        compileExp("select case when pop = 0 then 'nobody' else null end from zips"))
    }

    "compile conditional (switch) without else" in {
      testLogicalPlanCompile(
                   "select case pop when 0 then 'nobody' end from zips",
        compileExp("select case pop when 0 then 'nobody' else null end from zips"))
    }

    "compile array length" in {
      testLogicalPlanCompile(
        "select array_length(bar, 1) from foo",
        Squash(
          makeObj(
            "0" ->
              ArrayLength(
                ObjectProject(read("foo"), Constant(Data.Str("bar"))),
                Constant(Data.Int(1))))))
    }

    "compile concat" in {
      testLogicalPlanCompile(
        "select concat(foo, concat(' ', bar)) from baz",
        Let('tmp0, read("baz"),
          Squash(
            makeObj(
              "0" ->
                Concat(
                  ObjectProject(Free('tmp0), Constant(Data.Str("foo"))),
                  Concat(
                    Constant(Data.Str(" ")),
                    ObjectProject(Free('tmp0), Constant(Data.Str("bar")))))))))
    }

    "compile between" in {
      testLogicalPlanCompile(
        "select * from foo where bar between 1 and 10",
        Let('tmp0, read("foo"),
          Squash(
            Filter(
              Free('tmp0),
              Between(
                ObjectProject(Free('tmp0), Constant(Data.Str("bar"))),
                Constant(Data.Int(1)),
                Constant(Data.Int(10)))))))
    }

    "compile not between" in {
      testLogicalPlanCompile(
        "select * from foo where bar not between 1 and 10",
        Let('tmp0, read("foo"),
          Squash(
            Filter(
              Free('tmp0),
              Not(
                Between(
                  ObjectProject(Free('tmp0), Constant(Data.Str("bar"))),
                  Constant(Data.Int(1)),
                  Constant(Data.Int(10))))))))
    }

    "compile like" in {
      testLogicalPlanCompile(
        "select bar from foo where bar like 'a%'",
        Let('tmp0, read("foo"),
          Squash(
            makeObj(
              "bar" ->
                ObjectProject(
                  Filter(
                    Free('tmp0),
                    Search(
                      ObjectProject(Free('tmp0), Constant(Data.Str("bar"))),
                      Constant(Data.Str("^a.*$")),
                      Constant(Data.Bool(false)))),
                  Constant(Data.Str("bar")))))))
    }

    "compile like with escape char" in {
      testLogicalPlanCompile(
        "select bar from foo where bar like 'a=%' escape '='",
        Let('tmp0, read("foo"),
          Squash(
            makeObj(
              "bar" ->
                ObjectProject(
                  Filter(
                    Free('tmp0),
                    Search(
                      ObjectProject(Free('tmp0), Constant(Data.Str("bar"))),
                      Constant(Data.Str("^a%$")),
                      Constant(Data.Bool(false)))),
                  Constant(Data.Str("bar")))))))
    }

    "compile not like" in {
      testLogicalPlanCompile(
        "select bar from foo where bar not like 'a%'",
        Let('tmp0, read("foo"),
          Squash(makeObj("bar" -> ObjectProject(Filter(
            Free('tmp0),
            Not(
              Search(
                ObjectProject(Free('tmp0), Constant(Data.Str("bar"))),
                Constant(Data.Str("^a.*$")),
                Constant(Data.Bool(false))))),
            Constant(Data.Str("bar")))))))
    }

    "compile ~" in {
      testLogicalPlanCompile(
        "select bar from foo where bar ~ 'a.$'",
        Let('tmp0, read("foo"),
          Squash(
            makeObj(
              "bar" ->
                ObjectProject(
                  Filter(
                    Free('tmp0),
                    Search(
                      ObjectProject(Free('tmp0), Constant(Data.Str("bar"))),
                      Constant(Data.Str("a.$")),
                      Constant(Data.Bool(false)))),
                  Constant(Data.Str("bar")))))))
    }

    "compile complex expression" in {
      testLogicalPlanCompile(
        "select avgTemp*9/5 + 32 from cities",
        Squash(
          makeObj(
            "0" ->
              Add(
                Divide(
                  Multiply(
                    ObjectProject(read("cities"), Constant(Data.Str("avgTemp"))),
                    Constant(Data.Int(9))),
                  Constant(Data.Int(5))),
                Constant(Data.Int(32))))))
    }

    "compile parenthesized expression" in {
      testLogicalPlanCompile(
        "select (avgTemp + 32)/5 from cities",
        Squash(
          makeObj(
            "0" ->
              Divide(
                Add(
                  ObjectProject(read("cities"), Constant(Data.Str("avgTemp"))),
                  Constant(Data.Int(32))),
                Constant(Data.Int(5))))))
    }

    "compile cross select *" in {
      testLogicalPlanCompile(
        "select * from person, car",
        Let('tmp0,
          InnerJoin(read("person"), read("car"), Constant(Data.Bool(true))),
          Squash(
            ObjectConcat(
              ObjectProject(Free('tmp0), Constant(Data.Str("left"))),
              ObjectProject(Free('tmp0), Constant(Data.Str("right")))))))
    }

    "compile two term multiplication from two tables" in {
      testLogicalPlanCompile(
        "select person.age * car.modelYear from person, car",
        Let('tmp0,
          InnerJoin(read("person"), read("car"), Constant(Data.Bool(true))),
          Squash(
            makeObj(
              "0" ->
                Multiply(
                  ObjectProject(
                    ObjectProject(Free('tmp0), Constant(Data.Str("left"))),
                    Constant(Data.Str("age"))),
                  ObjectProject(
                    ObjectProject(Free('tmp0), Constant(Data.Str("right"))),
                    Constant(Data.Str("modelYear"))))))))
    }

    "compile simple where (with just a constant)" in {
      testLogicalPlanCompile(
        "select name from person where 1",
        Squash(
          makeObj(
            "name" ->
              ObjectProject(
                Filter(read("person"), Constant(Data.Int(1))),
                Constant(Data.Str("name"))))))
    }

    "compile simple where" in {
      testLogicalPlanCompile(
        "select name from person where age > 18",
        Let('tmp0, read("person"),
          Squash(
            makeObj(
              "name" ->
                ObjectProject(
                  Filter(
                    Free('tmp0),
                    Gt(
                      ObjectProject(Free('tmp0), Constant(Data.Str("age"))),
                      Constant(Data.Int(18)))),
                  Constant(Data.Str("name")))))))
    }

    "compile simple group by" in {
      testLogicalPlanCompile(
        "select count(*) from person group by name",
        Let('tmp0, read("person"),
          Squash(
            makeObj(
              "0" ->
                Count(
                  GroupBy(
                    Free('tmp0),
                    MakeArrayN(ObjectProject(
                      Free('tmp0),
                      Constant(Data.Str("name"))))))))))
    }

    "compile group by with projected keys" in {
      testLogicalPlanCompile(
        "select lower(name), person.gender, avg(age) from person group by lower(person.name), gender",
        Let('tmp0, read("person"),
          Let('tmp1,
            GroupBy(
              Free('tmp0),
              MakeArrayN(
                Lower(
                  ObjectProject(
                    Free('tmp0),
                    Constant(Data.Str("name")))),
                ObjectProject(
                  Free('tmp0),
                  Constant(Data.Str("gender"))))),
            Squash(
              makeObj(
                "0" ->
                  Arbitrary(
                    Lower(
                      ObjectProject(Free('tmp1), Constant(Data.Str("name"))))),
                "gender" ->
                  Arbitrary(
                    ObjectProject(Free('tmp1), Constant(Data.Str("gender")))),
                "2" ->
                  Avg(
                    ObjectProject(Free('tmp1), Constant(Data.Str("age")))))))))
    }

    "compile group by with perverse aggregated expression" in {
      testLogicalPlanCompile(
        "select count(name) from person group by name",
        Let('tmp0, read("person"),
          Squash(
            makeObj(
              "0" ->
                Count(
                  ObjectProject(
                    GroupBy(
                      Free('tmp0),
                      MakeArrayN(ObjectProject(
                        Free('tmp0),
                        Constant(Data.Str("name"))))),
                    Constant(Data.Str("name"))))))))
    }

    "compile sum in expression" in {
      testLogicalPlanCompile(
        "select sum(pop) * 100 from zips",
        Squash(
          makeObj(
            "0" ->
              Multiply(
                Sum(ObjectProject(read("zips"), Constant(Data.Str("pop")))),
                Constant(Data.Int(100))))))
    }

    "expand top-level object flatten" in {
      testLogicalPlanCompile(
                   "SELECT foo{*} FROM foo",
        compileExp("SELECT Flatten_Object(foo) AS \"0\" FROM foo"))
    }

    "expand nested object flatten" in {
      testLogicalPlanCompile(
                   "SELECT foo.bar{*} FROM foo",
        compileExp("SELECT Flatten_Object(foo.bar) AS \"bar\" FROM foo"))
    }

    "expand field object flatten" in {
      testLogicalPlanCompile(
                   "SELECT bar{*} FROM foo",
        compileExp("SELECT Flatten_Object(foo.bar) AS \"bar\" FROM foo"))
    }

    "expand top-level array flatten" in {
      testLogicalPlanCompile(
                   "SELECT foo[*] FROM foo",
        compileExp("SELECT Flatten_Array(foo) AS \"0\" FROM foo"))
    }

    "expand nested array flatten" in {
      testLogicalPlanCompile(
                   "SELECT foo.bar[*] FROM foo",
        compileExp("SELECT Flatten_Array(foo.bar) AS \"bar\" FROM foo"))
    }

    "expand field array flatten" in {
      testLogicalPlanCompile(
                   "SELECT bar[*] FROM foo",
        compileExp("SELECT Flatten_Array(foo.bar) AS \"bar\" FROM foo"))
    }

    "compile top-level object flatten" in {
      testLogicalPlanCompile(
        "select zips{*} from zips",
        Squash(makeObj("0" -> FlattenObject(read("zips")))))
    }

    "compile array flatten" in {
      testLogicalPlanCompile(
        "select loc[*] from zips",
        Squash(
          makeObj(
            "loc" ->
              FlattenArray(ObjectProject(read("zips"), Constant(Data.Str("loc")))))))
    }

    "compile simple order by" in {
      testLogicalPlanCompile(
        "select name from person order by height",
        Let('tmp0, read("person"),
          Let('tmp1,
            Squash(
              makeObj(
                "name" -> ObjectProject(Free('tmp0), Constant(Data.Str("name"))),
                "__sd__0" -> ObjectProject(Free('tmp0), Constant(Data.Str("height"))))),
            DeleteField(
              OrderBy(
                Free('tmp1),
                MakeArrayN(
                  ObjectProject(Free('tmp1), Constant(Data.Str("__sd__0")))),
                MakeArrayN(
                  Constant(Data.Str("ASC")))),
              Constant(Data.Str("__sd__0"))))))
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
              Squash(
                makeObj(
                  "name"    -> ObjectProject(Free('tmp1), Constant(Data.Str("name"))),
                  "__sd__0" -> ObjectProject(Free('tmp1), Constant(Data.Str("height"))))),
              DeleteField(
                OrderBy(
                  Free('tmp2),
                  MakeArrayN(
                    ObjectProject(Free('tmp2), Constant(Data.Str("name"))),
                    ObjectProject(Free('tmp2), Constant(Data.Str("__sd__0")))),
                  MakeArrayN(
                    Constant(Data.Str("ASC")),
                    Constant(Data.Str("ASC")))),
                Constant(Data.Str("__sd__0")))))))
    }

    "compile simple order by with wildcard" in {
      testLogicalPlanCompile(
        "select * from person order by height",
        Let('tmp1, Squash(read("person")),
          OrderBy(
            Free('tmp1),
            MakeArrayN(
              ObjectProject(Free('tmp1), Constant(Data.Str("height")))),
            MakeArrayN(
              Constant(Data.Str("ASC"))))))
    }

    "compile simple order by with ascending and descending" in {
      testLogicalPlanCompile(
        "select * from person order by height desc, name",
        Let('tmp1, Squash(read("person")),
          OrderBy(
            Free('tmp1),
            MakeArrayN(
              ObjectProject(Free('tmp1), Constant(Data.Str("height"))),
              ObjectProject(Free('tmp1), Constant(Data.Str("name")))),
            MakeArrayN(
              Constant(Data.Str("DESC")),
              Constant(Data.Str("ASC"))))))
    }

    "compile simple order by with expression" in {
      testLogicalPlanCompile(
        "select * from person order by height*2.54",
        Let('tmp0, read("person"),
          Let('tmp1,
            Squash(
              ObjectConcat(
                Free('tmp0),
                makeObj(
                  "__sd__0" -> Multiply(
                    ObjectProject(Free('tmp0), Constant(Data.Str("height"))),
                    Constant(Data.Dec(2.54)))))),
            DeleteField(
              OrderBy(
                Free('tmp1),
                MakeArrayN(
                  ObjectProject(Free('tmp1), Constant(Data.Str("__sd__0")))),
                MakeArrayN(
                  Constant(Data.Str("ASC")))),
              Constant(Data.Str("__sd__0"))))))
    }

    "compile order by with alias" in {
      testLogicalPlanCompile(
        "select firstName as name from person order by name",
        Let('tmp1,
          Squash(
            makeObj(
              "name" -> ObjectProject(read("person"), Constant(Data.Str("firstName"))))),
          OrderBy(
            Free('tmp1),
            MakeArrayN(ObjectProject(Free('tmp1), Constant(Data.Str("name")))),
            MakeArrayN(Constant(Data.Str("ASC"))))))
    }

    "compile simple order by with expression in synthetic field" in {
      testLogicalPlanCompile(
        "select name from person order by height*2.54",
        Let('tmp0, read("person"),
          Let('tmp1,
            Squash(
              makeObj(
                "name" -> ObjectProject(Free('tmp0), Constant(Data.Str("name"))),
                "__sd__0" ->
                  Multiply(
                    ObjectProject(Free('tmp0), Constant(Data.Str("height"))),
                    Constant(Data.Dec(2.54))))),
            DeleteField(
              OrderBy(
                Free('tmp1),
                MakeArrayN(
                  ObjectProject(Free('tmp1), Constant(Data.Str("__sd__0")))),
                MakeArrayN(
                  Constant(Data.Str("ASC")))),
              Constant(Data.Str("__sd__0"))))))
    }

    "compile order by with nested projection" in {
      testLogicalPlanCompile(
        "select bar from foo order by foo.bar.baz.quux/3",
        Let('tmp0, read("foo"),
          Let('tmp1,
            Squash(
              makeObj(
                "bar" -> ObjectProject(Free('tmp0), Constant(Data.Str("bar"))),
                "__sd__0" -> Divide(
                  ObjectProject(
                    ObjectProject(
                      ObjectProject(Free('tmp0),
                        Constant(Data.Str("bar"))),
                      Constant(Data.Str("baz"))),
                    Constant(Data.Str("quux"))),
                  Constant(Data.Int(3))))),
            DeleteField(
              OrderBy(
                Free('tmp1),
                MakeArrayN(
                  ObjectProject(Free('tmp1), Constant(Data.Str("__sd__0")))),
                MakeArrayN(
                  Constant(Data.Str("ASC")))),
              Constant(Data.Str("__sd__0"))))))
    }

    "compile order by with root projection a table ref" in {
      // Note: not using wildcard here because the simple case is optimized
      //       differently
      testLogicalPlanCompile(
                   "select foo from bar order by bar.baz",
        compileExp("select foo from bar order by baz"))
    }

    "compile order by with root projection a table ref with alias" in {
      // Note: not using wildcard here because the simple case is optimized
      //       differently
      testLogicalPlanCompile(
                   "select foo from bar b order by b.baz",
        compileExp("select foo from bar b order by baz"))
    }

    "compile order by with root projection a table ref with alias, mismatched" in {
      testLogicalPlanCompile(
                   "select * from bar b order by bar.baz",
        compileExp("select * from bar b order by b.bar.baz"))
    }

    "compile order by with root projection a table ref, embedded in expr" in {
      testLogicalPlanCompile(
                   "select * from bar order by bar.baz/10",
        compileExp("select * from bar order by baz/10"))
    }

    "compile order by with root projection a table ref, embedded in complex expr" in {
      testLogicalPlanCompile(
                   "select * from bar order by bar.baz/10 - 3*bar.quux",
        compileExp("select * from bar order by baz/10 - 3*quux"))
    }

    "compile multiple stages" in {
      testLogicalPlanCompile(
        "select height*2.54 as cm" +
          " from person" +
          " where height > 60" +
          " group by gender, height" +
          " having count(*) > 10" +
          " order by cm" +
          " offset 10" +
          " limit 5",
        Take(
          Drop(
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
                  Let('tmp4,
                    Squash(    // select height*2.54 as cm
                      makeObj(
                        "cm" ->
                          Multiply(
                            Arbitrary(
                              ObjectProject(
                                Filter(  // having count(*) > 10
                                  Free('tmp2),
                                  Gt(Count(Free('tmp2)), Constant(Data.Int(10)))),
                                Constant(Data.Str("height")))),
                            Constant(Data.Dec(2.54))))),
                    OrderBy(  // order by cm
                      Free('tmp4),
                      MakeArrayN(
                        ObjectProject(Free('tmp4), Constant(Data.Str("cm")))),
                      MakeArrayN(
                        Constant(Data.Str("ASC")))))))),
            Constant(Data.Int(10))), // offset 10
          Constant(Data.Int(5))))    // limit 5
    }

    "compile simple sum" in {
      testLogicalPlanCompile(
        "select sum(height) from person",
        Squash(
          makeObj(
            "0" ->
              Sum(ObjectProject(read("person"), Constant(Data.Str("height")))))))
    }

    "compile simple inner equi-join" in {
      testLogicalPlanCompile(
        "select foo.name, bar.address from foo join bar on foo.id = bar.foo_id",
        Let('tmp0,
          Let('left1, read("foo"),
            Let('right2, read("bar"),
              InnerJoin(Free('left1), Free('right2),
                relations.Eq(
                  ObjectProject(Free('left1), Constant(Data.Str("id"))),
                  ObjectProject(Free('right2), Constant(Data.Str("foo_id"))))))),
          Squash(
            makeObj(
              "name" ->
                ObjectProject(
                  ObjectProject(Free('tmp0), Constant(Data.Str("left"))),
                  Constant(Data.Str("name"))),
              "address" ->
                ObjectProject(
                  ObjectProject(Free('tmp0), Constant(Data.Str("right"))),
                  Constant(Data.Str("address")))))))
    }

    "compile simple left ineq-join" in {
      testLogicalPlanCompile(
        "select foo.name, bar.address " +
          "from foo left join bar on foo.id < bar.foo_id",
        Let('tmp0,
          Let('left1, read("foo"),
            Let('right2, read("bar"),
              LeftOuterJoin(Free('left1), Free('right2),
                relations.Lt(
                  ObjectProject(Free('left1), Constant(Data.Str("id"))),
                  ObjectProject(Free('right2), Constant(Data.Str("foo_id"))))))),
          Squash(
            makeObj(
              "name" ->
                ObjectProject(
                  ObjectProject(Free('tmp0), Constant(Data.Str("left"))),
                  Constant(Data.Str("name"))),
              "address" ->
                ObjectProject(
                  ObjectProject(Free('tmp0), Constant(Data.Str("right"))),
                  Constant(Data.Str("address")))))))
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
                InnerJoin(Free('left3), Free('right4),
                  relations.Eq(
                    ObjectProject(
                      Free('left3),
                      Constant(Data.Str("id"))),
                    ObjectProject(
                      Free('right4),
                      Constant(Data.Str("foo_id"))))))),
            Let('right2, read("baz"),
              InnerJoin(Free('left1), Free('right2),
                relations.Eq(
                  ObjectProject(Free('right2),
                    Constant(Data.Str("bar_id"))),
                  ObjectProject(
                    ObjectProject(Free('left1),
                      Constant(Data.Str("right"))),
                    Constant(Data.Str("id"))))))),
          Squash(
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
                  Constant(Data.Str("address")))))))
    }

    "compile sub-select in filter" in {
      testLogicalPlanCompile(
        "select city, pop from zips where pop > (select avg(pop) from zips)",
        read("zips"))
    }.pendingUntilFixed

    "compile simple sub-select" in {
      testLogicalPlanCompile(
        "select temp.name, temp.size from (select zips.city as name, zips.pop as size from zips) temp",
        Let('tmp0,
          Let('tmp1,
            read("zips"),
            Squash(
              makeObj(
                "name" -> ObjectProject(Free('tmp1), Constant(Data.Str("city"))),
                "size" -> ObjectProject(Free('tmp1), Constant(Data.Str("pop")))))),
          Squash(
            makeObj(
              "name" -> ObjectProject(Free('tmp0), Constant(Data.Str("name"))),
              "size" -> ObjectProject(Free('tmp0), Constant(Data.Str("size")))))))
    }

    "compile simple distinct" in {
      testLogicalPlanCompile(
        "select distinct city from zips",
        Distinct(
          Squash(
            makeObj(
              "city" -> ObjectProject(read("zips"), Constant(Data.Str("city")))))))
    }

    "compile simple distinct ordered" in {
      testLogicalPlanCompile(
        "select distinct city from zips order by city",
        Let('tmp1,
          Squash(
            makeObj(
              "city" ->
                ObjectProject(read("zips"), Constant(Data.Str("city"))))),
          Distinct(
            OrderBy(
              Free('tmp1),
              MakeArrayN(
                ObjectProject(Free('tmp1), Constant(Data.Str("city")))),
              MakeArrayN(
                Constant(Data.Str("ASC")))))))
    }

    "compile distinct with unrelated order by" in {
      testLogicalPlanCompile(
        "select distinct city from zips order by pop desc",
        Let('tmp0,
          read("zips"),
          Let('tmp1,
            Squash(
              makeObj(
                "city" -> ObjectProject(Free('tmp0), Constant(Data.Str("city"))),
                "__sd__0" -> ObjectProject(Free('tmp0), Constant(Data.Str("pop"))))),
            Let('tmp2,
              OrderBy(
                Free('tmp1),
                MakeArrayN(
                  ObjectProject(Free('tmp1), Constant(Data.Str("__sd__0")))),
                MakeArrayN(
                  Constant(Data.Str("DESC")))),
              DeleteField(
                DistinctBy(Free('tmp2),
                  DeleteField(Free('tmp2), Constant(Data.Str("__sd__0")))),
                Constant(Data.Str("__sd__0")))))))
    }

    "compile count(distinct(...))" in {
      testLogicalPlanCompile(
        "select count(distinct(lower(city))) from zips",
        Squash(
          makeObj(
            "0" -> Count(Distinct(Lower(ObjectProject(read("zips"), Constant(Data.Str("city")))))))))
    }

    "compile simple distinct with two named projections" in {
      testLogicalPlanCompile(
        "select distinct city as CTY, state as ST from zips",
        Let('tmp0, read("zips"),
          Distinct(
            Squash(
              makeObj(
                "CTY" -> ObjectProject(Free('tmp0), Constant(Data.Str("city"))),
                "ST" -> ObjectProject(Free('tmp0), Constant(Data.Str("state"))))))))
    }

    "compile count distinct with two exprs" in {
      testLogicalPlanCompile(
        "select count(distinct city, state) from zips",
        read("zips"))
    }.pendingUntilFixed

    "compile distinct as function" in {
      testLogicalPlanCompile(
        "select distinct(city, state) from zips",
        read("zips"))
    }.pendingUntilFixed

    "fail with ambiguous reference" in {
      compile("select foo from bar, baz") must beLeftDisjunction
    }

    "fail with ambiguous reference in cond" in {
      compile("select (case when a = 1 then 'ok' else 'reject' end) from bar, baz") must beLeftDisjunction
    }

    "fail with ambiguous reference in else" in {
      compile("select (case when bar.a = 1 then 'ok' else foo end) from bar, baz") must beLeftDisjunction
    }

    "translate free variable" in {
      testLogicalPlanCompile("select name from zips where age < :age",
        Let('tmp0, read("zips"),
          Squash(
            makeObj(
              "name" ->
                ObjectProject(
                  Filter(
                    Free('tmp0),
                    Lt(
                      ObjectProject(Free('tmp0), Constant(Data.Str("age"))),
                      Free('age))),
                  Constant(Data.Str("name")))))))
    }
  }

  "reduceGroupKeys" should {
    import Compiler.reduceGroupKeys

    "insert ARBITRARY" in {
      val lp =
        Let('tmp0, read("zips"),
          Let('tmp1,
            GroupBy(
              Free('tmp0),
              MakeArrayN(ObjectProject(Free('tmp0), Constant(Data.Str("city"))))),
            ObjectProject(Free('tmp1), Constant(Data.Str("city")))))
      val exp =
        Let('tmp0, read("zips"),
          Arbitrary(
            ObjectProject(
              GroupBy(
                Free('tmp0),
                MakeArrayN(ObjectProject(Free('tmp0), Constant(Data.Str("city"))))), Constant(Data.Str("city")))))

      Compiler.reduceGroupKeys(lp) must equalToPlan(exp)
    }

    "insert ARBITRARY with intervening filter" in {
      val lp =
        Let('tmp0, read("zips"),
          Let('tmp1,
            GroupBy(
              Free('tmp0),
              MakeArrayN(ObjectProject(Free('tmp0), Constant(Data.Str("city"))))),
            Let('tmp2,
              Filter(Free('tmp1), Gt(Count(Free('tmp1)), Constant(Data.Int(10)))),
              ObjectProject(Free('tmp2), Constant(Data.Str("city"))))))
      val exp =
        Let('tmp0, read("zips"),
          Let('tmp1,
            GroupBy(
              Free('tmp0),
              MakeArrayN(ObjectProject(Free('tmp0), Constant(Data.Str("city"))))),
            Arbitrary(
              ObjectProject(
                Filter(
                  Free('tmp1),
                  Gt(Count(Free('tmp1)), Constant(Data.Int(10)))),
                Constant(Data.Str("city"))))))

      Compiler.reduceGroupKeys(lp) must equalToPlan(exp)
    }

    "not insert redundant Reduction" in {
      val lp =
        Let('tmp0, read("zips"),
          Count(
            ObjectProject(
              GroupBy(
                Free('tmp0),
                MakeArrayN(ObjectProject(Free('tmp0),
                  Constant(Data.Str("city"))))), Constant(Data.Str("city")))))

      Compiler.reduceGroupKeys(lp) must equalToPlan(lp)
    }

    "insert ARBITRARY with multiple keys and mixed projections" in {
      val lp =
        Let('tmp0,
          read("zips"),
          Let('tmp1,
            GroupBy(
              Free('tmp0),
              MakeArrayN(
                ObjectProject(Free('tmp0), Constant(Data.Str("city"))),
                ObjectProject(Free('tmp0), Constant(Data.Str("state"))))),
            makeObj(
              "city" -> ObjectProject(Free('tmp1), Constant(Data.Str("city"))),
              "1"    -> Count(ObjectProject(Free('tmp1), Constant(Data.Str("state")))),
              "loc"  -> ObjectProject(Free('tmp1), Constant(Data.Str("loc"))),
              "2"    -> Sum(ObjectProject(Free('tmp1), Constant(Data.Str("pop")))))))
      val exp =
        Let('tmp0,
          read("zips"),
          Let('tmp1,
            GroupBy(
              Free('tmp0),
              MakeArrayN(
                ObjectProject(Free('tmp0), Constant(Data.Str("city"))),
                ObjectProject(Free('tmp0), Constant(Data.Str("state"))))),
            makeObj(
              "city" -> Arbitrary(ObjectProject(Free('tmp1), Constant(Data.Str("city")))),
              "1"    -> Count(ObjectProject(Free('tmp1), Constant(Data.Str("state")))),
              "loc"  -> ObjectProject(Free('tmp1), Constant(Data.Str("loc"))),
              "2"    -> Sum(ObjectProject(Free('tmp1), Constant(Data.Str("pop")))))))

      Compiler.reduceGroupKeys(lp) must equalToPlan(exp)
    }
  }
}
