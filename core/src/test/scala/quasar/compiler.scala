package quasar

import quasar.Predef._
import quasar.recursionschemes.Fix
import quasar.sql.SQLParser
import quasar.std._
import quasar.specs2.PendingWithAccurateCoverage

import org.specs2.mutable._
import org.specs2.matcher.{Matcher, Expectable}
import org.specs2.scalaz._


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
              Substring[FLP](
                ObjectProject(read("foo"), Constant(Data.Str("bar"))),
                Constant(Data.Int(2)),
                Constant(Data.Int(3))))))
    }

    "compile select length" in {
      testLogicalPlanCompile(
        "select length(bar) from foo",
        Squash(
          makeObj(
            "0" -> Length[FLP](ObjectProject(read("foo"), Constant(Data.Str("bar")))))))
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
        Let('__tmp0,
          InnerJoin(read("foo"), read("bar"), Constant(Data.Bool(true))),
          Squash[FLP](
            ObjectConcat[FLP](
              ObjectProject(Free('__tmp0), Constant(Data.Str("left"))),
              makeObj(
                "address" ->
                  ObjectProject[FLP](
                    ObjectProject(Free('__tmp0), Constant(Data.Str("right"))),
                    Constant(Data.Str("address"))))))))
    }

    "compile deeply-nested qualified select *" in {
      testLogicalPlanCompile(
        "select foo.bar.baz.*, bar.address from foo, bar",
        Let('__tmp0,
          InnerJoin(read("foo"), read("bar"), Constant(Data.Bool(true))),
          Squash[FLP](
            ObjectConcat[FLP](
              ObjectProject[FLP](
                ObjectProject[FLP](
                  ObjectProject(Free('__tmp0), Constant(Data.Str("left"))),
                  Constant(Data.Str("bar"))),
                Constant(Data.Str("baz"))),
              makeObj(
                "address" ->
                  ObjectProject[FLP](
                    ObjectProject(Free('__tmp0), Constant(Data.Str("right"))),
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
              ObjectProject[FLP](
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
        Let('__tmp0, read("baz"),
          Squash(
            makeObj(
              "0" ->
                Add[FLP](
                  ObjectProject(Free('__tmp0), Constant(Data.Str("foo"))),
                  ObjectProject(Free('__tmp0), Constant(Data.Str("bar"))))))))
    }

    "compile negate" in {
      testLogicalPlanCompile(
        "select -foo from bar",
        Squash(
          makeObj(
            "0" ->
              Negate[FLP](ObjectProject(read("bar"), Constant(Data.Str("foo")))))))
    }

    "compile modulo" in {
      testLogicalPlanCompile(
        "select foo % baz from bar",
        Let('__tmp0, read("bar"),
          Squash(
            makeObj(
              "0" ->
                Modulo[FLP](
                  ObjectProject(Free('__tmp0), Constant(Data.Str("foo"))),
                  ObjectProject(Free('__tmp0), Constant(Data.Str("baz"))))))))
    }

    "compile coalesce" in {
      testLogicalPlanCompile(
        "select coalesce(bar, baz) from foo",
        Let('__tmp0, read("foo"),
          Squash(
            makeObj(
              "0" ->
                Coalesce[FLP](
                  ObjectProject(Free('__tmp0), Constant(Data.Str("bar"))),
                  ObjectProject(Free('__tmp0), Constant(Data.Str("baz"))))))))
    }

    "compile date field extraction" in {
      testLogicalPlanCompile(
        "select date_part('day', baz) from foo",
        Squash(
          makeObj(
            "0" ->
              Extract[FLP](
                Constant(Data.Str("day")),
                ObjectProject(read("foo"), Constant(Data.Str("baz")))))))
    }

    "compile conditional" in {
      testLogicalPlanCompile(
        "select case when pop < 10000 then city else loc end from zips",
        Let('__tmp0, read("zips"),
          Squash(makeObj(
            "0" ->
              Cond[FLP](
                Lt[FLP](
                  ObjectProject(Free('__tmp0), Constant(Data.Str("pop"))),
                  Constant(Data.Int(10000))),
                ObjectProject(Free('__tmp0), Constant(Data.Str("city"))),
                ObjectProject(Free('__tmp0), Constant(Data.Str("loc"))))))))
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

    "have ~~ as alias for LIKE" in {
      testLogicalPlanCompile(
                   "select pop from zips where city ~~ '%BOU%'",
        compileExp("select pop from zips where city LIKE '%BOU%'"))
    }

    "have !~~ as alias for NOT LIKE" in {
      testLogicalPlanCompile(
                   "select pop from zips where city !~~ '%BOU%'",
        compileExp("select pop from zips where city NOT LIKE '%BOU%'"))
    }

    "compile array length" in {
      testLogicalPlanCompile(
        "select array_length(bar, 1) from foo",
        Squash(
          makeObj(
            "0" ->
              ArrayLength[FLP](
                ObjectProject(read("foo"), Constant(Data.Str("bar"))),
                Constant(Data.Int(1))))))
    }

    "compile concat" in {
      testLogicalPlanCompile(
        "select concat(foo, concat(' ', bar)) from baz",
        Let('__tmp0, read("baz"),
          Squash(
            makeObj(
              "0" ->
                Concat[FLP](
                  ObjectProject(Free('__tmp0), Constant(Data.Str("foo"))),
                  Concat[FLP](
                    Constant(Data.Str(" ")),
                    ObjectProject(Free('__tmp0), Constant(Data.Str("bar")))))))))
    }

    "compile between" in {
      testLogicalPlanCompile(
        "select * from foo where bar between 1 and 10",
        Let('__tmp0, read("foo"),
          Squash[FLP](
            Filter[FLP](
              Free('__tmp0),
              Between[FLP](
                ObjectProject(Free('__tmp0), Constant(Data.Str("bar"))),
                Constant(Data.Int(1)),
                Constant(Data.Int(10)))))))
    }

    "compile not between" in {
      testLogicalPlanCompile(
        "select * from foo where bar not between 1 and 10",
        Let('__tmp0, read("foo"),
          Squash[FLP](
            Filter[FLP](
              Free('__tmp0),
              Not[FLP](
                Between[FLP](
                  ObjectProject(Free('__tmp0), Constant(Data.Str("bar"))),
                  Constant(Data.Int(1)),
                  Constant(Data.Int(10))))))))
    }

    "compile like" in {
      testLogicalPlanCompile(
        "select bar from foo where bar like 'a%'",
        Let('__tmp0, read("foo"),
          Squash(
            makeObj(
              "bar" ->
                ObjectProject[FLP](
                  Filter[FLP](
                    Free('__tmp0),
                    Search[FLP](
                      ObjectProject(Free('__tmp0), Constant(Data.Str("bar"))),
                      Constant(Data.Str("^a.*$")),
                      Constant(Data.Bool(false)))),
                  Constant(Data.Str("bar")))))))
    }

    "compile like with escape char" in {
      testLogicalPlanCompile(
        "select bar from foo where bar like 'a=%' escape '='",
        Let('__tmp0, read("foo"),
          Squash(
            makeObj(
              "bar" ->
                ObjectProject[FLP](
                  Filter[FLP](
                    Free('__tmp0),
                    Search[FLP](
                      ObjectProject(Free('__tmp0), Constant(Data.Str("bar"))),
                      Constant(Data.Str("^a%$")),
                      Constant(Data.Bool(false)))),
                  Constant(Data.Str("bar")))))))
    }

    "compile not like" in {
      testLogicalPlanCompile(
        "select bar from foo where bar not like 'a%'",
        Let('__tmp0, read("foo"),
          Squash(makeObj("bar" -> ObjectProject[FLP](Filter[FLP](
            Free('__tmp0),
            Not[FLP](
              Search[FLP](
                ObjectProject(Free('__tmp0), Constant(Data.Str("bar"))),
                Constant(Data.Str("^a.*$")),
                Constant(Data.Bool(false))))),
            Constant(Data.Str("bar")))))))
    }

    "compile ~" in {
      testLogicalPlanCompile(
        "select bar from foo where bar ~ 'a.$'",
        Let('__tmp0, read("foo"),
          Squash(
            makeObj(
              "bar" ->
                ObjectProject[FLP](
                  Filter[FLP](
                    Free('__tmp0),
                    Search[FLP](
                      ObjectProject(Free('__tmp0), Constant(Data.Str("bar"))),
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
              Add[FLP](
                Divide[FLP](
                  Multiply[FLP](
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
              Divide[FLP](
                Add[FLP](
                  ObjectProject(read("cities"), Constant(Data.Str("avgTemp"))),
                  Constant(Data.Int(32))),
                Constant(Data.Int(5))))))
    }

    "compile cross select *" in {
      testLogicalPlanCompile(
        "select * from person, car",
        Let('__tmp0,
          InnerJoin(read("person"), read("car"), Constant(Data.Bool(true))),
          Squash[FLP](
            ObjectConcat[FLP](
              ObjectProject(Free('__tmp0), Constant(Data.Str("left"))),
              ObjectProject(Free('__tmp0), Constant(Data.Str("right")))))))
    }

    "compile two term multiplication from two tables" in {
      testLogicalPlanCompile(
        "select person.age * car.modelYear from person, car",
        Let('__tmp0,
          InnerJoin(read("person"), read("car"), Constant(Data.Bool(true))),
          Squash(
            makeObj(
              "0" ->
                Multiply[FLP](
                  ObjectProject[FLP](
                    ObjectProject(Free('__tmp0), Constant(Data.Str("left"))),
                    Constant(Data.Str("age"))),
                  ObjectProject[FLP](
                    ObjectProject(Free('__tmp0), Constant(Data.Str("right"))),
                    Constant(Data.Str("modelYear"))))))))
    }

    "compile simple where (with just a constant)" in {
      testLogicalPlanCompile(
        "select name from person where 1",
        Squash(
          makeObj(
            "name" ->
              ObjectProject[FLP](
                Filter(read("person"), Constant(Data.Int(1))),
                Constant(Data.Str("name"))))))
    }

    "compile simple where" in {
      testLogicalPlanCompile(
        "select name from person where age > 18",
        Let('__tmp0, read("person"),
          Squash(
            makeObj(
              "name" ->
                ObjectProject[FLP](
                  Filter[FLP](
                    Free('__tmp0),
                    Gt[FLP](
                      ObjectProject(Free('__tmp0), Constant(Data.Str("age"))),
                      Constant(Data.Int(18)))),
                  Constant(Data.Str("name")))))))
    }

    "compile simple group by" in {
      testLogicalPlanCompile(
        "select count(*) from person group by name",
        Let('__tmp0, read("person"),
          Squash(
            makeObj(
              "0" ->
                Count[FLP](
                  GroupBy[FLP](
                    Free('__tmp0),
                    MakeArrayN[Fix](ObjectProject(
                      Free('__tmp0),
                      Constant(Data.Str("name"))))))))))
    }

    "compile group by with projected keys" in {
      testLogicalPlanCompile(
        "select lower(name), person.gender, avg(age) from person group by lower(person.name), gender",
        Let('__tmp0, read("person"),
          Let('__tmp1,
            GroupBy[FLP](
              Free('__tmp0),
              MakeArrayN[Fix](
                Lower[FLP](
                  ObjectProject(
                    Free('__tmp0),
                    Constant(Data.Str("name")))),
                ObjectProject(
                  Free('__tmp0),
                  Constant(Data.Str("gender"))))),
            Squash(
              makeObj(
                "0" ->
                  Arbitrary[FLP](
                    Lower[FLP](
                      ObjectProject(Free('__tmp1), Constant(Data.Str("name"))))),
                "gender" ->
                  Arbitrary[FLP](
                    ObjectProject(Free('__tmp1), Constant(Data.Str("gender")))),
                "2" ->
                  Avg[FLP](
                    ObjectProject(Free('__tmp1), Constant(Data.Str("age")))))))))
    }

    "compile group by with perverse aggregated expression" in {
      testLogicalPlanCompile(
        "select count(name) from person group by name",
        Let('__tmp0, read("person"),
          Squash(
            makeObj(
              "0" ->
                Count[FLP](
                  ObjectProject[FLP](
                    GroupBy[FLP](
                      Free('__tmp0),
                      MakeArrayN[Fix](ObjectProject(
                        Free('__tmp0),
                        Constant(Data.Str("name"))))),
                    Constant(Data.Str("name"))))))))
    }

    "compile sum in expression" in {
      testLogicalPlanCompile(
        "select sum(pop) * 100 from zips",
        Squash(
          makeObj(
            "0" ->
              Multiply[FLP](
                Sum[FLP](ObjectProject(read("zips"), Constant(Data.Str("pop")))),
                Constant(Data.Int(100))))))
    }

    val setA =
      Let('__tmp0, read("zips"),
        Squash(makeObj(
          "loc" -> ObjectProject(Free('__tmp0), Constant(Data.Str("loc"))),
          "pop" -> ObjectProject(Free('__tmp0), Constant(Data.Str("pop"))))))
    val setB =
      Squash(makeObj(
        "city" -> ObjectProject(read("zips"), Constant(Data.Str("city")))))

    "compile union" in {
      testLogicalPlanCompile(
        "select loc, pop from zips union select city from zips",
        Distinct[FLP](Union[FLP](setA, setB)))
    }

    "compile union all" in {
      testLogicalPlanCompile(
        "select loc, pop from zips union all select city from zips",
        Union[FLP](setA, setB))
    }

    "compile intersect" in {
      testLogicalPlanCompile(
        "select loc, pop from zips intersect select city from zips",
        Distinct[FLP](Intersect[FLP](setA, setB)))
    }

    "compile intersect all" in {
      testLogicalPlanCompile(
        "select loc, pop from zips intersect all select city from zips",
        Intersect[FLP](setA, setB))
    }

    "compile except" in {
      testLogicalPlanCompile(
        "select loc, pop from zips except select city from zips",
        Except[FLP](setA, setB))
    }

    "have {*} as alias for {:*}" in {
      testLogicalPlanCompile(
                   "SELECT bar{*} FROM foo",
        compileExp("SELECT bar{:*} FROM foo"))
    }

    "have [*] as alias for [:*]" in {
      testLogicalPlanCompile(
                   "SELECT foo[*] FROM foo",
        compileExp("SELECT foo[:*] FROM foo"))
    }

    "expand top-level map flatten" in {
      testLogicalPlanCompile(
                   "SELECT foo{:*} FROM foo",
        compileExp("SELECT Flatten_Map(foo) AS \"0\" FROM foo"))
    }

    "expand nested map flatten" in {
      testLogicalPlanCompile(
                   "SELECT foo.bar{:*} FROM foo",
        compileExp("SELECT Flatten_Map(foo.bar) AS \"bar\" FROM foo"))
    }

    "expand field map flatten" in {
      testLogicalPlanCompile(
                   "SELECT bar{:*} FROM foo",
        compileExp("SELECT Flatten_Map(foo.bar) AS \"bar\" FROM foo"))
    }

    "expand top-level array flatten" in {
      testLogicalPlanCompile(
                   "SELECT foo[:*] FROM foo",
        compileExp("SELECT Flatten_Array(foo) AS \"0\" FROM foo"))
    }

    "expand nested array flatten" in {
      testLogicalPlanCompile(
        "SELECT foo.bar[:*] FROM foo",
        compileExp("SELECT Flatten_Array(foo.bar) AS \"bar\" FROM foo"))
    }

    "expand field array flatten" in {
      testLogicalPlanCompile(
                   "SELECT bar[:*] FROM foo",
        compileExp("SELECT Flatten_Array(foo.bar) AS \"bar\" FROM foo"))
    }

    "compile top-level map flatten" in {
      testLogicalPlanCompile(
        "select zips{:*} from zips",
        Squash(makeObj("0" -> FlattenMap(read("zips")))))
    }

    "have {_} as alias for {:_}" in {
      testLogicalPlanCompile(
                   "select length(commit.author{_}) from slamengine_commits",
        compileExp("select length(commit.author{:_}) from slamengine_commits"))
    }

    "have [_] as alias for [:_]" in {
      testLogicalPlanCompile(
                   "select loc[_] / 10 from zips",
        compileExp("select loc[:_] / 10 from zips"))
    }

    "compile map shift / unshift" in {
      testLogicalPlanCompile(
        "select {length(commit.author{:_})...} from slamengine_commits",
        Squash(makeObj("0" ->
          UnshiftMap[FLP](
            Length[FLP](
              ShiftMap[FLP](ObjectProject[FLP](ObjectProject(read("slamengine_commits"), Constant(Data.Str("commit"))), Constant(Data.Str("author")))))))))
    }

    "compile map shift / unshift keys" in {
      testLogicalPlanCompile(
        "select {length(commit.author{_:})...} from slamengine_commits",
        Squash(makeObj("0" -> UnshiftMap[FLP](Length[FLP](ShiftMapKeys[FLP](ObjectProject[FLP](ObjectProject(read("slamengine_commits"), Constant(Data.Str("commit"))), Constant(Data.Str("author")))))))))
    }

    "compile array shift / unshift" in {
      testLogicalPlanCompile(
        "select [loc[:_] / 10 ...] from zips",
        Squash(
          makeObj(
            "0" ->
              UnshiftArray[FLP](
                Divide[FLP](
                  ShiftArray[FLP](ObjectProject(read("zips"), Constant(Data.Str("loc")))),
                  Constant(Data.Int(10)))))))
    }

    "compile array shift / unshift indices" in {
      testLogicalPlanCompile(
        "select [loc[_:] * 10 ...] from zips",
        Squash(
          makeObj(
            "0" ->
              UnshiftArray[FLP](
                Multiply[FLP](
                  ShiftArrayIndices[FLP](ObjectProject(read("zips"), Constant(Data.Str("loc")))),
                  Constant(Data.Int(10)))))))
    }

    "compile array flatten" in {
      testLogicalPlanCompile(
        "select loc[:*] from zips",
        Squash(
          makeObj(
            "loc" ->
              FlattenArray[FLP](ObjectProject(read("zips"), Constant(Data.Str("loc")))))))
    }

    "compile simple order by" in {
      testLogicalPlanCompile(
        "select name from person order by height",
        Let('__tmp0, read("person"),
          Let('__tmp1,
            Squash(
              makeObj(
                "name" -> ObjectProject(Free('__tmp0), Constant(Data.Str("name"))),
                "__sd__0" -> ObjectProject(Free('__tmp0), Constant(Data.Str("height"))))),
            DeleteField[FLP](
              OrderBy[FLP](
                Free('__tmp1),
                MakeArrayN[Fix](
                  ObjectProject(Free('__tmp1), Constant(Data.Str("__sd__0")))),
                MakeArrayN(
                  Constant(Data.Str("ASC")))),
              Constant(Data.Str("__sd__0"))))))
    }

    "compile simple order by with filter" in {
      testLogicalPlanCompile(
        "select name from person where gender = 'male' order by name, height",
        Let('__tmp0, read("person"),
          Let('__tmp1,
            Filter[FLP](
              Free('__tmp0),
              Eq[FLP](
                ObjectProject(Free('__tmp0), Constant(Data.Str("gender"))),
                Constant(Data.Str("male")))),
            Let('__tmp2,
              Squash(
                makeObj(
                  "name"    -> ObjectProject(Free('__tmp1), Constant(Data.Str("name"))),
                  "__sd__0" -> ObjectProject(Free('__tmp1), Constant(Data.Str("height"))))),
              DeleteField[FLP](
                OrderBy[FLP](
                  Free('__tmp2),
                  MakeArrayN[Fix](
                    ObjectProject(Free('__tmp2), Constant(Data.Str("name"))),
                    ObjectProject(Free('__tmp2), Constant(Data.Str("__sd__0")))),
                  MakeArrayN(
                    Constant(Data.Str("ASC")),
                    Constant(Data.Str("ASC")))),
                Constant(Data.Str("__sd__0")))))))
    }

    "compile simple order by with wildcard" in {
      testLogicalPlanCompile(
        "select * from person order by height",
        Let('__tmp0, Squash(read("person")),
          OrderBy[FLP](
            Free('__tmp0),
            MakeArrayN[Fix](
              ObjectProject(Free('__tmp0), Constant(Data.Str("height")))),
            MakeArrayN(
              Constant(Data.Str("ASC"))))))
    }

    "compile simple order by with ascending and descending" in {
      testLogicalPlanCompile(
        "select * from person order by height desc, name",
        Let('__tmp0, Squash(read("person")),
          OrderBy[FLP](
            Free('__tmp0),
            MakeArrayN[Fix](
              ObjectProject(Free('__tmp0), Constant(Data.Str("height"))),
              ObjectProject(Free('__tmp0), Constant(Data.Str("name")))),
            MakeArrayN(
              Constant(Data.Str("DESC")),
              Constant(Data.Str("ASC"))))))
    }

    "compile simple order by with expression" in {
      testLogicalPlanCompile(
        "select * from person order by height*2.54",
        Let('__tmp0, read("person"),
          Let('__tmp1,
            Squash[FLP](
              ObjectConcat(
                Free('__tmp0),
                makeObj(
                  "__sd__0" -> Multiply[FLP](
                    ObjectProject(Free('__tmp0), Constant(Data.Str("height"))),
                    Constant(Data.Dec(2.54)))))),
            DeleteField[FLP](
              OrderBy[FLP](
                Free('__tmp1),
                MakeArrayN[Fix](
                  ObjectProject(Free('__tmp1), Constant(Data.Str("__sd__0")))),
                MakeArrayN(
                  Constant(Data.Str("ASC")))),
              Constant(Data.Str("__sd__0"))))))
    }

    "compile order by with alias" in {
      testLogicalPlanCompile(
        "select firstName as name from person order by name",
        Let('__tmp0,
          Squash(
            makeObj(
              "name" -> ObjectProject(read("person"), Constant(Data.Str("firstName"))))),
          OrderBy[FLP](
            Free('__tmp0),
            MakeArrayN[Fix](ObjectProject(Free('__tmp0), Constant(Data.Str("name")))),
            MakeArrayN(Constant(Data.Str("ASC"))))))
    }

    "compile simple order by with expression in synthetic field" in {
      testLogicalPlanCompile(
        "select name from person order by height*2.54",
        Let('__tmp0, read("person"),
          Let('__tmp1,
            Squash(
              makeObj(
                "name" -> ObjectProject(Free('__tmp0), Constant(Data.Str("name"))),
                "__sd__0" ->
                  Multiply[FLP](
                    ObjectProject(Free('__tmp0), Constant(Data.Str("height"))),
                    Constant(Data.Dec(2.54))))),
            DeleteField[FLP](
              OrderBy[FLP](
                Free('__tmp1),
                MakeArrayN[Fix](
                  ObjectProject(Free('__tmp1), Constant(Data.Str("__sd__0")))),
                MakeArrayN(
                  Constant(Data.Str("ASC")))),
              Constant(Data.Str("__sd__0"))))))
    }

    "compile order by with nested projection" in {
      testLogicalPlanCompile(
        "select bar from foo order by foo.bar.baz.quux/3",
        Let('__tmp0, read("foo"),
          Let('__tmp1,
            Squash(
              makeObj(
                "bar" -> ObjectProject(Free('__tmp0), Constant(Data.Str("bar"))),
                "__sd__0" -> Divide[FLP](
                  ObjectProject[FLP](
                    ObjectProject[FLP](
                      ObjectProject(Free('__tmp0),
                        Constant(Data.Str("bar"))),
                      Constant(Data.Str("baz"))),
                    Constant(Data.Str("quux"))),
                  Constant(Data.Int(3))))),
            DeleteField[FLP](
              OrderBy[FLP](
                Free('__tmp1),
                MakeArrayN[Fix](
                  ObjectProject(Free('__tmp1), Constant(Data.Str("__sd__0")))),
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
        Take[FLP](
          Drop(
            Let('__tmp0, read("person"), // from person
              Let('__tmp1,    // where height > 60
                Filter[FLP](
                  Free('__tmp0),
                  Gt[FLP](
                    ObjectProject(Free('__tmp0), Constant(Data.Str("height"))),
                    Constant(Data.Int(60)))),
                Let('__tmp2,    // group by gender, height
                  GroupBy[FLP](
                    Free('__tmp1),
                    MakeArrayN[Fix](
                      ObjectProject(Free('__tmp1), Constant(Data.Str("gender"))),
                      ObjectProject(Free('__tmp1), Constant(Data.Str("height"))))),
                  Let('__tmp3,
                    Squash(    // select height*2.54 as cm
                      makeObj(
                        "cm" ->
                          Multiply[FLP](
                            Arbitrary[FLP](
                              ObjectProject[FLP](
                                Filter[FLP](  // having count(*) > 10
                                  Free('__tmp2),
                                  Gt[FLP](Count(Free('__tmp2)), Constant(Data.Int(10)))),
                                Constant(Data.Str("height")))),
                            Constant(Data.Dec(2.54))))),
                    OrderBy[FLP](  // order by cm
                      Free('__tmp3),
                      MakeArrayN[Fix](
                        ObjectProject(Free('__tmp3), Constant(Data.Str("cm")))),
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
              Sum[FLP](ObjectProject(read("person"), Constant(Data.Str("height")))))))
    }

    "compile simple inner equi-join" in {
      testLogicalPlanCompile(
        "select foo.name, bar.address from foo join bar on foo.id = bar.foo_id",
        Let('__tmp0, read("foo"),
          Let('__tmp1, read("bar"),
            Let('__tmp2,
              InnerJoin[FLP](Free('__tmp0), Free('__tmp1),
                relations.Eq[FLP](
                  ObjectProject(Free('__tmp0), Constant(Data.Str("id"))),
                  ObjectProject(Free('__tmp1), Constant(Data.Str("foo_id"))))),
              Squash(
                makeObj(
                  "name" ->
                    ObjectProject[FLP](
                      ObjectProject(Free('__tmp2), Constant(Data.Str("left"))),
                      Constant(Data.Str("name"))),
                  "address" ->
                    ObjectProject[FLP](
                      ObjectProject(Free('__tmp2), Constant(Data.Str("right"))),
                      Constant(Data.Str("address")))))))))
    }

    "compile cross join to the equivalent inner equi-join" in {
      val query = "select foo.name, bar.address from foo, bar where foo.id = bar.foo_id"
      val equiv = "select foo.name, bar.address from foo join bar on foo.id = bar.foo_id"

      testLogicalPlanCompile(query, compileExp(equiv))
    }

    "compile inner join with additional equi-condition to the equivalent inner equi-join" in {
      val query = "select foo.name, bar.address from foo join bar on foo.id = bar.foo_id where foo.x = bar.y"
      val equiv = "select foo.name, bar.address from foo join bar on foo.id = bar.foo_id and foo.x = bar.y"

      testLogicalPlanCompile(query, compileExp(equiv))
    }.pendingUntilFixed("SD-1190")

    "compile inner non-equi join to the equivalent cross join" in {
      val query = "select foo.name, bar.address from foo join bar on foo.x < bar.y"
      val equiv = "select foo.name, bar.address from foo, bar where foo.x < bar.y"

      testLogicalPlanCompile(query, compileExp(equiv))
    }.pendingUntilFixed("SD-1190")

    "compile nested cross join to the equivalent inner equi-join" in {
      val query = "select a.x, b.y, c.z from a, b, c where a.id = b.a_id and b._id = c.b_id"
      val equiv = "select a.x, b.y, c.z from (a join b on a.id = b.a_id) join c on b._id = c.b_id"

      testLogicalPlanCompile(query, compileExp(equiv))
    }.pendingUntilFixed("SD-1190")

    "compile filtered join with one-sided conditions" in {
      val query = "select foo.name, bar.address from foo, bar where foo.id = bar.foo_id and foo.x < 10 and bar.y = 20"

      // NB: this query produces what we want but currently doesn't match due to spurious SQUASHes
      // val equiv = "select foo.name, bar.address from (select * from foo where x < 10) foo join (select * from bar where y = 20) bar on foo.id = bar.foo_id"

      testLogicalPlanCompile(query,
        Let('__tmp0, read("foo"),
          Let('__tmp1,
             Fix(Filter(
               Free('__tmp0),
               Fix(Lt(
                 ObjectProject(Free('__tmp0), Constant(Data.Str("x"))),
                 Constant(Data.Int(10)))))),
             Let('__tmp2, read("bar"),
               Let('__tmp3,
                 Fix(Filter(
                   Free('__tmp2),
                   Fix(Eq(
                     ObjectProject(Free('__tmp2), Constant(Data.Str("y"))),
                     Constant(Data.Int(20)))))),
                  Let('__tmp4,
                    Fix(InnerJoin(Free('__tmp1), Free('__tmp3),
                      Fix(Eq(
                        ObjectProject(Free('__tmp1), Constant(Data.Str("id"))),
                        ObjectProject(Free('__tmp3), Constant(Data.Str("foo_id"))))))),
                    Fix(Squash(
                       makeObj(
                         "name" -> ObjectProject[FLP](
                           ObjectProject(Free('__tmp4), Constant(Data.Str("left"))),
                           Constant(Data.Str("name"))),
                         "address" -> ObjectProject[FLP](
                           ObjectProject(Free('__tmp4), Constant(Data.Str("right"))),
                           Constant(Data.Str("address"))))))))))))
    }


    "compile simple left ineq-join" in {
      testLogicalPlanCompile(
        "select foo.name, bar.address " +
          "from foo left join bar on foo.id < bar.foo_id",
        Let('__tmp0, read("foo"),
          Let('__tmp1, read("bar"),
            Let('__tmp2,
              LeftOuterJoin[FLP](Free('__tmp0), Free('__tmp1),
                relations.Lt[FLP](
                  ObjectProject(Free('__tmp0), Constant(Data.Str("id"))),
                  ObjectProject(Free('__tmp1), Constant(Data.Str("foo_id"))))),
              Squash(
                makeObj(
                  "name" ->
                    ObjectProject[FLP](
                      ObjectProject(Free('__tmp2), Constant(Data.Str("left"))),
                      Constant(Data.Str("name"))),
                  "address" ->
                    ObjectProject[FLP](
                      ObjectProject(Free('__tmp2), Constant(Data.Str("right"))),
                      Constant(Data.Str("address")))))))))
    }

    "compile complex equi-join" in {
      testLogicalPlanCompile(
        "select foo.name, bar.address " +
          "from foo join bar on foo.id = bar.foo_id " +
          "join baz on baz.bar_id = bar.id",
        Let('__tmp0, read("foo"),
          Let('__tmp1, read("bar"),
            Let('__tmp2,
              InnerJoin[FLP](Free('__tmp0), Free('__tmp1),
                relations.Eq[FLP](
                  ObjectProject(
                    Free('__tmp0),
                    Constant(Data.Str("id"))),
                  ObjectProject(
                    Free('__tmp1),
                    Constant(Data.Str("foo_id"))))),
              Let('__tmp3, read("baz"),
                Let('__tmp4,
                  InnerJoin[FLP](Free('__tmp2), Free('__tmp3),
                    relations.Eq[FLP](
                      ObjectProject(Free('__tmp3),
                        Constant(Data.Str("bar_id"))),
                      ObjectProject[FLP](
                        ObjectProject(Free('__tmp2),
                          Constant(Data.Str("right"))),
                        Constant(Data.Str("id"))))),
                  Squash(
                    makeObj(
                      "name" ->
                        ObjectProject[FLP](
                          ObjectProject[FLP](
                            ObjectProject(Free('__tmp4), Constant(Data.Str("left"))),
                            Constant(Data.Str("left"))),
                          Constant(Data.Str("name"))),
                      "address" ->
                        ObjectProject[FLP](
                          ObjectProject[FLP](
                            ObjectProject(Free('__tmp4), Constant(Data.Str("left"))),
                            Constant(Data.Str("right"))),
                          Constant(Data.Str("address")))))))))))
    }

    "compile sub-select in filter" in {
      testLogicalPlanCompile(
        "select city, pop from zips where pop > (select avg(pop) from zips)",
        read("zips"))
    }.pendingUntilFixed

    "compile simple sub-select" in {
      testLogicalPlanCompile(
        "select temp.name, temp.size from (select zips.city as name, zips.pop as size from zips) temp",
        Let('__tmp0, read("zips"),
          Let('__tmp1,
            Squash(
              makeObj(
                "name" -> ObjectProject(Free('__tmp0), Constant(Data.Str("city"))),
                "size" -> ObjectProject(Free('__tmp0), Constant(Data.Str("pop"))))),
            Squash(
              makeObj(
                "name" -> ObjectProject(Free('__tmp1), Constant(Data.Str("name"))),
                "size" -> ObjectProject(Free('__tmp1), Constant(Data.Str("size"))))))))
    }

    "compile sub-select with same un-qualified names" in {
      testLogicalPlanCompile(
        "select city, pop from (select city, pop from zips) temp",
        Let('__tmp0, read("zips"),
          Let('__tmp1,
            Squash(
              makeObj(
                "city" -> ObjectProject(Free('__tmp0), Constant(Data.Str("city"))),
                "pop" -> ObjectProject(Free('__tmp0), Constant(Data.Str("pop"))))),
            Squash(
              makeObj(
                "city" -> ObjectProject(Free('__tmp1), Constant(Data.Str("city"))),
                "pop" -> ObjectProject(Free('__tmp1), Constant(Data.Str("pop"))))))))
    }

    "compile simple distinct" in {
      testLogicalPlanCompile(
        "select distinct city from zips",
        Distinct[FLP](
          Squash(
            makeObj(
              "city" -> ObjectProject(read("zips"), Constant(Data.Str("city")))))))
    }

    "compile simple distinct ordered" in {
      testLogicalPlanCompile(
        "select distinct city from zips order by city",
        Let('__tmp0,
          Squash(
            makeObj(
              "city" ->
                ObjectProject(read("zips"), Constant(Data.Str("city"))))),
          Distinct[FLP](
            OrderBy[FLP](
              Free('__tmp0),
              MakeArrayN[Fix](
                ObjectProject(Free('__tmp0), Constant(Data.Str("city")))),
              MakeArrayN(
                Constant(Data.Str("ASC")))))))
    }

    "compile distinct with unrelated order by" in {
      testLogicalPlanCompile(
        "select distinct city from zips order by pop desc",
        Let('__tmp0,
          read("zips"),
          Let('__tmp1,
            Squash(
              makeObj(
                "city" -> ObjectProject(Free('__tmp0), Constant(Data.Str("city"))),
                "__sd__0" -> ObjectProject(Free('__tmp0), Constant(Data.Str("pop"))))),
            Let('__tmp2,
              OrderBy[FLP](
                Free('__tmp1),
                MakeArrayN[Fix](
                  ObjectProject(Free('__tmp1), Constant(Data.Str("__sd__0")))),
                MakeArrayN(
                  Constant(Data.Str("DESC")))),
              DeleteField[FLP](
                DistinctBy[FLP](Free('__tmp2),
                  DeleteField(Free('__tmp2), Constant(Data.Str("__sd__0")))),
                Constant(Data.Str("__sd__0")))))))
    }

    "compile count(distinct(...))" in {
      testLogicalPlanCompile(
        "select count(distinct(lower(city))) from zips",
        Squash(
          makeObj(
            "0" -> Count[FLP](Distinct[FLP](Lower[FLP](ObjectProject(read("zips"), Constant(Data.Str("city")))))))))
    }

    "compile simple distinct with two named projections" in {
      testLogicalPlanCompile(
        "select distinct city as CTY, state as ST from zips",
        Let('__tmp0, read("zips"),
          Distinct[FLP](
            Squash(
              makeObj(
                "CTY" -> ObjectProject(Free('__tmp0), Constant(Data.Str("city"))),
                "ST" -> ObjectProject(Free('__tmp0), Constant(Data.Str("state"))))))))
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
        Let('__tmp0, read("zips"),
          Squash(
            makeObj(
              "name" ->
                ObjectProject[FLP](
                  Filter[FLP](
                    Free('__tmp0),
                    Lt[FLP](
                      ObjectProject(Free('__tmp0), Constant(Data.Str("age"))),
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
            GroupBy[FLP](
              Free('tmp0),
              MakeArrayN[Fix](ObjectProject(Free('tmp0), Constant(Data.Str("city"))))),
            ObjectProject(Free('tmp1), Constant(Data.Str("city")))))
      val exp =
        Let('tmp0, read("zips"),
          Arbitrary[FLP](
            ObjectProject[FLP](
              GroupBy[FLP](
                Free('tmp0),
                MakeArrayN[Fix](ObjectProject(Free('tmp0), Constant(Data.Str("city"))))), Constant(Data.Str("city")))))

      reduceGroupKeys(lp) must equalToPlan(exp)
    }

    "insert ARBITRARY with intervening filter" in {
      val lp =
        Let('tmp0, read("zips"),
          Let('tmp1,
            GroupBy[FLP](
              Free('tmp0),
              MakeArrayN[Fix](ObjectProject(Free('tmp0), Constant(Data.Str("city"))))),
            Let('tmp2,
              Filter[FLP](Free('tmp1), Gt[FLP](Count(Free('tmp1)), Constant(Data.Int(10)))),
              ObjectProject(Free('tmp2), Constant(Data.Str("city"))))))
      val exp =
        Let('tmp0, read("zips"),
          Let('tmp1,
            GroupBy[FLP](
              Free('tmp0),
              MakeArrayN[Fix](ObjectProject(Free('tmp0), Constant(Data.Str("city"))))),
            Arbitrary[FLP](
              ObjectProject[FLP](
                Filter[FLP](
                  Free('tmp1),
                  Gt[FLP](Count(Free('tmp1)), Constant(Data.Int(10)))),
                Constant(Data.Str("city"))))))

      reduceGroupKeys(lp) must equalToPlan(exp)
    }

    "not insert redundant Reduction" in {
      val lp =
        Let('tmp0, read("zips"),
          Count[FLP](
            ObjectProject[FLP](
              GroupBy[FLP](
                Free('tmp0),
                MakeArrayN[Fix](ObjectProject(Free('tmp0),
                  Constant(Data.Str("city"))))), Constant(Data.Str("city")))))

      reduceGroupKeys(lp) must equalToPlan(lp)
    }

    "insert ARBITRARY with multiple keys and mixed projections" in {
      val lp =
        Let('tmp0,
          read("zips"),
          Let('tmp1,
            GroupBy[FLP](
              Free('tmp0),
              MakeArrayN[Fix](
                ObjectProject(Free('tmp0), Constant(Data.Str("city"))),
                ObjectProject(Free('tmp0), Constant(Data.Str("state"))))),
            makeObj(
              "city" -> ObjectProject(Free('tmp1), Constant(Data.Str("city"))),
              "1"    -> Count[FLP](ObjectProject(Free('tmp1), Constant(Data.Str("state")))),
              "loc"  -> ObjectProject(Free('tmp1), Constant(Data.Str("loc"))),
              "2"    -> Sum[FLP](ObjectProject(Free('tmp1), Constant(Data.Str("pop")))))))
      val exp =
        Let('tmp0,
          read("zips"),
          Let('tmp1,
            GroupBy[FLP](
              Free('tmp0),
              MakeArrayN[Fix](
                ObjectProject(Free('tmp0), Constant(Data.Str("city"))),
                ObjectProject(Free('tmp0), Constant(Data.Str("state"))))),
            makeObj(
              "city" -> Arbitrary[FLP](ObjectProject(Free('tmp1), Constant(Data.Str("city")))),
              "1"    -> Count[FLP](ObjectProject(Free('tmp1), Constant(Data.Str("state")))),
              "loc"  -> ObjectProject(Free('tmp1), Constant(Data.Str("loc"))),
              "2"    -> Sum[FLP](ObjectProject(Free('tmp1), Constant(Data.Str("pop")))))))

      reduceGroupKeys(lp) must equalToPlan(exp)
    }
  }
}
