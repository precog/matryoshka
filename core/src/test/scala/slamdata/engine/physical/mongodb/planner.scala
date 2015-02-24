package slamdata.engine.physical.mongodb

import slamdata.engine._
import slamdata.engine.fp._
import slamdata.engine.fs.Path
import slamdata.engine.analysis.fixplate._
import slamdata.engine.analysis._
import slamdata.engine.sql.{SQLParser, Query}
import slamdata.engine.std._
import slamdata.engine.javascript._

import scalaz._
import Scalaz._

import collection.immutable.ListMap

import org.specs2.mutable._
import org.specs2.matcher.{Matcher, Expectable}
import org.specs2.ScalaCheck
import org.scalacheck._
import slamdata.specs2._

class PlannerSpec extends Specification with ScalaCheck with CompilerHelpers with DisjunctionMatchers with PendingWithAccurateCoverage {
  import StdLib._
  import structural._
  import math._
  import LogicalPlan._
  import SemanticAnalysis._
  import Reshape._
  import Workflow._
  import ExprOp._
  import IdHandling._
  import JsCore._

  case class equalToWorkflow(expected: Workflow)
      extends Matcher[Workflow] {
    def apply[S <: Workflow](s: Expectable[S]) = {
      def diff(l: S, r: Workflow): String = {
        val lt = RenderTree[Workflow].render(l)
        val rt = RenderTree[Workflow].render(r)
        RenderTree.show(lt diff rt)(new RenderTree[RenderedTree] {
          override def render(v: RenderedTree) = v
        }).toString
      }
      result(expected == s.value,
             "\ntrees are equal:\n" + diff(s.value, expected),
             "\ntrees are not equal:\n" + diff(s.value, expected),
             s)
    }
  }

  val queryPlanner = MongoDbPlanner.queryPlanner(κ("Mongo" -> Cord.empty))

  def plan(query: String): Either[Error, Workflow] = {
    queryPlanner(QueryRequest(Query(query), None))._2.toEither
  }

  def plan(logical: Term[LogicalPlan]): Either[Error, Workflow] =
    (for {
      simplified <- \/-(Optimizer.simplify(logical))
      phys <- MongoDbPlanner.plan(simplified)
    } yield phys).toEither

  def beWorkflow(wf: Workflow) = beRight(equalToWorkflow(wf))

  "plan from query string" should {
    "plan simple constant example 1" in {
      plan("select 1") must
        beWorkflow($pure(Bson.Doc(ListMap("0" -> Bson.Int64(1)))))
    }

    "plan simple select *" in {
      plan("select * from foo") must beWorkflow($read(Collection("foo")))
    }

    "plan count(*)" in {
      plan("select count(*) from foo") must beWorkflow(
        chain(
          $read(Collection("foo")),
          $group(
            Grouped(ListMap(BsonField.Name("0") -> Sum(ExprOp.Literal(Bson.Int32(1))))),
            -\/(ExprOp.Literal(Bson.Null)))))
    }

    "plan simple field projection on single set" in {
      plan("select foo.bar from foo") must
        beWorkflow(chain(
          $read(Collection("foo")),
          $project(Reshape(ListMap(
            BsonField.Name("bar") -> -\/(DocField(BsonField.Name("bar"))))),
            IgnoreId)))
    }

    "plan simple field projection on single set when table name is inferred" in {
      plan("select bar from foo") must
       beWorkflow(chain(
         $read(Collection("foo")),
         $project(Reshape(ListMap(
           BsonField.Name("bar") -> -\/(DocField(BsonField.Name("bar"))))),
           IgnoreId)))
    }

    "plan multiple field projection on single set when table name is inferred" in {
      plan("select bar, baz from foo") must
       beWorkflow(chain(
         $read(Collection("foo")),
         $project(Reshape(ListMap(
           BsonField.Name("bar") -> -\/(DocField(BsonField.Name("bar"))),
           BsonField.Name("baz") -> -\/(DocField(BsonField.Name("baz"))))),
           IgnoreId)))
    }

    "plan simple addition on two fields" in {
      plan("select foo + bar from baz") must
       beWorkflow(chain(
         $read(Collection("baz")),
         $project(Reshape(ListMap(
           BsonField.Name("0") -> -\/ (ExprOp.Add(
             DocField(BsonField.Name("foo")),
             DocField(BsonField.Name("bar")))))),
           IgnoreId)))
    }

    "plan concat" in {
      plan("select concat(bar, baz) from foo") must
       beWorkflow(chain(
         $read(Collection("foo")),
         $project(Reshape(ListMap(
           BsonField.Name("0") -> -\/ (Concat(
             DocField(BsonField.Name("bar")),
             DocField(BsonField.Name("baz")),
             Nil)))),
           IgnoreId)))
    }

    "plan lower" in {
      plan("select lower(bar) from foo") must
      beWorkflow(chain(
        $read(Collection("foo")),
        $project(Reshape(ListMap(
          BsonField.Name("0") ->
            -\/(ToLower(DocField(BsonField.Name("bar")))))),
          IgnoreId)))
    }

    "plan coalesce" in {
      plan("select coalesce(bar, baz) from foo") must
       beWorkflow(chain(
         $read(Collection("foo")),
         $project(Reshape(ListMap(
           BsonField.Name("0") ->
             -\/(IfNull(
               DocField(BsonField.Name("bar")),
               DocField(BsonField.Name("baz")))))),
           IgnoreId)))
    }

    "plan date field extraction" in {
      plan("select date_part('day', baz) from foo") must
       beWorkflow(chain(
         $read(Collection("foo")),
         $project(Reshape(ListMap(
           BsonField.Name("0") ->
             -\/(DayOfMonth(DocField(BsonField.Name("baz")))))),
           IgnoreId)))
    }

    "plan complex date field extraction" in {
      plan("select date_part('quarter', baz) from foo") must
       beWorkflow(chain(
         $read(Collection("foo")),
         $project(Reshape(ListMap(
           BsonField.Name("0") ->
             -\/(
               ExprOp.Add(
                 ExprOp.Divide(
                   DayOfYear(DocField(BsonField.Name("baz"))),
                   ExprOp.Literal(Bson.Int32(92))),
                 ExprOp.Literal(Bson.Int32(1)))))),
           IgnoreId)))
    }

    "plan date field extraction: 'dow'" in {
      plan("select date_part('dow', baz) from foo") must
       beWorkflow(chain(
         $read(Collection("foo")),
         $project(Reshape(ListMap(
           BsonField.Name("0") ->
             -\/ (ExprOp.Add(
               DayOfWeek(DocField(BsonField.Name("baz"))),
               ExprOp.Literal(Bson.Int64(-1)))))),
           IgnoreId)))
    }

    "plan date field extraction: 'isodow'" in {
      plan("select date_part('isodow', baz) from foo") must
       beWorkflow(chain(
         $read(Collection("foo")),
         $project(Reshape(ListMap(
           BsonField.Name("0") ->
             -\/ (Cond(
               ExprOp.Eq(
                 DayOfWeek(DocField(BsonField.Name("baz"))),
                 ExprOp.Literal(Bson.Int64(1))),
               ExprOp.Literal(Bson.Int64(7)),
               ExprOp.Add(
                 DayOfWeek(DocField(BsonField.Name("baz"))),
                 ExprOp.Literal(Bson.Int64(-1))))))),
           IgnoreId)))
    }

    "plan filter array element" in {
      plan("select loc from zips where loc[0] < -73") must
      beWorkflow(chain(
        $read(Collection("zips")),
        $simpleMap(JsMacro(value => Obj(ListMap(
          "loc" -> Select(value, "loc").fix,
          "__tmp0" ->
            Access(Select(value, "loc").fix, JsCore.Literal(Js.Num(0, false)).fix).fix)).fix), Nil),
        $match(Selector.Doc(BsonField.Name("__tmp0") -> Selector.Lt(Bson.Int64(-73)))),
        // FIXME: This match _could_ be implemented as below (without the
        //        $simpleMap) if it weren’t for Mongo’s broken index
        //        projections. Need to figure out how to recover this. (#455)
        // $match(Selector.Doc(
        //   BsonField.Name("loc") \ BsonField.Index(0) -> Selector.Lt(Bson.Int64(-73)))),
        $project(Reshape(ListMap(
          BsonField.Name("loc") ->
            -\/(DocField(BsonField.Name("loc"))))),
          ExcludeId)))
    }

    "plan select array element" in {
      plan("select loc[0] from zips") must
      beWorkflow(chain(
        $read(Collection("zips")),
        $simpleMap(JsMacro(value => Obj(ListMap(
          "0" -> Access(Select(value, "loc").fix,
            JsCore.Literal(Js.Num(0, false)).fix).fix)).fix), Nil)))
    }

    "plan array length" in {
      plan("select array_length(bar, 1) from foo") must
       beWorkflow(chain(
         $read(Collection("foo")),
         $project(Reshape(ListMap(
           BsonField.Name("0") ->
             -\/(Size(DocField(BsonField.Name("bar")))))),
           IgnoreId)))
    }

    "plan sum in expression" in {
      plan("select sum(pop) * 100 from zips") must
      beWorkflow(chain(
        $read(Collection("zips")),
        $group(
          Grouped(ListMap(BsonField.Name("__tmp0") ->
            Sum(DocField(BsonField.Name("pop"))))),
          -\/(ExprOp.Literal(Bson.Null))),
        $project(Reshape(ListMap(
          BsonField.Name("0") ->
            -\/(ExprOp.Multiply(
              DocField(BsonField.Name("__tmp0")),
              ExprOp.Literal(Bson.Int64(100)))))),
          IgnoreId)))
    }

    "plan conditional" in {
      plan("select case when pop < 10000 then city else loc end from zips") must
       beWorkflow(chain(
         $read(Collection("zips")),
         $project(Reshape(ListMap(
           BsonField.Name("0") ->
             -\/(Cond(
               ExprOp.Lt(
                 DocField(BsonField.Name("pop")),
                 ExprOp.Literal(Bson.Int64(10000))),
               DocField(BsonField.Name("city")),
               DocField(BsonField.Name("loc")))))),
           IgnoreId)))
    }

    "plan negate" in {
      plan("select -bar from foo") must
       beWorkflow(chain(
         $read(Collection("foo")),
         $project(Reshape(ListMap(
           BsonField.Name("0") ->
             -\/(ExprOp.Multiply(
               ExprOp.Literal(Bson.Int32(-1)),
               DocField(BsonField.Name("bar")))))),
           IgnoreId)))
    }

    "plan simple filter" in {
      plan("select * from foo where bar > 10") must
       beWorkflow(chain(
         $read(Collection("foo")),
         $match(
           Selector.Doc(BsonField.Name("bar") -> Selector.Gt(Bson.Int64(10))))))
    }

    "plan simple reversed filter" in {
      plan("select * from foo where 10 < bar") must
       beWorkflow(chain(
         $read(Collection("foo")),
         $match(
           Selector.Doc(BsonField.Name("bar") -> Selector.Gt(Bson.Int64(10))))))
    }

    "plan simple filter with expression in projection" in {
      plan("select a + b from foo where bar > 10") must
       beWorkflow(chain(
         $read(Collection("foo")),
         $match(
           Selector.Doc(BsonField.Name("bar") -> Selector.Gt(Bson.Int64(10)))),
         $project(Reshape(ListMap(
           BsonField.Name("0") -> -\/ (ExprOp.Add(
             DocField(BsonField.Name("a")),
             DocField(BsonField.Name("b")))))),
           IgnoreId)))
    }

    "plan simple js filter" in {
      plan("select * from zips where length(city) < 4") must
      beWorkflow(chain(
        $read(Collection("zips")),
        // FIXME: Inline this $simpleMap with the $match (#454)
        $simpleMap(JsMacro(x => Obj(ListMap(
          "__tmp0" -> Select(Select(x, "city").fix, "length").fix,
          "__tmp1" -> x)).fix), Nil),
        $match(Selector.Doc(
          BsonField.Name("__tmp0") -> Selector.Lt(Bson.Int64(4)))),
        $project(Reshape(ListMap(
          BsonField.Name("value") -> -\/(DocField(BsonField.Name("__tmp1"))))),
          ExcludeId)))
    }

    "plan filter with js and non-js" in {
      plan("select * from zips where length(city) < 4 and pop < 20000") must
      beWorkflow(chain(
        $read(Collection("zips")),
        // FIXME: Inline this $simpleMap with the $match (#454)
        $simpleMap(JsMacro(value => Obj(ListMap(
          "__tmp4" -> Select(Select(value, "city").fix, "length").fix,
          "__tmp5" -> value,
          "__tmp6" -> Select(value, "pop").fix)).fix), Nil),
        $match(Selector.And(
          Selector.Doc(BsonField.Name("__tmp4") -> Selector.Lt(Bson.Int64(4))),
          Selector.Doc(BsonField.Name("__tmp6") -> Selector.Lt(Bson.Int64(20000))))),
        $project(Reshape(ListMap(
          BsonField.Name("value") ->
            -\/(DocField(BsonField.Name("__tmp5"))))),
          ExcludeId)))
    }

    "plan filter with between" in {
      plan("select * from foo where bar between 10 and 100") must
       beWorkflow(chain(
         $read(Collection("foo")),
         $match(
           Selector.And(
             Selector.Doc(BsonField.Name("bar") ->
               Selector.Gte(Bson.Int64(10))),
             Selector.Doc(BsonField.Name("bar") ->
               Selector.Lte(Bson.Int64(100)))))))
    }

    "plan filter with like" in {
      plan("select * from foo where bar like 'A%'") must
       beWorkflow(chain(
         $read(Collection("foo")),
         $match(Selector.Doc(
           BsonField.Name("bar") ->
             Selector.Regex("^A.*$", false, false, false, false)))))
    }

    "plan filter with LIKE and OR" in {
      plan("select * from foo where bar like 'A%' or bar like 'Z%'") must
       beWorkflow(chain(
         $read(Collection("foo")),
         $match(
           Selector.Or(
             Selector.Doc(BsonField.Name("bar") ->
               Selector.Regex("^A.*$", false, false, false, false)),
             Selector.Doc(BsonField.Name("bar") ->
               Selector.Regex("^Z.*$", false, false, false, false))))))
    }

    "plan filter with field in constant array" in {
      plan("select * from zips where state in ('AZ', 'CO')") must
        beWorkflow(chain(
          $read(Collection("zips")),
          $match(Selector.Doc(BsonField.Name("state") ->
            Selector.In(Bson.Arr(List(Bson.Text("AZ"), Bson.Text("CO"))))))))
    }

    "plan filter with field containing constant value" in {
      plan("select * from zips where 43.058514 in loc") must
        beWorkflow(chain(
          $read(Collection("zips")),
          $match(Selector.Doc(BsonField.Name("loc") ->
            Selector.ElemMatch(\/-(Selector.In(Bson.Arr(List(Bson.Dec(43.058514))))))))))
    }

    "plan filter with field containing other field" in {
      import JsCore._
      plan("select * from zips where pop in loc") must
        beWorkflow(chain(
          $read(Collection("zips")),
          $match(Selector.Where(
            BinOp(Neq,
              JsCore.Literal(Js.Num(-1.0,false)).fix,
              Call(Select(Select(Ident("this").fix, "loc").fix, "indexOf").fix,
                List(Select(Ident("this").fix, "pop").fix)).fix).fix.toJs))))
    }

    "plan filter with ~" in {
      plan("select * from zips where city ~ '^B[AEIOU]+LD.*'").disjunction must beRightDisjOrDiff(chain(
        $read(Collection("zips")),
        $match(
          Selector.Doc(
            BsonField.Name("city") -> Selector.Regex("^B[AEIOU]+LD.*", false, false, false, false)))))
    }

    "plan filter with alternative ~" in {
      plan("select * from a where 'foo' ~ pattern or target ~ pattern").disjunction must beRightDisjOrDiff(chain(
        $read(Collection("a")),
        $simpleMap(JsMacro(x => Obj(ListMap(
          "__tmp4" -> Call(
            Select(New("RegExp", List(Select(x, "pattern").fix)).fix, "test").fix,
            List(JsCore.Literal(Js.Str("foo")).fix)).fix,
          "__tmp5" -> x,
          "__tmp6" -> Call(
            Select(New("RegExp", List(Select(x, "pattern").fix)).fix, "test").fix,
            List(Select(x, "target").fix)).fix
        )).fix), Nil),
        $match(
          Selector.Or(
            Selector.Doc(
              BsonField.Name("__tmp4") -> Selector.Eq(Bson.Bool(true))),
            Selector.Doc(
              BsonField.Name("__tmp6") -> Selector.Eq(Bson.Bool(true))))),
        $project(
          Reshape(ListMap(
            BsonField.Name("value") -> -\/(DocField(BsonField.Name("__tmp5"))))),
          ExcludeId)))
    }

    "plan filter with negate(s)" in {
      plan("select * from foo where bar != -10 and baz > -1.0") must
       beWorkflow(chain(
         $read(Collection("foo")),
         $match(
           Selector.And(
             Selector.Doc(BsonField.Name("bar") ->
               Selector.Neq(Bson.Int64(-10))),
             Selector.Doc(BsonField.Name("baz") ->
               Selector.Gt(Bson.Dec(-1.0)))))))
    }

    "plan complex filter" in {
      plan("select * from foo where bar > 10 and (baz = 'quux' or foop = 'zebra')") must
       beWorkflow(chain(
         $read(Collection("foo")),
         $match(
           Selector.And(
             Selector.Doc(BsonField.Name("bar") -> Selector.Gt(Bson.Int64(10))),
             Selector.Or(
               Selector.Doc(BsonField.Name("baz") ->
                 Selector.Eq(Bson.Text("quux"))),
               Selector.Doc(BsonField.Name("foop") ->
                 Selector.Eq(Bson.Text("zebra"))))))))
    }

    "plan filter with both index and field projections" in {
      plan("select count(parents[0].sha) as count from slamengine_commits where parents[0].sha = '56d1caf5d082d1a6840090986e277d36d03f1859'") must
        beWorkflow(chain(
          $read(Collection("slamengine_commits")),
          $simpleMap(JsMacro(x => Obj(ListMap(
            "__tmp2" ->
              Select(
                Access(
                  Select(x, "parents").fix,
                  JsCore.Literal(Js.Num(0, false)).fix).fix,
                "sha").fix)).fix), Nil),
          $project(Reshape(ListMap(
            BsonField.Name("__tmp0") ->
              -\/(DocField(BsonField.Name("__tmp2"))))),
            IgnoreId),
          $match(Selector.Doc(
            BsonField.Name("__tmp0") ->
              Selector.Eq(Bson.Text("56d1caf5d082d1a6840090986e277d36d03f1859")))),
          $group(Grouped(ListMap(
            BsonField.Name("count") -> Sum(ExprOp.Literal(Bson.Int32(1))))),
            -\/(ExprOp.Literal(Bson.Null)))))
    }

    "plan simple having filter" in {
      plan("select city from zips group by city having count(*) > 10") must
      beWorkflow(chain(
        $read(Collection("zips")),
        $group(Grouped(ListMap(
          BsonField.Name ("city") -> Push(DocField(BsonField.Name("city"))),
          BsonField.Name ("__tmp0") -> Sum(ExprOp.Literal(Bson.Int32(1))))),
          -\/(DocField(BsonField.Name("city")))),
        $unwind(DocField(BsonField.Name("city"))),
        $match(Selector.Doc(
          BsonField.Name("__tmp0") -> Selector.Gt(Bson.Int64(10)))),
        $project(Reshape(ListMap(
          BsonField.Name("city") -> -\/(DocField(BsonField.Name("city"))))),
          ExcludeId)))
    }

    "plan having with multiple projections" in {
      plan("select city, sum(pop) from zips group by city having sum(pop) > 50000") must
      beWorkflow(chain(
        $read(Collection("zips")),
        $group(Grouped(ListMap(
          BsonField.Name ("city") -> Push(DocField(BsonField.Name("city"))),
          BsonField.Name ("1") -> Sum(DocField(BsonField.Name("pop"))))),
          -\/(DocField(BsonField.Name("city")))),
        $unwind(DocField(BsonField.Name("city"))),
        $match(Selector.Doc(
          BsonField.Name("1") -> Selector.Gt(Bson.Int64(50000))))))
    }

    "prefer projection+filter over JS filter" in {
      plan("select * from zips where city <> state") must
      beWorkflow(chain(
        $read(Collection("zips")),
        $project(
          Reshape(ListMap(
            BsonField.Name("__tmp0") ->
                -\/(ExprOp.Neq(
                  DocField(BsonField.Name("city")),
                  DocField(BsonField.Name("state")))),
            BsonField.Name("__tmp1") -> -\/(DocVar.ROOT()))),
          IgnoreId),
        $match(Selector.Doc(
          BsonField.Name("__tmp0") -> Selector.Eq(Bson.Bool(true)))),
        $project(Reshape(ListMap(
          BsonField.Name("value") -> -\/(DocField(BsonField.Name("__tmp1"))))),
          ExcludeId)))
    }

    "prefer projection+filter over nested JS filter" in {
      plan("select * from zips where city <> state and pop < 10000") must
      beWorkflow(chain(
        $read(Collection("zips")),
        $project(
          Reshape(ListMap(
            BsonField.Name("__tmp2") ->
                -\/(ExprOp.Neq(
                  DocField(BsonField.Name("city")),
                  DocField(BsonField.Name("state")))),
            BsonField.Name("__tmp3") -> -\/(DocVar.ROOT()),
            BsonField.Name("__tmp4") -> -\/(DocField(BsonField.Name("pop"))))),
          IgnoreId),
        $match(Selector.And(
          Selector.Doc(
            BsonField.Name("__tmp2") -> Selector.Eq(Bson.Bool(true))),
          Selector.Doc(
            BsonField.Name("__tmp4") -> Selector.Lt(Bson.Int64(10000))))),
        $project(Reshape(ListMap(
          BsonField.Name("value") -> -\/(DocField(BsonField.Name("__tmp3"))))),
          ExcludeId)))
    }

    "filter on constant" in {
      plan("select * from zips where true") must
      beWorkflow(chain(
        $read(Collection("zips")),
        $project(Reshape(ListMap(
          BsonField.Name("__tmp0") -> -\/(ExprOp.Literal(Bson.Bool(true))),
          BsonField.Name("__tmp1") -> -\/(DocVar.ROOT()))),
          IgnoreId),
        $match(Selector.Doc(
          BsonField.Name("__tmp0") -> Selector.Eq(Bson.Bool(true)))),
        $project(Reshape(ListMap(
          BsonField.Name("value") -> -\/(DocField(BsonField.Name("__tmp1"))))),
          ExcludeId)))
    }

    "plan simple sort with field in projection" in {
      plan("select bar from foo order by bar") must
        beWorkflow(chain(
          $read(Collection("foo")),
          $sort(NonEmptyList(BsonField.Name("bar") -> Ascending)),
          $project(
            Reshape(ListMap(
              BsonField.Name("bar") ->
                -\/(ExprOp.DocField(BsonField.Name("bar"))))),
            IgnoreId)))
    }

    "plan simple sort with wildcard" in {
      plan("select * from zips order by pop") must
        beWorkflow(chain(
          $read(Collection("zips")),
          $sort(NonEmptyList(BsonField.Name("pop") -> Ascending))))
    }

    "plan sort with expression in key" in {
      plan("select baz from foo order by bar/10") must
        beWorkflow(chain(
          $read(Collection("foo")),
          $project(Reshape(ListMap(
            BsonField.Name("baz") ->
              -\/(ExprOp.DocField(BsonField.Name("baz"))),
            BsonField.Name("__tmp0") ->
              -\/(ExprOp.Divide(
                DocField(BsonField.Name("bar")),
                ExprOp.Literal(Bson.Int64(10)))))),
            IgnoreId),
          $sort(NonEmptyList(BsonField.Name("__tmp0") -> Ascending)),
          $project(Reshape(ListMap(
            BsonField.Name("baz") ->
              -\/(ExprOp.DocField(BsonField.Name("baz"))))),
            ExcludeId)))
    }

    "plan select with wildcard and field" in {
      plan("select *, pop from zips") must
        beWorkflow(chain(
          $read(Collection("zips")),
          $simpleMap(JsMacro(x =>
            Splice(List(
              x,
              Obj(ListMap(
                "pop" -> Select(x, "pop").fix)).fix)).fix), Nil)))
    }

    "plan select with wildcard and two fields" in {
      plan("select *, city as city2, pop as pop2 from zips") must
        beWorkflow(chain(
          $read(Collection("zips")),
          $simpleMap(JsMacro(x =>
            Splice(List(
              x,
              Obj(ListMap(
                "city2" -> Select(x, "city").fix)).fix,
              Obj(ListMap(
                "pop2"  -> Select(x, "pop").fix)).fix)).fix), Nil)))
    }

    "plan select with multiple wildcards and fields" in {
      plan("select state as state2, *, city as city2, *, pop as pop2 from zips where pop < 1000") must
        beWorkflow(chain(
          $read(Collection("zips")),
          $match(Selector.Doc(
            BsonField.Name("pop") -> Selector.Lt(Bson.Int64(1000)))),
          $simpleMap(JsMacro(x => Obj(ListMap(
            "__tmp0" -> Splice(List(
              Obj(ListMap(
                "state2" -> Select(x, "state").fix)).fix,
              x,
              Obj(ListMap(
                "city2" -> Select(x, "city").fix)).fix,
              x,
              Obj(ListMap(
                "pop2"  -> Select(x, "pop").fix)).fix)).fix,
            "__tmp1" -> x)).fix), Nil),
          $project(
            Reshape(ListMap(
              BsonField.Name("value") -> -\/(DocField(BsonField.Name("__tmp0"))))),
            ExcludeId)))
    }

    "plan sort with wildcard and expression in key" in {
      plan("select * from zips order by pop/10 desc") must
        beWorkflow(chain(
          $read(Collection("zips")),
          $simpleMap(JsMacro(x => Splice(List(
            x,
            Obj(ListMap(
              "__sd__0" -> BinOp(Div, Select(x, "pop").fix, JsCore.Literal(Js.Num(10, false)).fix).fix)).fix)).fix), Nil),
          $sort(NonEmptyList(BsonField.Name("__sd__0") -> Descending))))
    }

    "plan simple sort with field not in projections" in {
      plan("select name from person order by height") must
        beWorkflow(chain(
          $read(Collection("person")),
          $sort(NonEmptyList(BsonField.Name("height") -> Ascending)),
          $project(Reshape(ListMap(
            BsonField.Name("name") ->
              -\/(DocField(BsonField.Name("name"))))),
            IgnoreId)))
    }

    "plan sort with expression and alias" in {
      plan("select pop/1000 as popInK from zips order by popInK") must
        beWorkflow(chain(
          $read(Collection("zips")),
          $project(Reshape(ListMap(
            BsonField.Name("popInK") ->
              -\/(ExprOp.Divide(
                DocField(BsonField.Name("pop")),
                ExprOp.Literal(Bson.Int64(1000)))))),
            IgnoreId),
          $sort(NonEmptyList(BsonField.Name("popInK") -> Ascending))))
    }

    "plan sort with filter" in {
      plan("select city, pop from zips where pop <= 1000 order by pop desc, city") must
        beWorkflow(chain(
          $read(Collection("zips")),
          $match(Selector.Doc(
            BsonField.Name("pop") -> Selector.Lte(Bson.Int64(1000)))),
          $project(Reshape(ListMap(
            BsonField.Name("city") -> -\/(DocField(BsonField.Name("city"))),
            BsonField.Name("pop") -> -\/(DocField(BsonField.Name("pop"))))),
            IgnoreId),
          $sort(NonEmptyList(
            BsonField.Name("pop") -> Descending,
            BsonField.Name("city") -> Ascending))))
    }

    "plan sort with expression, alias, and filter" in {
      plan("select pop/1000 as popInK from zips where pop >= 1000 order by popInK") must
        beWorkflow(chain(
          $read(Collection("zips")),
          $match(Selector.Doc(BsonField.Name("pop") -> Selector.Gte(Bson.Int64(1000)))),
          $project(Reshape(ListMap(
            BsonField.Name("popInK") ->
              -\/(ExprOp.Divide(
                DocField(BsonField.Name("pop")),
                ExprOp.Literal(Bson.Int64(1000)))))),
            IgnoreId),
          $sort(NonEmptyList(BsonField.Name("popInK") -> Ascending)))
        )
    }

    "plan multiple column sort with wildcard" in {
      plan("select * from zips order by pop, city desc") must
       beWorkflow(chain(
         $read(Collection("zips")),
         $sort(NonEmptyList(
           BsonField.Name("pop") -> Ascending,
           BsonField.Name("city") -> Descending))))
    }

    "plan many sort columns" in {
      plan("select * from zips order by pop, state, city, a4, a5, a6") must
       beWorkflow(chain(
         $read(Collection("zips")),
         $sort(NonEmptyList(
           BsonField.Name("pop") -> Ascending,
           BsonField.Name("state") -> Ascending,
           BsonField.Name("city") -> Ascending,
           BsonField.Name("a4") -> Ascending,
           BsonField.Name("a5") -> Ascending,
           BsonField.Name("a6") -> Ascending))))
    }

    "plan efficient count and field ref" in {
      plan("SELECT city, COUNT(*) AS cnt FROM zips ORDER BY cnt DESC") must
        beWorkflow {
          chain(
            $read(Collection("zips")),
            $group(
              Grouped(ListMap(
                BsonField.Name("cnt") -> Sum(ExprOp.Literal(Bson.Int32(1))),
                BsonField.Name("__tmp0") -> Push(DocVar.ROOT()))),
              -\/(ExprOp.Literal(Bson.Null))),
            $unwind(DocField(BsonField.Name("__tmp0"))),
            $project(Reshape(ListMap(
              BsonField.Name("cnt") -> -\/(DocField(BsonField.Name("cnt"))),
              BsonField.Name("city") ->
                -\/(DocField(BsonField.Name("__tmp0") \ BsonField.Name("city"))))),
              IgnoreId),
            $sort(NonEmptyList(BsonField.Name("cnt") -> Descending)))
        }
    }

    "plan count and js expr" in {
      plan("SELECT COUNT(*) as cnt, LENGTH(city) FROM zips") must
        beWorkflow {
          chain(
            $read(Collection("zips")),
            $group(
              Grouped(ListMap(
                BsonField.Name("cnt") -> Sum(ExprOp.Literal(Bson.Int32(1))),
                BsonField.Name("__tmp0") -> Push(DocVar.ROOT()))),
              -\/(ExprOp.Literal(Bson.Null))),
            $unwind(DocField(BsonField.Name("__tmp0"))),
            $simpleMap(JsMacro(x => Obj(ListMap(
              "cnt" -> Select(x, "cnt").fix,
              "1" -> Select(Select(Select(x, "__tmp0").fix, "city").fix, "length").fix)).fix), Nil))
        }
    }

    "plan trivial group by" in {
      plan("select city from zips group by city") must
      beWorkflow(chain(
        $read(Collection("zips")),
        $group(
          Grouped(ListMap(
            BsonField.Name("city") ->
              Push(DocField(BsonField.Name("city"))))),
          -\/ (DocField(BsonField.Name("city")))),
        $unwind(DocField(BsonField.Name("city")))))
    }

    "plan group by expression" in {
      plan("select city from zips group by lower(city)") must
      beWorkflow(chain(
        $read(Collection("zips")),
        $group(
          Grouped(ListMap(
            BsonField.Name("city") ->
              Push(DocField(BsonField.Name("city"))))),
          -\/ (ToLower(DocField(BsonField.Name("city"))))),
        $unwind(DocField(BsonField.Name("city")))))
    }

    "plan group by month" in {
      plan("select avg(score) as a, DATE_PART('month', \"date\") as m from caloriesBurnedData group by DATE_PART('month', \"date\")") must
        beWorkflow(chain(
          $read(Collection("caloriesBurnedData")),
          $project(
            Reshape(ListMap(
              BsonField.Name("__tmp0") -> -\/(DocField(BsonField.Name("score"))),
              BsonField.Name("__tmp1") -> -\/(Month(DocField(BsonField.Name("date")))))),
            IgnoreId),
          $group(
            Grouped(ListMap(
              BsonField.Name("a") -> Avg(DocField(BsonField.Name("__tmp0"))),
              BsonField.Name("m") -> Push(DocField(BsonField.Name("__tmp1"))))),
            -\/(DocField(BsonField.Name("__tmp1")))),
          $unwind(DocField(BsonField.Name("m")))))
    }

    "plan expr3 with grouping" in {
      plan("select case when pop > 1000 then city else lower(city) end, count(*) from zips group by city") must
        beRight
    }

    "plan trivial group by with wildcard" in {
      plan("select * from zips group by city") must
        beWorkflow($read(Collection("zips")))
    }

    "plan count grouped by single field" in {
      plan("select count(*) from bar group by baz") must
        beWorkflow {
          chain(
            $read(Collection("bar")),
            $group(Grouped(ListMap(
              BsonField.Name("0") -> Sum(ExprOp.Literal(Bson.Int32(1))))),
              -\/(DocField(BsonField.Name("baz")))))
        }
    }

    "plan count and sum grouped by single field" in {
      plan("select count(*) as cnt, sum(biz) as sm from bar group by baz") must
        beWorkflow {
          chain(
            $read(Collection("bar")),
            $group(
              Grouped(ListMap(
                BsonField.Name("cnt") -> Sum(ExprOp.Literal(Bson.Int32(1))),
                BsonField.Name("sm") -> Sum(DocField(BsonField.Name("biz"))))),
              -\/(DocField(BsonField.Name("baz")))))
        }
    }

    "plan sum grouped by single field with filter" in {
      plan("select sum(pop) as sm from zips where state='CO' group by city") must
        beWorkflow {
          chain(
            $read(Collection("zips")),
            $match(Selector.Doc(
              BsonField.Name("state") -> Selector.Eq(Bson.Text("CO")))),
            $group(
              Grouped(ListMap(
                BsonField.Name("sm") -> Sum(DocField(BsonField.Name("pop"))))),
              -\/(DocField(BsonField.Name("city")))))
        }
    }

    "plan count and field when grouped" in {
      plan("select count(*) as cnt, city from zips group by city") must
        beWorkflow {
          chain(
            $read(Collection("zips")),
            $group(
              Grouped(ListMap(
                BsonField.Name("cnt") -> Sum(ExprOp.Literal(Bson.Int32(1))),
                BsonField.Name("city") -> Push(DocField(BsonField.Name("city"))))),
              -\/(DocField(BsonField.Name("city")))),
            $unwind(DocField(BsonField.Name("city"))))
        }
    }

    "collect unaggregated fields into single doc when grouping" in {
      plan("select city, state, sum(pop) from zips") must
      beWorkflow(chain(
        $read(Collection("zips")),
        $group(
          Grouped(ListMap(
            BsonField.Name("2") -> Sum(DocField(BsonField.Name("pop"))),
            BsonField.Name("__tmp0") -> Push(DocVar.ROOT()))),
          -\/(ExprOp.Literal(Bson.Null))),
        $unwind(DocField(BsonField.Name("__tmp0"))),
        $project(Reshape(ListMap(
          BsonField.Name ("2") -> -\/(DocField(BsonField.Name("2"))),
          BsonField.Name ("city") ->
            -\/(DocField(BsonField.Name("__tmp0") \ BsonField.Name("city"))),
          BsonField.Name ("state") ->
            -\/(DocField(BsonField.Name("__tmp0") \ BsonField.Name("state"))))),
          IgnoreId)))
    }

    "plan multiple expressions using same field" in {
      plan("select pop, sum(pop), pop/1000 from zips") must
      beWorkflow (chain (
        $read (Collection ("zips")),
        $group(
          Grouped(ListMap(
            BsonField.Name("1") -> Sum(DocField(BsonField.Name("pop"))),
            BsonField.Name("__tmp0") -> Push(DocVar.ROOT()))),
          -\/(ExprOp.Literal(Bson.Null))),
        $unwind(DocField(BsonField.Name("__tmp0"))),
        $project(Reshape(ListMap(
          BsonField.Name("2") ->
            -\/(ExprOp.Divide(
              DocField(BsonField.Name("__tmp0") \ BsonField.Name("pop")),
              ExprOp.Literal(Bson.Int64(1000)))),
          BsonField.Name("1") -> -\/(DocField(BsonField.Name("1"))),
          BsonField.Name("pop") ->
            -\/(DocField(BsonField.Name("__tmp0") \ BsonField.Name("pop"))))),
          IgnoreId)))
    }

    "plan sum of expression in expression with another projection when grouped" in {
      plan("select city, sum(pop-1)/1000 from zips group by city") must
      beWorkflow(chain(
        $read(Collection("zips")),
        $group(
          Grouped(ListMap(
            BsonField.Name("city") -> Push(DocField(BsonField.Name("city"))),
            BsonField.Name("__tmp2") -> Sum(ExprOp.Subtract(DocField(BsonField.Name("pop")), ExprOp.Literal(Bson.Int64(1)))))),
          -\/(DocField(BsonField.Name("city")))),
        $unwind(DocField(BsonField.Name("city"))),
        $project(
          Reshape(ListMap(
            BsonField.Name("city") -> -\/(DocField(BsonField.Name("city"))),
            BsonField.Name("1") ->
              -\/(ExprOp.Divide(
                DocField(BsonField.Name("__tmp2")),
                ExprOp.Literal(Bson.Int64(1000)))))),
          IgnoreId)))
    }

    "plan length of min (JS on top of reduce)" in {
      plan("select state, length(min(city)) as shortest from zips group by state") must
        beWorkflow(chain(
          $read(Collection("zips")),
          $group(
            Grouped(ListMap(
              BsonField.Name("state") -> Push(DocField(BsonField.Name("state"))),
              BsonField.Name("__tmp0") -> Min(DocField(BsonField.Name("city"))))),
            -\/(DocField(BsonField.Name("state")))),
          $unwind(DocField(BsonField.Name("state"))),
          $simpleMap(JsMacro(x => Obj(ListMap(
            "state" -> Select(x, "state").fix,
            "shortest" -> Select(Select(x, "__tmp0").fix, "length").fix)).fix), Nil)))
    }

    "plan js expr grouped by js expr" in {
      plan("select length(city) as len, count(*) as cnt from zips group by length(city)") must
        beWorkflow(chain(
          $read(Collection("zips")),
          $simpleMap(JsMacro(js =>
            Obj(ListMap(
              "__tmp0" -> Select(Select(js, "city").fix, "length").fix,
              "__tmp1" -> js)).fix), Nil),
          $group(
            Grouped(ListMap(
              BsonField.Name("len") -> Push(DocField(BsonField.Name("__tmp0"))),
              BsonField.Name("cnt") -> Sum(ExprOp.Literal(Bson.Int32(1))))),
              -\/(DocField(BsonField.Name("__tmp0")))),
          $unwind(DocField(BsonField.Name("len")))))
    }

    "plan simple JS inside expression" in {
      plan("select length(city) + 1 from zips") must
        beWorkflow(chain(
          $read(Collection("zips")),
          $simpleMap(JsMacro(x => Obj(ListMap(
            "0" -> BinOp(JsCore.Add,
              Select(Select(x, "city").fix, "length").fix,
              JsCore.Literal(Js.Num(1, false)).fix).fix)).fix), Nil)))
    }

    "plan expressions with ~"in {
      plan("select foo ~ 'bar.*', 'abc' ~ 'a|b', 'baz' ~ regex, target ~ regex from a").disjunction must beRightDisjOrDiff(chain(
        $read(Collection("a")),
        $simpleMap(JsMacro(x =>
          Obj(ListMap(
            "0" -> Call(
              Select(New("RegExp", List(JsCore.Literal(Js.Str("bar.*")).fix)).fix, "test").fix,
              List(Select(x, "foo").fix)).fix,
            "1" -> JsCore.Literal(Js.Bool(true)).fix,
            "2" -> Call(
              Select(New("RegExp", List(Select(x, "regex").fix)).fix, "test").fix,
              List(JsCore.Literal(Js.Str("baz")).fix)).fix,
            "3" -> Call(
              Select(New("RegExp", List(Select(x, "regex").fix)).fix, "test").fix,
              List(Select(x, "target").fix)).fix)).fix), Nil)))
    }

    "plan object flatten" in {
      plan("select geo{*} from usa_factbook") must
        beWorkflow {
          chain(
            $read(Collection("usa_factbook")),
            $simpleMap(JsMacro(Predef.identity), List(JsMacro(Select(_, "geo").fix))),
            $project(Reshape(ListMap(
              BsonField.Name("geo") -> -\/(DocField(BsonField.Name("geo"))))),
              IgnoreId))
        }
    }

    "plan array project with concat" in {
      plan("select city, loc[0] from zips") must
        beWorkflow {
          chain(
            $read(Collection("zips")),
            $simpleMap(JsMacro(value => Obj(ListMap(
              "city" -> JsCore.Select(value, "city").fix,
              "1" -> JsCore.Access(
                JsCore.Select(value, "loc").fix,
                JsCore.Literal(Js.Num(0, false)).fix).fix)).fix), Nil))
        }
    }

    "plan array flatten" in {
      plan("select loc[*] from zips") must
        beWorkflow {
          chain(
            $read(Collection("zips")),
            $unwind(DocField(BsonField.Name("loc"))),
            $project(Reshape(ListMap(
              BsonField.Name("loc") -> -\/(DocField(BsonField.Name("loc"))))),
              IgnoreId))
        }
    }

    "plan array flatten with unflattened field" in {
      plan("SELECT _id as zip, loc as loc, loc[*] as coord FROM zips") must
        beWorkflow {
          chain(
            $read(Collection("zips")),
            $project(Reshape(ListMap(
              BsonField.Name("__tmp0") -> -\/(DocField(BsonField.Name("loc"))),
              BsonField.Name("__tmp1") -> -\/(DocVar.ROOT()))),
              IgnoreId),
            $unwind(DocField(BsonField.Name("__tmp0"))),
            $project(Reshape(ListMap(
              BsonField.Name("zip") ->
                -\/(DocField(BsonField.Name("__tmp1") \ BsonField.Name("_id"))),
              BsonField.Name("loc") ->
                -\/(DocField(BsonField.Name("__tmp1") \ BsonField.Name("loc"))),
              BsonField.Name("coord") ->
                -\/(DocField(BsonField.Name("__tmp0"))))),
              IgnoreId))
        }
    }

    "unify flattened fields" in {
      plan("select loc[*] from zips where loc[*] < 0") must
      beWorkflow(chain(
        $read(Collection("zips")),
        $unwind(DocField(BsonField.Name("loc"))),
        $project(Reshape(ListMap(
          BsonField.Name("loc") -> -\/(DocField(BsonField.Name("loc"))))),
          IgnoreId),
        $match(Selector.Doc(
          BsonField.Name("loc") -> Selector.Lt(Bson.Int64(0))))))
    }

    "unify flattened fields with unflattened field" in {
      plan("select _id as zip, loc[*] from zips order by loc[*]") must
      beWorkflow(chain(
        $read(Collection("zips")),
        $project(Reshape(ListMap(
          BsonField.Name("__tmp0") -> -\/(DocField(BsonField.Name("loc"))),
          BsonField.Name("__tmp1") -> -\/(DocVar.ROOT()))),
          IgnoreId),
        $unwind(DocField(BsonField.Name("__tmp0"))),
        $project(Reshape(ListMap(
          BsonField.Name("zip") ->
            -\/(DocField(BsonField.Name("__tmp1") \ BsonField.Name("_id"))),
          BsonField.Name("loc") -> -\/(DocField(BsonField.Name("__tmp0"))))),
          IgnoreId),
        $sort(NonEmptyList(BsonField.Name("loc") -> Ascending))))
    }

    "plan limit with offset" in {
      plan("SELECT * FROM zips LIMIT 5 OFFSET 100") must
        beWorkflow(chain($read(Collection("zips")), $limit(105), $skip(100)))
    }

    "plan sort and limit" in {
      plan("SELECT city, pop FROM zips ORDER BY pop DESC LIMIT 5") must
        beWorkflow {
          chain(
            $read(Collection("zips")),
            $sort(NonEmptyList(BsonField.Name("pop") -> Descending)),
            $project(Reshape(ListMap(
              BsonField.Name("city") ->
                -\/(DocField(BsonField.Name("city"))),
              BsonField.Name("pop") ->
                -\/(DocField(BsonField.Name("pop"))))),
              IgnoreId),
            $limit(5))
        }
    }

    "plan simple single field selection and limit" in {
      plan("SELECT city FROM zips LIMIT 5") must
        beWorkflow {
          chain(
            $read(Collection("zips")),
            $project(Reshape(ListMap(BsonField.Name("city") -> -\/ (DocField(BsonField.Name("city"))))),
              IgnoreId),
            $limit(5))
        }
    }

    "plan complex group by with sorting and limiting" in {
      plan("SELECT city, SUM(pop) AS pop FROM zips GROUP BY city ORDER BY pop") must
        beWorkflow {
          chain(
            $read(Collection("zips")),
            $group(Grouped(ListMap(
              BsonField.Name("city") -> Push(DocField(BsonField.Name("city"))),
              BsonField.Name("pop") -> Sum(DocField(BsonField.Name("pop"))))),
              -\/(DocField(BsonField.Name("city")))),
            $unwind(DocField(BsonField.Name("city"))),
            $sort(NonEmptyList(BsonField.Name("pop") -> Ascending)))
        }
    }

    "plan filter and expressions with IS NULL" in {
      plan("select foo is null from zips where foo is null") must
        beWorkflow(chain(
          $read(Collection("zips")),
          $match(Selector.Doc(
            BsonField.Name("foo") -> Selector.Eq(Bson.Null))),
          $project(
            Reshape(ListMap(
              BsonField.Name("0") -> -\/(ExprOp.Eq(DocField(BsonField.Name("foo")), ExprOp.Literal(Bson.Null))))),
            IgnoreId)))
    }

    "plan implicit group by with filter" in {
      plan("select avg(pop), min(city) from zips where state = 'CO'") must
        beWorkflow(chain(
          $read(Collection("zips")),
          $match(Selector.Doc(
            BsonField.Name("state") -> Selector.Eq(Bson.Text("CO")))),
          $group(
            Grouped(ListMap(
              BsonField.Name("0") -> Avg(DocField(BsonField.Name("pop"))),
              BsonField.Name("1") -> Min(DocField(BsonField.Name("city"))))),
            -\/(ExprOp.Literal(Bson.Null)))))
    }

    "plan simple distinct" in {
      plan("select distinct city, state from zips") must
      beWorkflow(
        chain(
          $read(Collection("zips")),
          $project(Reshape(ListMap(
              BsonField.Name("city") -> -\/ (DocField(BsonField.Name("city"))),
              BsonField.Name("state") -> -\/ (DocField(BsonField.Name("state"))))),
            IgnoreId),
          $group(
            Grouped(ListMap(
              BsonField.Name("__tmp0") -> First(DocVar.ROOT()))),
              \/-(Reshape(ListMap(
                BsonField.Name("city") -> -\/ (DocField(BsonField.Name("city"))),
                BsonField.Name("state") -> -\/ (DocField(BsonField.Name("state"))))))),
          $project(Reshape(ListMap(
              BsonField.Name("city") -> -\/ (DocField(BsonField.Name("__tmp0") \ BsonField.Name("city"))),
              BsonField.Name("state") -> -\/ (DocField(BsonField.Name("__tmp0") \ BsonField.Name("state"))))),
            ExcludeId)))
    }

    "plan distinct as expression" in {
      plan("select count(distinct(city)) from zips") must
        beWorkflow(chain(
          $read(Collection("zips")),
          $group(
            Grouped(ListMap()),
            -\/(DocField(BsonField.Name("city")))),
          $group(
            Grouped(ListMap(
              BsonField.Name("0") ->
                Sum(ExprOp.Literal(Bson.Int32(1))))),
            -\/(ExprOp.Literal(Bson.Null)))))
    }

    "plan distinct of expression as expression" in {
      plan("select count(distinct substring(city, 0, 1)) from zips") must
        beWorkflow(chain(
          $read(Collection("zips")),
          $group(
            Grouped(ListMap()),
            -\/(Substr(
              DocField(BsonField.Name("city")),
              ExprOp.Literal(Bson.Int64(0)),
              ExprOp.Literal(Bson.Int64(1))))),
          $group(
            Grouped(ListMap(
              BsonField.Name("0") ->
                Sum(ExprOp.Literal(Bson.Int32(1))))),
            -\/(ExprOp.Literal(Bson.Null)))))
    }

    "plan distinct of wildcard" in {
      plan("select distinct * from zips") must
        beWorkflow(
          $read(Collection("zips")))
    }.pendingUntilFixed("#283")

    "plan distinct of wildcard as expression" in {
      plan("select count(distinct *) from zips") must
        beWorkflow(
          $read(Collection("zips")))
    }.pendingUntilFixed("#283")

    "plan distinct with simple order by" in {
      plan("select distinct city from zips order by city") must
        beWorkflow(
          chain(
            $read(Collection("zips")),
            $sort(NonEmptyList(BsonField.Name("city") -> Ascending)),
            $project(Reshape(ListMap(
              BsonField.Name("city") -> -\/(DocField(BsonField.Name("city"))))),
              IgnoreId),
            $group(
              Grouped(ListMap(
                BsonField.Name("__tmp0") -> First(DocVar.ROOT()),
                BsonField.Name("__sd_key_0") -> First(DocField(BsonField.Name("city"))))),
              \/-(Reshape(ListMap(
                BsonField.Name("city") ->
                  -\/(DocField(BsonField.Name("city"))))))),
            $sort(NonEmptyList(BsonField.Name("__sd_key_0") -> Ascending)),
            $project(Reshape(ListMap(
              BsonField.Name("city") ->
                -\/(DocField(BsonField.Name("__tmp0") \ BsonField.Name("city"))))),
              ExcludeId)))
    }

    "plan distinct with unrelated order by" in {
      plan("select distinct city from zips order by pop desc") must
        beWorkflow(
          chain(
              $read(Collection("zips")),
              $sort(NonEmptyList(
                BsonField.Name("pop") -> Descending)),
              $project(
                Reshape(ListMap(
                  BsonField.Name("city") ->
                    -\/(DocField(BsonField.Name("city"))),
                  BsonField.Name("__sd__0") ->
                    -\/(DocField(BsonField.Name("pop"))))),
                IgnoreId),
              $group(
                Grouped(ListMap(
                  BsonField.Name("__tmp0") ->
                    First(DocVar.ROOT()),
                  BsonField.Name("__sd_key_0") ->
                    First(DocField(BsonField.Name("__sd__0"))))),
                -\/(DocField(BsonField.Name("city")))),
              $sort(NonEmptyList(
                BsonField.Name("__sd_key_0") -> Descending)),
              $project(
                Reshape(ListMap(
                  BsonField.Name("city") ->
                    -\/(DocField(BsonField.Name("__tmp0") \ BsonField.Name("city"))))),
                IgnoreId)))
    }

    "plan distinct as function with group" in {
      plan("select state, count(distinct(city)) from zips group by state") must
        beWorkflow(
          $read(Collection("zips")))
    }.pendingUntilFixed

    "plan distinct with sum and group" in {
      plan("SELECT DISTINCT SUM(pop) AS totalPop, city FROM zips GROUP BY city") must
        beWorkflow(chain(
          $read(Collection("zips")),
          $group(
            Grouped(ListMap(
              BsonField.Name("totalPop") -> Sum(DocField(BsonField.Name("pop"))),
              BsonField.Name("city") -> Push(DocField(BsonField.Name("city"))))),
            -\/ (DocField(BsonField.Name("city")))),
          $unwind(DocField(BsonField.Name("city"))),
          $group(
            Grouped(ListMap(
              BsonField.Name("__tmp0") -> First(DocVar.ROOT()))),
            \/-(Reshape(ListMap(
              BsonField.Name("totalPop") ->
                -\/(DocField(BsonField.Name("totalPop"))),
              BsonField.Name("city") ->
                -\/(DocField(BsonField.Name("city"))))))),
          $project(
            Reshape(ListMap(
              BsonField.Name("totalPop") ->
                -\/(DocField(BsonField.Name("__tmp0") \ BsonField.Name("totalPop"))),
              BsonField.Name("city") ->
                -\/(DocField(BsonField.Name("__tmp0") \ BsonField.Name("city"))))),
              ExcludeId)))
    }

    "plan distinct with sum, group, and orderBy" in {
      plan("SELECT DISTINCT SUM(pop) AS totalPop, city FROM zips GROUP BY city ORDER BY totalPop DESC") must
        beWorkflow(
          chain(
              $read(Collection("zips")),
              $group(
                Grouped(ListMap(
                  BsonField.Name("totalPop") -> Sum(DocField(BsonField.Name("pop"))),
                  BsonField.Name("city") -> Push(DocField(BsonField.Name("city"))))),
                -\/(DocField(BsonField.Name("city")))),
              $unwind(DocField(BsonField.Name("city"))),
              $sort(NonEmptyList(BsonField.Name("totalPop") -> Descending)),
              $group(
                Grouped(ListMap(
                  BsonField.Name("__tmp0") -> First(DocVar.ROOT()),
                  BsonField.Name("__sd_key_0") -> First(DocField(BsonField.Name("totalPop"))))),
                \/-(Reshape(ListMap(
                  BsonField.Name("totalPop") -> -\/ (DocField(BsonField.Name("totalPop"))),
                  BsonField.Name("city") -> -\/ (DocField(BsonField.Name("city"))))))),
              $sort(NonEmptyList(BsonField.Name("__sd_key_0") -> Descending)),
              $project(Reshape(ListMap(
                BsonField.Name("totalPop") -> -\/(DocField(BsonField.Name("__tmp0") \ BsonField.Name("totalPop"))),
                BsonField.Name("city") -> -\/(DocField(BsonField.Name("__tmp0") \ BsonField.Name("city"))))),
                ExcludeId)))

    }

    "plan select length()" in {
      plan("select length(city) from zips") must
        beWorkflow(chain(
          $read(Collection("zips")),
          $simpleMap(JsMacro(x => Obj(ListMap(
            "0" -> Select(Select(x, "city").fix, "length").fix)).fix), Nil)))
    }

    "plan select length() and simple field" in {
      plan("select city, length(city) from zips") must
      beWorkflow(chain(
        $read(Collection("zips")),
        $simpleMap(JsMacro(value => Obj(ListMap(
          "city" -> Select(value, "city").fix,
          "1" -> Select(Select(value, "city").fix, "length").fix)).fix), Nil)))
    }

    "plan combination of two distinct sets" in {
      plan("SELECT (DISTINCT foo.bar) + (DISTINCT foo.baz) FROM foo") must
        beWorkflow(
          $read(Collection("zips")))
    }.pendingUntilFixed

    "plan expression with timestamp, date, time, and interval" in {
      import org.threeten.bp.{Instant, LocalDateTime, ZoneOffset}

      plan("select timestamp '2014-11-17T22:00:00Z' + interval 'PT43M40S', date '2015-01-19', time '14:21' from foo") must
        beWorkflow(
          $pure(Bson.Doc(ListMap(
            "0" -> Bson.Date(Instant.parse("2014-11-17T22:43:40Z")),
            "1" -> Bson.Date(LocalDateTime.parse("2015-01-19T00:00:00").atZone(ZoneOffset.UTC).toInstant),
            "2" -> Bson.Text("14:21:00.000")))))
    }

    "plan filter with timestamp and interval" in {
      import org.threeten.bp.Instant

      plan("select * from days where \"date\" < timestamp '2014-11-17T22:00:00Z' and \"date\" - interval 'PT12H' > timestamp '2014-11-17T00:00:00Z'") must
        beWorkflow(chain(
          $read(Collection("days")),
          $project(
            Reshape(ListMap(
              BsonField.Name("__tmp2") ->
                -\/(ExprOp.Subtract(
                  DocField(BsonField.Name("date")),
                  ExprOp.Literal(Bson.Dec(12*60*60*1000)))),
              BsonField.Name("__tmp3") -> -\/(DocVar.ROOT()))),
            IgnoreId),
          $match(
            Selector.And(
              Selector.Doc(
                BsonField.Name("__tmp3") \ BsonField.Name("date") ->
                  Selector.Lt(Bson.Date(Instant.parse("2014-11-17T22:00:00Z")))),
              Selector.Doc(
                BsonField.Name("__tmp2") ->
                  Selector.Gt(Bson.Date(Instant.parse("2014-11-17T00:00:00Z")))))),
          $project(Reshape(ListMap(
            BsonField.Name("value") ->
              -\/(DocField(BsonField.Name("__tmp3"))))),
            ExcludeId)))
    }

    "plan time_of_day" in {
      plan("select time_of_day(ts) from foo") must
        beRight  // NB: way too complicated to spell out here, and will change as JS generation improves
    }

    "plan filter on date" in {
      import org.threeten.bp.Instant

      // Note: both of these boundaries require comparing with the start of the *next* day.
      plan("select * from logs " +
        "where ((ts > date '2015-01-22' and ts <= date '2015-01-27') and ts != date '2015-01-25') " +
        "or ts = date '2015-01-29'") must
        beWorkflow(chain(
          $read(Collection("logs")),
          $match(Selector.Or(
            Selector.And(
              Selector.And(
                Selector.Doc(
                  BsonField.Name("ts") -> Selector.Gte(Bson.Date(Instant.parse("2015-01-23T00:00:00Z")))),
                Selector.Doc(
                  BsonField.Name("ts") -> Selector.Lt(Bson.Date(Instant.parse("2015-01-28T00:00:00Z"))))),
              Selector.Or(
                Selector.Doc(
                  BsonField.Name("ts") -> Selector.Lt(Bson.Date(Instant.parse("2015-01-25T00:00:00Z")))),
                Selector.Doc(
                  BsonField.Name("ts") -> Selector.Gte(Bson.Date(Instant.parse("2015-01-26T00:00:00Z")))))),
            Selector.And(
              Selector.Doc(
                BsonField.Name("ts") -> Selector.Gte(Bson.Date(Instant.parse("2015-01-29T00:00:00Z")))),
              Selector.Doc(
                BsonField.Name("ts") -> Selector.Lt(Bson.Date(Instant.parse("2015-01-30T00:00:00Z")))))))))
    }

    "plan js and filter with id" in {
      plan("select length(city) < oid '0123456789abcdef01234567' from days where _id = oid '0123456789abcdef01234567'") must
        beWorkflow(chain(
          $read(Collection("days")),
          $match(Selector.Doc(
            BsonField.Name("_id") -> Selector.Eq(Bson.ObjectId("0123456789abcdef01234567").toOption.get))),
          $simpleMap(
            JsMacro(x => Obj(ListMap(
              "__tmp0" -> Obj(ListMap(
                "0" -> BinOp(JsCore.Lt,
                  Select(Select(x, "city").fix, "length").fix,
                  New("ObjectId", List(JsCore.Literal(Js.Str("0123456789abcdef01234567")).fix)).fix).fix)).fix,
              "__tmp1" -> x)).fix),
            Nil),
          $project(Reshape(ListMap(
            BsonField.Name("0") ->
              -\/(DocField(BsonField.Name("__tmp0") \ BsonField.Name("0"))))),
            ExcludeId)))
    }

    def joinStructure(
      left: Workflow, leftName: String, right: Workflow,
      leftKey: ExprOp, rightKey: Term[JsCore],
      fin: WorkflowOp) = {
      def initialPipeOps(src: Workflow): Workflow =
        chain(
          src,
          $group(
            Grouped(ListMap(BsonField.Name(leftName) -> Push(DocVar.ROOT()))),
            -\/(leftKey)),
          $project(Reshape(ListMap(
            BsonField.Name("left")  -> -\/(DocField(BsonField.Name(leftName))),
            BsonField.Name("right") -> -\/(ExprOp.Literal(Bson.Arr(List()))),
            BsonField.Name("_id")   -> -\/(Include))),
            IncludeId))
      fin(
        $foldLeft(
          initialPipeOps(left),
          chain(
            right,
            $map($Map.mapKeyVal(("key", "value"),
              rightKey.toJs,
              Js.AnonObjDecl(List(
                ("left", Js.AnonElem(List())),
                ("right", Js.AnonElem(List(Js.Ident("value"))))))),
              ListMap()),
            $reduce(
              Js.AnonFunDecl(List("key", "values"),
                List(
                  Js.VarDef(List(
                    ("result", Js.AnonObjDecl(List(
                      ("left", Js.AnonElem(List())),
                      ("right", Js.AnonElem(List()))))))),
                  Js.Call(Js.Select(Js.Ident("values"), "forEach"),
                    List(Js.AnonFunDecl(List("value"),
                      List(
                        Js.BinOp("=",
                          Js.Select(Js.Ident("result"), "left"),
                          Js.Call(
                            Js.Select(Js.Select(Js.Ident("result"), "left"), "concat"),
                            List(Js.Select(Js.Ident("value"), "left")))),
                        Js.BinOp("=",
                          Js.Select(Js.Ident("result"), "right"),
                          Js.Call(
                            Js.Select(Js.Select(Js.Ident("result"), "right"), "concat"),
                            List(Js.Select(Js.Ident("value"), "right")))))))),
                  Js.Return(Js.Ident("result")))),
              ListMap()))))
    }

    "plan simple join" in {
      plan("select zips2.city from zips join zips2 on zips._id = zips2._id") must
        beWorkflow(
          joinStructure(
            $read(Collection("zips")), "__tmp0",
            $read(Collection("zips2")),
            DocField(BsonField.Name("_id")),
            Select(Ident("value").fix, "_id").fix,
            chain(_,
              $match(Selector.Doc(ListMap[BsonField, Selector.SelectorExpr](
                BsonField.Name("left") -> Selector.NotExpr(Selector.Size(0)),
                BsonField.Name("right") -> Selector.NotExpr(Selector.Size(0))))),
              $unwind(DocField(BsonField.Name("left"))),
              $unwind(DocField(BsonField.Name("right"))),
              $project(Reshape(ListMap(
                BsonField.Name("city") ->
                  -\/(DocField(BsonField.Name("right") \ BsonField.Name("city"))))),
                IgnoreId))))  // Note: becomes ExcludeId in conversion to WorkflowTask
    }

    "plan non-equi join" in {
      plan("select zips2.city from zips join zips2 on zips._id < zips2._id") must beLeft
    }

    "plan simple inner equi-join" in {
      plan(
        "select foo.name, bar.address from foo join bar on foo.id = bar.foo_id") must
      beWorkflow(
        joinStructure(
          $read(Collection("foo")), "__tmp0",
          $read(Collection("bar")),
          DocField(BsonField.Name("id")),
          Select(Ident("value").fix, "foo_id").fix,
          chain(_,
            $match(Selector.Doc(ListMap[BsonField, Selector.SelectorExpr](
              BsonField.Name("left") -> Selector.NotExpr(Selector.Size(0)),
              BsonField.Name("right") -> Selector.NotExpr(Selector.Size(0))))),
            $unwind(DocField(BsonField.Name("left"))),
            $unwind(DocField(BsonField.Name("right"))),
            $project(Reshape(ListMap(
              BsonField.Name("name") ->
                -\/(DocField(BsonField.Name("left") \ BsonField.Name("name"))),
              BsonField.Name("address") ->
                -\/(DocField(BsonField.Name("right") \ BsonField.Name("address"))))),
              IgnoreId))))  // Note: becomes ExcludeId in conversion to WorkflowTask
    }

    "plan simple outer equi-join with wildcard" in {
      plan("select * from foo full join bar on foo.id = bar.foo_id") must
      beWorkflow(
        joinStructure(
          $read(Collection("foo")), "__tmp0",
          $read(Collection("bar")),
          DocField(BsonField.Name("id")),
          Select(Ident("value").fix, "foo_id").fix,
          chain(_,
            $project(Reshape(ListMap(
              BsonField.Name("left") -> -\/(Cond(
                ExprOp.Eq(
                  Size(DocField(BsonField.Name("left"))),
                  ExprOp.Literal(Bson.Int32(0))),
                ExprOp.Literal(Bson.Arr(List(Bson.Doc(ListMap())))),
                DocField(BsonField.Name("left")))),
              BsonField.Name("right") -> -\/(Cond(
                ExprOp.Eq(
                  Size(DocField(BsonField.Name("right"))),
                  ExprOp.Literal(Bson.Int32(0))),
                ExprOp.Literal(Bson.Arr(List(Bson.Doc(ListMap())))),
                DocField(BsonField.Name("right")))))),
              IgnoreId),
            $unwind(DocField(BsonField.Name("left"))),
            $unwind(DocField(BsonField.Name("right"))),
            $simpleMap(JsMacro(x => Splice(List(Select(x, "left").fix, Select(x, "right").fix)).fix), Nil))))
    }

    "plan simple left equi-join" in {
      plan(
        "select foo.name, bar.address " +
          "from foo left join bar on foo.id = bar.foo_id") must
      beWorkflow(
        joinStructure(
          $read(Collection("foo")), "__tmp0",
          $read(Collection("bar")),
          DocField(BsonField.Name("id")),
          Select(Ident("value").fix, "foo_id").fix,
          chain(_,
            $match(Selector.Doc(ListMap[BsonField, Selector.SelectorExpr](
              BsonField.Name("left") -> Selector.NotExpr(Selector.Size(0))))),
            $project(Reshape(ListMap(
              BsonField.Name("left") -> -\/(DocField(BsonField.Name("left"))),
              BsonField.Name("right") -> -\/(Cond(
                ExprOp.Eq(
                  Size(DocField(BsonField.Name("right"))),
                  ExprOp.Literal(Bson.Int32(0))),
                ExprOp.Literal(Bson.Arr(List(Bson.Doc(ListMap())))),
                DocField(BsonField.Name("right")))))),
              IgnoreId),
            $unwind(DocField(BsonField.Name("left"))),
            $unwind(DocField(BsonField.Name("right"))),
            $project(Reshape(ListMap(
              BsonField.Name("name") -> -\/(DocField(BsonField.Name("left") \ BsonField.Name("name"))),
              BsonField.Name("address") -> -\/(DocField(BsonField.Name("right") \ BsonField.Name("address"))))),
              IgnoreId))))  // Note: becomes ExcludeId in conversion to WorkflowTask
    }

    "plan 3-way right equi-join" in {
      plan(
        "select foo.name, bar.address, baz.zip " +
          "from foo join bar on foo.id = bar.foo_id " +
          "right join baz on bar.id = baz.bar_id") must
      beWorkflow(
        joinStructure(
          joinStructure(
            $read(Collection("foo")), "__tmp0",
            $read(Collection("bar")),
            DocField(BsonField.Name("id")),
            Select(Ident("value").fix, "foo_id").fix,
            chain(_,
              $match(Selector.Doc(ListMap[BsonField, Selector.SelectorExpr](
                BsonField.Name("left") -> Selector.NotExpr(Selector.Size(0)),
                BsonField.Name("right") -> Selector.NotExpr(Selector.Size(0))))),
              $unwind(DocField(BsonField.Name("left"))),
              $unwind(DocField(BsonField.Name("right"))))),
          "__tmp1",
          $read(Collection("baz")),
          DocField(BsonField.Name("right") \ BsonField.Name("id")),
          Select(Ident("value").fix, "bar_id").fix,
          chain(_,
            $match(Selector.Doc(ListMap[BsonField, Selector.SelectorExpr](
              BsonField.Name("right") -> Selector.NotExpr(Selector.Size(0))))),
            $project(Reshape(ListMap(
              BsonField.Name("left") -> -\/(Cond(
                ExprOp.Eq(
                  Size(DocField(BsonField.Name("left"))),
                  ExprOp.Literal(Bson.Int32(0))),
                ExprOp.Literal(Bson.Arr(List(Bson.Doc(ListMap())))),
                DocField(BsonField.Name("left")))),
              BsonField.Name("right") -> -\/(DocField(BsonField.Name("right"))))),
              IgnoreId),
            $unwind(DocField(BsonField.Name("left"))),
            $unwind(DocField(BsonField.Name("right"))),
            $project(Reshape(ListMap(
              BsonField.Name("name") -> -\/(DocField(BsonField.Name("left") \ BsonField.Name("left") \ BsonField.Name("name"))),
              BsonField.Name("address") -> -\/(DocField(BsonField.Name("left") \ BsonField.Name("right") \ BsonField.Name("address"))),
              BsonField.Name("zip") -> -\/(DocField(BsonField.Name("right") \ BsonField.Name("zip"))))),
              IgnoreId))))  // Note: becomes ExcludeId in conversion to WorkflowTask
    }

    "plan simple cross" in {
      plan("select zips2.city from zips, zips2 where zips._id = zips2._id") must
      beWorkflow(
        joinStructure(
          $read(Collection("zips")), "__tmp2",
          $read(Collection("zips2")),
          ExprOp.Literal(Bson.Null),
          JsCore.Literal(Js.Null).fix,
          chain(_,
            $match(Selector.Doc(ListMap[BsonField, Selector.SelectorExpr](
              BsonField.Name("left") -> Selector.NotExpr(Selector.Size(0)),
              BsonField.Name("right") -> Selector.NotExpr(Selector.Size(0))))),
            $unwind(DocField(BsonField.Name("left"))),
            $unwind(DocField(BsonField.Name("right"))),
            $project(
              Reshape(ListMap(
                BsonField.Name("city") ->
                  -\/(DocField(BsonField.Name("right") \ BsonField.Name("city"))),
                BsonField.Name("__tmp3") ->
                  -\/(ExprOp.Eq(
                    DocField(BsonField.Name("left") \ BsonField.Name("_id")),
                    DocField(BsonField.Name("right") \ BsonField.Name("_id")))))),
              IgnoreId),
            $match(Selector.Doc(
              BsonField.Name("__tmp3") -> Selector.Eq(Bson.Bool(true)))),
            $project(Reshape(ListMap(
              BsonField.Name("city") -> -\/(DocField(BsonField.Name("city"))))),
              ExcludeId))))
    }

    def countOps(wf: Workflow, p: PartialFunction[WorkflowF[Term[WorkflowF]], Boolean]): Int = {
      wf.foldMap(op => if (p.lift(op.unFix).getOrElse(false)) 1 else 0)
    }

    def noConsecutiveProjectOps(wf: Workflow) =
      countOps(wf, { case $Project(Term($Project(_, _, _)), _, _) => true }) aka "the occurrences of consecutive $project ops:" must_== 0
    def noConsecutiveSimpleMapOps(wf: Workflow) =
      countOps(wf, { case $SimpleMap(Term($SimpleMap(_, _, _)), _, _) => true }) aka "the occurrences of consecutive $simpleMap ops:" must_== 0
    def maxGroupOps(wf: Workflow, max: Int) =
      countOps(wf, { case $Group(_, _, _) => true }) aka "the number of $group ops:" must beLessThanOrEqualTo(max)
    def maxUnwindOps(wf: Workflow, max: Int) =
      countOps(wf, { case $Unwind(_, _) => true }) aka "the number of $unwind ops:" must beLessThanOrEqualTo(max)
    def maxMatchOps(wf: Workflow, max: Int) =
      countOps(wf, { case $Match(_, _) => true }) aka "the number of $match ops:" must beLessThanOrEqualTo(max)
    def brokenProjectOps(wf: Workflow) =
      countOps(wf, { case $Project(_, Reshape(shape), _) => shape.isEmpty }) aka "$project ops with no fields"

    "plan multiple reducing projections (all, distinct)" ! Prop.forAll(select(distinct, maybeReducingExpr, Gen.option(filter), Gen.option(groupBySeveral), noOrderBy)) { q =>
      plan(q.value) must beRight.which { wf =>
        noConsecutiveProjectOps(wf)
        noConsecutiveSimpleMapOps(wf)
        maxGroupOps(wf, 2)
        maxUnwindOps(wf, 1)
        maxMatchOps(wf, 1)
        brokenProjectOps(wf) must_== 0
        val fields = fieldNames(wf)
        (fields aka "column order" must beSome(columnNames(q))) or
          (fields must beSome(List("value")))  // NB: some edge cases (all constant projections) end up under "value" and aren't interesting anyway
      }
    }.set(maxSize = 10)

    "plan multiple reducing projections (all)" ! Prop.forAll(select(notDistinct, maybeReducingExpr, Gen.option(filter), Gen.option(groupBySeveral), noOrderBy)) { q =>
      plan(q.value) must beRight.which { wf =>
        noConsecutiveProjectOps(wf)
        noConsecutiveSimpleMapOps(wf)
        maxGroupOps(wf, 1)
        maxUnwindOps(wf, 1)
        maxMatchOps(wf, 1)
        brokenProjectOps(wf) must_== 0
        fieldNames(wf) aka "column order" must beSome(columnNames(q))
      }
    }.set(maxSize = 10)

    // NB: tighter constraint because we know there's no filter.
    "plan multiple reducing projections (no filter)" ! Prop.forAll(select(notDistinct, maybeReducingExpr, noFilter, Gen.option(groupBySeveral), noOrderBy)) { q =>
      plan(q.value) must beRight.which { wf =>
        noConsecutiveProjectOps(wf)
        noConsecutiveSimpleMapOps(wf)
        maxGroupOps(wf, 1)
        maxUnwindOps(wf, 1)
        maxMatchOps(wf, 0)
        brokenProjectOps(wf) must_== 0
        fieldNames(wf) aka "column order" must beSome(columnNames(q))
      }
    }.set(maxSize = 10)
  }

  def columnNames(q: Query): List[String] =
    (new SQLParser).parse(q).toOption.map { case stmt @ sql.SelectStmt(_, _, _, _, _, _, _, _) => stmt.namedProjections(None).map(_._1) }.get
  def fieldNames(wf: Workflow): Option[List[String]] =
    Workflow.simpleShape(wf).map(_.map(_.asText))

  import sql.{Binop => _, Ident => _, Splice => _, _}

  val notDistinct = Gen.const(SelectAll)
  val distinct = Gen.const(SelectDistinct)

  val noGroupBy = Gen.const[Option[GroupBy]](None)
  val groupByCity = Gen.const(Some(GroupBy(List(sql.Ident("city")), None)))
  val groupBySeveral = Gen.nonEmptyListOf(Gen.oneOf(genInnerStr, genInnerInt)).map(keys => GroupBy(keys.distinct, None))

  val noFilter = Gen.const[Option[Expr]](None)
  val filter = Gen.oneOf(
    for {
      x <- genInnerInt
    } yield sql.Binop(x, IntLiteral(100), sql.Lt),
    for {
      x <- genInnerStr
    } yield InvokeFunction(StdLib.string.Like.name, List(x, StringLiteral("BOULDER%"), StringLiteral(""))),
    Gen.const(sql.Binop(sql.Ident("p"), sql.Ident("q"), sql.Eq)))  // Comparing two fields requires a $project before the $match

  val noOrderBy: Gen[Option[OrderBy]] = Gen.const(None)

  val maybeReducingExpr = Gen.oneOf(genOuterInt, genOuterStr)

  def select(distinctGen: Gen[IsDistinct], exprGen: Gen[Expr], filterGen: Gen[Option[Expr]], groupByGen: Gen[Option[GroupBy]], orderByGen: Gen[Option[OrderBy]]): Gen[Query] =
    for {
      distinct <- distinctGen
      projs    <- Gen.nonEmptyListOf(exprGen).map(_.zipWithIndex.map { case (x, n) => Proj.Named(x, "p" + n) })
      filter   <- filterGen
      groupBy  <- groupByGen
      orderBy  <- orderByGen
    } yield Query(SelectStmt(distinct, projs, Some(TableRelationAST("zips", None)), filter, groupBy, orderBy, None, None).sql)

  def genInnerInt = Gen.oneOf(
    sql.Ident("pop"),
    // IntLiteral(0),  // TODO: exposes bugs (see #476)
    sql.Binop(sql.Ident("pop"), IntLiteral(1), Minus), // an ExprOp
    InvokeFunction("length", List(sql.Ident("city"))))     // requires JS
  def genReduceInt = genInnerInt.flatMap(x => Gen.oneOf(
    x,
    InvokeFunction("min", List(x)),
    InvokeFunction("max", List(x)),
    InvokeFunction("sum", List(x)),
    InvokeFunction("count", List(sql.Splice(None)))))
  def genOuterInt = Gen.oneOf(
    Gen.const(IntLiteral(0)),
    genReduceInt,
    genReduceInt.flatMap(x => sql.Binop(x, IntLiteral(1000), sql.Div)))

  def genInnerStr = Gen.oneOf(
    sql.Ident("city"),
    // StringLiteral("foo"),  // TODO: exposes bugs (see #476)
    InvokeFunction("lower", List(sql.Ident("city"))))
  def genReduceStr = genInnerStr.flatMap(x => Gen.oneOf(
    x,
    InvokeFunction("min", List(x)),
    InvokeFunction("max", List(x))))
  def genOuterStr = Gen.oneOf(
    Gen.const(StringLiteral("foo")),
    genReduceStr,
    genReduceStr.flatMap(x => InvokeFunction("lower", List(x))),   // an ExprOp
    genReduceStr.flatMap(x => InvokeFunction("length", List(x))))  // requires JS

  implicit def shrinkQuery(implicit SS: Shrink[SelectStmt]): Shrink[Query] = Shrink { q =>
    (new SQLParser).parse(q).fold(κ(Stream.empty), SS.shrink(_).map(sel => Query(sel.sql)))
  }

  /**
   Shrink a query by reducing the number of projections or grouping expressions. Do not
   change the "shape" of the query, by removing the group by entirely, etc.
   */
  implicit def shrinkSelect: Shrink[SelectStmt] = {
    /** Shrink a list, removing a single item at a time, but never producing an empty list. */
    def shortened[A](as: List[A]): Stream[List[A]] =
      if (as.length <= 1) Stream.empty
      else as.toStream.map(a => as.filterNot(_ == a))

    Shrink {
      case sel @ SelectStmt(d, projs, rel, filter, groupBy, orderBy, limit, offset) =>
        val sDistinct = if (d == SelectDistinct) Stream(sel.copy(isDistinct = SelectAll)) else Stream.empty
        val sProjs = shortened(projs).map(ps => SelectStmt(d, ps, rel, filter, groupBy, orderBy, limit, offset))
        val sGroupBy = groupBy.map { case GroupBy(keys, having) =>
          shortened(keys).map(ks => SelectStmt(d, projs, rel, filter, Some(GroupBy(ks, having)), orderBy, limit, offset))
        }.getOrElse(Stream.empty)
        sDistinct ++ sProjs ++ sGroupBy
    }
  }

  "plan from LogicalPlan" should {
    import StdLib._

    "plan simple OrderBy" in {
      val lp =
        LogicalPlan.Let(
          'tmp0, read("foo"),
          LogicalPlan.Let(
            'tmp1, makeObj("bar" -> ObjectProject(Free('tmp0), Constant(Data.Str("bar")))),
            LogicalPlan.Let('tmp2,
              StdLib.set.OrderBy(
                Free('tmp1),
                MakeArrayN(ObjectProject(Free('tmp1), Constant(Data.Str("bar")))),
                MakeArrayN(Constant(Data.Str("ASC")))),
              Free('tmp2))))

      plan(lp) must beWorkflow(chain(
        $read(Collection("foo")),
        $sort(NonEmptyList(BsonField.Name("bar") -> Ascending)),
        $project(Reshape(ListMap(
          BsonField.Name("bar") -> -\/(DocField(BsonField.Name("bar"))))),
          IgnoreId)))
    }

    "plan OrderBy with expression" in {
      val lp =
        LogicalPlan.Let(
          'tmp0, read("foo"),
          StdLib.set.OrderBy(
            Free('tmp0),
            MakeArrayN(math.Divide(
              ObjectProject(Free('tmp0), Constant(Data.Str("bar"))),
              Constant(Data.Dec(10.0)))),
            MakeArrayN(Constant(Data.Str("ASC")))))

      plan(lp) must beWorkflow(chain(
        $read(Collection("foo")),
        $project(Reshape(ListMap(
          BsonField.Name("__tmp0") -> -\/(ExprOp.Divide(
            DocField(BsonField.Name("bar")),
            ExprOp.Literal(Bson.Dec(10.0)))),
          BsonField.Name("__tmp1") -> -\/(DocVar.ROOT()))),
          IgnoreId),
        $sort(NonEmptyList(BsonField.Name("__tmp0") -> Ascending)),
        $project(Reshape(ListMap(
          BsonField.Name("value") -> -\/(DocField(BsonField.Name("__tmp1"))))),
          ExcludeId)))
    }

    "plan OrderBy with expression and earlier pipeline op" in {
      val lp =
        LogicalPlan.Let(
          'tmp0, read("foo"),
          LogicalPlan.Let(
            'tmp1,
            StdLib.set.Filter(
              Free('tmp0),
              relations.Eq(
                ObjectProject(Free('tmp0), Constant(Data.Str("baz"))),
                Constant(Data.Int(0)))),
            StdLib.set.OrderBy(
              Free('tmp1),
              MakeArrayN(ObjectProject(Free('tmp1), Constant(Data.Str("bar")))),
              MakeArrayN(Constant(Data.Str("ASC"))))))

      plan(lp) must beWorkflow(chain(
        $read(Collection("foo")),
        $match(Selector.Doc(
          BsonField.Name("baz") -> Selector.Eq(Bson.Int64(0)))),
        $sort(NonEmptyList(BsonField.Name("bar") -> Ascending))))
    }

    "plan OrderBy with expression (and extra project)" in {
      val lp =
        LogicalPlan.Let(
          'tmp0, read("foo"),
          LogicalPlan.Let(
            'tmp9,
            makeObj(
              "bar" -> ObjectProject(Free('tmp0), Constant(Data.Str("bar")))),
            StdLib.set.OrderBy(
              Free('tmp9),
              MakeArrayN(math.Divide(
                ObjectProject(Free('tmp9), Constant(Data.Str("bar"))),
                Constant(Data.Dec(10.0)))),
              MakeArrayN(Constant(Data.Str("ASC"))))))

      plan(lp) must beWorkflow(chain(
        $read(Collection("foo")),
        $project(Reshape(ListMap(
          BsonField.Name("bar") -> -\/(DocField(BsonField.Name("bar"))),
          BsonField.Name("__tmp0") -> -\/(ExprOp.Divide(
            DocField(BsonField.Name("bar")),
            ExprOp.Literal(Bson.Dec(10.0)))))),
          IgnoreId),
        $sort(NonEmptyList(BsonField.Name("__tmp0") -> Ascending)),
        $project(Reshape(ListMap(
          BsonField.Name("bar") -> -\/(DocField(BsonField.Name("bar"))))),
          ExcludeId)))
    }
  }
}
