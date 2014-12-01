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

  val queryPlanner = MongoDbPlanner.queryPlanner(_ => Cord.empty)

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
            Grouped(ListMap(BsonField.Name("0") -> Sum(Literal(Bson.Int32(1))))),
            -\/(Literal(Bson.Null)))))
    }

    "plan simple field projection on single set" in {
      plan("select foo.bar from foo") must
        beWorkflow(chain(
          $read(Collection("foo")),
          $project(Reshape.Doc(ListMap(
            BsonField.Name("bar") -> -\/(DocField(BsonField.Name("bar"))))),
            IgnoreId)))
    }

    "plan simple field projection on single set when table name is inferred" in {
      plan("select bar from foo") must
       beWorkflow(chain(
         $read(Collection("foo")),
         $project(Reshape.Doc(ListMap(
           BsonField.Name("bar") -> -\/(DocField(BsonField.Name("bar"))))),
           IgnoreId)))
    }
    
    "plan multiple field projection on single set when table name is inferred" in {
      plan("select bar, baz from foo") must
       beWorkflow(chain(
         $read(Collection("foo")),
         $project(Reshape.Doc(ListMap(
           BsonField.Name("bar") -> -\/(DocField(BsonField.Name("bar"))),
           BsonField.Name("baz") -> -\/(DocField(BsonField.Name("baz"))))),
           IgnoreId)))
    }

    "plan simple addition on two fields" in {
      plan("select foo + bar from baz") must
       beWorkflow(chain(
         $read(Collection("baz")),
         $project(Reshape.Doc(ListMap(
           BsonField.Name("0") -> -\/ (ExprOp.Add(
             DocField(BsonField.Name("foo")),
             DocField(BsonField.Name("bar")))))),
           IgnoreId)))
    }
    
    "plan concat" in {
      plan("select concat(bar, baz) from foo") must
       beWorkflow(chain(
         $read(Collection("foo")),
         $project(Reshape.Doc(ListMap(
           BsonField.Name("0") -> -\/ (ExprOp.Concat(
             DocField(BsonField.Name("bar")),
             DocField(BsonField.Name("baz")),
             Nil)))),
           IgnoreId)))
    }

    "plan lower" in {
      plan("select lower(bar) from foo") must
      beWorkflow(chain(
        $read(Collection("foo")),
        $project(Reshape.Doc(ListMap(
          BsonField.Name("0") ->
            -\/(ExprOp.ToLower(DocField(BsonField.Name("bar")))))),
          IgnoreId)))
    }

    "plan coalesce" in {
      plan("select coalesce(bar, baz) from foo") must
       beWorkflow(chain(
         $read(Collection("foo")),
         $project(Reshape.Doc(ListMap(
           BsonField.Name("0") ->
             -\/(ExprOp.IfNull(
               DocField(BsonField.Name("bar")),
               DocField(BsonField.Name("baz")))))),
           IgnoreId)))
    }

    "plan date field extraction" in {
      plan("select date_part('day', baz) from foo") must
       beWorkflow(chain(
         $read(Collection("foo")),
         $project(Reshape.Doc(ListMap(
           BsonField.Name("0") ->
             -\/(ExprOp.DayOfMonth(DocField(BsonField.Name("baz")))))),
           IgnoreId)))
    }

    "plan complex date field extraction" in {
      plan("select date_part('quarter', baz) from foo") must
       beWorkflow(chain(
         $read(Collection("foo")),
         $project(Reshape.Doc(ListMap(
           BsonField.Name("0") ->
             -\/(
               ExprOp.Add(
                 ExprOp.Divide(
                   ExprOp.DayOfYear(DocField(BsonField.Name("baz"))),
                   ExprOp.Literal(Bson.Int32(92))),
                 ExprOp.Literal(Bson.Int32(1)))))),
           IgnoreId)))
    }

    "plan date field extraction: 'dow'" in {
      plan("select date_part('dow', baz) from foo") must
       beWorkflow(chain(
         $read(Collection("foo")),
         $project(Reshape.Doc(ListMap(
           BsonField.Name("0") ->
             -\/ (ExprOp.Add(
               ExprOp.DayOfWeek(ExprOp.DocField(BsonField.Name("baz"))),
               ExprOp.Literal(Bson.Int64(-1)))))),
           IgnoreId)))
    }

    "plan date field extraction: 'isodow'" in {
      plan("select date_part('isodow', baz) from foo") must
       beWorkflow(chain(
         $read(Collection("foo")),
         $project(Reshape.Doc(ListMap(
           BsonField.Name("0") ->
             -\/ (ExprOp.Cond(
               ExprOp.Eq(
                 ExprOp.DayOfWeek(ExprOp.DocField(BsonField.Name("baz"))),
                 ExprOp.Literal(Bson.Int64(1))),
               ExprOp.Literal(Bson.Int64(7)),
               ExprOp.Add(
                 ExprOp.DayOfWeek(ExprOp.DocField(BsonField.Name("baz"))),
                 ExprOp.Literal(Bson.Int64(-1))))))),
           IgnoreId)))
    }

    "plan filter array element" in {
      import JsCore._
      plan("select loc from zips where loc[0] < -73") must
      beWorkflow(chain(
        $read(Collection("zips")),
        $simpleMap(JsMacro(value => Obj(ListMap(
          "__tmp0" ->
            Access(Select(value, "loc").fix, Literal(Js.Num(0, false)).fix).fix,
          "__tmp1" -> value)).fix)),
        $match(Selector.Doc(BsonField.Name("__tmp0") -> Selector.Lt(Bson.Int64(-73)))),
        // FIXME: This match _could_ be implemented as below (without the
        //        $simpleMap) if it weren’t for Mongo’s broken index
        //        projections. Need to figure out how to recover this. (#455)
        // $match(Selector.Doc(
        //   BsonField.Name("loc") \ BsonField.Index(0) -> Selector.Lt(Bson.Int64(-73)))),
        $project(Reshape.Doc(ListMap(
          BsonField.Name("loc") ->
            -\/(ExprOp.DocField(BsonField.Name("__tmp1") \ BsonField.Name("loc"))))),
          IgnoreId)))
    }

    "plan select array element" in {
      import JsCore._
      plan("select loc[0] from zips") must
      beWorkflow(chain(
        $read(Collection("zips")),
        $simpleMap(JsMacro(value =>
          Access(Select(value, "loc").fix, Literal(Js.Num(0, false)).fix).fix)),
        $project(Reshape.Doc(ListMap(
          BsonField.Name("0") -> -\/(ExprOp.DocVar(DocVar.ROOT, None)))),
          IgnoreId)))
    }

    "plan array length" in {
      plan("select array_length(bar, 1) from foo") must
       beWorkflow(chain(
         $read(Collection("foo")),
         $project(Reshape.Doc(ListMap(
           BsonField.Name("0") ->
             -\/(ExprOp.Size(DocField(BsonField.Name("bar")))))),
           IgnoreId)))
    }

    "plan sum in expression" in {
      plan("select sum(pop) * 100 from zips") must
      beWorkflow(chain(
        $read(Collection("zips")),
        $group(
          Grouped(ListMap(BsonField.Name("__tmp0") ->
            Sum(ExprOp.DocField(BsonField.Name("pop"))))),
          -\/(Literal(Bson.Null))),
        $project(Reshape.Doc(ListMap(
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
         $project(Reshape.Doc(ListMap(
           BsonField.Name("0") ->
             -\/(Cond(
               Lt(
                 DocField(BsonField.Name("pop")),
                 ExprOp.Literal(Bson.Int64(10000))),
               DocField(BsonField.Name("city")),
               DocField(BsonField.Name("loc")))))),
           // FIXME: This should be ExcludeId, but this is _very_ minor.
           IncludeId)))
    }

    "plan negate" in {
      plan("select -bar from foo") must
       beWorkflow(chain(
         $read(Collection("foo")),
         $project(Reshape.Doc(ListMap(
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
         $project(Reshape.Doc(ListMap(
           BsonField.Name("0") -> -\/ (ExprOp.Add(
             DocField(BsonField.Name("a")),
             DocField(BsonField.Name("b")))))),
           IgnoreId)))
    }

    "plan simple js filter" in {
      import JsCore._
      plan("select * from zips where length(city) < 4") must
      beWorkflow(chain(
        $read(Collection("zips")),
        // FIXME: Inline this $simpleMap with the $match (#454)
        $simpleMap(JsMacro(x => Obj(ListMap(
          "__tmp0" -> x,
          "__tmp1" -> Select(Select(x, "city").fix, "length").fix)).fix)),
        $match(Selector.Doc(
          BsonField.Name("__tmp1") -> Selector.Lt(Bson.Int64(4)))),
        $project(Reshape.Doc(ListMap(
          BsonField.Name("value") -> -\/(DocField(BsonField.Name("__tmp0"))))),
          ExcludeId)))
    }

    "plan filter with js and non-js" in {
      import JsCore._
      plan("select * from zips where length(city) < 4 and pop < 20000") must
      beWorkflow(chain(
        $read(Collection("zips")),
        // FIXME: Inline this $simpleMap with the $match (#454)
        $simpleMap(JsMacro(value => Obj(ListMap(
          "__tmp2" -> value,
          "__tmp3" -> Select(Select(value, "city").fix, "length").fix,
          "__tmp4" -> Select(value, "pop").fix)).fix)),
        $match(Selector.And(
          Selector.Doc(BsonField.Name("__tmp3") -> Selector.Lt(Bson.Int64(4))),
          Selector.Doc(BsonField.Name("__tmp4") -> Selector.Lt(Bson.Int64(20000))))),
        $project(Reshape.Doc(ListMap(
          BsonField.Name("value") ->
            -\/(DocField(BsonField.Name("__tmp2"))))),
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
        $simpleMap(JsMacro(x => JsCore.Obj(ListMap(
          "__tmp2" -> x,
          "__tmp3" -> JsCore.Call(
            JsCore.Select(JsCore.New("RegExp", List(JsCore.Select(x, "pattern").fix)).fix, "test").fix,
            List(JsCore.Literal(Js.Str("foo")).fix)).fix,
          "__tmp4" -> JsCore.Call(
            JsCore.Select(JsCore.New("RegExp", List(JsCore.Select(x, "pattern").fix)).fix, "test").fix,
            List(JsCore.Select(x, "target").fix)).fix
        )).fix)),
        $match(
          Selector.Or(
            Selector.Doc(
              BsonField.Name("__tmp3") -> Selector.Eq(Bson.Bool(true))),
            Selector.Doc(
              BsonField.Name("__tmp4") -> Selector.Eq(Bson.Bool(true))))),
        $project(
          Reshape.Doc(ListMap(
            BsonField.Name("value") -> -\/(DocField(BsonField.Name("__tmp2"))))),
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

    "plan simple having filter" in {
      plan("select city from zips group by city having count(*) > 10") must
      beWorkflow(chain(
        $read(Collection("zips")),
        $group(Grouped(ListMap(
          BsonField.Name ("__tmp0") -> Push(DocVar.ROOT()),
          BsonField.Name ("__tmp1") -> Sum(Literal(Bson.Int32(1))))),
          -\/(DocField(BsonField.Name("city")))),
        $unwind(DocField(BsonField.Name("__tmp0"))),
        $match(Selector.Doc(
          BsonField.Name("__tmp1") -> Selector.Gt(Bson.Int64(10)))),
        $project(Reshape.Doc(ListMap(
          BsonField.Name("city") ->
            -\/(DocField(BsonField.Name("__tmp0") \ BsonField.Name("city"))))),
          IgnoreId)))
    }

    "prefer projection+filter over JS filter" in {
      plan("select * from zips where city <> state") must
      beWorkflow(chain(
        $read(Collection("zips")),
        $project(
          Reshape.Doc(ListMap(
            BsonField.Name("__tmp0") -> -\/(DocVar.ROOT()),
            BsonField.Name("__tmp1") ->
                -\/(Neq(
                  DocField(BsonField.Name("city")),
                  DocField(BsonField.Name("state")))))),
          IgnoreId),
        $match(Selector.Doc(
          BsonField.Name("__tmp1") -> Selector.Eq(Bson.Bool(true)))),
        $project(Reshape.Doc(ListMap(
          BsonField.Name("value") -> -\/(DocField(BsonField.Name("__tmp0"))))),
          ExcludeId)))
    }

    "prefer projection+filter over nested JS filter" in {
      plan("select * from zips where city <> state and pop < 10000") must
      beWorkflow(chain(
        $read(Collection("zips")),
        $project(
          Reshape.Doc(ListMap(
            BsonField.Name("__tmp2") -> -\/(DocVar.ROOT()),
            BsonField.Name("__tmp3") ->
                -\/(Neq(
                  DocField(BsonField.Name("city")),
                  DocField(BsonField.Name("state")))),
            BsonField.Name("__tmp4") -> -\/(DocField(BsonField.Name("pop"))))),
          IgnoreId),
        $match(Selector.And(
          Selector.Doc(
            BsonField.Name("__tmp3") -> Selector.Eq(Bson.Bool(true))),
          Selector.Doc(
            BsonField.Name("__tmp4") -> Selector.Lt(Bson.Int64(10000))))),
        $project(Reshape.Doc(ListMap(
          BsonField.Name("value") -> -\/(DocField(BsonField.Name("__tmp2"))))),
          ExcludeId)))
    }

    "filter on constant" in {
      plan("select * from zips where true") must
      beWorkflow(chain(
        $read(Collection("zips")),
        $project(Reshape.Doc(ListMap(
          BsonField.Name("__tmp0") -> -\/(Literal(Bson.Bool(true))),
          BsonField.Name("__tmp1") -> -\/(DocVar.ROOT()))),
          ExcludeId),
        $match(Selector.Doc(
          BsonField.Name("__tmp0") -> Selector.Eq(Bson.Bool(true)))),
        $project(Reshape.Doc(ListMap(
          BsonField.Name("value") -> -\/(DocField(BsonField.Name("__tmp1"))))),
          ExcludeId)))
    }

    "plan simple sort with field in projection" in {
      plan("select bar from foo order by bar") must
        beWorkflow(chain(
          $read(Collection("foo")),
          $project(
            Reshape.Doc(ListMap(
              BsonField.Name("bar") ->
                -\/(ExprOp.DocField(BsonField.Name("bar"))))),
            IgnoreId),
          $sort(NonEmptyList(BsonField.Name("bar") -> Ascending))))
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
          $project(Reshape.Doc(ListMap(
            BsonField.Name("baz") ->
              -\/(ExprOp.DocField(BsonField.Name("baz"))),
            BsonField.Name("__sd__0") ->
              -\/(ExprOp.Divide(
                ExprOp.DocField(BsonField.Name("bar")),
                ExprOp.Literal(Bson.Int64(10)))))),
            IgnoreId),
          $sort(NonEmptyList(BsonField.Name("__sd__0") -> Ascending)),
          $project(Reshape.Doc(ListMap(
            BsonField.Name("baz") ->
              -\/(ExprOp.DocField(BsonField.Name("baz"))))),
            IgnoreId)))
    }

    "plan select with wildcard and field" in {
      import Js._

      plan("select *, pop from zips") must
        beWorkflow(chain(
          $read(Collection("zips")),
          $project(
            Reshape.Doc(ListMap(
              BsonField.Name("__tmp1") -> -\/(DocVar.ROOT()),
              BsonField.Name("pop") -> -\/(DocField(BsonField.Name("pop"))))),
            IgnoreId),
          $map($Map.mapMap("__tmp0",
            Call(AnonFunDecl(List("rez"),
              List(
                ForIn(
                  Ident("attr"),
                  Select(Ident("__tmp0"), "__tmp1"),
                  If(
                    Call(
                      Select(Select(Ident("__tmp0"), "__tmp1"),
                        "hasOwnProperty"),
                      List(Ident("attr"))),
                    BinOp("=",
                      Access(Ident("rez"), Ident("attr")),
                      Access(Select(Ident("__tmp0"), "__tmp1"),
                        Ident("attr"))),
                    None)),
                BinOp("=",
                  Access(Ident("rez"), Str("pop")),
                  Select(Ident("__tmp0"), "pop")),
                Return(Ident("rez")))),
              List(AnonObjDecl(Nil)))))))
    }

    "plan sort with wildcard and expression in key" in {
      import Js._

      plan("select * from zips order by pop/10 desc") must
        beWorkflow(chain(
          $read(Collection("zips")),
          $project(
            Reshape.Doc(ListMap(
              BsonField.Name("__tmp1") -> -\/(DocVar.ROOT()),
              BsonField.Name("__sd__0") -> -\/(ExprOp.Divide(
                  ExprOp.DocField(BsonField.Name("pop")),
                  ExprOp.Literal(Bson.Int64(10)))))),
            IgnoreId),
          $map($Map.mapMap("__tmp0",
            Call(AnonFunDecl(List("rez"),
              List(
                ForIn(Ident("attr"),Select(Ident("__tmp0"), "__tmp1"),If(Call(Select(Select(Ident("__tmp0"), "__tmp1"), "hasOwnProperty"),List(Ident("attr"))),BinOp("=",Access(Ident("rez"),Ident("attr")),Access(Select(Ident("__tmp0"), "__tmp1"),Ident("attr"))),None)),
                BinOp("=",Access(Ident("rez"),Str("__sd__0")),Select(Ident("__tmp0"), "__sd__0")), Return(Ident("rez")))),
              List(AnonObjDecl(Nil))))),
          $sort(NonEmptyList(BsonField.Name("__sd__0") -> Descending))))
    }
    
    "plan simple sort with field not in projections" in {
      plan("select name from person order by height") must
        beWorkflow(chain(
          $read(Collection("person")),
          $project(
            Reshape.Doc(ListMap(
              BsonField.Name("name") ->
                -\/(ExprOp.DocField(BsonField.Name("name"))),
              BsonField.Name("__sd__0") ->
                -\/(ExprOp.DocField(BsonField.Name("height"))))),
            IgnoreId),
          $sort(NonEmptyList(BsonField.Name("__sd__0") -> Ascending)),
          $project(Reshape.Doc(ListMap(
            BsonField.Name("name") ->
              -\/(ExprOp.DocField(BsonField.Name("name"))))),
            IgnoreId)))
    }
    
    "plan sort with expression and alias" in {
      plan("select pop/1000 as popInK from zips order by popInK") must
        beWorkflow(chain(
          $read(Collection("zips")),
          $project(Reshape.Doc(ListMap(
            BsonField.Name("popInK") ->
              -\/(ExprOp.Divide(
                ExprOp.DocField(BsonField.Name("pop")),
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
          $project(Reshape.Doc(ListMap(
            BsonField.Name("city") ->
              -\/(ExprOp.DocField(BsonField.Name("city"))),
            BsonField.Name("pop") ->
              -\/(ExprOp.DocField(BsonField.Name("pop"))))),
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
          $project(Reshape.Doc(ListMap(
            BsonField.Name("popInK") ->
              -\/(ExprOp.Divide(
                ExprOp.DocField(BsonField.Name("pop")),
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
                BsonField.Name("city") -> ExprOp.Push(ExprOp.DocField(BsonField.Name("city"))),
                BsonField.Name("cnt") -> ExprOp.Sum(ExprOp.Literal(Bson.Int32(1))))),
              -\/ (ExprOp.Literal(Bson.Null))),
            $unwind(ExprOp.DocField(BsonField.Name("city"))),
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
                BsonField.Name("cnt") ->
                  ExprOp.Sum(ExprOp.Literal(Bson.Int32(1))),
                BsonField.Name("__tmp0") ->
                  ExprOp.Push(ExprOp.DocVar.ROOT()))),
              -\/(ExprOp.Literal(Bson.Null))),
            $unwind(ExprOp.DocField(BsonField.Name("__tmp0"))),
            $simpleMap(JsMacro(x => JsCore.Obj(ListMap(
              "cnt" -> JsCore.Select(x, "cnt").fix,
              "1" -> JsCore.Select(JsCore.Select(JsCore.Select(x, "__tmp0").fix, "city").fix, "length").fix)).fix)))
        }
    }

    "plan trivial group by" in {
      plan("select city from zips group by city") must
      beWorkflow(chain(
        $read(Collection("zips")),
        $group(
          Grouped(ListMap(
            BsonField.Name("city") ->
              ExprOp.Push(ExprOp.DocField(BsonField.Name("city"))))),
          -\/ (ExprOp.DocField(BsonField.Name("city")))),
        $unwind(ExprOp.DocField(BsonField.Name("city")))))
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
              BsonField.Name("0") -> ExprOp.Sum(ExprOp.Literal(Bson.Int32(1))))),
              -\/(ExprOp.DocField(BsonField.Name("baz")))))
        }
    }

    "plan count and sum grouped by single field" in {
      plan("select count(*) as cnt, sum(biz) as sm from bar group by baz") must
        beWorkflow {
          chain(
            $read(Collection("bar")),
            $group(
              Grouped(ListMap(
                BsonField.Name("cnt") -> ExprOp.Sum(ExprOp.Literal(Bson.Int32(1))),
                BsonField.Name("sm") -> ExprOp.Sum(ExprOp.DocField(BsonField.Name("biz"))))),
              -\/ (ExprOp.DocField(BsonField.Name("baz")))))
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
                BsonField.Name("sm") -> ExprOp.Sum(ExprOp.DocField(BsonField.Name("pop"))))),
              -\/ (ExprOp.DocField(BsonField.Name("city")))))
        }
    }

    "plan count and field when grouped" in {
      plan("select count(*) as cnt, city from zips group by city") must
        beWorkflow {
          chain(
            $read(Collection("zips")),
            $group(
              Grouped(ListMap(
                BsonField.Name("cnt") -> ExprOp.Sum(ExprOp.Literal(Bson.Int32(1))),
                BsonField.Name("city") -> ExprOp.Push(ExprOp.DocField(BsonField.Name("city"))))),
              -\/(ExprOp.DocField(BsonField.Name("city")))),
            $unwind(ExprOp.DocField(BsonField.Name("city"))))
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
          -\/(Literal(Bson.Null))),
        $unwind(DocField(BsonField.Name("__tmp0"))),
        $project(Reshape.Doc(ListMap(
          BsonField.Name ("2") -> -\/(DocField(BsonField.Name("2"))),
          BsonField.Name ("city") ->
            -\/(DocField(BsonField.Name("__tmp0") \ BsonField.Name("city"))),
          BsonField.Name ("state") ->
            -\/(DocField(BsonField.Name("__tmp0") \ BsonField.Name("state"))))),
          IgnoreId)))
    }

    "plan sum of expression in expression with another projection when grouped" in {
      plan("select city, sum(pop-1)/1000 from zips group by city") must
      beWorkflow(chain(
        $read(Collection("zips")),
        $group(
          Grouped(ListMap(
            BsonField.Name("city") -> Push(DocField(BsonField.Name("city"))),
            BsonField.Name("__tmp0") -> Sum(ExprOp.Subtract(DocField(BsonField.Name("pop")), Literal(Bson.Int64(1)))))),
          -\/(DocField(BsonField.Name("city")))),
        $unwind(ExprOp.DocField(BsonField.Name("city"))),
        $project(
          Reshape.Doc(ListMap(
            BsonField.Name("city") -> -\/(ExprOp.DocField(BsonField.Name("city"))),
            BsonField.Name("1") ->
              -\/(ExprOp.Divide(
                DocField(BsonField.Name("__tmp0")),
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
          $unwind(ExprOp.DocField(BsonField.Name("state"))),
          $simpleMap(JsMacro(x => JsCore.Obj(ListMap(
            "state" -> JsCore.Select(x, "state").fix,
            "shortest" -> JsCore.Select(JsCore.Select(x, "__tmp0").fix, "length").fix)).fix))))
    }
    
    "plan simple JS inside expression" in {
      plan("select length(city) + 1 from zips") must 
        beWorkflow(chain(
          $read(Collection("zips")),
          $simpleMap(JsMacro(x => JsCore.Obj(ListMap(
            "0" -> JsCore.BinOp("+", 
              JsCore.Select(JsCore.Select(x, "city").fix, "length").fix,
              JsCore.Literal(Js.Num(1, false)).fix).fix)).fix))))
    }

    "plan expressions with ~"in {
      plan("select foo ~ 'bar.*', 'abc' ~ 'a|b', 'baz' ~ regex, target ~ regex from a").disjunction must beRightDisjOrDiff(chain(
        $read(Collection("a")),
        $simpleMap(JsMacro(x =>
          JsCore.Obj(ListMap(
            "0" -> JsCore.Call(
              JsCore.Select(JsCore.New("RegExp", List(JsCore.Literal(Js.Str("bar.*")).fix)).fix, "test").fix,
              List(JsCore.Select(x, "foo").fix)).fix,
            "1" -> JsCore.Literal(Js.Bool(true)).fix,
            "2" -> JsCore.Call(
              JsCore.Select(JsCore.New("RegExp", List(JsCore.Select(x, "regex").fix)).fix, "test").fix,
              List(JsCore.Literal(Js.Str("baz")).fix)).fix,
            "3" -> JsCore.Call(
              JsCore.Select(JsCore.New("RegExp", List(JsCore.Select(x, "regex").fix)).fix, "test").fix,
              List(JsCore.Select(x, "target").fix)).fix)).fix))))
    }

    "plan object flatten" in {
      import Js._

      plan("select geo{*} from usa_factbook") must
        beWorkflow {
          chain(
            $read(Collection("usa_factbook")),
            $flatMap(
              AnonFunDecl(List("key", "value"), List(
                VarDef(List("rez" -> AnonElem(Nil))),
                ForIn(
                  Ident("attr"),
                  Select(Ident("value"), "geo"),
                  Call(
                    Select(Ident("rez"), "push"),
                    List(
                      AnonElem(List(
                        Call(Ident("ObjectId"), Nil),
                        Access(
                          Select(Ident("value"), "geo"),
                          Ident("attr"))))))),
                Return(Ident("rez"))))),
            $project(Reshape.Doc(ListMap(
              BsonField.Name("geo") -> -\/(ExprOp.DocVar(DocVar.ROOT, None)))),
              IgnoreId))
        }
    }

    "plan array project with concat" in {
      import JsCore._
      plan("select city, loc[0] from zips") must
        beWorkflow {
          chain(
            $read(Collection("zips")),
            $simpleMap(JsMacro(value => Obj(ListMap(
              "__tmp0" ->
                JsCore.Access(JsCore.Select(value, "loc").fix,
                  Literal(Js.Num(0, false)).fix).fix,
              "__tmp1" -> value)).fix)),
            $project(Reshape.Doc(ListMap(
              BsonField.Name("city") ->
                -\/(ExprOp.DocField(BsonField.Name("__tmp1") \ BsonField.Name("city"))),
              BsonField.Name("1") ->
                -\/(ExprOp.DocField(BsonField.Name("__tmp0"))))),
              IgnoreId))
        }
    }

    "plan array flatten" in {
      plan("select loc[*] from zips") must
        beWorkflow {
          chain(
            $read(Collection("zips")),
            $unwind(ExprOp.DocField(BsonField.Name("loc"))),
            $project(Reshape.Doc(ListMap(
              BsonField.Name("loc") ->
                -\/(ExprOp.DocField(BsonField.Name("loc"))))),
              IgnoreId))  // Note: becomes ExcludeId in conversion to WorkflowTask
        }
    }

    "plan limit with offset" in {
      plan("SELECT * FROM zips LIMIT 5 OFFSET 100") must
        beWorkflow(chain($read(Collection("zips")), $limit(105), $skip(100)))
    }

    "plan filter and limit" in {
      plan("SELECT city, pop FROM zips ORDER BY pop DESC LIMIT 5") must
        beWorkflow {
          chain(
            $read(Collection("zips")),
            $project(Reshape.Doc(ListMap(
              BsonField.Name("city") ->
                -\/(ExprOp.DocField(BsonField.Name("city"))),
              BsonField.Name("pop") ->
                -\/(ExprOp.DocField(BsonField.Name("pop"))))),
              IgnoreId),
            $sort(NonEmptyList(BsonField.Name("pop") -> Descending)),
            $limit(5))
        }
    }

    "plan simple single field selection and limit" in {
      plan("SELECT city FROM zips LIMIT 5") must
        beWorkflow {
          chain(
            $read(Collection("zips")),
            $project(Reshape.Doc(ListMap(BsonField.Name("city") -> -\/ (ExprOp.DocField(BsonField.Name("city"))))),
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
            Reshape.Doc(ListMap(
              BsonField.Name("0") -> -\/(Eq(ExprOp.DocField(BsonField.Name("foo")), Literal(Bson.Null))))),
            IgnoreId)))
    }

    "plan simple distinct" in {
      plan("select distinct city, state from zips") must 
      beWorkflow(
        chain(
          $read(Collection("zips")),
          $project(Reshape.Doc(ListMap(
              BsonField.Name("city") -> -\/ (ExprOp.DocField(BsonField.Name("city"))),
              BsonField.Name("state") -> -\/ (ExprOp.DocField(BsonField.Name("state"))))),
            IgnoreId),
          $group(
            Grouped(ListMap(
              BsonField.Name("__tmp0") -> ExprOp.First(ExprOp.DocVar.ROOT()))),
              \/- (Reshape.Arr(ListMap(
                BsonField.Index(0) -> -\/ (ExprOp.DocField(BsonField.Name("city"))),
                BsonField.Index(1) -> -\/ (ExprOp.DocField(BsonField.Name("state"))))))),
          $project(Reshape.Doc(ListMap(
              BsonField.Name("city") -> -\/ (ExprOp.DocField(BsonField.Name("__tmp0") \ BsonField.Name("city"))),
              BsonField.Name("state") -> -\/ (ExprOp.DocField(BsonField.Name("__tmp0") \ BsonField.Name("state"))))),
            ExcludeId)))
    }

    "plan distinct as expression" in {
      plan("select count(distinct(city)) from zips") must 
        beWorkflow(chain(
          $read(Collection("zips")),
          $group(
            Grouped(ListMap()),
            -\/(ExprOp.DocField(BsonField.Name("city")))),
          $group(
            Grouped(ListMap(
              BsonField.Name("0") ->
                ExprOp.Sum(ExprOp.Literal(Bson.Int32(1))))),
            -\/(ExprOp.Literal(Bson.Null)))))
    }

    "plan distinct of expression as expression" in {
      plan("select count(distinct substring(city, 0, 1)) from zips") must 
        beWorkflow(chain(
          $read(Collection("zips")),
          $group(
            Grouped(ListMap()),
            -\/(ExprOp.Substr(
              ExprOp.DocField(BsonField.Name("city")),
              ExprOp.Literal(Bson.Int64(0)),
              ExprOp.Literal(Bson.Int64(1))))),
          $group(
            Grouped(ListMap(
              BsonField.Name("0") ->
                ExprOp.Sum(ExprOp.Literal(Bson.Int32(1))))),
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
              $project(Reshape.Doc(ListMap(
                BsonField.Name("city") ->
                  -\/(ExprOp.DocField(BsonField.Name("city"))))),
                IgnoreId),
              $sort(NonEmptyList(
                BsonField.Name("city") -> Ascending)),
              $group(
                Grouped(ListMap(
                  BsonField.Name("__tmp0") -> ExprOp.First(ExprOp.DocVar.ROOT()),
                  BsonField.Name("__sd_key_0") -> ExprOp.First(ExprOp.DocField(BsonField.Name("city"))))),
                \/-(Reshape.Arr(ListMap(
                  BsonField.Index(0) ->
                    -\/(ExprOp.DocField(BsonField.Name("city"))))))),
              $sort(NonEmptyList(BsonField.Name("__sd_key_0") -> Ascending)),
              $project(Reshape.Doc(ListMap(
                BsonField.Name("city") -> -\/(ExprOp.DocField(BsonField.Name("__tmp0") \ BsonField.Name("city"))))),
                ExcludeId)))
    }

    "plan distinct with unrelated order by" in {
      plan("select distinct city from zips order by pop desc") must
        beWorkflow(
          chain(
              $read(Collection("zips")),
              $project(
                Reshape.Doc(ListMap(
                  BsonField.Name("city") ->
                    -\/(ExprOp.DocField(BsonField.Name("city"))),
                  BsonField.Name("__sd__0") ->
                    -\/(ExprOp.DocField(BsonField.Name("pop"))))),
                IgnoreId),
              $sort(NonEmptyList(
                BsonField.Name("__sd__0") -> Descending)),
              $group(
                Grouped(ListMap(
                  BsonField.Name("__tmp0") ->
                    ExprOp.First(ExprOp.DocVar.ROOT()),
                  BsonField.Name("__sd_key_0") ->
                    ExprOp.First(ExprOp.DocField(BsonField.Name("__sd__0"))))),
                -\/(ExprOp.DocField(BsonField.Name("city")))),
              $sort(NonEmptyList(
                BsonField.Name("__sd_key_0") -> Descending)),
              $project(
                Reshape.Doc(ListMap(
                  BsonField.Name("city") ->
                    -\/(ExprOp.DocField(BsonField.Name("__tmp0") \ BsonField.Name("city"))))),
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
              BsonField.Name("totalPop") -> ExprOp.Sum(ExprOp.DocField(BsonField.Name("pop"))),
              BsonField.Name("city") -> ExprOp.Push(ExprOp.DocField(BsonField.Name("city"))))),
            -\/ (ExprOp.DocField(BsonField.Name("city")))),
          $unwind(ExprOp.DocField(BsonField.Name("city"))),
          $group(
            Grouped(ListMap(
              BsonField.Name("__tmp0") -> ExprOp.First(ExprOp.DocVar.ROOT()))),
            \/-(Reshape.Arr(ListMap(
              BsonField.Index(0) ->
                -\/(ExprOp.DocField(BsonField.Name("totalPop"))),
              BsonField.Index(1) ->
                -\/(ExprOp.DocField(BsonField.Name("city"))))))),
          $project(
            Reshape.Doc(ListMap(
              BsonField.Name("totalPop") ->
                -\/(ExprOp.DocField(BsonField.Name("__tmp0") \ BsonField.Name("totalPop"))),
              BsonField.Name("city") ->
                -\/(ExprOp.DocField(BsonField.Name("__tmp0") \ BsonField.Name("city"))))),
              ExcludeId)))
    }
    
    "plan distinct with sum, group, and orderBy" in {
      plan("SELECT DISTINCT SUM(pop) AS totalPop, city FROM zips GROUP BY city ORDER BY totalPop DESC") must
        beWorkflow(
          chain(
              $read(Collection("zips")),
              $group(
                Grouped(ListMap(
                  BsonField.Name("totalPop") -> ExprOp.Sum(ExprOp.DocField(BsonField.Name("pop"))),
                  BsonField.Name("city") -> ExprOp.Push(ExprOp.DocField(BsonField.Name("city"))))),
                -\/ (ExprOp.DocField(BsonField.Name("city")))),
              $unwind(ExprOp.DocField(BsonField.Name("city"))),
              $sort(NonEmptyList(BsonField.Name("totalPop") -> Descending)),
              $group(
                Grouped(ListMap(
                  BsonField.Name("__tmp0") -> ExprOp.First(ExprOp.DocVar.ROOT()),
                  BsonField.Name("__sd_key_0") -> ExprOp.First(ExprOp.DocField(BsonField.Name("totalPop"))))),
                \/-(Reshape.Arr(ListMap(
                  BsonField.Index(0) -> -\/ (ExprOp.DocField(BsonField.Name("totalPop"))),
                  BsonField.Index(1) -> -\/ (ExprOp.DocField(BsonField.Name("city"))))))),
              $sort(NonEmptyList(BsonField.Name("__sd_key_0") -> Descending)),
              $project(Reshape.Doc(ListMap(
                BsonField.Name("totalPop") -> -\/(ExprOp.DocField(BsonField.Name("__tmp0") \ BsonField.Name("totalPop"))),
                BsonField.Name("city") -> -\/(ExprOp.DocField(BsonField.Name("__tmp0") \ BsonField.Name("city"))))),
                ExcludeId)))

    }
    
    "plan select length()" in {
      plan("select length(city) from zips") must
        beWorkflow(chain(
          $read(Collection("zips")),
          $simpleMap(JsMacro(x => JsCore.Obj(ListMap(
            "0" ->
              JsCore.Select(JsCore.Select(x, "city").fix, "length").fix)).fix))))
    }
    
    "plan select length() and simple field" in {
      plan("select city, length(city) from zips") must
      beWorkflow(chain(
        $read(Collection("zips")),
        $simpleMap(JsMacro(value => JsCore.Obj(ListMap(
          "city" -> JsCore.Select(value, "city").fix,
          "1" -> JsCore.Select(JsCore.Select(value, "city").fix, "length").fix)).fix))))
    }
    
    "plan combination of two distinct sets" in {
      plan("SELECT (DISTINCT foo.bar) + (DISTINCT foo.baz) FROM foo") must
        beWorkflow(
          $read(Collection("zips")))
    }.pendingUntilFixed
    
    "plan expression with timestamp and interval" in {
      import org.threeten.bp.Instant
      
      plan("select timestamp '2014-11-17T22:00:00Z' + interval 'PT43M40S' from foo") must
        beWorkflow(chain(
          $pure(Bson.Date(Instant.parse("2014-11-17T22:00:00Z"))),
          $project(
            Reshape.Doc(ListMap(
              BsonField.Name("0") -> -\/(ExprOp.Add(
                ExprOp.DocVar.ROOT(),
                ExprOp.Literal(Bson.Dec(((43*60) + 40)*1000)))))),
            IgnoreId)))
    }
    
    "plan filter with timestamp and interval" in {
      import org.threeten.bp.Instant
      
      plan("select * from days where \"date\" < timestamp '2014-11-17T22:00:00Z' and \"date\" - interval 'PT12H' > timestamp '2014-11-17T00:00:00Z'") must
        beWorkflow(chain(
          $read(Collection("days")),
          $project(
            Reshape.Doc(ListMap(
              BsonField.Name("__tmp2") -> -\/(DocVar.ROOT()),
              BsonField.Name("__tmp3") ->
                -\/(ExprOp.Subtract(
                  ExprOp.DocField(BsonField.Name("date")),
                  ExprOp.Literal(Bson.Dec(12*60*60*1000)))))),
            IgnoreId),
          $match(
            Selector.And(
              Selector.Doc(
                BsonField.Name("__tmp2") \ BsonField.Name("date") ->
                  Selector.Lt(Bson.Date(Instant.parse("2014-11-17T22:00:00Z")))),
              Selector.Doc(
                BsonField.Name("__tmp3") ->
                  Selector.Gt(Bson.Date(Instant.parse("2014-11-17T00:00:00Z")))))),
          $project(Reshape.Doc(ListMap(
            BsonField.Name("value") ->
              -\/(ExprOp.DocField(BsonField.Name("__tmp2"))))),
            ExcludeId)))
    }
    
    import Js._

    def joinStructure(
      left: Workflow, right: Workflow,
      leftKey: ExprOp, rightKey: Js.Expr,
      fin: WorkflowOp) = {
      def initialPipeOps(src: Workflow): Workflow =
        chain(
          src,
          $group(
            Grouped(ListMap(BsonField.Name("left") ->
              ExprOp.Push(ExprOp.DocVar.ROOT()))),
            -\/(leftKey)),
          $project(Reshape.Doc(ListMap(
            BsonField.Name("left") ->
              -\/(ExprOp.DocField(BsonField.Name("left"))),
            BsonField.Name("right") -> -\/(ExprOp.Literal(Bson.Arr(List()))))),
            IncludeId))
      fin(
        $foldLeft(
          initialPipeOps(left),
          chain(
            right,
            $map($Map.mapKeyVal(("key", "value"),
              rightKey,
              AnonObjDecl(List(
                ("left", AnonElem(List())),
                ("right", AnonElem(List(Ident("value")))))))),
            $reduce(
              AnonFunDecl(List("key", "values"),
                List(
                  VarDef(List(
                    ("result", AnonObjDecl(List(
                      ("left", AnonElem(List())),
                      ("right", AnonElem(List()))))))),
                  Call(Select(Ident("values"), "forEach"),
                    List(AnonFunDecl(List("value"),
                      List(
                        BinOp("=",
                          Select(Ident("result"), "left"),
                          Call(
                            Select(Select(Ident("result"), "left"), "concat"),
                            List(Select(Ident("value"), "left")))),
                        BinOp("=",
                          Select(Ident("result"), "right"),
                          Call(
                            Select(Select(Ident("result"), "right"), "concat"),
                            List(Select(Ident("value"), "right")))))))),
                  Return(Ident("result"))))))))
    }
            
    "plan simple join" in {
      plan("select zips2.city from zips join zips2 on zips._id = zips2._id") must
        beWorkflow(
          joinStructure(
            $read(Collection("zips")),
            $read(Collection("zips2")),
            ExprOp.DocField(BsonField.Name("_id")),
            Select(Ident("value"), "_id"),
            chain(_,
              $match(Selector.Doc(ListMap[BsonField, Selector.SelectorExpr](
                BsonField.Name("left") -> Selector.NotExpr(Selector.Size(0)),
                BsonField.Name("right") -> Selector.NotExpr(Selector.Size(0))))),
              $unwind(ExprOp.DocField(BsonField.Name("left"))),
              $unwind(ExprOp.DocField(BsonField.Name("right"))),
              $project(Reshape.Doc(ListMap(
                BsonField.Name("city") ->
                  -\/(ExprOp.DocField(BsonField.Name("right") \ BsonField.Name("city"))))),
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
          $read(Collection("foo")),
          $read(Collection("bar")),
          ExprOp.DocField(BsonField.Name("id")),
          Select(Ident("value"), "foo_id"),
          chain(_,
            $match(Selector.Doc(ListMap[BsonField, Selector.SelectorExpr](
              BsonField.Name("left") -> Selector.NotExpr(Selector.Size(0)),
              BsonField.Name("right") -> Selector.NotExpr(Selector.Size(0))))),
            $unwind(ExprOp.DocField(BsonField.Name("left"))),
            $unwind(ExprOp.DocField(BsonField.Name("right"))),
            $project(Reshape.Doc(ListMap(
              BsonField.Name("name") ->
                -\/(ExprOp.DocField(BsonField.Name("left") \ BsonField.Name("name"))),
              BsonField.Name("address") ->
                -\/(ExprOp.DocField(BsonField.Name("right") \ BsonField.Name("address"))))),
              IgnoreId))))  // Note: becomes ExcludeId in conversion to WorkflowTask
    }

    "plan simple inner equi-join with wildcard" in {
      plan("select * from foo join bar on foo.id = bar.foo_id") must
      beWorkflow(
        joinStructure(
          $read(Collection("foo")),
          $read(Collection("bar")),
          ExprOp.DocField(BsonField.Name("id")),
          Select(Ident("value"), "foo_id"),
          chain(_,
            $match(Selector.Doc(ListMap[BsonField, Selector.SelectorExpr](
              BsonField.Name("left") -> Selector.NotExpr(Selector.Size(0)),
              BsonField.Name("right") -> Selector.NotExpr(Selector.Size(0))))),
            $unwind(ExprOp.DocField(BsonField.Name("left"))),
            $unwind(ExprOp.DocField(BsonField.Name("right"))),
            $map(
              AnonFunDecl(List("key", "__tmp0"), List(
                Return(AnonElem(List(Ident("key"), Call(
                  AnonFunDecl(List("rez"), List(
                    ForIn(Ident("attr"), Select(Ident("__tmp0"), "left"),If(Call(Select(Select(Ident("__tmp0"), "left"), "hasOwnProperty"), List(Ident("attr"))), BinOp("=",Access(Ident("rez"), Ident("attr")), Access(Select(Ident("__tmp0"), "left"), Ident("attr"))), None)),
                    ForIn(Ident("attr"), Select(Ident("__tmp0"), "right"), If(Call(Select(Select(Ident("__tmp0"), "right"), "hasOwnProperty"), List(Ident("attr"))), BinOp("=", Access(Ident("rez"), Ident("attr")), Access(Select(Ident("__tmp0"), "right"), Ident("attr"))), None)),
                    Return(Ident("rez")))),
                  List(AnonObjDecl(List()))))))))))))
    }

    "plan simple left equi-join" in {
      plan(
        "select foo.name, bar.address " +
          "from foo left join bar on foo.id = bar.foo_id") must
      beWorkflow(
        joinStructure(
          $read(Collection("foo")),
          $read(Collection("bar")),
          ExprOp.DocField(BsonField.Name("id")),
          Select(Ident("value"), "foo_id"),
          chain(_,
            $match(Selector.Doc(ListMap[BsonField, Selector.SelectorExpr](
              BsonField.Name("left") -> Selector.NotExpr(Selector.Size(0))))),
            $project(Reshape.Doc(ListMap(
              BsonField.Name("left") -> -\/(ExprOp.DocField(BsonField.Name("left"))),
              BsonField.Name("right") -> -\/(ExprOp.Cond(
                ExprOp.Eq(
                  ExprOp.Size(ExprOp.DocField(BsonField.Name("right"))),
                  ExprOp.Literal(Bson.Int32(0))),
                ExprOp.Literal(Bson.Arr(List(Bson.Doc(ListMap())))),
                ExprOp.DocField(BsonField.Name("right")))))),
              IgnoreId),
            $unwind(ExprOp.DocField(BsonField.Name("left"))),
            $unwind(ExprOp.DocField(BsonField.Name("right"))),
            $project(Reshape.Doc(ListMap(
              BsonField.Name("name") -> -\/(ExprOp.DocField(BsonField.Name("left") \ BsonField.Name("name"))),
              BsonField.Name("address") -> -\/(ExprOp.DocField(BsonField.Name("right") \ BsonField.Name("address"))))),
              IgnoreId))))  // Note: becomes ExcludeId in conversion to WorkflowTask
    }
 
    "plan 3-way equi-join" in {
      plan(
        "select foo.name, bar.address " +
          "from foo join bar on foo.id = bar.foo_id " +
          "join baz on bar.id = baz.bar_id") must
      beWorkflow(
        joinStructure(
          joinStructure(
            $read(Collection("foo")),
            $read(Collection("bar")),
            ExprOp.DocField(BsonField.Name("id")),
            Select(Ident("value"), "foo_id"),
            chain(_,
              $match(Selector.Doc(ListMap[BsonField, Selector.SelectorExpr](
                BsonField.Name("left") -> Selector.NotExpr(Selector.Size(0)),
                BsonField.Name("right") -> Selector.NotExpr(Selector.Size(0))))),
              $unwind(ExprOp.DocField(BsonField.Name("left"))),
              $unwind(ExprOp.DocField(BsonField.Name("right"))))),
          $read(Collection("baz")),
          ExprOp.DocField(BsonField.Name("right") \ BsonField.Name("id")),
          Select(Ident("value"), "bar_id"),
          chain(_,
            $match(Selector.Doc(ListMap[BsonField, Selector.SelectorExpr](
              BsonField.Name("left") -> Selector.NotExpr(Selector.Size(0)),
              BsonField.Name("right") -> Selector.NotExpr(Selector.Size(0))))),
            $unwind(ExprOp.DocField(BsonField.Name("left"))),
            $unwind(ExprOp.DocField(BsonField.Name("right"))),
            $project(Reshape.Doc(ListMap(
              BsonField.Name("name") -> -\/(ExprOp.DocField(BsonField.Name("left") \ BsonField.Name("left") \ BsonField.Name("name"))),
              BsonField.Name("address") -> -\/(ExprOp.DocField(BsonField.Name("left") \ BsonField.Name("right") \ BsonField.Name("address"))))),
              IgnoreId))))  // Note: becomes ExcludeId in conversion to WorkflowTask
    }

    "plan simple cross" in {
      import Js._
      plan("select zips2.city from zips, zips2 where zips._id = zips2._id") must
      beWorkflow(
        joinStructure(
          $read(Collection("zips")),
          $read(Collection("zips2")),
          ExprOp.Literal(Bson.Null),
          Null,
          chain(_,
            $match(Selector.Doc(ListMap[BsonField, Selector.SelectorExpr](
              BsonField.Name("left") -> Selector.NotExpr(Selector.Size(0)),
              BsonField.Name("right") -> Selector.NotExpr(Selector.Size(0))))),
            $unwind(ExprOp.DocField(BsonField.Name("left"))),
            $unwind(ExprOp.DocField(BsonField.Name("right"))),
            $project(
              Reshape.Doc(ListMap(
                BsonField.Name("__tmp0") -> -\/(DocVar.ROOT()),
                BsonField.Name("__tmp1") ->
                  -\/(Eq(
                    DocField(BsonField.Name("left") \ BsonField.Name("_id")),
                    DocField(BsonField.Name("right") \ BsonField.Name("_id")))))),
              IgnoreId),
            $match(Selector.Doc(
              BsonField.Name("__tmp1") ->
                Selector.Eq(Bson.Bool(true)))),
            $project(Reshape.Doc(ListMap(
              BsonField.Name("city") -> -\/(ExprOp.DocField(BsonField.Name("__tmp0") \ BsonField.Name("right") \ BsonField.Name("city"))))),
              IgnoreId))))  // Note: becomes ExcludeId in conversion to WorkflowTask
    }
    
    def countOps(wf: Workflow, p: PartialFunction[WorkflowF[Term[WorkflowF]], Boolean]): Int = {
      wf.foldMap(op => if (p.lift(op.unFix).getOrElse(false)) 1 else 0)
    }
    
    def noConsecutiveProjectOps(wf: Workflow) =
      countOps(wf, { case $Project(Term($Project(_, _, _)), _, _) => true }) aka "the occurrences of consecutive $project ops:" must_== 0
    def noConsecutiveSimpleMapOps(wf: Workflow) =
      countOps(wf, { case $SimpleMap(Term($SimpleMap(_, _)), _) => true }) aka "the occurrences of consecutive $simpleMap ops:" must_== 0
    def maxGroupOps(wf: Workflow, max: Int) =
      countOps(wf, { case $Group(_, _, _) => true }) aka "the number of $group ops:" must beLessThanOrEqualTo(max)
    def maxUnwindOps(wf: Workflow, max: Int) =
      countOps(wf, { case $Unwind(_, _) => true }) aka "the number of $unwind ops:" must beLessThanOrEqualTo(max)
    
    "plan multiple reducing projections without explicit group by" ! Prop.forAll(select(maybeReducingExpr, noGroupBy)) { q => 
      // println(q.sql)
      plan(q.sql) must beRight.which { wf =>
        // println(wf.show)
        noConsecutiveProjectOps(wf)
        noConsecutiveSimpleMapOps(wf)
        maxGroupOps(wf, 1)
        maxUnwindOps(wf, 1)
      }
    }.set(maxSize = 10)
    
    "plan multiple reducing projections with explicit group by" ! Prop.forAll(select(maybeReducingExpr, groupByCity)) { q =>
      // println(q.sql)
      plan(q.sql) must beRight.which { wf =>
        // println(wf.show)
        noConsecutiveProjectOps(wf)
        noConsecutiveSimpleMapOps(wf)
        maxGroupOps(wf, 1)
        maxUnwindOps(wf, 1)
      }
    }.set(maxSize = 10)
    
    "plan multiple reducing projections with complex group by" ! Prop.forAll(select(maybeReducingExpr, groupBySeveral)) { q =>
      // println(q.sql)
      plan(q.sql) must beRight.which { wf =>
        // println(wf.show)
        noConsecutiveProjectOps(wf)
        noConsecutiveSimpleMapOps(wf)
        maxGroupOps(wf, 1)
        maxUnwindOps(wf, 1)
      }
    }.set(maxSize = 10).pendingUntilFixed("#469")
  }


  import slamdata.engine.sql._

  val noGroupBy = Gen.const(None)
  val groupByCity = Gen.const(Some(GroupBy(List(Ident("city")), None)))
  val groupBySeveral = Gen.nonEmptyListOf(Gen.oneOf(genInnerStr, genInnerInt)).map(keys => Some(GroupBy(keys.distinct, None)))

  val maybeReducingExpr = Gen.oneOf(genOuterInt, genOuterStr)

  def select(exprGen: Gen[Expr], groupByGen: Gen[Option[GroupBy]]): Gen[SelectStmt] =
    for {
      projs <- Gen.nonEmptyListOf(exprGen).map(_.zipWithIndex.map { case (x, n) => Proj.Named(x, "p" + n) })
      groupBy <- groupByGen
    } yield SelectStmt(SelectAll, projs, Some(TableRelationAST("zips", None)), None, groupBy, None, None, None)

  def genInnerInt = Gen.oneOf(
    Ident("pop"),
    // IntLiteral(0),  // TODO: exposes bugs (see #476)
    Binop(Ident("pop"), IntLiteral(1), Minus),      // an ExprOp
    InvokeFunction("length", List(Ident("city"))))  // requires JS
  def genReduceInt = genInnerInt.flatMap(x => Gen.oneOf(
    x,
    InvokeFunction("min", List(x)),
    InvokeFunction("sum", List(x)),
    InvokeFunction("count", List(Splice(None)))))
  def genOuterInt = genReduceInt.flatMap(x => Gen.oneOf(
    x,
    Binop(x, IntLiteral(1000), Div)))

  def genInnerStr = Gen.oneOf(
    Ident("city"),
    // StringLiteral("foo"),  // TODO: exposes bugs (see #476)
    InvokeFunction("lower", List(Ident("city"))))
  def genReduceStr = genInnerStr.flatMap(x => Gen.oneOf(
    x,
    InvokeFunction("min", List(x))))
  def genOuterStr = genReduceStr.flatMap(x => Gen.oneOf(
    x,
    InvokeFunction("lower", List(x)),     // an ExprOp
    InvokeFunction("length", List(x))))   // requires JS

/*
  "plan from LogicalPlan" should {
    "plan simple OrderBy" in {
      val lp = LogicalPlan.Let(
                  'tmp0, read("foo"),
                  LogicalPlan.Let(
                    'tmp1, makeObj("bar" -> ObjectProject(Free('tmp0), Constant(Data.Str("bar")))),
                    LogicalPlan.Let('tmp2, 
                      set.OrderBy(
                        Free('tmp1),
                        MakeArrayN(
                          makeObj(
                            "key" -> ObjectProject(Free('tmp1), Constant(Data.Str("bar"))),
                            "order" -> Constant(Data.Str("ASC"))
                          )
                        )
                      ),
                      Free('tmp2)
                    )
                  )
                )

      val exp = chain(
        $read(Collection("foo")),
        $project(Reshape.Doc(ListMap(
          BsonField.Name("lEft") -> \/- (Reshape.Doc(ListMap(
            BsonField.Name("bar") -> -\/ (ExprOp.DocField(BsonField.Name("bar")))))), 
          BsonField.Name("rIght") -> \/- (Reshape.Arr(ListMap(
            BsonField.Index(0) -> \/- (Reshape.Doc(ListMap(
              BsonField.Name("key") -> -\/ (ExprOp.DocField(
                BsonField.Name("bar"))), 
              BsonField.Name("order") -> -\/ (ExprOp.Literal(Bson.Text("ASC")))))))))))), 
        $sort(NonEmptyList(BsonField.Name("rIght") \ BsonField.Index(0) \ BsonField.Name("key") -> Ascending)), 
        $project(Reshape.Doc(ListMap(BsonField.Name("bar") -> -\/(ExprOp.DocField(BsonField.Name("lEft") \ BsonField.Name("bar")))))))

      plan(lp) must beWorkflow(exp)
    }

    "plan OrderBy with expression" in {
      val lp = LogicalPlan.Let('tmp0, 
                  read("foo"),
                  set.OrderBy(
                    Free('tmp0),
                    MakeArrayN(
                      makeObj(
                        "key" -> math.Divide(
                                  ObjectProject(Free('tmp0), Constant(Data.Str("bar"))),
                                  Constant(Data.Dec(10.0))),
                        "order" -> Constant(Data.Str("ASC"))
                      )
                    )
                  )
                )

      val exp = chain(
                  $read(Collection("foo")),
                  $project(Reshape.Doc(ListMap(
                    BsonField.Name("__sd_tmp_1") ->  \/- (Reshape.Arr(ListMap(
                      BsonField.Index(0) -> -\/ (ExprOp.Divide(
                                                          DocField(BsonField.Name("bar")), 
                                                          Literal(Bson.Dec(10.0)))))))))),
                  $sort(NonEmptyList(BsonField.Name("__sd_tmp_1") \ BsonField.Index(0) -> Ascending))
                  // We'll want another Project here to remove the temporary field
                  )

      plan(lp) must beWorkflow(exp)
    }.pendingUntilFixed

    "plan OrderBy with expression and earlier pipeline op" in {
      val lp = LogicalPlan.Let('tmp0,
                  read("foo"),
                  LogicalPlan.Let('tmp1,
                    set.Filter(
                      Free('tmp0),
                      relations.Eq(
                        ObjectProject(Free('tmp0), Constant(Data.Str("baz"))),
                        Constant(Data.Int(0))
                      )
                    ),
                    set.OrderBy(
                      Free('tmp1),
                      MakeArrayN(
                        makeObj(
                          "key" -> ObjectProject(Free('tmp1), Constant(Data.Str("bar"))),
                          "order" -> Constant(Data.Str("ASC"))
                        )
                      )
                    )
                  )
                )

      val exp = chain(
                  $read(Collection("foo")),
                  $match(Selector.Doc(
                    BsonField.Name("baz") -> Selector.Eq(Bson.Int64(0)))),
                  $sort(NonEmptyList(BsonField.Name("bar") -> Ascending)))

      plan(lp) must beWorkflow(exp)
    }.pendingUntilFixed

    "plan OrderBy with expression (and extra project)" in {
      val lp = LogicalPlan.Let('tmp0, 
                  read("foo"),
                  LogicalPlan.Let('tmp9,
                    makeObj(
                      "bar" -> ObjectProject(Free('tmp0), Constant(Data.Str("bar")))
                    ),
                    set.OrderBy(
                      Free('tmp9),
                      MakeArrayN(
                        makeObj(
                          "key" -> math.Divide(
                                    ObjectProject(Free('tmp9), Constant(Data.Str("bar"))),
                                    Constant(Data.Dec(10.0))),
                          "order" -> Constant(Data.Str("ASC"))
                        )
                      )
                    )
                  )
                )

      val exp = chain(
        $read(Collection("foo")),
        $project(Reshape.Doc(ListMap(BsonField.Name("lEft") -> \/-(Reshape.Doc(ListMap(BsonField.Name("bar") -> -\/(ExprOp.DocField(BsonField.Name("bar")))))), BsonField.Name("rIght") -> \/-(Reshape.Arr(ListMap(BsonField.Index(0) -> \/-(Reshape.Doc(ListMap(BsonField.Name("key") -> -\/(ExprOp.Divide(ExprOp.DocField(BsonField.Name("bar")), ExprOp.Literal(Bson.Dec(10.0)))), BsonField.Name("order") -> -\/(ExprOp.Literal(Bson.Text("ASC")))))))))))), 
        $sort(NonEmptyList(BsonField.Name("rIght") \ BsonField.Index(0) \ BsonField.Name("key") -> Ascending)), 
        $project(Reshape.Doc(ListMap(BsonField.Name("bar") -> -\/(ExprOp.DocField(BsonField.Name("lEft") \ BsonField.Name("bar")))))))

      plan(lp) must beWorkflow(exp)
    }
  }
  */
}
