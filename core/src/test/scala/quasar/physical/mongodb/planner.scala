package quasar.physical.mongodb

import quasar.Predef._
import quasar.RenderTree, RenderTree.ops._
import quasar.fp._
import quasar.recursionschemes._, Recursive.ops._, FunctorT.ops._, Fix._
import quasar._
import quasar.fs.Path
import quasar.javascript._
import quasar.sql.{ParsingError, SQLParser, Query}
import quasar.std._
import quasar.specs2.PendingWithAccurateCoverage

import scala.Either

import org.scalacheck._
import org.specs2.execute.Result
import org.specs2.mutable._
import org.specs2.scalaz._
import org.specs2.matcher.{Matcher, Expectable}
import org.specs2.ScalaCheck
import org.threeten.bp.Instant
import scalaz._, Scalaz._

class PlannerSpec extends Specification with ScalaCheck with CompilerHelpers with DisjunctionMatchers with PendingWithAccurateCoverage {
  import StdLib.{set => s, _}
  import structural._
  import LogicalPlan._
  import Grouped.grouped
  import Reshape.reshape
  import Workflow._
  import quasar.physical.mongodb.accumulator._
  import quasar.physical.mongodb.expression._
  import IdHandling._
  import jscore._
  import Planner._

  case class equalToWorkflow(expected: Workflow)
      extends Matcher[Crystallized] {
    def apply[S <: Crystallized](s: Expectable[S]) = {
      def diff(l: S, r: Workflow): String = {
        val lt = RenderTree[Crystallized].render(l)
        (lt diff r.render).shows
      }
      result(expected == s.value.op,
             "\ntrees are equal:\n" + diff(s.value, expected),
             "\ntrees are not equal:\n" + diff(s.value, expected),
             s)
    }
  }

  val queryPlanner = MongoDbPlanner.queryPlanner(κ("Mongo" -> Cord.empty))

  def plan(query: String): Either[CompilationError, Crystallized] =
    SQLParser.parseInContext(Query(query), Path("/db/")).fold(
      e => scala.sys.error("parsing error: " + e.message),
      expr => queryPlanner(QueryRequest(expr, Variables(Map()))).run._2).toEither

  def plan(logical: Fix[LogicalPlan]): Either[PlannerError, Crystallized] =
    (for {
      simplified <- emit(Vector.empty, \/-(logical.transCata(repeatedly(Optimizer.simplifyƒ))))
      phys       <- MongoDbPlanner.plan(simplified)
    } yield phys).run._2.toEither

  def planLog(query: String): ParsingError \/ Vector[PhaseResult] =
    for {
      expr <- SQLParser.parseInContext(Query(query), Path("/db/"))
    } yield queryPlanner(QueryRequest(expr, Variables(Map()))).run._1

  def beWorkflow(wf: Workflow) = beRight(equalToWorkflow(wf))

  implicit def toBsonField(name: String) = BsonField.Name(name)
  implicit def toLeftShape(shape: Reshape):      Reshape.Shape = -\/ (shape)
  implicit def toRightShape(exprOp: Expression): Reshape.Shape =  \/-(exprOp)

  "plan from query string" should {
    "plan simple constant example 1" in {
      plan("select 1") must
        beWorkflow($pure(Bson.Doc(ListMap("0" -> Bson.Int64(1)))))
    }

    "compile simple constant example 2" in {
      plan("select 1 * 1") must
        beWorkflow($pure(Bson.Doc(ListMap("0" -> Bson.Int64(1)))))
    }

    "plan simple constant from collection" in {
      plan("select 1 from zips") must
        beWorkflow(chain(
          $read(Collection("db", "zips")),
          $project(
            reshape("0" -> $literal(Bson.Int64(1))),
            IgnoreId)))
    }

    "plan simple select *" in {
      plan("select * from foo") must beWorkflow($read(Collection("db", "foo")))
    }

    "plan count(*)" in {
      plan("select count(*) from foo") must beWorkflow(
        chain(
          $read(Collection("db", "foo")),
          $group(
            grouped("0" -> $sum($literal(Bson.Int32(1)))),
            \/-($literal(Bson.Null)))))
    }

    "plan simple field projection on single set" in {
      plan("select foo.bar from foo") must
        beWorkflow(chain(
          $read(Collection("db", "foo")),
          $project(
            reshape("bar" -> $field("bar")),
            IgnoreId)))
    }

    "plan simple field projection on single set when table name is inferred" in {
      plan("select bar from foo") must
       beWorkflow(chain(
         $read(Collection("db", "foo")),
         $project(
           reshape("bar" -> $field("bar")),
           IgnoreId)))
    }

    "plan multiple field projection on single set when table name is inferred" in {
      plan("select bar, baz from foo") must
       beWorkflow(chain(
         $read(Collection("db", "foo")),
         $project(
           reshape(
             "bar" -> $field("bar"),
             "baz" -> $field("baz")),
           IgnoreId)))
    }

    "plan simple addition on two fields" in {
      plan("select foo + bar from baz") must
       beWorkflow(chain(
         $read(Collection("db", "baz")),
         $project(
           reshape("0" ->
             $cond(
               $and(
                 $lt($literal(Bson.Null), $field("bar")),
                 $lt($field("bar"), $literal(Bson.Text("")))),
               $cond(
                 $or(
                   $and(
                     $lt($literal(Bson.Null), $field("foo")),
                     $lt($field("foo"), $literal(Bson.Text("")))),
                   $and(
                     $lte($literal(Bson.Date(Instant.ofEpochMilli(0))), $field("foo")),
                     $lt($field("foo"), $literal(Bson.Regex("", ""))))),
                 $add($field("foo"), $field("bar")),
                 $literal(Bson.Undefined)),
               $literal(Bson.Undefined))),
           IgnoreId)))
    }

    "plan concat" in {
      plan("select concat(bar, baz) from foo") must
       beWorkflow(chain(
         $read(Collection("db", "foo")),
         $project(
           reshape("0" ->
             $cond(
               $and(
                 $lte($literal(Bson.Text("")), $field("baz")),
                 $lt($field("baz"), $literal(Bson.Doc(ListMap())))),
               $cond(
                 $and(
                   $lte($literal(Bson.Text("")), $field("bar")),
                   $lt($field("bar"), $literal(Bson.Doc(ListMap())))),
                 $concat($field("bar"), $field("baz")),
                 $literal(Bson.Undefined)),
               $literal(Bson.Undefined))),
           IgnoreId)))
    }

    "plan concat strings with ||" in {
      plan("select city || ', ' || state from zips") must
       beWorkflow(chain(
         $read(Collection("db", "zips")),
         $project(
           reshape(
             "0" ->
               $cond(
                 $or(
                   $and(
                     $lte($literal(Bson.Arr(Nil)), $field("state")),
                     $lt($field("state"), $literal(Bson.Binary(scala.Array[Byte]())))),
                   $and(
                     $lte($literal(Bson.Text("")), $field("state")),
                     $lt($field("state"), $literal(Bson.Doc(ListMap()))))),
                 $cond(
                   $or(
                     $and(
                       $lte($literal(Bson.Arr(Nil)), $field("city")),
                       $lt($field("city"), $literal(Bson.Binary(scala.Array[Byte]())))),
                     $and(
                       $lte($literal(Bson.Text("")), $field("city")),
                       $lt($field("city"), $literal(Bson.Doc(ListMap()))))),
                   $concat( // TODO: ideally, this would be a single $concat
                     $concat($field("city"), $literal(Bson.Text(", "))),
                     $field("state")),
                   $literal(Bson.Undefined)),
                 $literal(Bson.Undefined))),
           IgnoreId)))
    }

    "plan concat strings with ||, constant on the right" in {
      plan("select a || b || '...' from foo") must
       beWorkflow(chain(
         $read(Collection("db", "foo")),
         $project(
           reshape(
             "0" ->
               $cond(
                 $or(
                   $and(
                     $lte($literal(Bson.Arr(Nil)), $field("b")),
                     $lt($field("b"), $literal(Bson.Binary(scala.Array[Byte]())))),
                   $and(
                     $lte($literal(Bson.Text("")), $field("b")),
                     $lt($field("b"), $literal(Bson.Doc(ListMap()))))),
                 $cond(
                   $or(
                     $and(
                       $lte($literal(Bson.Arr(Nil)), $field("a")),
                       $lt($field("a"), $literal(Bson.Binary(scala.Array[Byte]())))),
                     $and(
                       $lte($literal(Bson.Text("")), $field("a")),
                       $lt($field("a"), $literal(Bson.Doc(ListMap()))))),
                   $concat( // TODO: ideally, this would be a single $concat
                     $concat($field("a"), $field("b")),
                     $literal(Bson.Text("..."))),
                   $literal(Bson.Undefined)),
                 $literal(Bson.Undefined))),
           IgnoreId)))
    }.pendingUntilFixed("SD-639")

    "plan concat with unknown types" in {
      plan("select a || b from foo") must
        beRight
    }.pendingUntilFixed("SD-639")

    "plan lower" in {
      plan("select lower(bar) from foo") must
      beWorkflow(chain(
        $read(Collection("db", "foo")),
        $project(
          reshape("0" ->
            $cond(
              $and(
                $lte($literal(Bson.Text("")), $field("bar")),
                $lt($field("bar"), $literal(Bson.Doc(ListMap())))),
              $toLower($field("bar")),
              $literal(Bson.Undefined))),
          IgnoreId)))
    }

    "plan coalesce" in {
      plan("select coalesce(bar, baz) from foo") must
       beWorkflow(chain(
         $read(Collection("db", "foo")),
         $project(
           reshape("0" -> $ifNull($field("bar"), $field("baz"))),
           IgnoreId)))
    }

    "plan date field extraction" in {
      plan("select date_part('day', baz) from foo") must
       beWorkflow(chain(
         $read(Collection("db", "foo")),
         $project(
           reshape("0" ->
             $cond(
               $and(
                 $lte($literal(Bson.Date(Instant.ofEpochMilli(0))), $field("baz")),
                 $lt($field("baz"), $literal(Bson.Regex("", "")))),
               $dayOfMonth($field("baz")),
               $literal(Bson.Undefined))),
           IgnoreId)))
    }

    "plan complex date field extraction" in {
      plan("select date_part('quarter', baz) from foo") must
       beWorkflow(chain(
         $read(Collection("db", "foo")),
         $project(
           reshape(
             "0" ->
               $cond(
                 $and(
                   $lte($literal(Bson.Date(Instant.ofEpochMilli(0))), $field("baz")),
                   $lt($field("baz"), $literal(Bson.Regex("", "")))),
                 $add(
                   $divide($dayOfYear($field("baz")), $literal(Bson.Int32(92))),
                   $literal(Bson.Int32(1))),
                 $literal(Bson.Undefined))),
           IgnoreId)))
    }

    "plan date field extraction: 'dow'" in {
      plan("select date_part('dow', baz) from foo") must
       beWorkflow(chain(
         $read(Collection("db", "foo")),
         $project(
           reshape(
             "0" ->
               $cond(
                 $and(
                   $lte($literal(Bson.Date(Instant.ofEpochMilli(0))), $field("baz")),
                   $lt($field("baz"), $literal(Bson.Regex("", "")))),
                 $add($dayOfWeek($field("baz")), $literal(Bson.Int64(-1))),
                 $literal(Bson.Undefined))),
           IgnoreId)))
    }

    "plan date field extraction: 'isodow'" in {
      plan("select date_part('isodow', baz) from foo") must
       beWorkflow(chain(
         $read(Collection("db", "foo")),
         $project(
           reshape(
             "0" ->
               $cond(
                 $and(
                   $lte($literal(Bson.Date(Instant.ofEpochMilli(0))), $field("baz")),
                   $lt($field("baz"), $literal(Bson.Regex("", "")))),
                 $cond($eq($dayOfWeek($field("baz")), $literal(Bson.Int64(1))),
                   $literal(Bson.Int64(7)),
                   $add($dayOfWeek($field("baz")), $literal(Bson.Int64(-1)))),
                 $literal(Bson.Undefined))),
           IgnoreId)))
    }

    "plan filter array element" in {
      plan("select loc from zips where loc[0] < -73") must
      beWorkflow(chain(
        $read(Collection("db", "zips")),
        $match(Selector.Where(
          If(
            BinOp(And,
              Call(Select(ident("Array"), "isArray"), List(Select(ident("this"), "loc"))),
              BinOp(Or,
                BinOp(Or,
                  BinOp(jscore.Or, BinOp(jscore.Or,
                    Call(ident("isNumber"), List(Access(Select(ident("this"), "loc"), Literal(Js.Num(0, false))))),
                    BinOp(Instance, Access(Select(ident("this"), "loc"), Literal(Js.Num(0, false))), ident("NumberInt"))),
                    BinOp(Instance, Access(Select(ident("this"), "loc"), Literal(Js.Num(0, false))), ident("NumberLong"))),
                  Call(ident("isString"), List(Access(Select(ident("this"), "loc"), Literal(Js.Num(0, false)))))),
                BinOp(Or,
                  BinOp(Instance, Access(Select(ident("this"), "loc"), Literal(Js.Num(0, false))), ident("Date")),
                  BinOp(Eq, UnOp(TypeOf, Access(Select(ident("this"), "loc"), Literal(Js.Num(0, false)))), jscore.Literal(Js.Str("boolean")))))),
            BinOp(Lt, Access(Select(ident("this"), "loc"), Literal(Js.Num(0, false))), Literal(Js.Num(-73, false))),
            ident("undefined")).toJs)),
        $project(
          reshape("loc" -> $field("loc")),
          IgnoreId)))
    }

    "plan select array element" in {
      plan("select loc[0] from zips") must
      beWorkflow(chain(
        $read(Collection("db", "zips")),
        $simpleMap(NonEmptyList(MapExpr(JsFn(Name("x"), obj(
          "0" ->
            jscore.If(Call(Select(ident("Array"), "isArray"), List(Select(ident("x"), "loc"))),
              Access(Select(ident("x"), "loc"), jscore.Literal(Js.Num(0, false))),
              ident("undefined")))))),
          ListMap()),
        $project(
          reshape("0" -> $include()),
          IgnoreId)))
    }

    "plan array length" in {
      plan("select array_length(bar, 1) from foo") must
       beWorkflow(chain(
         $read(Collection("db", "foo")),
         $project(
           reshape("0" ->
             $cond(
               $and(
                 $lte($literal(Bson.Arr(Nil)), $field("bar")),
                 $lt($field("bar"), $literal(Bson.Binary(scala.Array[Byte]())))),
               $size($field("bar")),
               $literal(Bson.Undefined))),
           IgnoreId)))
    }

    "plan sum in expression" in {
      plan("select sum(pop) * 100 from zips") must
      beWorkflow(chain(
        $read(Collection("db", "zips")),
        $group(
          grouped("__tmp4" ->
            $sum(
              $cond(
                $and(
                  $lt($literal(Bson.Null), $field("pop")),
                  $lt($field("pop"), $literal(Bson.Text("")))),
                $field("pop"),
                $literal(Bson.Undefined)))),
          \/-($literal(Bson.Null))),
        $project(
          reshape("0" -> $multiply($field("__tmp4"), $literal(Bson.Int64(100)))),
          IgnoreId)))
    }

    "plan conditional" in {
      plan("select case when pop < 10000 then city else loc end from zips") must
       beWorkflow(chain(
         $read(Collection("db", "zips")),
         $project(
           reshape(
             "0" ->
               $cond(
                 $or(
                   $and(
                     $lt($literal(Bson.Null), $field("pop")),
                     $lt($field("pop"), $literal(Bson.Doc(ListMap())))),
                   $and(
                     $lte($literal(Bson.Bool(false)), $field("pop")),
                     $lt($field("pop"), $literal(Bson.Regex("", ""))))),
                 $cond($lt($field("pop"), $literal(Bson.Int64(10000))),
                   $field("city"),
                   $field("loc")),
                 $literal(Bson.Undefined))),
           IgnoreId)))
    }

    "plan negate" in {
      plan("select -bar from foo") must
       beWorkflow(chain(
         $read(Collection("db", "foo")),
         $project(
           reshape("0" ->
             $cond(
               $and(
                 $lt($literal(Bson.Null), $field("bar")),
                 $lt($field("bar"), $literal(Bson.Text("")))),
               $multiply($literal(Bson.Int32(-1)), $field("bar")),
               $literal(Bson.Undefined))),
           IgnoreId)))
    }

    "plan simple filter" in {
      plan("select * from foo where bar > 10") must
       beWorkflow(chain(
         $read(Collection("db", "foo")),
         $match(Selector.And(
           Selector.Or(
             Selector.Doc(BsonField.Name("bar") ->
               Selector.Type(BsonType.Int32)),
             Selector.Doc(BsonField.Name("bar") ->
               Selector.Type(BsonType.Int64)),
             Selector.Doc(BsonField.Name("bar") ->
               Selector.Type(BsonType.Dec)),
             Selector.Doc(BsonField.Name("bar") ->
               Selector.Type(BsonType.Text)),
             Selector.Or(
               Selector.Doc(BsonField.Name("bar") ->
                 Selector.Type(BsonType.Date)),
               Selector.Doc(BsonField.Name("bar") ->
                 Selector.Type(BsonType.Bool)))),
           Selector.Doc(BsonField.Name("bar") ->
             Selector.Gt(Bson.Int64(10)))))))
    }

    "plan simple reversed filter" in {
      plan("select * from foo where 10 < bar") must
       beWorkflow(chain(
         $read(Collection("db", "foo")),
         $match(Selector.And(
           Selector.Or(
             Selector.Doc(BsonField.Name("bar") ->
               Selector.Type(BsonType.Int32)),
             Selector.Doc(BsonField.Name("bar") ->
               Selector.Type(BsonType.Int64)),
             Selector.Doc(BsonField.Name("bar") ->
               Selector.Type(BsonType.Dec)),
             Selector.Doc(BsonField.Name("bar") ->
               Selector.Type(BsonType.Text)),
             Selector.Or(
               Selector.Doc(BsonField.Name("bar") ->
                 Selector.Type(BsonType.Date)),
               Selector.Doc(BsonField.Name("bar") ->
                 Selector.Type(BsonType.Bool)))),
           Selector.Doc(BsonField.Name("bar") ->
             Selector.Gt(Bson.Int64(10)))))))
    }

    "plan simple filter with expression in projection" in {
      plan("select a + b from foo where bar > 10") must
       beWorkflow(chain(
         $read(Collection("db", "foo")),
         $match(Selector.And(
           Selector.Or(
             Selector.Doc(BsonField.Name("bar") ->
               Selector.Type(BsonType.Int32)),
             Selector.Doc(BsonField.Name("bar") ->
               Selector.Type(BsonType.Int64)),
             Selector.Doc(BsonField.Name("bar") ->
               Selector.Type(BsonType.Dec)),
             Selector.Doc(BsonField.Name("bar") ->
               Selector.Type(BsonType.Text)),
             Selector.Or(
               Selector.Doc(BsonField.Name("bar") ->
                 Selector.Type(BsonType.Date)),
               Selector.Doc(BsonField.Name("bar") ->
                 Selector.Type(BsonType.Bool)))),
           Selector.Doc(BsonField.Name("bar") ->
             Selector.Gt(Bson.Int64(10))))),
         $project(
           reshape("0" ->
             $cond(
               $and(
                 $lt($literal(Bson.Null), $field("b")),
                 $lt($field("b"), $literal(Bson.Text("")))),
               $cond(
                 $or(
                   $and(
                     $lt($literal(Bson.Null), $field("a")),
                     $lt($field("a"), $literal(Bson.Text("")))),
                   $and(
                     $lte($literal(Bson.Date(Instant.ofEpochMilli(0))), $field("a")),
                     $lt($field("a"), $literal(Bson.Regex("", ""))))),
                 $add($field("a"), $field("b")),
                 $literal(Bson.Undefined)),
               $literal(Bson.Undefined))),
           ExcludeId)))
    }

    "plan simple js filter" in {
      plan("select * from zips where length(city) < 4") must
      beWorkflow(chain(
        $read(Collection("db", "zips")),
        // FIXME: Inline this $simpleMap with the $match (SD-456)
        $simpleMap(NonEmptyList(MapExpr(JsFn(Name("x"), obj(
          "__tmp4" -> Select(Select(ident("x"), "city"), "length"),
          "__tmp5" -> ident("x"))))),
          ListMap()),
        $match(Selector.And(
          Selector.Doc(
            BsonField.Name("__tmp5") \ BsonField.Name("city") ->
              Selector.Type(BsonType.Text)),
          Selector.Doc(
            BsonField.Name("__tmp4") -> Selector.Lt(Bson.Int64(4))))),
        $project(
          reshape("value" -> $field("__tmp5")),
          ExcludeId)))
    }

    "plan filter with js and non-js" in {
      plan("select * from zips where length(city) < 4 and pop < 20000") must
      beWorkflow(chain(
        $read(Collection("db", "zips")),
        // FIXME: Inline this $simpleMap with the $match (SD-456)
        $simpleMap(NonEmptyList(MapExpr(JsFn(Name("x"), obj(
          "__tmp8" -> Select(Select(ident("x"), "city"), "length"),
          "__tmp9" -> ident("x"),
          "__tmp10" -> Select(ident("x"), "pop"))))),
          ListMap()),
        $match(Selector.And(
          Selector.Or(
            Selector.Doc(BsonField.Name("__tmp9") \ BsonField.Name("pop") ->
              Selector.Type(BsonType.Int32)),
            Selector.Doc(BsonField.Name("__tmp9") \ BsonField.Name("pop") ->
              Selector.Type(BsonType.Int64)),
            Selector.Doc(BsonField.Name("__tmp9") \ BsonField.Name("pop") ->
              Selector.Type(BsonType.Dec)),
            Selector.Doc(BsonField.Name("__tmp9") \ BsonField.Name("pop") ->
              Selector.Type(BsonType.Text)),
            Selector.Or(
              Selector.Doc(BsonField.Name("__tmp9") \ BsonField.Name("pop") ->
                Selector.Type(BsonType.Date)),
              Selector.Doc(BsonField.Name("__tmp9") \ BsonField.Name("pop") ->
                Selector.Type(BsonType.Bool)))),
          Selector.And(
            Selector.Doc(BsonField.Name("__tmp9") \ BsonField.Name("city") ->
              Selector.Type(BsonType.Text)),
            Selector.And(
              Selector.Doc(BsonField.Name("__tmp8") -> Selector.Lt(Bson.Int64(4))),
              Selector.Doc(BsonField.Name("__tmp10") -> Selector.Lt(Bson.Int64(20000))))))),
        $project(
          reshape("value" -> $field("__tmp9")),
          ExcludeId)))
    }

    "plan filter with between" in {
      plan("select * from foo where bar between 10 and 100") must
       beWorkflow(chain(
         $read(Collection("db", "foo")),
         $match(
           Selector.And(
             Selector.Or(
               Selector.Doc(BsonField.Name("bar") ->
                 Selector.Type(BsonType.Int32)),
               Selector.Doc(BsonField.Name("bar") ->
                 Selector.Type(BsonType.Int64)),
               Selector.Doc(BsonField.Name("bar") ->
                 Selector.Type(BsonType.Dec)),
               Selector.Doc(BsonField.Name("bar") ->
                 Selector.Type(BsonType.Text)),
               Selector.Or(
                 Selector.Doc(BsonField.Name("bar") ->
                   Selector.Type(BsonType.Date)),
                 Selector.Doc(BsonField.Name("bar") ->
                   Selector.Type(BsonType.Bool)))),
             Selector.And(
               Selector.Doc(BsonField.Name("bar") ->
                 Selector.Gte(Bson.Int64(10))),
               Selector.Doc(BsonField.Name("bar") ->
                 Selector.Lte(Bson.Int64(100))))))))
    }

    "plan filter with like" in {
      plan("select * from foo where bar like 'A.%'") must
       beWorkflow(chain(
         $read(Collection("db", "foo")),
         $match(Selector.And(
           Selector.Doc(BsonField.Name("bar") ->
             Selector.Type(BsonType.Text)),
           Selector.Doc(
             BsonField.Name("bar") ->
               Selector.Regex("^A\\..*$", false, true, false, false))))))
    }

    "plan filter with LIKE and OR" in {
      plan("select * from foo where bar like 'A%' or bar like 'Z%'") must
       beWorkflow(chain(
         $read(Collection("db", "foo")),
         $match(
           Selector.Or(
             Selector.And(
               Selector.Doc(BsonField.Name("bar") ->
                 Selector.Type(BsonType.Text)),
               Selector.Doc(BsonField.Name("bar") ->
                 Selector.Regex("^A.*$", false, true, false, false))),
             Selector.And(
               Selector.Doc(BsonField.Name("bar") ->
                 Selector.Type(BsonType.Text)),
               Selector.Doc(BsonField.Name("bar") ->
                 Selector.Regex("^Z.*$", false, true, false, false)))))))
    }

    "plan filter with field in constant array" in {
      plan("select * from zips where state in ('AZ', 'CO')") must
        beWorkflow(chain(
          $read(Collection("db", "zips")),
          $match(Selector.Doc(BsonField.Name("state") ->
            Selector.In(Bson.Arr(List(Bson.Text("AZ"), Bson.Text("CO"))))))))
    }

    "plan filter with field containing constant value" in {
      plan("select * from zips where 43.058514 in loc") must
        beWorkflow(chain(
          $read(Collection("db", "zips")),
          $match(Selector.Where(
            If(Call(Select(ident("Array"), "isArray"), List(Select(ident("this"), "loc"))),
              BinOp(Neq, jscore.Literal(Js.Num(-1, false)), Call(Select(Select(ident("this"), "loc"), "indexOf"), List(jscore.Literal(Js.Num(43.058514, true))))),
            ident("undefined")).toJs))))
    }

    "plan filter with field containing other field" in {
      import jscore._
      plan("select * from zips where pop in loc") must
        beWorkflow(chain(
          $read(Collection("db", "zips")),
          $match(Selector.Where(
            If(
              Call(Select(ident("Array"), "isArray"), List(Select(ident("this"), "loc"))),
              BinOp(Neq,
                jscore.Literal(Js.Num(-1.0,false)),
                Call(Select(Select(ident("this"), "loc"), "indexOf"),
                  List(Select(ident("this"), "pop")))),
              ident("undefined")).toJs))))
    }

    "plan filter with ~" in {
      plan("select * from zips where city ~ '^B[AEIOU]+LD.*'") must beWorkflow(chain(
        $read(Collection("db", "zips")),
        $match(Selector.And(
          Selector.Doc(
            BsonField.Name("city") -> Selector.Type(BsonType.Text)),
          Selector.Doc(
            BsonField.Name("city") -> Selector.Regex("^B[AEIOU]+LD.*", false, true, false, false))))))
    }

    "plan filter with ~*" in {
      plan("select * from zips where city ~* '^B[AEIOU]+LD.*'") must beWorkflow(chain(
        $read(Collection("db", "zips")),
        $match(Selector.And(
          Selector.Doc(
            BsonField.Name("city") -> Selector.Type(BsonType.Text)),
          Selector.Doc(
            BsonField.Name("city") -> Selector.Regex("^B[AEIOU]+LD.*", true, true, false, false))))))
    }

    "plan filter with !~" in {
      plan("select * from zips where city !~ '^B[AEIOU]+LD.*'") must beWorkflow(chain(
        $read(Collection("db", "zips")),
        $match(Selector.And(
          Selector.Doc(
            BsonField.Name("city") -> Selector.Type(BsonType.Text)),
          Selector.Doc(ListMap[BsonField, Selector.SelectorExpr](
            BsonField.Name("city") -> Selector.NotExpr(Selector.Regex("^B[AEIOU]+LD.*", false, true, false, false))))))))
    }

    "plan filter with !~*" in {
      plan("select * from zips where city !~* '^B[AEIOU]+LD.*'") must beWorkflow(chain(
        $read(Collection("db", "zips")),
        $match(Selector.And(
          Selector.Doc(
            BsonField.Name("city") -> Selector.Type(BsonType.Text)),
          Selector.Doc(ListMap[BsonField, Selector.SelectorExpr](
            BsonField.Name("city") -> Selector.NotExpr(Selector.Regex("^B[AEIOU]+LD.*", true, true, false, false))))))))
    }

    "plan filter with alternative ~" in {
      plan("select * from a where 'foo' ~ pattern or target ~ pattern") must beWorkflow(chain(
        $read(Collection("db", "a")),
        $simpleMap(NonEmptyList(MapExpr(JsFn(Name("x"), obj(
          "__tmp10" -> Call(
            Select(New(Name("RegExp"), List(Select(ident("x"), "pattern"), jscore.Literal(Js.Str("m")))), "test"),
            List(jscore.Literal(Js.Str("foo")))),
          "__tmp11" -> ident("x"),
          "__tmp12" -> Select(ident("x"), "pattern"),
          "__tmp13" -> Select(ident("x"), "target"),
          "__tmp14" -> Call(
            Select(New(Name("RegExp"), List(Select(ident("x"), "pattern"), jscore.Literal(Js.Str("m")))), "test"),
            List(Select(ident("x"), "target"))))))),
          ListMap()),
        $match(
          Selector.Or(
            Selector.And(
              Selector.Doc(
                BsonField.Name("__tmp11") \ BsonField.Name("pattern") ->
                  Selector.Type(BsonType.Text)),
              Selector.Doc(
                BsonField.Name("__tmp10") -> Selector.Eq(Bson.Bool(true)))),
            Selector.And(
              Selector.Doc(
                BsonField.Name("__tmp12") -> Selector.Type(BsonType.Text)),
              Selector.And(
                Selector.Doc(
                  BsonField.Name("__tmp13") -> Selector.Type(BsonType.Text)),
                Selector.Doc(
                  BsonField.Name("__tmp14") -> Selector.Eq(Bson.Bool(true))))))),
        $project(
          reshape("value" -> $field("__tmp11")),
          ExcludeId)))
    }

    "plan filter with negate(s)" in {
      plan("select * from foo where bar != -10 and baz > -1.0") must
       beWorkflow(chain(
         $read(Collection("db", "foo")),
         $match(
           Selector.And(
             Selector.Or(
               Selector.Doc(BsonField.Name("baz") ->
                 Selector.Type(BsonType.Int32)),
               Selector.Doc(BsonField.Name("baz") ->
                 Selector.Type(BsonType.Int64)),
               Selector.Doc(BsonField.Name("baz") ->
                 Selector.Type(BsonType.Dec)),
               Selector.Doc(BsonField.Name("baz") ->
                 Selector.Type(BsonType.Text)),
               Selector.Or(
                 Selector.Doc(BsonField.Name("baz") ->
                   Selector.Type(BsonType.Date)),
                 Selector.Doc(BsonField.Name("baz") ->
                   Selector.Type(BsonType.Bool)))),
             Selector.And(
               Selector.Or(
                 Selector.Doc(BsonField.Name("bar") ->
                   Selector.Type(BsonType.Int32)),
                 Selector.Doc(BsonField.Name("bar") ->
                   Selector.Type(BsonType.Int64)),
                 Selector.Doc(BsonField.Name("bar") ->
                   Selector.Type(BsonType.Dec)),
                 Selector.Doc(BsonField.Name("bar") ->
                   Selector.Type(BsonType.Text)),
                 Selector.Or(
                   Selector.Doc(BsonField.Name("bar") ->
                     Selector.Type(BsonType.Date)),
                   Selector.Doc(BsonField.Name("bar") ->
                     Selector.Type(BsonType.Bool)))),
               Selector.And(
                 Selector.Doc(BsonField.Name("bar") ->
                   Selector.Neq(Bson.Int64(-10))),
                 Selector.Doc(BsonField.Name("baz") ->
                   Selector.Gt(Bson.Dec(-1.0)))))))))
    }

    "plan complex filter" in {
      plan("select * from foo where bar > 10 and (baz = 'quux' or foop = 'zebra')") must
       beWorkflow(chain(
         $read(Collection("db", "foo")),
         $match(
           Selector.And(
             Selector.Or(
               Selector.Doc(BsonField.Name("bar") ->
                 Selector.Type(BsonType.Int32)),
               Selector.Doc(BsonField.Name("bar") ->
                 Selector.Type(BsonType.Int64)),
               Selector.Doc(BsonField.Name("bar") ->
                 Selector.Type(BsonType.Dec)),
               Selector.Doc(BsonField.Name("bar") ->
                 Selector.Type(BsonType.Text)),
               Selector.Or(
                 Selector.Doc(BsonField.Name("bar") ->
                   Selector.Type(BsonType.Date)),
                 Selector.Doc(BsonField.Name("bar") ->
                   Selector.Type(BsonType.Bool)))),
             Selector.And(
               Selector.Doc(BsonField.Name("bar") -> Selector.Gt(Bson.Int64(10))),
               Selector.Or(
                 Selector.Doc(BsonField.Name("baz") ->
                   Selector.Eq(Bson.Text("quux"))),
                 Selector.Doc(BsonField.Name("foop") ->
                   Selector.Eq(Bson.Text("zebra")))))))))
    }

    "plan filter with not" in {
      plan("select * from zips where not (pop > 0 and pop < 1000)") must
       beWorkflow(chain(
         $read(Collection("db", "zips")),
         $match(
           Selector.And(
             // TODO: eliminate duplication
             Selector.Or(
               Selector.Doc(BsonField.Name("pop") ->
                 Selector.Type(BsonType.Int32)),
               Selector.Doc(BsonField.Name("pop") ->
                 Selector.Type(BsonType.Int64)),
               Selector.Doc(BsonField.Name("pop") ->
                 Selector.Type(BsonType.Dec)),
               Selector.Doc(BsonField.Name("pop") ->
                 Selector.Type(BsonType.Text)),
               Selector.Or(
                 Selector.Doc(BsonField.Name("pop") ->
                   Selector.Type(BsonType.Date)),
                 Selector.Doc(BsonField.Name("pop") ->
                   Selector.Type(BsonType.Bool)))),
             Selector.And(
               Selector.Or(
                 Selector.Doc(BsonField.Name("pop") ->
                   Selector.Type(BsonType.Int32)),
                 Selector.Doc(BsonField.Name("pop") ->
                   Selector.Type(BsonType.Int64)),
                 Selector.Doc(BsonField.Name("pop") ->
                   Selector.Type(BsonType.Dec)),
                 Selector.Doc(BsonField.Name("pop") ->
                   Selector.Type(BsonType.Text)),
                 Selector.Or(
                   Selector.Doc(BsonField.Name("pop") ->
                     Selector.Type(BsonType.Date)),
                   Selector.Doc(BsonField.Name("pop") ->
                     Selector.Type(BsonType.Bool)))),
               Selector.Or(
                 Selector.Doc(ListMap[BsonField, Selector.SelectorExpr](
                   BsonField.Name("pop") -> Selector.NotExpr(Selector.Gt(Bson.Int64(0))))),
                 Selector.Doc(ListMap[BsonField, Selector.SelectorExpr](
                   BsonField.Name("pop") -> Selector.NotExpr(Selector.Lt(Bson.Int64(1000)))))))))))
    }

    "plan filter with not and equality" in {
      plan("select * from zips where not (pop = 0)") must
        beWorkflow(chain(
          $read(Collection("db", "zips")),
          $match(
            Selector.Doc(ListMap[BsonField, Selector.SelectorExpr](
              BsonField.Name("pop") -> Selector.NotExpr(Selector.Eq(Bson.Int64(0))))))))
    }

    "plan filter with 'is not null'" in {
      plan("select * from zips where city is not null") must
        beWorkflow(chain(
          $read(Collection("db", "zips")),
          $match(
            Selector.Doc(ListMap[BsonField, Selector.SelectorExpr](
              BsonField.Name("city") -> Selector.NotExpr(Selector.Eq(Bson.Null)))))))
    }

    "plan filter with both index and field projections" in {
      plan("select count(parents[0].sha) as count from slamengine_commits where parents[0].sha = '56d1caf5d082d1a6840090986e277d36d03f1859'") must
        beWorkflow(chain(
          $read(Collection("db", "slamengine_commits")),
          $match(Selector.Where(
            If(
              BinOp(jscore.And,
                Call(Select(ident("Array"), "isArray"), List(Select(ident("this"), "parents"))),
                BinOp(jscore.And,
                  Call(ident("isObject"), List(
                    Access(
                      Select(ident("this"), "parents"),
                      jscore.Literal(Js.Num(0, false))))),
                  UnOp(jscore.Not,
                    Call(Select(ident("Array"), "isArray"), List(
                      Access(
                        Select(ident("this"), "parents"),
                        jscore.Literal(Js.Num(0, false)))))))),
              BinOp(jscore.Eq,
                Select(
                  Access(
                    Select(ident("this"), "parents"),
                    jscore.Literal(Js.Num(0, false))),
                  "sha"),
                jscore.Literal(Js.Str("56d1caf5d082d1a6840090986e277d36d03f1859"))),
              ident("undefined")).toJs)),
          // NB: This map _looks_ unnecessary, but is actually simpler than the
          //     default impl that would be triggered by the $where selector
          //     above.
          $simpleMap(
            NonEmptyList(MapExpr(JsFn(Name("x"), obj()))),
            ListMap()),
          $group(
            grouped("count" -> $sum($literal(Bson.Int32(1)))),
            \/-($literal(Bson.Null)))))
    }

    "plan simple having filter" in {
      plan("select city from zips group by city having count(*) > 10") must
      beWorkflow(chain(
        $read(Collection("db", "zips")),
        $group(
          grouped(
            "city"   -> $first($field("city")),
            "__tmp0" -> $sum($literal(Bson.Int32(1)))),
          -\/(reshape("" -> $field("city")))),
        $match(Selector.Doc(
          BsonField.Name("__tmp0") -> Selector.Gt(Bson.Int64(10)))),
        $project(
          reshape("city" -> $field("city")),
          ExcludeId)))
    }

    "plan having with multiple projections" in {
      plan("select city, sum(pop) from zips group by city having sum(pop) > 50000") must
      beWorkflow(chain(
        $read(Collection("db", "zips")),
        $group(
          grouped(
            "city" -> $first($field("city")),
            "1" ->
              $sum(
                $cond(
                  $and(
                    $lt($literal(Bson.Null), $field("pop")),
                    $lt($field("pop"), $literal(Bson.Text("")))),
                  $field("pop"),
                  $literal(Bson.Undefined)))),
          -\/(reshape("" -> $field("city")))),
        $match(Selector.Doc(
          BsonField.Name("1") -> Selector.Gt(Bson.Int64(50000))))))
    }

    "prefer projection+filter over JS filter" in {
      plan("select * from zips where city <> state") must
      beWorkflow(chain(
        $read(Collection("db", "zips")),
        $project(
          reshape(
            "__tmp4" -> $neq($field("city"), $field("state")),
            "__tmp5" -> $$ROOT),
          IgnoreId),
        $match(Selector.And(
          Selector.Or(
            Selector.Doc(BsonField.Name("__tmp5") \ BsonField.Name("state") ->
              Selector.Type(BsonType.Int32)),
            Selector.Doc(BsonField.Name("__tmp5") \ BsonField.Name("state") ->
              Selector.Type(BsonType.Int64)),
            Selector.Doc(BsonField.Name("__tmp5") \ BsonField.Name("state") ->
              Selector.Type(BsonType.Dec)),
            Selector.Doc(BsonField.Name("__tmp5") \ BsonField.Name("state") ->
              Selector.Type(BsonType.Text)),
            Selector.Or(
              Selector.Doc(BsonField.Name("__tmp5") \ BsonField.Name("state") ->
                Selector.Type(BsonType.Date)),
              Selector.Doc(BsonField.Name("__tmp5") \ BsonField.Name("state") ->
                Selector.Type(BsonType.Bool)))),
          Selector.And(
            Selector.Or(
              Selector.Doc(BsonField.Name("__tmp5") \ BsonField.Name("city") ->
                Selector.Type(BsonType.Int32)),
              Selector.Doc(BsonField.Name("__tmp5") \ BsonField.Name("city") ->
                Selector.Type(BsonType.Int64)),
              Selector.Doc(BsonField.Name("__tmp5") \ BsonField.Name("city") ->
                Selector.Type(BsonType.Dec)),
              Selector.Doc(BsonField.Name("__tmp5") \ BsonField.Name("city") ->
                Selector.Type(BsonType.Text)),
              Selector.Or(
                Selector.Doc(BsonField.Name("__tmp5") \ BsonField.Name("city") ->
                  Selector.Type(BsonType.Date)),
                Selector.Doc(BsonField.Name("__tmp5") \ BsonField.Name("city") ->
                  Selector.Type(BsonType.Bool)))),
            Selector.Doc(
              BsonField.Name("__tmp4") -> Selector.Eq(Bson.Bool(true)))))),
        $project(
          reshape("value" -> $field("__tmp5")),
          ExcludeId)))
    }

    "prefer projection+filter over nested JS filter" in {
      plan("select * from zips where city <> state and pop < 10000") must
      beWorkflow(chain(
        $read(Collection("db", "zips")),
        $project(
          reshape(
            "__tmp8" -> $neq($field("city"), $field("state")),
            "__tmp9" -> $$ROOT,
            "__tmp10" -> $field("pop")),
          IgnoreId),
        $match(Selector.And(
          Selector.Or(
            Selector.Doc(BsonField.Name("__tmp9") \ BsonField.Name("pop") ->
              Selector.Type(BsonType.Int32)),
            Selector.Doc(BsonField.Name("__tmp9") \ BsonField.Name("pop") ->
              Selector.Type(BsonType.Int64)),
            Selector.Doc(BsonField.Name("__tmp9") \ BsonField.Name("pop") ->
              Selector.Type(BsonType.Dec)),
            Selector.Doc(BsonField.Name("__tmp9") \ BsonField.Name("pop") ->
              Selector.Type(BsonType.Text)),
            Selector.Or(
              Selector.Doc(BsonField.Name("__tmp9") \ BsonField.Name("pop") ->
                Selector.Type(BsonType.Date)),
              Selector.Doc(BsonField.Name("__tmp9") \ BsonField.Name("pop") ->
                Selector.Type(BsonType.Bool)))),
          Selector.And(
            Selector.Or(
              Selector.Doc(BsonField.Name("__tmp9") \ BsonField.Name("state") ->
                Selector.Type(BsonType.Int32)),
              Selector.Doc(BsonField.Name("__tmp9") \ BsonField.Name("state") ->
                Selector.Type(BsonType.Int64)),
              Selector.Doc(BsonField.Name("__tmp9") \ BsonField.Name("state") ->
                Selector.Type(BsonType.Dec)),
              Selector.Doc(BsonField.Name("__tmp9") \ BsonField.Name("state") ->
                Selector.Type(BsonType.Text)),
              Selector.Or(
                Selector.Doc(BsonField.Name("__tmp9") \ BsonField.Name("state") ->
                  Selector.Type(BsonType.Date)),
                Selector.Doc(BsonField.Name("__tmp9") \ BsonField.Name("state") ->
                  Selector.Type(BsonType.Bool)))),
            Selector.And(Selector.Or(
              Selector.Doc(BsonField.Name("__tmp9") \ BsonField.Name("city") ->
                Selector.Type(BsonType.Int32)),
              Selector.Doc(BsonField.Name("__tmp9") \ BsonField.Name("city") ->
                Selector.Type(BsonType.Int64)),
              Selector.Doc(BsonField.Name("__tmp9") \ BsonField.Name("city") ->
                Selector.Type(BsonType.Dec)),
              Selector.Doc(BsonField.Name("__tmp9") \ BsonField.Name("city") ->
                Selector.Type(BsonType.Text)),
              Selector.Or(
                Selector.Doc(BsonField.Name("__tmp9") \ BsonField.Name("city") ->
                  Selector.Type(BsonType.Date)),
                Selector.Doc(BsonField.Name("__tmp9") \ BsonField.Name("city") ->
                  Selector.Type(BsonType.Bool)))),
              Selector.And(
                Selector.Doc(
                  BsonField.Name("__tmp8") -> Selector.Eq(Bson.Bool(true))),
                Selector.Doc(
                  BsonField.Name("__tmp10") -> Selector.Lt(Bson.Int64(10000)))))))),
        $project(
          reshape("value" -> $field("__tmp9")),
          ExcludeId)))
    }

    "filter on constant true" in {
      plan("select * from zips where true") must
        beWorkflow($read(Collection("db", "zips")))
    }

    "filter on constant false" in {
      plan("select * from zips where false") must
        beWorkflow($pure(Bson.Arr(Nil)))
    }

    "select partially-applied substing" in {
      plan ("select substring('abcdefghijklmnop', 5, pop / 10000) from zips") must
        beWorkflow(chain(
          $read(Collection("db", "zips")),
          $project(
            reshape(
              "0" ->
                $cond(
                  $and(
                    $lt($literal(Bson.Null), $field("pop")),
                    $lt($field("pop"), $literal(Bson.Text("")))),
                  $substr(
                    $literal(Bson.Text("fghijklmnop")),
                    $literal(Bson.Int64(0)),
                    $divide($field("pop"), $literal(Bson.Int64(10000)))),
                  $literal(Bson.Undefined))),
            IgnoreId)))
    }

    "drop nothing" in {
      plan("select * from zips limit 5 offset 0") must
        beWorkflow(chain(
          $read(Collection("db", "zips")),
          $limit(5)))
    }

    "concat with empty string" in {
      plan("select '' || city || '' from zips") must
        beWorkflow(chain(
          $read(Collection("db", "zips")),
          $project(
            reshape("0" ->
              $cond(
                $or(
                  $and(
                    $lte($literal(Bson.Arr(Nil)), $field("city")),
                    $lt($field("city"), $literal(Bson.Binary(scala.Array[Byte]())))),
                  $and(
                    $lte($literal(Bson.Text("")), $field("city")),
                    $lt($field("city"), $literal(Bson.Doc(ListMap()))))),
                $field("city"),
                $literal(Bson.Undefined))),
            IgnoreId)))
    }

    "plan simple sort with field in projection" in {
      plan("select bar from foo order by bar") must
        beWorkflow(chain(
          $read(Collection("db", "foo")),
          $project(
            reshape("bar" -> $field("bar")),
            IgnoreId),
          $sort(NonEmptyList(BsonField.Name("bar") -> Ascending))))
    }

    "plan simple sort with wildcard" in {
      plan("select * from zips order by pop") must
        beWorkflow(chain(
          $read(Collection("db", "zips")),
          $sort(NonEmptyList(BsonField.Name("pop") -> Ascending))))
    }

    "plan sort with expression in key" in {
      plan("select baz from foo order by bar/10") must
        beWorkflow(chain(
          $read(Collection("db", "foo")),
          $project(
            reshape(
              "baz"    -> $field("baz"),
              "__tmp2" ->
                $cond(
                  $or(
                    $and(
                      $lt($literal(Bson.Null), $field("bar")),
                      $lt($field("bar"), $literal(Bson.Text("")))),
                    $and(
                      $lte($literal(Bson.Date(Instant.ofEpochMilli(0))), $field("bar")),
                      $lt($field("bar"), $literal(Bson.Regex("", ""))))),
                  $divide($field("bar"), $literal(Bson.Int64(10))),
                  $literal(Bson.Undefined))),
            IgnoreId),
          $sort(NonEmptyList(BsonField.Name("__tmp2") -> Ascending)),
          $project(
            reshape("baz" -> $field("baz")),
            ExcludeId)))
    }

    "plan select with wildcard and field" in {
      plan("select *, pop from zips") must
        beWorkflow(chain(
          $read(Collection("db", "zips")),
          $simpleMap(
            NonEmptyList(MapExpr(JsFn(Name("x"),
              SpliceObjects(List(
                ident("x"),
                obj(
                  "pop" -> Select(ident("x"), "pop"))))))),
            ListMap())))
    }

    "plan select with wildcard and two fields" in {
      plan("select *, city as city2, pop as pop2 from zips") must
        beWorkflow(chain(
          $read(Collection("db", "zips")),
          $simpleMap(
            NonEmptyList(MapExpr(JsFn(Name("x"),
              SpliceObjects(List(
                ident("x"),
                obj(
                  "city2" -> Select(ident("x"), "city")),
                obj(
                  "pop2"  -> Select(ident("x"), "pop"))))))),
            ListMap())))
    }

    "plan select with wildcard and two constants" in {
      plan("select *, '1', '2' from zips") must
        beWorkflow(chain(
          $read(Collection("db", "zips")),
          $simpleMap(
            NonEmptyList(MapExpr(JsFn(Name("x"),
              SpliceObjects(List(
                ident("x"),
                obj(
                  "1" -> jscore.Literal(Js.Str("1"))),
                obj(
                  "2" -> jscore.Literal(Js.Str("2")))))))),
            ListMap())))
    }

    "plan select with multiple wildcards and fields" in {
      plan("select state as state2, *, city as city2, *, pop as pop2 from zips where pop < 1000") must
        beWorkflow(chain(
          $read(Collection("db", "zips")),
          $match(Selector.And(
            Selector.Or(
              Selector.Doc(BsonField.Name("pop") ->
                Selector.Type(BsonType.Int32)),
              Selector.Doc(BsonField.Name("pop") ->
                Selector.Type(BsonType.Int64)),
              Selector.Doc(BsonField.Name("pop") ->
                Selector.Type(BsonType.Dec)),
              Selector.Doc(BsonField.Name("pop") ->
                Selector.Type(BsonType.Text)),
              Selector.Or(
                Selector.Doc(BsonField.Name("pop") ->
                  Selector.Type(BsonType.Date)),
                Selector.Doc(BsonField.Name("pop") ->
                  Selector.Type(BsonType.Bool)))),
            Selector.Doc(
              BsonField.Name("pop") -> Selector.Lt(Bson.Int64(1000))))),
          $simpleMap(
            NonEmptyList(MapExpr(JsFn(Name("x"),
              obj(
                "__tmp2" -> SpliceObjects(List(
                  obj(
                    "state2" -> Select(ident("x"), "state")),
                  ident("x"),
                  obj(
                    "city2" -> Select(ident("x"), "city")),
                  ident("x"),
                  obj(
                    "pop2" -> Select(ident("x"), "pop")))))))),
            ListMap()),
          $project(
            reshape("value" -> $field("__tmp2")),
            ExcludeId)))
    }

    "plan sort with wildcard and expression in key" in {
      plan("select * from zips order by pop/10 desc") must
        beWorkflow(chain(
          $read(Collection("db", "zips")),
          $simpleMap(
            NonEmptyList(MapExpr(JsFn(Name("__val"), SpliceObjects(List(
              ident("__val"),
              obj(
                "__sd__0" ->
                  jscore.If(
                    BinOp(jscore.Or, BinOp(jscore.Or, BinOp(jscore.Or,
                      Call(ident("isNumber"), List(Select(ident("__val"), "pop"))),
                      BinOp(Instance, Select(ident("__val"), "pop"), ident("NumberInt"))),
                      BinOp(Instance, Select(ident("__val"), "pop"), ident("NumberLong"))),
                      BinOp(jscore.Or,
                        BinOp(Instance, Select(ident("__val"), "pop"), ident("Date")),
                        BinOp(Instance, Select(ident("__val"), "pop"), ident("Timestamp")))),
                    BinOp(Div, Select(ident("__val"), "pop"), jscore.Literal(Js.Num(10, false))),
                    ident("undefined")))))))),
            ListMap()),
          $simpleMap(
            NonEmptyList(MapExpr(JsFn(Name("__val"), obj(
              "__tmp2" ->
                Call(ident("remove"),
                  List(ident("__val"), jscore.Literal(Js.Str("__sd__0")))),
              "__tmp3" -> Select(ident("__val"), "__sd__0"))))),
            ListMap()),
          $sort(NonEmptyList(BsonField.Name("__tmp3") -> Descending)),
          $project(
            reshape("value" -> $field("__tmp2")),
            ExcludeId)))
    }

    "plan simple sort with field not in projections" in {
      plan("select name from person order by height") must
        beWorkflow(chain(
          $read(Collection("db", "person")),
          $project(
            reshape(
              "name"   -> $field("name"),
              "__tmp0" -> $field("height")),
            IgnoreId),
          $sort(NonEmptyList(BsonField.Name("__tmp0") -> Ascending)),
          $project(
            reshape("name" -> $field("name")),
            ExcludeId)))
    }

    "plan sort with expression and alias" in {
      plan("select pop/1000 as popInK from zips order by popInK") must
        beWorkflow(chain(
          $read(Collection("db", "zips")),
          $project(
            reshape(
              "popInK" ->
                $cond(
                  $or(
                    $and(
                      $lt($literal(Bson.Null), $field("pop")),
                      $lt($field("pop"), $literal(Bson.Text("")))),
                    $and(
                      $lte($literal(Bson.Date(Instant.ofEpochMilli(0))), $field("pop")),
                      $lt($field("pop"), $literal(Bson.Regex("", ""))))),
                  $divide($field("pop"), $literal(Bson.Int64(1000))),
                  $literal(Bson.Undefined))),
            IgnoreId),
          $sort(NonEmptyList(BsonField.Name("popInK") -> Ascending))))
    }

    "plan sort with filter" in {
      plan("select city, pop from zips where pop <= 1000 order by pop desc, city") must
        beWorkflow(chain(
          $read(Collection("db", "zips")),
          $match(Selector.And(
            Selector.Or(
              Selector.Doc(BsonField.Name("pop") ->
                Selector.Type(BsonType.Int32)),
              Selector.Doc(BsonField.Name("pop") ->
                Selector.Type(BsonType.Int64)),
              Selector.Doc(BsonField.Name("pop") ->
                Selector.Type(BsonType.Dec)),
              Selector.Doc(BsonField.Name("pop") ->
                Selector.Type(BsonType.Text)),
              Selector.Or(
                Selector.Doc(BsonField.Name("pop") ->
                  Selector.Type(BsonType.Date)),
                Selector.Doc(BsonField.Name("pop") ->
                  Selector.Type(BsonType.Bool)))),
            Selector.Doc(
              BsonField.Name("pop") -> Selector.Lte(Bson.Int64(1000))))),
          $project(
            reshape(
              "city" -> $field("city"),
              "pop"  -> $field("pop")),
            IgnoreId),
          $sort(NonEmptyList(
            BsonField.Name("pop") -> Descending,
            BsonField.Name("city") -> Ascending))))
    }

    "plan sort with expression, alias, and filter" in {
      plan("select pop/1000 as popInK from zips where pop >= 1000 order by popInK") must
        beWorkflow(chain(
          $read(Collection("db", "zips")),
          $match(Selector.And(
            Selector.Or(
              Selector.Doc(BsonField.Name("pop") ->
                Selector.Type(BsonType.Int32)),
              Selector.Doc(BsonField.Name("pop") ->
                Selector.Type(BsonType.Int64)),
              Selector.Doc(BsonField.Name("pop") ->
                Selector.Type(BsonType.Dec)),
              Selector.Doc(BsonField.Name("pop") ->
                Selector.Type(BsonType.Text)),
              Selector.Or(
                Selector.Doc(BsonField.Name("pop") ->
                  Selector.Type(BsonType.Date)),
                Selector.Doc(BsonField.Name("pop") ->
                  Selector.Type(BsonType.Bool)))),

            Selector.Doc(BsonField.Name("pop") -> Selector.Gte(Bson.Int64(1000))))),
          $project(
            reshape(
              "popInK" ->
                $cond(
                  $or(
                    $and(
                      $lt($literal(Bson.Null), $field("pop")),
                      $lt($field("pop"), $literal(Bson.Text("")))),
                    $and(
                      $lte($literal(Bson.Date(Instant.ofEpochMilli(0))), $field("pop")),
                      $lt($field("pop"), $literal(Bson.Regex("", ""))))),
                  $divide($field("pop"), $literal(Bson.Int64(1000))),
                  $literal(Bson.Undefined))),
            ExcludeId),
          $sort(NonEmptyList(BsonField.Name("popInK") -> Ascending)))
        )
    }

    "plan multiple column sort with wildcard" in {
      plan("select * from zips order by pop, city desc") must
       beWorkflow(chain(
         $read(Collection("db", "zips")),
         $sort(NonEmptyList(
           BsonField.Name("pop") -> Ascending,
           BsonField.Name("city") -> Descending))))
    }

    "plan many sort columns" in {
      plan("select * from zips order by pop, state, city, a4, a5, a6") must
       beWorkflow(chain(
         $read(Collection("db", "zips")),
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
            $read(Collection("db", "zips")),
            $group(
              grouped(
                "city" -> $push($field("city")),
                "cnt"  -> $sum($literal(Bson.Int32(1)))),
              \/-($literal(Bson.Null))),
            $unwind(DocField("city")),
            $sort(NonEmptyList(BsonField.Name("cnt") -> Descending)))
        }
    }

    "plan count and js expr" in {
      plan("SELECT COUNT(*) as cnt, LENGTH(city) FROM zips") must
        beWorkflow {
          chain(
            $read(Collection("db", "zips")),
            $simpleMap(NonEmptyList(MapExpr(JsFn(Name("x"), obj(
              "1" ->
                If(Call(ident("isString"), List(Select(ident("x"), "city"))),
                  Select(Select(ident("x"), "city"), "length"),
                  ident("undefined")))))),
              ListMap()),
            $group(
              grouped(
                "cnt" -> $sum($literal(Bson.Int32(1))),
                "1"   -> $push($field("1"))),
              \/-($literal(Bson.Null))),
            $unwind(DocField("1")))
        }
    }

    "plan trivial group by" in {
      plan("select city from zips group by city") must
      beWorkflow(chain(
        $read(Collection("db", "zips")),
        $group(
          grouped("city" -> $first($field("city"))),
          -\/(reshape("" -> $field("city"))))))
    }

    "plan useless group by expression" in {
      plan("select city from zips group by lower(city)") must
      beWorkflow(chain(
        $read(Collection("db", "zips")),
        $project(
          reshape("city" -> $field("city")),
          IgnoreId)))
    }

    "plan useful group by" in {
      plan("select city || ', ' || state, sum(pop) from zips group by city, state") must
      beWorkflow(chain(
        $read(Collection("db", "zips")),
        $group(
          grouped(
            "__tmp10" ->
              $first(
                $cond(
                  $or(
                    $and(
                      $lte($literal(Bson.Arr(Nil)), $field("city")),
                      $lt($field("city"), $literal(Bson.Binary(scala.Array[Byte]())))),
                    $and(
                      $lte($literal(Bson.Text("")), $field("city")),
                      $lt($field("city"), $literal(Bson.Doc(ListMap()))))),
                  $field("city"),
                  $literal(Bson.Undefined))),
            "__tmp11" ->
              $first(
                $cond(
                  $or(
                    $and(
                      $lte($literal(Bson.Arr(Nil)), $field("state")),
                      $lt($field("state"), $literal(Bson.Binary(scala.Array[Byte]())))),
                    $and(
                      $lte($literal(Bson.Text("")), $field("state")),
                      $lt($field("state"), $literal(Bson.Doc(ListMap()))))),
                  $field("state"),
                  $literal(Bson.Undefined))),
            "1" ->
              $sum(
                $cond(
                  $and(
                    $lt($literal(Bson.Null), $field("pop")),
                    $lt($field("pop"), $literal(Bson.Text("")))),
                  $field("pop"),
                  $literal(Bson.Undefined)))),
          -\/(reshape(
            "0" -> $field("city"),
            "1" -> $field("state")))),
        $project(
          reshape(
            "0" ->
              $concat(
                $concat($field("__tmp10"), $literal(Bson.Text(", "))),
                $field("__tmp11")),
            "1" -> $field("1")),
          IgnoreId)))
    }

    "plan group by expression" in {
      plan("select city, sum(pop) from zips group by lower(city)") must
      beWorkflow(chain(
        $read(Collection("db", "zips")),
        $group(
          grouped(
            "city" -> $push($field("city")),
            "1" ->
              $sum(
                $cond(
                  $and(
                    $lt($literal(Bson.Null), $field("pop")),
                    $lt($field("pop"), $literal(Bson.Text("")))),
                  $field("pop"),
                  $literal(Bson.Undefined)))),
          -\/(reshape(
            "" ->
              $cond(
                $and(
                  $lte($literal(Bson.Text("")), $field("city")),
                  $lt($field("city"), $literal(Bson.Doc(ListMap())))),
                $toLower($field("city")),
                $literal(Bson.Undefined))))),
        $unwind(DocField(BsonField.Name("city")))))
    }

    "plan group by month" in {
      plan("select avg(score) as a, DATE_PART('month', \"date\") as m from caloriesBurnedData group by DATE_PART('month', \"date\")") must
        beWorkflow(chain(
          $read(Collection("db", "caloriesBurnedData")),
          $group(
            grouped(
              "a" ->
                $avg(
                  $cond(
                    $and(
                      $lt($literal(Bson.Null), $field("score")),
                      $lt($field("score"), $literal(Bson.Text("")))),
                    $field("score"),
                    $literal(Bson.Undefined))),
              "m" ->
                $first(
                  $cond(
                    $and(
                      $lte($literal(Bson.Date(Instant.ofEpochMilli(0))), $field("date")),
                      $lt($field("date"), $literal(Bson.Regex("", "")))),
                    $month($field("date")),
                    $literal(Bson.Undefined)))),
            -\/(reshape(
              "" ->
                $cond(
                  $and(
                    $lte($literal(Bson.Date(Instant.ofEpochMilli(0))), $field("date")),
                    $lt($field("date"), $literal(Bson.Regex("", "")))),
                  $month($field("date")),
                  $literal(Bson.Undefined)))))))
    }

    "plan expr3 with grouping" in {
      plan("select case when pop > 1000 then city else lower(city) end, count(*) from zips group by city") must
        beRight
    }

    "plan trivial group by with wildcard" in {
      plan("select * from zips group by city") must
        beWorkflow($read(Collection("db", "zips")))
    }

    "plan count grouped by single field" in {
      plan("select count(*) from bar group by baz") must
        beWorkflow {
          chain(
            $read(Collection("db", "bar")),
            $group(
              grouped("0" -> $sum($literal(Bson.Int32(1)))),
              -\/(reshape("" -> $field("baz")))))
        }
    }

    "plan count and sum grouped by single field" in {
      plan("select count(*) as cnt, sum(biz) as sm from bar group by baz") must
        beWorkflow {
          chain(
            $read(Collection("db", "bar")),
            $group(
              grouped(
                "cnt" -> $sum($literal(Bson.Int32(1))),
                "sm" ->
                  $sum(
                    $cond(
                      $and(
                        $lt($literal(Bson.Null), $field("biz")),
                        $lt($field("biz"), $literal(Bson.Text("")))),
                      $field("biz"),
                      $literal(Bson.Undefined)))),
              -\/(reshape("" -> $field("baz")))))
        }
    }

    "plan sum grouped by single field with filter" in {
      plan("select sum(pop) as sm from zips where state='CO' group by city") must
        beWorkflow {
          chain(
            $read(Collection("db", "zips")),
            $match(Selector.Doc(
              BsonField.Name("state") -> Selector.Eq(Bson.Text("CO")))),
            $group(
              grouped("sm" ->
                $sum(
                  $cond(
                    $and(
                      $lt($literal(Bson.Null), $field("pop")),
                      $lt($field("pop"), $literal(Bson.Text("")))),
                    $field("pop"),
                    $literal(Bson.Undefined)))),
              -\/(reshape("" -> $field("city")))))
        }
    }

    "plan count and field when grouped" in {
      plan("select count(*) as cnt, city from zips group by city") must
        beWorkflow {
          chain(
            $read(Collection("db", "zips")),
            $group(
              grouped(
                "cnt"  -> $sum($literal(Bson.Int32(1))),
                "city" -> $first($field("city"))),
              -\/(reshape("" -> $field("city")))))
        }
    }

    "collect unaggregated fields into single doc when grouping" in {
      plan("select city, state, sum(pop) from zips") must
      beWorkflow(chain(
        $read(Collection("db", "zips")),
        $project(
          reshape(
            "__tmp3" -> reshape(
              "city"  -> $field("city"),
              "state" -> $field("state")),
            "__tmp4" -> reshape(
              "__tmp2" ->
                $cond(
                  $and(
                    $lt($literal(Bson.Null), $field("pop")),
                    $lt($field("pop"), $literal(Bson.Text("")))),
                  $field("pop"),
                  $literal(Bson.Undefined)))),
          IgnoreId),
        $group(
          grouped(
            "2" -> $sum($field("__tmp4", "__tmp2")),
            "__tmp3" -> $push($field("__tmp3"))),
          \/-($literal(Bson.Null))),
        $unwind(DocField("__tmp3")),
        $project(
          reshape(
            "city"  -> $field("__tmp3", "city"),
            "state" -> $field("__tmp3", "state"),
            "2"     -> $field("2")),
          IgnoreId)))
    }

    "plan unaggregated field when grouping, second case" in {
      // NB: the point being that we don't want to push $$ROOT
      plan("select max(pop)/1000, pop from zips") must
        beWorkflow {
          chain(
            $read(Collection("db", "zips")),
            $group(
              grouped(
                "__tmp3" ->
                  $max($cond(
                    $or(
                      $and(
                        $lt($literal(Bson.Null), $field("pop")),
                        $lt($field("pop"), $literal(Bson.Text("")))),
                      $and(
                        $lte($literal(Bson.Date(Instant.ofEpochMilli(0))), $field("pop")),
                        $lt($field("pop"), $literal(Bson.Regex("", ""))))),
                    $field("pop"),
                    $literal(Bson.Undefined))),
                "pop"    -> $push($field("pop"))),
              \/-($literal(Bson.Null))),
            $unwind(DocField("pop")),
            $project(
              reshape(
                "0"   -> $divide($field("__tmp3"), $literal(Bson.Int64(1000))),
                "pop" -> $field("pop")),
              IgnoreId))
        }
    }

    "plan double aggregation with another projection" in {
      plan("select sum(avg(pop)), min(city) from zips group by foo") must
        beWorkflow {
          chain(
            $read(Collection("db", "zips")),
            $group(
              grouped(
                "1"    ->
                  $min($cond(
                    $or(
                      $and(
                        $lt($literal(Bson.Null), $field("city")),
                        $lt($field("city"), $literal(Bson.Doc(ListMap())))),
                      $and(
                        $lte($literal(Bson.Bool(false)), $field("city")),
                        $lt($field("city"), $literal(Bson.Regex("", ""))))),
                    $field("city"),
                    $literal(Bson.Undefined))),
                "__tmp10" ->
                  $avg($cond(
                    $and(
                      $lt($literal(Bson.Null), $field("pop")),
                      $lt($field("pop"), $literal(Bson.Text("")))),
                    $field("pop"),
                    $literal(Bson.Undefined)))),
              -\/(reshape("" -> $field("foo")))),
            $group(
              grouped(
                "0" -> $sum($field("__tmp10")),
                "1" -> $push($field("1"))),
              \/-($literal(Bson.Null))),
            $unwind(DocField("1")))
        }
    }

    "plan aggregation on grouped field" in {
      plan("select city, count(city) from zips group by city") must
        beWorkflow {
          chain(
            $read(Collection("db", "zips")),
            $group(
              grouped(
                "city" -> $first($field("city")),
                "1"    -> $sum($literal(Bson.Int32(1)))),
              -\/(reshape("" -> $field("city")))))
        }
    }

    "plan multiple expressions using same field" in {
      plan("select pop, sum(pop), pop/1000 from zips") must
      beWorkflow(chain(
        $read (Collection("db", "zips")),
        $project(
          reshape(
            "__tmp5" -> reshape(
              "pop" -> $field("pop"),
              "2"   ->
                $cond(
                  $or(
                    $and(
                      $lt($literal(Bson.Null), $field("pop")),
                      $lt($field("pop"), $literal(Bson.Text("")))),
                    $and(
                      $lte($literal(Bson.Date(Instant.ofEpochMilli(0))), $field("pop")),
                      $lt($field("pop"), $literal(Bson.Regex("", ""))))),
                  $divide($field("pop"), $literal(Bson.Int64(1000))),
                  $literal(Bson.Undefined))),
            "__tmp6" -> reshape(
              "__tmp2" ->
                $cond(
                  $and(
                    $lt($literal(Bson.Null), $field("pop")),
                    $lt($field("pop"), $literal(Bson.Text("")))),
                  $field("pop"),
                  $literal(Bson.Undefined)))),
          IgnoreId),
        $group(
          grouped(
            "1" -> $sum($field("__tmp6", "__tmp2")),
            "__tmp5" -> $push($field("__tmp5"))),
          \/-($literal(Bson.Null))),
        $unwind(DocField("__tmp5")),
        $project(
          reshape(
            "pop" -> $field("__tmp5", "pop"),
            "1"   -> $field("1"),
            "2"   -> $field("__tmp5", "2")),
          IgnoreId)))
    }

    "plan sum of expression in expression with another projection when grouped" in {
      plan("select city, sum(pop-1)/1000 from zips group by city") must
      beWorkflow(chain(
        $read(Collection("db", "zips")),
        $group(
          grouped(
            "city"   -> $first($field("city")),
            "__tmp6" ->
              $sum(
                $cond(
                  $and(
                    $lt($literal(Bson.Null), $field("pop")),
                    $lt($field("pop"), $literal(Bson.Text("")))),
                  $subtract($field("pop"), $literal(Bson.Int64(1))),
                  $literal(Bson.Undefined)))),
          -\/(reshape("" -> $field("city")))),
        $project(
          reshape(
            "city" -> $field("city"),
            "1"    -> $divide($field("__tmp6"), $literal(Bson.Int64(1000)))),
          IgnoreId)))
    }

    "plan length of min (JS on top of reduce)" in {
      plan("select state, length(min(city)) as shortest from zips group by state") must
        beWorkflow(chain(
          $read(Collection("db", "zips")),
          $group(
            grouped(
              "state" -> $first($field("state")),
              "__tmp6" ->
                $min(
                  $cond(
                    $and(
                      $lte($literal(Bson.Text("")), $field("city")),
                      $lt($field("city"), $literal(Bson.Doc(ListMap())))),
                    $field("city"),
                    $literal(Bson.Undefined)))),
            -\/(reshape("" -> $field("state")))),
          $simpleMap(NonEmptyList(
            MapExpr(JsFn(Name("x"), obj(
              "state" -> Select(ident("x"), "state"),
              "shortest" -> Select(Select(ident("x"), "__tmp6"), "length"))))),
            ListMap()),
          $project(
            reshape(
              "state"    -> $include(),
              "shortest" -> $include()),
            IgnoreId)))
    }

    "plan js expr grouped by js expr" in {
      plan("select length(city) as len, count(*) as cnt from zips group by length(city)") must
        beWorkflow(chain(
          $read(Collection("db", "zips")),
          $simpleMap(
            NonEmptyList(MapExpr(JsFn(Name("x"),
              obj(
                "__tmp10" ->
                  If(Call(ident("isString"), List(Select(ident("x"), "city"))),
                    Select(Select(ident("x"), "city"), "length"),
                    ident("undefined")))))),
            ListMap()),
          $group(
            grouped(
              "len" -> $first($field("__tmp10")),
              "cnt" -> $sum($literal(Bson.Int32(1)))),
            -\/(reshape("" -> $field("__tmp10"))))))
    }

    "plan simple JS inside expression" in {
      plan("select length(city) + 1 from zips") must
        beWorkflow(chain(
          $read(Collection("db", "zips")),
          $simpleMap(NonEmptyList(MapExpr(JsFn(Name("x"), obj(
            "0" ->
              If(Call(ident("isString"), List(Select(ident("x"), "city"))),
                BinOp(jscore.Add,
                  Select(Select(ident("x"), "city"), "length"),
                  jscore.Literal(Js.Num(1, false))),
                ident("undefined")))))),
            ListMap()),
          $project(
            reshape("0" -> $include()),
            IgnoreId)))
    }

    "plan expressions with ~"in {
      plan("select foo ~ 'bar.*', 'abc' ~ 'a|b', 'baz' ~ regex, target ~ regex from a") must beWorkflow(chain(
        $read(Collection("db", "a")),
        $simpleMap(NonEmptyList(MapExpr(JsFn(Name("x"),
          obj(
            "0" -> If(Call(ident("isString"), List(Select(ident("x"), "foo"))),
              Call(
                Select(New(Name("RegExp"), List(jscore.Literal(Js.Str("bar.*")), jscore.Literal(Js.Str("m")))), "test"),
                List(Select(ident("x"), "foo"))),
              ident("undefined")),
            "1" -> jscore.Literal(Js.Bool(true)),
            "2" -> If(Call(ident("isString"), List(Select(ident("x"), "regex"))),
              Call(
                Select(New(Name("RegExp"), List(Select(ident("x"), "regex"), jscore.Literal(Js.Str("m")))), "test"),
                List(jscore.Literal(Js.Str("baz")))),
              ident("undefined")),
            "3" ->
              If(
                BinOp(jscore.And,
                  Call(ident("isString"), List(Select(ident("x"), "regex"))),
                  Call(ident("isString"), List(Select(ident("x"), "target")))),
                Call(
                  Select(New(Name("RegExp"), List(Select(ident("x"), "regex"), jscore.Literal(Js.Str("m")))), "test"),
                  List(Select(ident("x"), "target"))),
                ident("undefined")))))),
          ListMap()),
        $project(
          reshape(
            "0" -> $include(),
            "1" -> $include(),
            "2" -> $include(),
            "3" -> $include()),
          IgnoreId)))
    }

    "plan object flatten" in {
      plan("select geo{*} from usa_factbook") must
        beWorkflow {
          chain(
            $read(Collection("db", "usa_factbook")),
            $simpleMap(
              NonEmptyList(
                MapExpr(JsFn(Name("x"), obj(
                  "__tmp2" ->
                    If(
                      BinOp(jscore.And,
                        Call(ident("isObject"), List(Select(ident("x"), "geo"))),
                        UnOp(jscore.Not,
                          Call(Select(ident("Array"), "isArray"), List(Select(ident("x"), "geo"))))),
                      Select(ident("x"), "geo"),
                      Obj(ListMap(Name("") -> ident("undefined"))))))),
                FlatExpr(JsFn(Name("x"), Select(ident("x"), "__tmp2")))),
              ListMap()),
            $project(
              reshape("geo" -> $field("__tmp2")),
              IgnoreId))
        }
    }

    "plan array project with concat" in {
      plan("select city, loc[0] from zips") must
        beWorkflow {
          chain(
            $read(Collection("db", "zips")),
            $simpleMap(
              NonEmptyList(
                MapExpr(JsFn(Name("x"), obj(
                  "city" -> Select(ident("x"), "city"),
                  "1" ->
                    If(Call(Select(ident("Array"), "isArray"), List(Select(ident("x"), "loc"))),
                      Access(Select(ident("x"), "loc"), jscore.Literal(Js.Num(0, false))),
                      ident("undefined")))))),
              ListMap()),
            $project(
              reshape(
                "city" -> $include(),
                "1"    -> $include()),
              IgnoreId))
        }
    }

    "plan array concat with filter" in {
      plan("select loc || [ pop ] from zips where city = 'BOULDER'") must
        beWorkflow {
          chain(
            $read(Collection("db", "zips")),
            $match(Selector.Doc(
              BsonField.Name("city") -> Selector.Eq(Bson.Text("BOULDER")))),
            $simpleMap(NonEmptyList(MapExpr(JsFn(Name("x"), obj(
              "0" ->
                If(
                  BinOp(jscore.Or,
                    Call(Select(ident("Array"), "isArray"), List(Select(ident("x"), "loc"))),
                    Call(ident("isString"), List(Select(ident("x"), "loc")))),
                  SpliceArrays(List(
                    jscore.Select(ident("x"), "loc"),
                    jscore.Arr(List(jscore.Select(ident("x"), "pop"))))),
                  ident("undefined")))))),
              ListMap()),
            $project(
              reshape("0" -> $field("0")),
              ExcludeId))
        }
    }

    "plan array flatten" in {
      plan("select loc[*] from zips") must
        beWorkflow {
          chain(
            $read(Collection("db", "zips")),
            $project(
              reshape(
                "__tmp2" ->
                  $cond(
                    $and(
                      $lte($literal(Bson.Arr(List())), $field("loc")),
                      $lt($field("loc"), $literal(Bson.Binary(scala.Array[Byte]())))),
                    $field("loc"),
                    $literal(Bson.Arr(List(Bson.Undefined))))),
              IgnoreId),
            $unwind(DocField(BsonField.Name("__tmp2"))),
            $project(
              reshape("loc" -> $field("__tmp2")),
              IgnoreId))
        }
    }

    "plan array concat" in {
      plan("select loc || [ 0, 1, 2 ] from zips") must beWorkflow {
        chain(
          $read(Collection("db", "zips")),
          $simpleMap(NonEmptyList(
            MapExpr(JsFn(Name("x"),
              Obj(ListMap(
                Name("0") ->
                  If(
                    BinOp(jscore.Or,
                      Call(Select(ident("Array"), "isArray"), List(Select(ident("x"), "loc"))),
                      Call(ident("isString"), List(Select(ident("x"), "loc")))),
                    SpliceArrays(List(
                      Select(ident("x"), "loc"),
                      Arr(List(
                        jscore.Literal(Js.Num(0, false)),
                        jscore.Literal(Js.Num(1, false)),
                        jscore.Literal(Js.Num(2, false)))))),
                    ident("undefined"))))))),
            ListMap()),
          $project(
            reshape("0" -> $include()),
            IgnoreId))
      }
    }

    "plan array flatten with unflattened field" in {
      plan("SELECT _id as zip, loc as loc, loc[*] as coord FROM zips") must
        beWorkflow {
          chain(
            $read(Collection("db", "zips")),
            $project(
              reshape(
                "__tmp2" ->
                  $cond(
                    $and(
                      $lte($literal(Bson.Arr(List())), $field("loc")),
                      $lt($field("loc"), $literal(Bson.Binary(scala.Array[Byte]())))),
                    $field("loc"),
                    $literal(Bson.Arr(List(Bson.Undefined)))),
                "__tmp3" -> $$ROOT),
              IgnoreId),
            $unwind(DocField(BsonField.Name("__tmp2"))),
            $project(
              reshape(
                "zip"   -> $field("__tmp3", "_id"),
                "loc"   -> $field("__tmp3", "loc"),
                "coord" -> $field("__tmp2")),
              IgnoreId))
        }
    }

    "unify flattened fields" in {
      plan("select loc[*] from zips where loc[*] < 0") must
      beWorkflow(chain(
        $read(Collection("db", "zips")),
        $project(
          reshape(
            "__tmp5" ->
              $cond(
                $and(
                  $lte($literal(Bson.Arr(List())), $field("loc")),
                  $lt($field("loc"), $literal(Bson.Binary(scala.Array[Byte]())))),
                $field("loc"),
                $literal(Bson.Arr(List(Bson.Undefined))))),
          IgnoreId),
        $unwind(DocField(BsonField.Name("__tmp5"))),
        $match(Selector.Doc(
          BsonField.Name("__tmp5") -> Selector.Lt(Bson.Int64(0)))),
        $project(
          reshape("loc" -> $field("__tmp5")),
          IgnoreId)))
    }

    "group by flattened field" in {
      plan("select substring(parents[*].sha, 0, 1), count(*) from slamengine_commits group by substring(parents[*].sha, 0, 1)") must
      beWorkflow(chain(
        $read(Collection("db", "slamengine_commits")),
        $project(
          reshape(
            "__tmp12" ->
              $cond(
                $and(
                  $lte($literal(Bson.Arr(List())), $field("parents")),
                  $lt($field("parents"), $literal(Bson.Binary(scala.Array[Byte]())))),
                $field("parents"),
                $literal(Bson.Arr(List(Bson.Undefined))))),
          IgnoreId),
        $unwind(DocField(BsonField.Name("__tmp12"))),
        $group(
          grouped(
            "0" ->
              $first(
                $cond(
                  $and(
                    $lte($literal(Bson.Text("")), $field("__tmp12", "sha")),
                    $lt($field("__tmp12", "sha"), $literal(Bson.Doc(ListMap())))),
                  $substr($field("__tmp12", "sha"), $literal(Bson.Int64(0)), $literal(Bson.Int64(1))),
                  $literal(Bson.Undefined))),
            "1" -> $sum($literal(Bson.Int32(1)))),
          -\/(reshape(
            "" ->
              $cond(
                $and(
                  $lte($literal(Bson.Text("")), $field("__tmp12", "sha")),
                  $lt($field("__tmp12", "sha"), $literal(Bson.Doc(ListMap())))),
                $substr($field("__tmp12", "sha"), $literal(Bson.Int64(0)), $literal(Bson.Int64(1))),
                $literal(Bson.Undefined)))))))
    }

    "unify flattened fields with unflattened field" in {
      plan("select _id as zip, loc[*] from zips order by loc[*]") must
      beWorkflow(chain(
        $read(Collection("db", "zips")),
        $project(
          reshape(
            "__tmp2" ->
              $cond(
                $and(
                  $lte($literal(Bson.Arr(List())), $field("loc")),
                  $lt($field("loc"), $literal(Bson.Binary(scala.Array[Byte]())))),
                $field("loc"),
                $literal(Bson.Arr(List(Bson.Undefined)))),
            "__tmp3" -> $$ROOT),
          IgnoreId),
        $unwind(DocField(BsonField.Name("__tmp2"))),
        $project(
          reshape(
            "zip" -> $field("__tmp3", "_id"),
            "loc" -> $field("__tmp2")),
          IgnoreId),
        $sort(NonEmptyList(BsonField.Name("loc") -> Ascending))))
    }

    "unify flattened with double-flattened" in {
      plan("select * from user_comments where (comments[*].id LIKE '%Dr%' OR comments[*].replyTo[*] LIKE '%Dr%')") must
      beWorkflow(chain(
        $read(Collection("db", "user_comments")),
        $project(
          reshape(
            "__tmp16" ->
              $cond(
                $and(
                  $lte($literal(Bson.Arr(List())), $field("comments")),
                  $lt($field("comments"), $literal(Bson.Binary(scala.Array[Byte]())))),
                $field("comments"),
                $literal(Bson.Arr(List(Bson.Undefined)))),
            "__tmp17" -> $$ROOT),
          IgnoreId),
        $unwind(DocField(BsonField.Name("__tmp16"))),
        $project(
          reshape(
            "__tmp20" ->
              $cond(
                $and(
                  $lte($literal(Bson.Arr(List())), $field("__tmp16", "replyTo")),
                  $lt($field("__tmp16", "replyTo"), $literal(Bson.Binary(scala.Array[Byte]())))),
                $field("__tmp16", "replyTo"),
                $literal(Bson.Arr(List(Bson.Undefined)))),
            "__tmp21" -> $$ROOT),
          IgnoreId),
        $unwind(DocField(BsonField.Name("__tmp20"))),
        $match(Selector.Or(
          Selector.And(
            Selector.Doc(
              BsonField.Name("__tmp21") \ BsonField.Name("__tmp16") \ BsonField.Name("id") -> Selector.Type(BsonType.Text)),
            Selector.Doc(
              BsonField.Name("__tmp21") \ BsonField.Name("__tmp16") \ BsonField.Name("id") -> Selector.Regex("^.*Dr.*$", false, true, false, false))),
          Selector.Doc(
            BsonField.Name("__tmp20") -> Selector.Regex("^.*Dr.*$", false, true, false, false)))),
        $project(
          reshape("value" -> $field("__tmp21", "__tmp17")),
          ExcludeId)))
    }

    "plan limit with offset" in {
      plan("SELECT * FROM zips OFFSET 100 LIMIT 5") must
        beWorkflow(chain(
          $read(Collection("db", "zips")),
          $limit(105),
          $skip(100)))
    }

    "plan sort and limit" in {
      plan("SELECT city, pop FROM zips ORDER BY pop DESC LIMIT 5") must
        beWorkflow {
          chain(
            $read(Collection("db", "zips")),
            $project(
              reshape(
                "city" -> $field("city"),
                "pop"  -> $field("pop")),
              IgnoreId),
            $sort(NonEmptyList(BsonField.Name("pop") -> Descending)),
            $limit(5))
        }
    }

    "plan simple single field selection and limit" in {
      plan("SELECT city FROM zips LIMIT 5") must
        beWorkflow {
          chain(
            $read(Collection("db", "zips")),
            $limit(5),
            $project(
              reshape("city" -> $field("city")),
              IgnoreId))
        }
    }

    "plan complex group by with sorting and limiting" in {
      plan("SELECT city, SUM(pop) AS pop FROM zips GROUP BY city ORDER BY pop") must
        beWorkflow {
          chain(
            $read(Collection("db", "zips")),
            $group(
              grouped(
                "city" -> $first($field("city")),
                "pop"  ->
                  $sum(
                    $cond(
                      $and(
                        $lt($literal(Bson.Null), $field("pop")),
                        $lt($field("pop"), $literal(Bson.Text("")))),
                      $field("pop"),
                      $literal(Bson.Undefined)))),
              -\/(reshape("" -> $field("city")))),
            $sort(NonEmptyList(BsonField.Name("pop") -> Ascending)))
        }
    }

    "plan filter and expressions with IS NULL" in {
      plan("select foo is null from zips where foo is null") must
        beWorkflow(chain(
          $read(Collection("db", "zips")),
          $match(Selector.Doc(
            BsonField.Name("foo") -> Selector.Eq(Bson.Null))),
          $project(
            reshape("0" -> $eq($field("foo"), $literal(Bson.Null))),
            ExcludeId)))
    }

    "plan implicit group by with filter" in {
      plan("select avg(pop), min(city) from zips where state = 'CO'") must
        beWorkflow(chain(
          $read(Collection("db", "zips")),
          $match(Selector.Doc(
            BsonField.Name("state") -> Selector.Eq(Bson.Text("CO")))),
          $group(
            grouped(
              "0" ->
                $avg(
                  $cond(
                    $and(
                      $lt($literal(Bson.Null), $field("pop")),
                      $lt($field("pop"), $literal(Bson.Text("")))),
                    $field("pop"),
                    $literal(Bson.Undefined))),
              "1" ->
                $min(
                  $cond($or(
                    $and(
                      $lt($literal(Bson.Null), $field("city")),
                      $lt($field("city"), $literal(Bson.Doc(ListMap())))),
                    $and(
                      $lte($literal(Bson.Bool(false)), $field("city")),
                      $lt($field("city"), $literal(Bson.Regex("", ""))))),
                    $field("city"),
                    $literal(Bson.Undefined)))),
            \/-($literal(Bson.Null)))))
    }

    "plan simple distinct" in {
      plan("select distinct city, state from zips") must
      beWorkflow(
        chain(
          $read(Collection("db", "zips")),
          $project(
            reshape(
              "city"  -> $field("city"),
              "state" -> $field("state")),
            IgnoreId),
          $group(
            grouped("__tmp0" -> $first($$ROOT)),
            -\/(reshape(
              "0"  -> $field("city"),
              "1" -> $field("state")))),
          $project(
            reshape(
              "city"  -> $field("__tmp0", "city"),
              "state" -> $field("__tmp0", "state")),
            ExcludeId)))
    }

    "plan distinct as expression" in {
      plan("select count(distinct(city)) from zips") must
        beWorkflow(chain(
          $read(Collection("db", "zips")),
          $group(
            grouped(),
            -\/(reshape("" -> $field("city")))),
          $group(
            grouped("0" -> $sum($literal(Bson.Int32(1)))),
            \/-($literal(Bson.Null)))))
    }

    "plan distinct of expression as expression" in {
      plan("select count(distinct substring(city, 0, 1)) from zips") must
        beWorkflow(chain(
          $read(Collection("db", "zips")),
          $group(
            grouped(),
            -\/(reshape(
              "" ->
                $cond(
                  $and(
                    $lte($literal(Bson.Text("")), $field("city")),
                    $lt($field("city"), $literal(Bson.Doc(ListMap())))),
                  $substr(
                    $field("city"),
                    $literal(Bson.Int64(0)),
                    $literal(Bson.Int64(1))),
                  $literal(Bson.Undefined))))),
          $group(
            grouped("0" -> $sum($literal(Bson.Int32(1)))),
            \/-($literal(Bson.Null)))))
    }

    "plan distinct of wildcard" in {
      plan("select distinct * from zips") must
        beWorkflow(chain(
          $read(Collection("db", "zips")),
          $simpleMap(NonEmptyList(MapExpr(JsFn(Name("x"),
            obj(
              "__tmp1" ->
                Call(ident("remove"),
                  List(ident("x"), jscore.Literal(Js.Str("_id")))))))),
            ListMap()),
          $group(
            grouped("__tmp2" -> $first($field("__tmp1"))),
            -\/(reshape("" -> $field("__tmp1")))),
          $project(
            reshape("value" -> $field("__tmp2")),
            ExcludeId)))
    }

    "plan distinct of wildcard as expression" in {
      plan("select count(distinct *) from zips") must
        beWorkflow(chain(
          $read(Collection("db", "zips")),
          $simpleMap(NonEmptyList(MapExpr(JsFn(Name("x"),
            obj(
              "__tmp4" ->
                Call(ident("remove"),
                  List(ident("x"), jscore.Literal(Js.Str("_id")))))))),
            ListMap()),
          $group(
            grouped(),
            -\/(reshape("" -> $field("__tmp4")))),
          $group(
            grouped("0" -> $sum($literal(Bson.Int32(1)))),
            \/-($literal(Bson.Null)))))
    }

    "plan distinct with simple order by" in {
      plan("select distinct city from zips order by city") must
        beWorkflow(
          chain(
            $read(Collection("db", "zips")),
            $project(
              reshape("city" -> $field("city")),
              IgnoreId),
            $sort(NonEmptyList(BsonField.Name("city") -> Ascending)),
            $group(
              grouped("__tmp1" -> $first($$ROOT)),
              -\/(reshape("" -> $field("city")))),
            $sort(NonEmptyList(
              BsonField.Name("__tmp1") \ BsonField.Name("city") -> Ascending)),
            $project(
              reshape("city" -> $field("__tmp1", "city")),
              ExcludeId)))
    }

    "plan distinct with unrelated order by" in {
      plan("select distinct city from zips order by pop desc") must
        beWorkflow(
          chain(
            $read(Collection("db", "zips")),
            $project(
              reshape(
                "__sd__0" -> $field("pop"),
                "city"    -> $field("city")),
              IgnoreId),
            $sort(NonEmptyList(
              BsonField.Name("__sd__0") -> Descending)),
            $group(
              grouped("__tmp2" -> $first($$ROOT)),
              -\/(reshape("city" -> $field("city")))),
            $project(
              reshape(
                "city" -> $field("__tmp2", "city"),
                "__tmp0" -> $field("__tmp2", "__sd__0")),
              IgnoreId),
            $sort(NonEmptyList(
              BsonField.Name("__tmp0") -> Descending)),
            $project(
              reshape("city" -> $field("city")),
              ExcludeId)))
    }

    "plan distinct as function with group" in {
      plan("select state, count(distinct(city)) from zips group by state") must
        beWorkflow(chain(
          $read(Collection("db", "zips")),
          $group(
            grouped("__tmp0" -> $first($$ROOT)),
            -\/(reshape("" -> $field("city")))),
          $group(
            grouped(
              "state" -> $first($field("__tmp0", "state")),
              "1"     -> $sum($literal(Bson.Int64(1)))),
            \/-($literal(Bson.Null)))))
    }.pendingUntilFixed

    "plan distinct with sum and group" in {
      plan("SELECT DISTINCT SUM(pop) AS totalPop, city, state FROM zips GROUP BY city") must
        beWorkflow(chain(
          $read(Collection("db", "zips")),
          $group(
            grouped(
              "totalPop" ->
                $sum(
                  $cond(
                    $and(
                      $lt($literal(Bson.Null), $field("pop")),
                      $lt($field("pop"), $literal(Bson.Text("")))),
                    $field("pop"),
                    $literal(Bson.Undefined))),
              "city"     -> $first($field("city")),
              "state"    -> $push($field("state"))),
            -\/(reshape("" -> $field("city")))),
          $unwind(DocField("state")),
          $group(
            grouped("__tmp7" -> $first($$ROOT)),
            -\/(reshape(
              "0" -> $field("totalPop"),
              "1" -> $field("city"),
              "2" -> $field("state")))),
          $project(
            reshape(
              "totalPop" -> $field("__tmp7", "totalPop"),
              "city"     -> $field("__tmp7", "city"),
              "state"    -> $field("__tmp7", "state")),
            ExcludeId)))
    }

    "plan distinct with sum, group, and orderBy" in {
      plan("SELECT DISTINCT SUM(pop) AS totalPop, city, state FROM zips GROUP BY city ORDER BY totalPop DESC") must
        beWorkflow(
          chain(
            $read(Collection("db", "zips")),
            $group(
              grouped(
                "totalPop" ->
                  $sum(
                    $cond(
                      $and(
                        $lt($literal(Bson.Null), $field("pop")),
                        $lt($field("pop"), $literal(Bson.Text("")))),
                      $field("pop"),
                      $literal(Bson.Undefined))),
                "city"     -> $first($field("city")),
                "state"    -> $push($field("state"))),
              -\/(reshape("" -> $field("city")))),
            $unwind(DocField("state")),
            $sort(NonEmptyList(BsonField.Name("totalPop") -> Descending)),
            $group(
              grouped("__tmp8" -> $first($$ROOT)),
              -\/(reshape(
                "0" -> $field("totalPop"),
                "1" -> $field("city"),
                "2" -> $field("state")))),
            $sort(NonEmptyList(
              BsonField.Name("__tmp8") \ BsonField.Name("totalPop") -> Descending)),
            $project(
              reshape(
                "totalPop" -> $field("__tmp8", "totalPop"),
                "city"     -> $field("__tmp8", "city"),
                "state"    -> $field("__tmp8", "state")),
              ExcludeId)))

    }

    "plan order by JS expr with filter" in {
      plan("select city, pop from zips where pop > 1000 order by length(city)") must
        beWorkflow(chain(
          $read(Collection("db", "zips")),
          $match(Selector.And(
            Selector.Or(
              Selector.Doc(BsonField.Name("pop") ->
                Selector.Type(BsonType.Int32)),
              Selector.Doc(BsonField.Name("pop") ->
                Selector.Type(BsonType.Int64)),
              Selector.Doc(BsonField.Name("pop") ->
                Selector.Type(BsonType.Dec)),
              Selector.Doc(BsonField.Name("pop") ->
                Selector.Type(BsonType.Text)),
              Selector.Or(
                Selector.Doc(BsonField.Name("pop") ->
                  Selector.Type(BsonType.Date)),
                Selector.Doc(BsonField.Name("pop") ->
                  Selector.Type(BsonType.Bool)))),
            Selector.Doc(
              BsonField.Name("pop") -> Selector.Gt(Bson.Int64(1000))))),
          $simpleMap(NonEmptyList(MapExpr(JsFn(Name("x"), obj(
            "city"   -> Select(ident("x"), "city"),
            "pop"    -> Select(ident("x"), "pop"),
            "__tmp6" ->
              If(Call(ident("isString"), List(Select(ident("x"), "city"))),
                Select(Select(ident("x"), "city"), "length"),
                ident("undefined")))))),
            ListMap()),
          $sort(NonEmptyList(BsonField.Name("__tmp6") -> Ascending)),
          $project(
            reshape(
              "city" -> $field("city"),
              "pop"  -> $field("pop")),
            ExcludeId)))
    }

    "plan select length()" in {
      plan("select length(city) from zips") must
        beWorkflow(chain(
          $read(Collection("db", "zips")),
          $simpleMap(NonEmptyList(MapExpr(JsFn(Name("x"), obj(
            "0" ->
              If(Call(ident("isString"), List(Select(ident("x"), "city"))),
                Select(Select(ident("x"), "city"), "length"),
                ident("undefined")))))),
            ListMap()),
          $project(
            reshape("0" -> $include()),
            IgnoreId)))
    }

    "plan select length() and simple field" in {
      plan("select city, length(city) from zips") must
      beWorkflow(chain(
        $read(Collection("db", "zips")),
        $simpleMap(NonEmptyList(MapExpr(JsFn(Name("x"), obj(
          "city" -> Select(ident("x"), "city"),
          "1" ->
            If(Call(ident("isString"), List(Select(ident("x"), "city"))),
              Select(Select(ident("x"), "city"), "length"),
              ident("undefined")))))),
          ListMap()),
        $project(
          reshape(
            "city" -> $include(),
            "1"    -> $include()),
          IgnoreId)))
    }

    "plan combination of two distinct sets" in {
      plan("SELECT (DISTINCT foo.bar) + (DISTINCT foo.baz) FROM foo") must
        beWorkflow(
          $read(Collection("db", "zips")))
    }.pendingUntilFixed

    "plan expression with timestamp, date, time, and interval" in {
      import org.threeten.bp.{Instant, LocalDateTime, ZoneOffset}

      plan("select timestamp '2014-11-17T22:00:00Z' + interval 'PT43M40S', date '2015-01-19', time '14:21'") must
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
          $read(Collection("db", "days")),
          $project(
            reshape(
              "__tmp6" ->
                $subtract($field("date"), $literal(Bson.Dec(12*60*60*1000))),
              "__tmp7" -> $$ROOT),
              IgnoreId),
          $match(
            Selector.And(
              // TODO: eliminate duplication
              Selector.Or(
                Selector.Doc(BsonField.Name("__tmp7") \ BsonField.Name("date") ->
                  Selector.Type(BsonType.Int32)),
                Selector.Doc(BsonField.Name("__tmp7") \ BsonField.Name("date") ->
                  Selector.Type(BsonType.Int64)),
                Selector.Doc(BsonField.Name("__tmp7") \ BsonField.Name("date") ->
                  Selector.Type(BsonType.Dec)),
                Selector.Doc(BsonField.Name("__tmp7") \ BsonField.Name("date") ->
                  Selector.Type(BsonType.Date))),
              Selector.And(
                Selector.Or(
                  Selector.Doc(BsonField.Name("__tmp7") \ BsonField.Name("date") ->
                    Selector.Type(BsonType.Int32)),
                  Selector.Doc(BsonField.Name("__tmp7") \ BsonField.Name("date") ->
                    Selector.Type(BsonType.Int64)),
                  Selector.Doc(BsonField.Name("__tmp7") \ BsonField.Name("date") ->
                    Selector.Type(BsonType.Dec)),
                  Selector.Doc(BsonField.Name("__tmp7") \ BsonField.Name("date") ->
                    Selector.Type(BsonType.Text)),
                  Selector.Or(
                    Selector.Doc(BsonField.Name("__tmp7") \ BsonField.Name("date") ->
                      Selector.Type(BsonType.Date)),
                    Selector.Doc(BsonField.Name("__tmp7") \ BsonField.Name("date") ->
                      Selector.Type(BsonType.Bool)))),
                Selector.And(
                  Selector.Doc(
                    BsonField.Name("__tmp7") \ BsonField.Name("date") ->
                      Selector.Lt(Bson.Date(Instant.parse("2014-11-17T22:00:00Z")))),
                  Selector.Doc(
                    BsonField.Name("__tmp6") ->
                      Selector.Gt(Bson.Date(Instant.parse("2014-11-17T00:00:00Z")))))))),
          $project(
            reshape("value" -> $field("__tmp7")),
            ExcludeId)))
    }

    "plan time_of_day" in {
      plan("select time_of_day(ts) from foo") must
        beRight // NB: way too complicated to spell out here, and will change as JS generation improves
    }

    "plan filter on date" in {
      import org.threeten.bp.Instant

      // Note: both of these boundaries require comparing with the start of the *next* day.
      plan("select * from logs " +
        "where ((ts > date '2015-01-22' and ts <= date '2015-01-27') and ts != date '2015-01-25') " +
        "or ts = date '2015-01-29'") must
        beWorkflow(chain(
          $read(Collection("db", "logs")),
          $match(Selector.Or(
            // TODO: Eliminate duplicates
            Selector.And(
              Selector.Or(
                Selector.Doc(BsonField.Name("ts") ->
                  Selector.Type(BsonType.Int32)),
                Selector.Doc(BsonField.Name("ts") ->
                  Selector.Type(BsonType.Int64)),
                Selector.Doc(BsonField.Name("ts") ->
                  Selector.Type(BsonType.Dec)),
                Selector.Doc(BsonField.Name("ts") ->
                  Selector.Type(BsonType.Text)),
                Selector.Or(
                  Selector.Doc(BsonField.Name("ts") ->
                    Selector.Type(BsonType.Date)),
                  Selector.Doc(BsonField.Name("ts") ->
                    Selector.Type(BsonType.Bool)))),
              Selector.And(
                Selector.Or(
                  Selector.Doc(BsonField.Name("ts") ->
                    Selector.Type(BsonType.Int32)),
                  Selector.Doc(BsonField.Name("ts") ->
                    Selector.Type(BsonType.Int64)),
                  Selector.Doc(BsonField.Name("ts") ->
                    Selector.Type(BsonType.Dec)),
                  Selector.Doc(BsonField.Name("ts") ->
                    Selector.Type(BsonType.Text)),
                  Selector.Or(
                    Selector.Doc(BsonField.Name("ts") ->
                      Selector.Type(BsonType.Date)),
                    Selector.Doc(BsonField.Name("ts") ->
                      Selector.Type(BsonType.Bool)))),
                Selector.And(
                  Selector.Or(
                    Selector.Doc(BsonField.Name("ts") ->
                      Selector.Type(BsonType.Int32)),
                    Selector.Doc(BsonField.Name("ts") ->
                      Selector.Type(BsonType.Int64)),
                    Selector.Doc(BsonField.Name("ts") ->
                      Selector.Type(BsonType.Dec)),
                    Selector.Doc(BsonField.Name("ts") ->
                      Selector.Type(BsonType.Text)),
                    Selector.Or(
                      Selector.Doc(BsonField.Name("ts") ->
                        Selector.Type(BsonType.Date)),
                      Selector.Doc(BsonField.Name("ts") ->
                        Selector.Type(BsonType.Bool)))),
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
                        BsonField.Name("ts") -> Selector.Gte(Bson.Date(Instant.parse("2015-01-26T00:00:00Z"))))))))),
            Selector.And(
              Selector.Doc(
                BsonField.Name("ts") -> Selector.Gte(Bson.Date(Instant.parse("2015-01-29T00:00:00Z")))),
              Selector.Doc(
                BsonField.Name("ts") -> Selector.Lt(Bson.Date(Instant.parse("2015-01-30T00:00:00Z")))))))))
    }

    "plan js and filter with id" in {
      Bson.ObjectId("0123456789abcdef01234567").fold[Result](
        failure("Couldn’t create ObjectId."))(
        oid => plan("select length(city), foo = oid '0123456789abcdef01234567' from days where _id = oid '0123456789abcdef01234567'") must
          beWorkflow(chain(
            $read(Collection("db", "days")),
            $match(Selector.Doc(
              BsonField.Name("_id") -> Selector.Eq(oid))),
            $simpleMap(
              NonEmptyList(MapExpr(JsFn(Name("x"),
                obj(
                  "0" ->
                    If(Call(ident("isString"), List(Select(ident("x"), "city"))),
                      Select(Select(ident("x"), "city"), "length"),
                      ident("undefined")),
                  "1" ->
                    BinOp(jscore.Eq,
                      Select(ident("x"), "foo"),
                      New(Name("ObjectId"), List(jscore.Literal(Js.Str("0123456789abcdef01234567"))))))))),
              ListMap()),
            $project(
              reshape(
                "0" -> $field("0"),
                "1" -> $field("1")),
              ExcludeId))))
    }

    "plan convert to timestamp" in {
      import org.threeten.bp.Instant

      plan("select to_timestamp(epoch) from foo") must beWorkflow {
        chain(
          $read(Collection("db", "foo")),
          $project(
            reshape(
              "0" ->
                $cond(
                  $and(
                    $lt($literal(Bson.Null), $field("epoch")),
                    $lt($field("epoch"), $literal(Bson.Text("")))),
                  $add($literal(Bson.Date(Instant.ofEpochMilli(0))), $field("epoch")),
                  $literal(Bson.Undefined))),
            IgnoreId))
      }
    }

    "plan convert to timestamp in map-reduce" in {
      import org.threeten.bp.Instant

      plan("select length(name), to_timestamp(epoch) from foo") must beWorkflow {
        chain(
          $read(Collection("db", "foo")),
          $simpleMap(
            NonEmptyList(MapExpr(JsFn(Name("x"), obj(
              "0" ->
                If(Call(ident("isString"), List(Select(ident("x"), "name"))),
                  Select(Select(ident("x"), "name"), "length"),
                  ident("undefined")),
              "1" ->
                If(
                  BinOp(jscore.Or, BinOp(jscore.Or,
                    Call(ident("isNumber"), List(Select(ident("x"), "epoch"))),
                    BinOp(Instance, Select(ident("x"), "epoch"), ident("NumberInt"))),
                    BinOp(Instance, Select(ident("x"), "epoch"), ident("NumberLong"))),
                  New(Name("Date"), List(Select(ident("x"), "epoch"))),
                  ident("undefined")))))),
            ListMap()),
          $project(
            reshape(
              "0" -> $include(),
              "1" -> $include()),
            IgnoreId))
      }
    }

    def joinStructure0(
      left: Workflow, leftName: String, base: Expression, right: Workflow,
      leftKey: Reshape.Shape, rightKey: JsCore,
      fin: WorkflowOp,
      swapped: Boolean) = {

      val (leftLabel, rightLabel) =
        if (swapped) ("right", "left") else ("left", "right")
      def initialPipeOps(src: Workflow): Workflow =
        chain(
          src,
          $group(grouped(leftName -> $push(base)), leftKey),
          $project(
            reshape(
              leftLabel  -> $field(leftName),
              rightLabel -> $literal(Bson.Arr(List())),
              "_id"      -> $include()),
            IncludeId))
      fin(
        $foldLeft(
          initialPipeOps(left),
          chain(
            right,
            $map($Map.mapKeyVal(("key", "value"),
              rightKey.toJs,
              Js.AnonObjDecl(List(
                (leftLabel, Js.AnonElem(List())),
                (rightLabel, Js.AnonElem(List(Js.Ident("value"))))))),
              ListMap()),
            $reduce(
              Js.AnonFunDecl(List("key", "values"),
                List(
                  Js.VarDef(List(
                    ("result", Js.AnonObjDecl(List(
                      (leftLabel, Js.AnonElem(List())),
                      (rightLabel, Js.AnonElem(List()))))))),
                  Js.Call(Js.Select(Js.Ident("values"), "forEach"),
                    List(Js.AnonFunDecl(List("value"),
                      List(
                        Js.BinOp("=",
                          Js.Select(Js.Ident("result"), leftLabel),
                          Js.Call(
                            Js.Select(Js.Select(Js.Ident("result"), leftLabel), "concat"),
                            List(Js.Select(Js.Ident("value"), leftLabel)))),
                        Js.BinOp("=",
                          Js.Select(Js.Ident("result"), rightLabel),
                          Js.Call(
                            Js.Select(Js.Select(Js.Ident("result"), rightLabel), "concat"),
                            List(Js.Select(Js.Ident("value"), rightLabel)))))))),
                  Js.Return(Js.Ident("result")))),
              ListMap()))))
    }

    def joinStructure(
        left: Workflow, leftName: String, base: Expression, right: Workflow,
        leftKey: Reshape.Shape, rightKey: JsCore,
        fin: WorkflowOp,
        swapped: Boolean) =
      crystallize(joinStructure0(left, leftName, base, right, leftKey, rightKey, fin, swapped))

    "plan simple join" in {
      plan("select zips2.city from zips join zips2 on zips._id = zips2._id") must
        beWorkflow(
          joinStructure(
            $read(Collection("db", "zips")), "__tmp0", $$ROOT,
            $read(Collection("db", "zips2")),
            reshape("" -> $field("_id")),
            Obj(ListMap(Name("") -> Select(ident("value"), "_id"))),
            chain(_,
              $match(Selector.Doc(ListMap[BsonField, Selector.SelectorExpr](
                BsonField.Name("left") -> Selector.NotExpr(Selector.Size(0)),
                BsonField.Name("right") -> Selector.NotExpr(Selector.Size(0))))),
              $unwind(DocField(BsonField.Name("left"))),
              $unwind(DocField(BsonField.Name("right"))),
              $project(
                reshape("city" ->
                  $cond(
                    $and(
                      $lte($literal(Bson.Doc(ListMap())), $field("right")),
                      $lt($field("right"), $literal(Bson.Arr(Nil)))),
                    $field("right", "city"),
                    $literal(Bson.Undefined))),
                IgnoreId)),
            false).op)
    }

    "plan non-equi join" in {
      plan("select zips2.city from zips join zips2 on zips._id < zips2._id") must beLeft
    }

    "plan simple inner equi-join" in {
      plan(
        "select foo.name, bar.address from foo join bar on foo.id = bar.foo_id") must
      beWorkflow(
        joinStructure(
          $read(Collection("db", "foo")), "__tmp0", $$ROOT,
          $read(Collection("db", "bar")),
          reshape("" -> $field("id")),
          Obj(ListMap(Name("") -> Select(ident("value"), "foo_id"))),
          chain(_,
            $match(Selector.Doc(ListMap[BsonField, Selector.SelectorExpr](
              BsonField.Name("left") -> Selector.NotExpr(Selector.Size(0)),
              BsonField.Name("right") -> Selector.NotExpr(Selector.Size(0))))),
            $unwind(DocField(BsonField.Name("left"))),
            $unwind(DocField(BsonField.Name("right"))),
            $project(
              reshape(
                "name"    ->
                  $cond(
                    $and(
                      $lte($literal(Bson.Doc(ListMap())), $field("left")),
                      $lt($field("left"), $literal(Bson.Arr(Nil)))),
                    $field("left", "name"),
                    $literal(Bson.Undefined)),
                "address" ->
                  $cond(
                    $and(
                      $lte($literal(Bson.Doc(ListMap())), $field("right")),
                      $lt($field("right"), $literal(Bson.Arr(Nil)))),
                    $field("right", "address"),
                    $literal(Bson.Undefined))),
              IgnoreId)),
          false).op)
    }

    "plan simple outer equi-join with wildcard" in {
      plan("select * from foo full join bar on foo.id = bar.foo_id") must
      beWorkflow(
        joinStructure(
          $read(Collection("db", "foo")), "__tmp0", $$ROOT,
          $read(Collection("db", "bar")),
          reshape("" -> $field("id")),
          Obj(ListMap(Name("") -> Select(ident("value"), "foo_id"))),
          chain(_,
            $project(
              reshape(
                "left" ->
                  $cond($eq($size($field("left")), $literal(Bson.Int32(0))),
                    $literal(Bson.Arr(List(Bson.Doc(ListMap())))),
                    $field("left")),
                "right" ->
                  $cond($eq($size($field("right")), $literal(Bson.Int32(0))),
                    $literal(Bson.Arr(List(Bson.Doc(ListMap())))),
                    $field("right"))),
              IgnoreId),
            $unwind(DocField(BsonField.Name("left"))),
            $unwind(DocField(BsonField.Name("right"))),
            $simpleMap(
              NonEmptyList(
                MapExpr(JsFn(Name("x"),
                  Obj(ListMap(
                    Name("__tmp7") ->
                      If(
                        BinOp(jscore.And,
                          Call(ident("isObject"), List(Select(ident("x"), "right"))),
                          UnOp(jscore.Not,
                            Call(Select(ident("Array"), "isArray"), List(Select(ident("x"), "right"))))),
                        If(
                          BinOp(jscore.And,
                            Call(ident("isObject"), List(Select(ident("x"), "left"))),
                            UnOp(jscore.Not,
                              Call(Select(ident("Array"), "isArray"), List(Select(ident("x"), "left"))))),
                          SpliceObjects(List(
                            Select(ident("x"), "left"),
                            Select(ident("x"), "right"))),
                          ident("undefined")),
                        ident("undefined"))))))),
              ListMap()),
            $project(
              reshape("value" -> $field("__tmp7")),
              ExcludeId)),
          false).op)
    }

    "plan simple left equi-join" in {
      plan(
        "select foo.name, bar.address " +
          "from foo left join bar on foo.id = bar.foo_id") must
      beWorkflow(
        joinStructure(
          $read(Collection("db", "foo")), "__tmp0", $$ROOT,
          $read(Collection("db", "bar")),
          reshape("" -> $field("id")),
          Obj(ListMap(Name("") -> Select(ident("value"), "foo_id"))),
          chain(_,
            $match(Selector.Doc(ListMap[BsonField, Selector.SelectorExpr](
              BsonField.Name("left") -> Selector.NotExpr(Selector.Size(0))))),
            $project(
              reshape(
                "left"  -> $field("left"),
                "right" ->
                  $cond($eq($size($field("right")), $literal(Bson.Int32(0))),
                    $literal(Bson.Arr(List(Bson.Doc(ListMap())))),
                    $field("right"))),
              IgnoreId),
            $unwind(DocField(BsonField.Name("left"))),
            $unwind(DocField(BsonField.Name("right"))),
            $project(
              reshape(
                "name"    ->
                  $cond(
                    $and(
                      $lte($literal(Bson.Doc(ListMap())), $field("left")),
                      $lt($field("left"), $literal(Bson.Arr(Nil)))),
                    $field("left", "name"),
                    $literal(Bson.Undefined)),
                "address" ->
                  $cond(
                    $and(
                      $lte($literal(Bson.Doc(ListMap())), $field("right")),
                      $lt($field("right"), $literal(Bson.Arr(Nil)))),
                    $field("right", "address"),
                    $literal(Bson.Undefined))),
              IgnoreId)),
          false).op)
    }

    "plan 3-way right equi-join" in {
      plan(
        "select foo.name, bar.address, baz.zip " +
          "from foo join bar on foo.id = bar.foo_id " +
          "right join baz on bar.id = baz.bar_id") must
      beWorkflow(
        joinStructure(
          $read(Collection("db", "baz")), "__tmp1", $$ROOT,
          joinStructure0(
            $read(Collection("db", "foo")), "__tmp0", $$ROOT,
            $read(Collection("db", "bar")),
            reshape("" -> $field("id")),
            Obj(ListMap(Name("") -> Select(ident("value"), "foo_id"))),
            chain(_,
              $match(Selector.Doc(ListMap[BsonField, Selector.SelectorExpr](
                BsonField.Name("left") -> Selector.NotExpr(Selector.Size(0)),
                BsonField.Name("right") -> Selector.NotExpr(Selector.Size(0))))),
              $unwind(DocField(BsonField.Name("left"))),
              $unwind(DocField(BsonField.Name("right")))),
            false),
          reshape("" -> $field("bar_id")),
          Obj(ListMap(Name("") -> Select(Select(ident("value"), "right"), "id"))),
          chain(_,
            $match(Selector.Doc(ListMap[BsonField, Selector.SelectorExpr](
              BsonField.Name("left") -> Selector.NotExpr(Selector.Size(0))))),
            $project(
              reshape(
                "right" ->
                  $cond($eq($size($field("right")), $literal(Bson.Int32(0))),
                    $literal(Bson.Arr(List(Bson.Doc(ListMap())))),
                    $field("right")),
                "left" -> $field("left")),
              IgnoreId),
            $unwind(DocField(BsonField.Name("right"))),
            $unwind(DocField(BsonField.Name("left"))),
            $project(
              reshape(
                "name"    ->
                  $cond(
                    $and(
                      $lte($literal(Bson.Doc(ListMap())), $field("left")),
                      $lt($field("left"), $literal(Bson.Arr(Nil)))),
                    $cond(
                      $and(
                        $lte($literal(Bson.Doc(ListMap())), $field("left", "left")),
                        $lt($field("left", "left"), $literal(Bson.Arr(Nil)))),
                      $field("left", "left", "name"),
                      $literal(Bson.Undefined)),
                    $literal(Bson.Undefined)),
                "address" ->
                  $cond(
                    $and(
                      $lte($literal(Bson.Doc(ListMap())), $field("left")),
                      $lt($field("left"), $literal(Bson.Arr(Nil)))),
                    $cond(
                      $and(
                        $lte($literal(Bson.Doc(ListMap())), $field("left", "right")),
                        $lt($field("left", "right"), $literal(Bson.Arr(Nil)))),
                      $field("left", "right", "address"),
                      $literal(Bson.Undefined)),
                    $literal(Bson.Undefined)),
                "zip"     ->
                  $cond(
                    $and(
                      $lte($literal(Bson.Doc(ListMap())), $field("right")),
                      $lt($field("right"), $literal(Bson.Arr(Nil)))),
                    $field("right", "zip"),
                    $literal(Bson.Undefined))),
              IgnoreId)),
          true).op)
    }

    "plan join with multiple conditions" in {
      plan("select l.sha as child, l.author.login as c_auth, r.sha as parent, r.author.login as p_auth from slamengine_commits l join slamengine_commits r on r.sha = l.parents[0].sha and l.author.login = r.author.login") must
      beWorkflow(
        joinStructure(
          chain(
            $read(Collection("db", "slamengine_commits")),
            $simpleMap(NonEmptyList(MapExpr(JsFn(Name("x"),
              obj(
                "__tmp4" -> Select(Access(Select(ident("x"), "parents"), jscore.Literal(Js.Num(0, false))), "sha"),
                "__tmp5" -> ident("x"),
                "__tmp6" -> Select(Select(ident("x"), "author"), "login"))))),
              ListMap())),
          "__tmp7", $field("__tmp5"),
          $read(Collection("db", "slamengine_commits")),
          reshape(
            "0" -> $field("__tmp4"),
            "1" -> $field("__tmp6")),
          obj(
            "0" -> Select(ident("value"), "sha"),
            "1" -> Select(Select(ident("value"), "author"), "login")),
          chain(_,
            $match(Selector.Doc(ListMap[BsonField, Selector.SelectorExpr](
              BsonField.Name("left") -> Selector.NotExpr(Selector.Size(0)),
              BsonField.Name("right") -> Selector.NotExpr(Selector.Size(0))))),
            $unwind(DocField(BsonField.Name("left"))),
            $unwind(DocField(BsonField.Name("right"))),
            $project(
              reshape(
                "child"  ->
                  $cond(
                    $and(
                      $lte($literal(Bson.Doc(ListMap())), $field("left")),
                      $lt($field("left"), $literal(Bson.Arr(Nil)))),
                    $field("left", "sha"),
                    $literal(Bson.Undefined)),
                "c_auth" ->
                  $cond(
                    $and(
                      $lte($literal(Bson.Doc(ListMap())), $field("left")),
                      $lt($field("left"), $literal(Bson.Arr(Nil)))),
                    $cond(
                      $and(
                        $lte($literal(Bson.Doc(ListMap())), $field("left", "author")),
                        $lt($field("left", "author"), $literal(Bson.Arr(Nil)))),
                      $field("left", "author", "login"),
                      $literal(Bson.Undefined)),
                    $literal(Bson.Undefined)),
                "parent" ->
                  $cond(
                    $and(
                      $lte($literal(Bson.Doc(ListMap())), $field("right")),
                      $lt($field("right"), $literal(Bson.Arr(Nil)))),
                    $field("right", "sha"),
                    $literal(Bson.Undefined)),
                "p_auth" ->
                  $cond(
                    $and(
                      $lte($literal(Bson.Doc(ListMap())), $field("right")),
                      $lt($field("right"), $literal(Bson.Arr(Nil)))),
                    $cond(
                      $and(
                        $lte($literal(Bson.Doc(ListMap())), $field("right", "author")),
                        $lt($field("right", "author"), $literal(Bson.Arr(Nil)))),
                      $field("right", "author", "login"),
                      $literal(Bson.Undefined)),
                    $literal(Bson.Undefined))),
              IgnoreId)),
        false).op)
    }

    "plan simple cross" in {
      plan("select zips2.city from zips, zips2 where zips._id = zips2._id") must
      beWorkflow(
        joinStructure(
          $read(Collection("db", "zips")), "__tmp0", $$ROOT,
          $read(Collection("db", "zips2")),
          $literal(Bson.Null),
          jscore.Literal(Js.Null),
          chain(_,
            $match(Selector.Doc(ListMap[BsonField, Selector.SelectorExpr](
              BsonField.Name("left") -> Selector.NotExpr(Selector.Size(0)),
              BsonField.Name("right") -> Selector.NotExpr(Selector.Size(0))))),
            $unwind(DocField(BsonField.Name("left"))),
            $unwind(DocField(BsonField.Name("right"))),
            $project(
              reshape(
                "city"   ->
                  $cond(
                    $and(
                      $lte($literal(Bson.Doc(ListMap())), $field("right")),
                      $lt($field("right"), $literal(Bson.Arr(Nil)))),
                    $field("right", "city"),
                    $literal(Bson.Undefined)),
                "__tmp7" -> $field("right"),
                "__tmp8" -> $field("left"),
                "__tmp9" -> $eq($field("left", "_id"), $field("right", "_id"))),
              IgnoreId),
            $match(Selector.And(
              Selector.Doc(
                BsonField.Name("__tmp7") -> Selector.Type(BsonType.Doc)),
              Selector.And(
                Selector.Doc(
                  BsonField.Name("__tmp8") -> Selector.Type(BsonType.Doc)),
                Selector.Doc(
                  BsonField.Name("__tmp9") -> Selector.Eq(Bson.Bool(true)))))),
            $project(
              reshape("city" -> $field("city")),
              ExcludeId)),
          false).op)
    }

    def countOps(wf: Workflow, p: PartialFunction[WorkflowF[Fix[WorkflowF]], Boolean]): Int = {
      wf.foldMap(op => if (p.lift(op.unFix).getOrElse(false)) 1 else 0)
    }

    def noConsecutiveProjectOps(wf: Workflow) =
      countOps(wf, { case $Project(Fix($Project(_, _, _)), _, _) => true }) aka "the occurrences of consecutive $project ops:" must_== 0
    def noConsecutiveSimpleMapOps(wf: Workflow) =
      countOps(wf, { case $SimpleMap(Fix($SimpleMap(_, _, _)), _, _) => true }) aka "the occurrences of consecutive $simpleMap ops:" must_== 0
    def maxAccumOps(wf: Workflow, max: Int) =
      countOps(wf, { case $Group(_, _, _) => true }) aka "the number of $group ops:" must beLessThanOrEqualTo(max)
    def maxUnwindOps(wf: Workflow, max: Int) =
      countOps(wf, { case $Unwind(_, _) => true }) aka "the number of $unwind ops:" must beLessThanOrEqualTo(max)
    def maxMatchOps(wf: Workflow, max: Int) =
      countOps(wf, { case $Match(_, _) => true }) aka "the number of $match ops:" must beLessThanOrEqualTo(max)
    def brokenProjectOps(wf: Workflow) =
      countOps(wf, { case $Project(_, Reshape(shape), _) => shape.isEmpty }) aka "$project ops with no fields"

    def danglingReferences(wf: Workflow) =
      wf.foldMap(_.unFix match {
        case op: SingleSourceF[Workflow] =>
          Workflow.simpleShape(op.src).map { shape =>
            val refs = Workflow.refs(op)
            val missing = refs.collect { case v @ DocVar(_, Some(f)) if !shape.contains(f.flatten.head) => v }
            if (missing.isEmpty) Nil
            else List(missing.map(_.bson).mkString(", ") + " missing in\n" + Fix[WorkflowF](op).show)
          }.getOrElse(Nil)
        case _ => Nil
      }) aka "dangling references"

    def rootPushes(wf: Workflow) =
      wf.foldMap(_.unFix match {
        case op @ $Group(src, Grouped(map), _) if map.values.toList.contains($push($$ROOT)) && simpleShape(src).isEmpty => List(op)
        case _ => Nil
      }) aka "group ops pushing $$ROOT"

    args.report(showtimes = true)

    "plan multiple reducing projections (all, distinct, orderBy)" ! Prop.forAll(select(distinct, maybeReducingExpr, Gen.option(filter), Gen.option(groupBySeveral), orderBySeveral)) { q =>
      plan(q.value) must beRight.which { fop =>
        val wf = fop.op
        noConsecutiveProjectOps(wf)
        noConsecutiveSimpleMapOps(wf)
        maxAccumOps(wf, 2)
        maxUnwindOps(wf, 1)
        maxMatchOps(wf, 1)
        danglingReferences(wf) must_== Nil
        brokenProjectOps(wf) must_== 0
        val fields = fieldNames(wf)
        (fields aka "column order" must beSome(columnNames(q))) or
          (fields must beSome(List("value")))  // NB: some edge cases (all constant projections) end up under "value" and aren't interesting anyway
        rootPushes(wf) must_== Nil
      }
    }.set(maxSize = 3)  // FIXME: with more then a few keys in the order by, the planner gets *very* slow (see SD-658)

    "plan multiple reducing projections (all, distinct)" ! Prop.forAll(select(distinct, maybeReducingExpr, Gen.option(filter), Gen.option(groupBySeveral), noOrderBy)) { q =>
      plan(q.value) must beRight.which { fop =>
        val wf = fop.op
        noConsecutiveProjectOps(wf)
        noConsecutiveSimpleMapOps(wf)
        maxAccumOps(wf, 2)
        maxUnwindOps(wf, 1)
        maxMatchOps(wf, 1)
        danglingReferences(wf) must_== Nil
        brokenProjectOps(wf) must_== 0
        val fields = fieldNames(wf)
        (fields aka "column order" must beSome(columnNames(q))) or
          (fields must beSome(List("value")))  // NB: some edge cases (all constant projections) end up under "value" and aren't interesting anyway
        rootPushes(wf) must_== Nil
      }
    }.set(maxSize = 10)

    "plan multiple reducing projections (all)" ! Prop.forAll(select(notDistinct, maybeReducingExpr, Gen.option(filter), Gen.option(groupBySeveral), noOrderBy)) { q =>
      plan(q.value) must beRight.which { fop =>
        val wf = fop.op
        noConsecutiveProjectOps(wf)
        noConsecutiveSimpleMapOps(wf)
        maxAccumOps(wf, 1)
        maxUnwindOps(wf, 1)
        maxMatchOps(wf, 1)
        danglingReferences(wf) must_== Nil
        brokenProjectOps(wf) must_== 0
        fieldNames(wf) aka "column order" must beSome(columnNames(q))
        rootPushes(wf) must_== Nil
      }
    }.set(maxSize = 10)

    // NB: tighter constraint because we know there's no filter.
    "plan multiple reducing projections (no filter)" ! Prop.forAll(select(notDistinct, maybeReducingExpr, noFilter, Gen.option(groupBySeveral), noOrderBy)) { q =>
      plan(q.value) must beRight.which { fop =>
        val wf = fop.op
        noConsecutiveProjectOps(wf)
        noConsecutiveSimpleMapOps(wf)
        maxAccumOps(wf, 1)
        maxUnwindOps(wf, 1)
        maxMatchOps(wf, 0)
        danglingReferences(wf) must_== Nil
        brokenProjectOps(wf) must_== 0
        fieldNames(wf) aka "column order" must beSome(columnNames(q))
        rootPushes(wf) must_== Nil
      }
    }.set(maxSize = 10)
  }

  def columnNames(q: Query): List[String] =
    (new SQLParser).parse(q).foldMap(sql.namedProjections(_, None).map(_._1))

  def fieldNames(wf: Workflow): Option[List[String]] =
    Workflow.simpleShape(wf).map(_.map(_.asText))

  import sql.{Binop => _, Ident => _, _}

  val notDistinct = Gen.const(SelectAll)
  val distinct = Gen.const(SelectDistinct)

  val noGroupBy = Gen.const[Option[GroupBy[Expr]]](None)
  val groupBySeveral = Gen.nonEmptyListOf(Gen.oneOf(
    sql.Ident("state"),
    sql.Ident("territory"))).map(keys => GroupBy(keys.distinct, None))

  val noFilter = Gen.const[Option[Expr]](None)
  val filter = Gen.oneOf(
    for {
      x <- genInnerInt
    } yield sql.Binop(x, IntLiteral(100), sql.Lt),
    for {
      x <- genInnerStr
    } yield InvokeFunction(StdLib.string.Like.name, List(x, StringLiteral("BOULDER%"), StringLiteral(""))),
    Gen.const(sql.Binop(sql.Ident("p"), sql.Ident("q"), sql.Eq)))  // Comparing two fields requires a $project before the $match

  val noOrderBy: Gen[Option[OrderBy[Expr]]] = Gen.const(None)
  val orderBySeveral: Gen[Option[OrderBy[Expr]]] = Gen.nonEmptyListOf(for {
    x <- Gen.oneOf(genInnerInt, genInnerStr)
    t <- Gen.oneOf(ASC, DESC)
  } yield (t, x)).map(ps => Some(OrderBy(ps)))

  val maybeReducingExpr = Gen.oneOf(genOuterInt, genOuterStr)

  def select(distinctGen: Gen[IsDistinct], exprGen: Gen[Expr], filterGen: Gen[Option[Expr]], groupByGen: Gen[Option[GroupBy[Expr]]], orderByGen: Gen[Option[OrderBy[Expr]]]): Gen[Query] =
    for {
      distinct <- distinctGen
      projs    <- Gen.nonEmptyListOf(exprGen).map(_.zipWithIndex.map {
        case (x, n) => Proj(x, Some("p" + n))
      })
      filter   <- filterGen
      groupBy  <- groupByGen
      orderBy  <- orderByGen
    } yield Query(pprint(sql.Select(distinct, projs, Some(TableRelationAST("zips", None)), filter, groupBy, orderBy)))

  def genInnerInt = Gen.oneOf(
    sql.Ident("pop"),
    // IntLiteral(0),  // TODO: exposes bugs (see SD-478)
    sql.Binop(sql.Ident("pop"), IntLiteral(1), Minus), // an ExprOp
    InvokeFunction("length", List(sql.Ident("city"))))     // requires JS
  def genReduceInt = genInnerInt.flatMap(x => Gen.oneOf(
    x,
    InvokeFunction("min", List(x)),
    InvokeFunction("max", List(x)),
    InvokeFunction("sum", List(x)),
    InvokeFunction("count", List(Splice(None)))))
  def genOuterInt = Gen.oneOf(
    Gen.const(IntLiteral(0)),
    genReduceInt,
    genReduceInt.flatMap(sql.Binop(_, IntLiteral(1000), sql.Div)),
    genInnerInt.flatMap(x => sql.Binop(sql.Ident("loc"), ArrayLiteral(List(x)), sql.Concat)))

  def genInnerStr = Gen.oneOf(
    sql.Ident("city"),
    // StringLiteral("foo"),  // TODO: exposes bugs (see SD-478)
    InvokeFunction("lower", List(sql.Ident("city"))))
  def genReduceStr = genInnerStr.flatMap(x => Gen.oneOf(
    x,
    InvokeFunction("min", List(x)),
    InvokeFunction("max", List(x))))
  def genOuterStr = Gen.oneOf(
    Gen.const(StringLiteral("foo")),
    Gen.const(sql.Ident("state")),  // possibly the grouping key, so never reduced
    genReduceStr,
    genReduceStr.flatMap(x => InvokeFunction("lower", List(x))),   // an ExprOp
    genReduceStr.flatMap(x => InvokeFunction("length", List(x))))  // requires JS

  implicit def shrinkQuery(implicit SS: Shrink[Expr]): Shrink[Query] = Shrink { q =>
    (new SQLParser).parse(q).fold(κ(Stream.empty), SS.shrink(_).map(sel => Query(pprint(sel))))
  }

  /**
   Shrink a query by reducing the number of projections or grouping expressions. Do not
   change the "shape" of the query, by removing the group by entirely, etc.
   */
  implicit def shrinkExpr: Shrink[Expr] = {
    /** Shrink a list, removing a single item at a time, but never producing an empty list. */
    def shortened[A](as: List[A]): Stream[List[A]] =
      if (as.length <= 1) Stream.empty
      else as.toStream.map(a => as.filterNot(_ == a))

    Shrink {
      case Select(d, projs, rel, filter, groupBy, orderBy) =>
        val sDistinct = if (d == SelectDistinct) Stream(sql.Select(SelectAll, projs, rel, filter, groupBy, orderBy)) else Stream.empty
        val sProjs = shortened(projs).map(ps => sql.Select(d, ps, rel, filter, groupBy, orderBy))
        val sGroupBy = groupBy.map { case GroupBy(keys, having) =>
          shortened(keys).map(ks => sql.Select(d, projs, rel, filter, Some(GroupBy(ks, having)), orderBy))
        }.getOrElse(Stream.empty)
        sDistinct ++ sProjs ++ sGroupBy
      case expr => Stream(expr)
    }
  }

  "plan from LogicalPlan" should {
    import StdLib._

    "plan simple OrderBy" in {
      val lp =
        LogicalPlan.Let(
          'tmp0, read("db/foo"),
          LogicalPlan.Let(
            'tmp1, makeObj("bar" -> ObjectProject(Free('tmp0), Constant(Data.Str("bar")))),
            LogicalPlan.Let('tmp2,
              s.OrderBy[FLP](
                Free('tmp1),
                MakeArrayN[Fix](ObjectProject(Free('tmp1), Constant(Data.Str("bar")))),
                MakeArrayN(Constant(Data.Str("ASC")))),
              Free('tmp2))))

      plan(lp) must beWorkflow(chain(
        $read(Collection("db", "foo")),
        $project(
          reshape("bar" -> $field("bar")),
          IgnoreId),
        $sort(NonEmptyList(BsonField.Name("bar") -> Ascending))))
    }

    "plan OrderBy with expression" in {
      val lp =
        LogicalPlan.Let(
          'tmp0, read("db/foo"),
          s.OrderBy[FLP](
            Free('tmp0),
            MakeArrayN[Fix](math.Divide[FLP](
              ObjectProject(Free('tmp0), Constant(Data.Str("bar"))),
              Constant(Data.Dec(10.0)))),
            MakeArrayN(Constant(Data.Str("ASC")))))

      plan(lp) must beWorkflow(chain(
        $read(Collection("db", "foo")),
        $project(
          reshape(
            "__tmp0" -> $divide($field("bar"), $literal(Bson.Dec(10.0))),
            "__tmp1" -> $$ROOT),
          IgnoreId),
        $sort(NonEmptyList(BsonField.Name("__tmp0") -> Ascending)),
        $project(
          reshape("value" -> $field("__tmp1")),
          ExcludeId)))
    }

    "plan OrderBy with expression and earlier pipeline op" in {
      val lp =
        LogicalPlan.Let(
          'tmp0, read("db/foo"),
          LogicalPlan.Let(
            'tmp1,
            s.Filter[FLP](
              Free('tmp0),
              relations.Eq[FLP](
                ObjectProject(Free('tmp0), Constant(Data.Str("baz"))),
                Constant(Data.Int(0)))),
            s.OrderBy[FLP](
              Free('tmp1),
              MakeArrayN[Fix](ObjectProject(Free('tmp1), Constant(Data.Str("bar")))),
              MakeArrayN(Constant(Data.Str("ASC"))))))

      plan(lp) must beWorkflow(chain(
        $read(Collection("db", "foo")),
        $match(Selector.Doc(
          BsonField.Name("baz") -> Selector.Eq(Bson.Int64(0)))),
        $sort(NonEmptyList(BsonField.Name("bar") -> Ascending))))
    }

    "plan OrderBy with expression (and extra project)" in {
      val lp =
        LogicalPlan.Let(
          'tmp0, read("db/foo"),
          LogicalPlan.Let(
            'tmp9,
            makeObj(
              "bar" -> ObjectProject(Free('tmp0), Constant(Data.Str("bar")))),
            s.OrderBy[FLP](
              Free('tmp9),
              MakeArrayN[Fix](math.Divide[FLP](
                ObjectProject(Free('tmp9), Constant(Data.Str("bar"))),
                Constant(Data.Dec(10.0)))),
              MakeArrayN(Constant(Data.Str("ASC"))))))

      plan(lp) must beWorkflow(chain(
        $read(Collection("db", "foo")),
        $project(
          reshape(
            "bar"    -> $field("bar"),
            "__tmp0" -> $divide($field("bar"), $literal(Bson.Dec(10.0)))),
          IgnoreId),
        $sort(NonEmptyList(BsonField.Name("__tmp0") -> Ascending)),
        $project(
          reshape("bar" -> $field("bar")),
          ExcludeId)))
    }

    "plan distinct on full collection" in {
      plan(s.Distinct(read("db/cities"))) must
        beWorkflow(chain(
          $read(Collection("db", "cities")),
          $simpleMap(NonEmptyList(MapExpr(JsFn(Name("x"),
            obj(
              "__tmp1" ->
                Call(ident("remove"),
                  List(ident("x"), jscore.Literal(Js.Str("_id")))))))),
            ListMap()),
          $group(
            grouped("__tmp2" -> $first($field("__tmp1"))),
            -\/(reshape("" -> $field("__tmp1")))),
          $project(
            reshape("value" -> $field("__tmp2")),
            ExcludeId)))
    }
  }

  "alignJoinsƒ" should {
    "leave well enough alone" in {
      MongoDbPlanner.alignJoinsƒ(
        InvokeF(s.InnerJoin,
          List(Free('left), Free('right),
            relations.And[FLP](
              relations.Eq[FLP](
                ObjectProject(Free('left), Constant(Data.Str("foo"))),
                ObjectProject(Free('right), Constant(Data.Str("bar")))),
              relations.Eq[FLP](
                ObjectProject(Free('left), Constant(Data.Str("baz"))),
                ObjectProject(Free('right), Constant(Data.Str("zab")))))))) must
      beRightDisjunction(
        Fix(s.InnerJoin[FLP](Free('left), Free('right),
          relations.And[FLP](
            relations.Eq[FLP](
              ObjectProject(Free('left), Constant(Data.Str("foo"))),
              ObjectProject(Free('right), Constant(Data.Str("bar")))),
            relations.Eq[FLP](
              ObjectProject(Free('left), Constant(Data.Str("baz"))),
              ObjectProject(Free('right), Constant(Data.Str("zab"))))))))
    }

    "swap a reversed condition" in {
      MongoDbPlanner.alignJoinsƒ(
        InvokeF(s.InnerJoin,
          List(Free('left), Free('right),
            relations.And[FLP](
              relations.Eq[FLP](
                ObjectProject(Free('right), Constant(Data.Str("bar"))),
                ObjectProject(Free('left), Constant(Data.Str("foo")))),
              relations.Eq[FLP](
                ObjectProject(Free('left), Constant(Data.Str("baz"))),
                ObjectProject(Free('right), Constant(Data.Str("zab")))))))) must
      beRightDisjunction(
        Fix(s.InnerJoin[FLP](Free('left), Free('right),
          relations.And[FLP](
            relations.Eq[FLP](
              ObjectProject(Free('left), Constant(Data.Str("foo"))),
              ObjectProject(Free('right), Constant(Data.Str("bar")))),
            relations.Eq[FLP](
              ObjectProject(Free('left), Constant(Data.Str("baz"))),
              ObjectProject(Free('right), Constant(Data.Str("zab"))))))))
    }

    "swap multiple reversed conditions" in {
      MongoDbPlanner.alignJoinsƒ(
        InvokeF(s.InnerJoin,
          List(Free('left), Free('right),
            relations.And[FLP](
              relations.Eq[FLP](
                ObjectProject(Free('right), Constant(Data.Str("bar"))),
                ObjectProject(Free('left), Constant(Data.Str("foo")))),
              relations.Eq[FLP](
                ObjectProject(Free('right), Constant(Data.Str("zab"))),
                ObjectProject(Free('left), Constant(Data.Str("baz")))))))) must
      beRightDisjunction(
        Fix(s.InnerJoin[FLP](Free('left), Free('right),
          relations.And[FLP](
            relations.Eq[FLP](
              ObjectProject(Free('left), Constant(Data.Str("foo"))),
              ObjectProject(Free('right), Constant(Data.Str("bar")))),
            relations.Eq[FLP](
              ObjectProject(Free('left), Constant(Data.Str("baz"))),
              ObjectProject(Free('right), Constant(Data.Str("zab"))))))))
    }

    "fail with “mixed” conditions" in {
      MongoDbPlanner.alignJoinsƒ(
        InvokeF(s.InnerJoin,
          List(Free('left), Free('right),
            relations.And[FLP](
              relations.Eq[FLP](
                math.Add[FLP](
                  ObjectProject(Free('right), Constant(Data.Str("bar"))),
                  ObjectProject(Free('left), Constant(Data.Str("baz")))),
                ObjectProject(Free('left), Constant(Data.Str("foo")))),
              relations.Eq[FLP](
                ObjectProject(Free('left), Constant(Data.Str("baz"))),
                ObjectProject(Free('right), Constant(Data.Str("zab")))))))) must
      beLeftDisjunction(UnsupportedJoinCondition(
        relations.Eq[FLP](
          math.Add[FLP](
            ObjectProject(Free('right), Constant(Data.Str("bar"))),
            ObjectProject(Free('left), Constant(Data.Str("baz")))),
          ObjectProject(Free('left), Constant(Data.Str("foo"))))))
    }
  }

  "planner log" should {
    "include all phases when successful" in {
      planLog("select city from zips").map(_.map(_.name)) must
        beRightDisjunction(Vector(
          "SQL AST", "Variables Substituted", "Annotated Tree",
          "Logical Plan", "Simplified", "Typechecked",
          "Logical Plan (reduced typechecks)", "Logical Plan (aligned joins)",
          "Logical Plan (projections preferred)", "Workflow Builder",
          "Workflow (raw)", "Workflow (crystallized)", "Physical Plan", "Mongo"))
    }

    "include correct phases with type error" in {
      planLog("select 'a' + 0 from zips").map(_.map(_.name)) must
        beRightDisjunction(Vector(
          "SQL AST", "Variables Substituted", "Annotated Tree", "Logical Plan",
          "Simplified"))
    }

    "include correct phases with alignment error" in {
      planLog("select * from a join b on a.foo + b.bar < b.baz").map(_.map(_.name)) must
        beRightDisjunction(Vector(
          "SQL AST", "Variables Substituted", "Annotated Tree",
          "Logical Plan", "Simplified", "Typechecked",
          "Logical Plan (reduced typechecks)"))
    }

    "include correct phases with planner error" in {
      planLog("select date_part('foo', bar) from zips").map(_.map(_.name)) must
        beRightDisjunction(Vector(
          "SQL AST", "Variables Substituted", "Annotated Tree",
          "Logical Plan", "Simplified", "Typechecked",
          "Logical Plan (reduced typechecks)", "Logical Plan (aligned joins)",
          "Logical Plan (projections preferred)"))
    }
  }
}
