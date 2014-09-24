package slamdata.engine.physical.mongodb

import slamdata.engine._
import slamdata.engine.fp._
import slamdata.engine.fs.Path
import slamdata.engine.analysis.fixplate._
import slamdata.engine.analysis._
import slamdata.engine.sql.{SQLParser, Query}
import slamdata.engine.std._

import scalaz._

import collection.immutable.ListMap

import org.specs2.mutable._
import org.specs2.matcher.{Matcher, Expectable}
import slamdata.specs2._

class PlannerSpec extends Specification with CompilerHelpers with PendingWithAccurateCoverage {
  import StdLib._
  import structural._
  import math._
  import LogicalPlan._
  import SemanticAnalysis._
  import WorkflowOp._
  import WorkflowTask._
  import PipelineOp._
  import ExprOp._

  case class equalToWorkflow(expected: Workflow) extends Matcher[Workflow] {
    def apply[S <: Workflow](s: Expectable[S]) = {
      def diff(l: S, r: Workflow): String = {
        val lt = RenderTree[Workflow].render(l)
        val rt = RenderTree[Workflow].render(r)
        RenderTree.show(lt diff rt)(new RenderTree[RenderedTree] { override def render(v: RenderedTree) = v }).toString
      }
      result(expected == s.value,
             "\ntrees are equal:\n" + diff(s.value, expected),
             "\ntrees are not equal:\n" + diff(s.value, expected),
             s)
    }
  }

  val queryPlanner = MongoDbPlanner.queryPlanner((_, _) => Cord.empty)

  def plan(query: String): Either[Error, Workflow] = {
    queryPlanner(QueryRequest(Query(query), Path("out")))._2.toEither
  }

  def plan(logical: Term[LogicalPlan]): Either[Error, Workflow] =
    (for {
      simplified <- \/-(Optimizer.simplify(logical))
      phys <- MongoDbPlanner.plan(simplified)
    } yield phys).toEither

  def beWorkflow(task: WorkflowTask) = beRight(equalToWorkflow(Workflow(task)))

  "plan from query string" should {
    "plan simple constant example 1" in {
      plan("select 1") must
        beWorkflow(PureTask(Bson.Doc(ListMap("0" -> Bson.Int64(1)))))
    }.pendingUntilFixed

    "plan simple select *" in {
      plan("select * from foo") must beWorkflow(ReadTask(Collection("foo")))
    }

    "plan count(*)" in {
      plan("select count(*) from foo") must beWorkflow( 
        PipelineTask(
          ReadTask(Collection("foo")),
          Pipeline(List(
            Group(
              Grouped(ListMap(BsonField.Name("0") -> Count)),
              -\/(Literal(Bson.Int32(1))))))))
    }

    "plan simple field projection on single set" in {
      plan("select foo.bar from foo") must
        beWorkflow(
          PipelineTask(
            ReadTask(Collection("foo")),
            Pipeline(List(
              Project(Reshape.Doc(ListMap(BsonField.Name("bar") -> -\/(DocField(BsonField.Name("bar"))))))
            ))
          )
        )
    }

    "plan simple field projection on single set when table name is inferred" in {
      plan("select bar from foo") must
       beWorkflow(
          PipelineTask(
            ReadTask(Collection("foo")),
            Pipeline(List(
              Project(Reshape.Doc(ListMap(BsonField.Name("bar") -> -\/(DocField(BsonField.Name("bar"))))))
            ))
          )
        )
    }
    
    "plan multiple field projection on single set when table name is inferred" in {
      plan("select bar, baz from foo") must
       beWorkflow(
          PipelineTask(
            ReadTask(Collection("foo")),
            Pipeline(List(
              Project(Reshape.Doc(ListMap(
                BsonField.Name("bar") -> -\/(DocField(BsonField.Name("bar"))),
                BsonField.Name("baz") -> -\/(DocField(BsonField.Name("baz")))
              )))
            ))
          )
        )
    }

    "plan simple addition on two fields" in {
      plan("select foo + bar from baz") must
       beWorkflow(
          PipelineTask(
            ReadTask(Collection("baz")),
            Pipeline(List(
              Project(Reshape.Doc(ListMap(BsonField.Name("0") -> -\/ (ExprOp.Add(DocField(BsonField.Name("foo")), DocField(BsonField.Name("bar")))))))
            ))
          )
        )
    }
    
    "plan concat" in {
      plan("select concat(bar, baz) from foo") must
       beWorkflow(
          PipelineTask(
            ReadTask(Collection("foo")),
            Pipeline(List(
              Project(Reshape.Doc(ListMap(
                BsonField.Name("0") -> -\/ (ExprOp.Concat(
                  DocField(BsonField.Name("bar")),
                  DocField(BsonField.Name("baz")),
                  Nil
                ))
              )))
            ))
          )
        )
    }

    "plan lower" in {
      plan("select lower(bar) from foo") must
       beWorkflow(
          PipelineTask(
            ReadTask(Collection("foo")),
            Pipeline(List(
              Project(Reshape.Doc(ListMap(
                BsonField.Name("0") ->
                  -\/(ExprOp.ToLower(DocField(BsonField.Name("bar")))))))))))
    }

    "plan coalesce" in {
      plan("select coalesce(bar, baz) from foo") must
       beWorkflow(
          PipelineTask(
            ReadTask(Collection("foo")),
            Pipeline(List(
              Project(Reshape.Doc(ListMap(
                BsonField.Name("0") ->
                  -\/(ExprOp.IfNull(
                    DocField(BsonField.Name("bar")),
                    DocField(BsonField.Name("baz")))))))))))
    }

    "plan date field extraction" in {
      plan("select date_part('day', baz) from foo") must
       beWorkflow(
          PipelineTask(
            ReadTask(Collection("foo")),
            Pipeline(List(
              Project(Reshape.Doc(ListMap(
                BsonField.Name("0") ->
                  -\/(ExprOp.DayOfMonth(DocField(BsonField.Name("baz")))))))))))
    }

    "plan complex date field extraction" in {
      plan("select date_part('quarter', baz) from foo") must
       beWorkflow(
          PipelineTask(
            ReadTask(Collection("foo")),
            Pipeline(List(
              Project(Reshape.Doc(ListMap(
                BsonField.Name("0") ->
                  -\/(
                    ExprOp.Add(
                      ExprOp.Divide(
                        ExprOp.DayOfYear(DocField(BsonField.Name("baz"))),
                        ExprOp.Literal(Bson.Int32(92))),
                      ExprOp.Literal(Bson.Int32(1)))))))))))
    }

    "plan date field extraction: 'dow'" in {
      plan("select date_part('dow', baz) from foo") must
       beWorkflow(
          PipelineTask(
            ReadTask(Collection("foo")),
            Pipeline(List(
              Project(Reshape.Doc(ListMap(
                BsonField.Name("0") ->
                  -\/ (ExprOp.Add(
                        ExprOp.DayOfWeek(ExprOp.DocField(BsonField.Name("baz"))),
                        ExprOp.Literal(Bson.Int64(-1)))))))))))
    }

    "plan date field extraction: 'isodow'" in {
      plan("select date_part('isodow', baz) from foo") must
       beWorkflow(
          PipelineTask(
            ReadTask(Collection("foo")),
            Pipeline(List(
              Project(Reshape.Doc(ListMap(
                BsonField.Name("0") ->
                  -\/ (ExprOp.Cond(
                        ExprOp.Eq(
                          ExprOp.DayOfWeek(ExprOp.DocField(BsonField.Name("baz"))),
                          ExprOp.Literal(Bson.Int64(1))),
                        ExprOp.Literal(Bson.Int64(7)),
                        ExprOp.Add(
                          ExprOp.DayOfWeek(ExprOp.DocField(BsonField.Name("baz"))),
                          ExprOp.Literal(Bson.Int64(-1))))))))))))
    }

    "plan filter array element" in {
      plan("select loc from zips where loc[0] < -73") must
      beWorkflow(
        PipelineTask(ReadTask(Collection("zips")),
          Pipeline(List(
            Match(Selector.Doc(
              BsonField.Name("loc") \ BsonField.Index(0) -> Selector.Lt(Bson.Int64(-73)))),
            Project(Reshape.Doc(ListMap(
              BsonField.Name("loc") -> -\/(ExprOp.DocField(BsonField.Name("loc"))))))))))
    }

    "plan select array element" in {
      import Js._
      plan("select loc[0] from zips") must
      beWorkflow(
        PipelineTask(MapReduceTask(PipelineTask(ReadTask(Collection("zips")),
          Pipeline(List(Project(Reshape.Doc(ListMap(
            BsonField.Name("value") -> -\/(ExprOp.DocField(BsonField.Name("loc"))))))))),
          MapReduce(
            AnonFunDecl(List(),
              List(Call(Select(Ident("emit"), "apply"),
                List(
                  Null,
                  Call(
                    Select(AnonFunDecl(List("key"),
                      List(Return(AnonElem(List(
                        Ident("key"),
                        Access(
                          Select(Ident("this"), "value"),
                          Num(0, false))))))), "apply"),
                    List(Ident("this"), AnonElem(List(Select(Ident("this"), "_id"))))))))),
            ReduceOp.reduceNOP)),
          Pipeline(List(Project(Reshape.Doc(ListMap(
            BsonField.Name("0") -> -\/(ExprOp.DocField(BsonField.Name("value"))))))))))
    }

    "plan array length" in {
      plan("select array_length(bar, 1) from foo") must
       beWorkflow(
          PipelineTask(
            ReadTask(Collection("foo")),
            Pipeline(List(
              Project(Reshape.Doc(ListMap(
                BsonField.Name("0") ->
                  -\/(ExprOp.Size(DocField(BsonField.Name("bar")))))))))))
    }

    "plan conditional" in {
      plan("select case when pop < 10000 then city else loc end from zips") must
       beWorkflow(
          PipelineTask(
            ReadTask(Collection("zips")),
            Pipeline(List(
              Project(Reshape.Doc(ListMap(
                BsonField.Name("0") ->
                  -\/(Cond(
                    Lt(
                      DocField(BsonField.Name("pop")),
                      ExprOp.Literal(Bson.Int64(10000))),
                    DocField(BsonField.Name("city")),
                    DocField(BsonField.Name("loc")))))))))))
    }

    "plan negate" in {
      plan("select -bar from foo") must
       beWorkflow(
          PipelineTask(
            ReadTask(Collection("foo")),
            Pipeline(List(
              Project(Reshape.Doc(ListMap(
                BsonField.Name("0") ->
                  -\/(ExprOp.Multiply(ExprOp.Literal(Bson.Int32(-1)), DocField(BsonField.Name("bar")))))))))))
    }

    "plan simple filter" in {
      plan("select * from foo where bar > 10") must
       beWorkflow(
          PipelineTask(
            ReadTask(Collection("foo")),
            Pipeline(List(
              Match(Selector.Doc(BsonField.Name("bar") -> Selector.Gt(Bson.Int64(10))))
            ))
          )
        )
    }
    
    "plan simple filter with expression in projection" in {
      plan("select a + b from foo where bar > 10") must
       beWorkflow(
          PipelineTask(
            ReadTask(Collection("foo")),
            Pipeline(List(
              Match(Selector.Doc(BsonField.Name("bar") -> Selector.Gt(Bson.Int64(10)))),
              Project(Reshape.Doc(ListMap(BsonField.Name("0") -> -\/ (ExprOp.Add(
                                                                    DocField(BsonField.Name("a")), 
                                                                    DocField(BsonField.Name("b"))
                                                                  ))
              )))
            ))
          )
        )
    }

    "plan simple js filter" in {
      import Js._
      plan("select * from zips where length(city) < 4") must
      beWorkflow(
        MapReduceTask(ReadTask(Collection("zips")),
          MapReduce(
            AnonFunDecl(List(),
              List(Call(Select(Ident("emit"), "apply"),
                List(
                  Null,
                  Call(
                    Select(AnonFunDecl(List("key"),
                      List(Return(AnonElem(List(Ident("key"), Ident("this")))))), "apply"),
                      List(Ident("this"), AnonElem(List(Select(Ident("this"), "_id"))))))))),
            AnonFunDecl(List("key", "values"),
              List(Return(Access(Ident("values"), Num(0, false))))),
            None,
            Some(Selector.Where(BinOp("<",
              Select(Select(Ident("this"), "city"), "length"),
              Num(4, false)))))))
    }

    "plan filter with js and non-js" in {
      import Js._
      plan("select * from zips where length(city) < 4 and pop < 20000") must
      beWorkflow(
        MapReduceTask(ReadTask(Collection("zips")),
          MapReduce(
            AnonFunDecl(List(),
              List(Call(Select(Ident("emit"), "apply"),
                List(
                  Null,
                  Call(
                    Select(AnonFunDecl(List("key"),
                      List(Return(AnonElem(List(Ident("key"), Ident("this")))))), "apply"),
                    List(Ident("this"), AnonElem(List(Select(Ident("this"), "_id"))))))))),
            AnonFunDecl(List("key", "values"),
              List(Return(Access(Ident("values"), Num(0, false))))),
            None,
            Some(Selector.And(
              Selector.Where(BinOp("<",
                Select(Select(Ident("this"), "city"), "length"),
                Num(4, false))),
              Selector.Doc(BsonField.Name("pop") ->
                Selector.Lt(Bson.Int64(20000))))))))
    }

    "plan filter with between" in {
      plan("select * from foo where bar between 10 and 100") must
       beWorkflow(
          PipelineTask(
            ReadTask(Collection("foo")),
            Pipeline(List(
              Match(
                Selector.And(
                  Selector.Doc(BsonField.Name("bar") -> Selector.Gte(Bson.Int64(10))),
                  Selector.Doc(BsonField.Name("bar") -> Selector.Lte(Bson.Int64(100)))
                )
              )
            ))
          )
        )
    }
    
    "plan filter with like" in {
      plan("select * from foo where bar like 'A%'") must
       beWorkflow(
          PipelineTask(
            ReadTask(Collection("foo")),
            Pipeline(List(
              Match(
                Selector.Doc(BsonField.Name("bar") -> Selector.Regex("^A.*$", false, false, false, false))
              )
            ))
          )
        )
    }
    
    "plan filter with LIKE and OR" in {
      plan("select * from foo where bar like 'A%' or bar like 'Z%'") must
       beWorkflow(
          PipelineTask(
            ReadTask(Collection("foo")),
            Pipeline(List(
              Match(
                Selector.Or(
                  Selector.Doc(BsonField.Name("bar") -> Selector.Regex("^A.*$", false, false, false, false)),
                  Selector.Doc(BsonField.Name("bar") -> Selector.Regex("^Z.*$", false, false, false, false))
                )
              )
            ))
          )
        )
    }
    
    "plan filter with negate(s)" in {
      plan("select * from foo where bar != -10 and baz > -1.0") must
       beWorkflow(
          PipelineTask(
            ReadTask(Collection("foo")),
            Pipeline(List(
              Match(
                Selector.And(
                  Selector.Doc(BsonField.Name("bar") -> Selector.Neq(Bson.Int64(-10))),
                  Selector.Doc(BsonField.Name("baz") -> Selector.Gt(Bson.Dec(-1.0)))
                )
              )
            ))
          )
        )
    }
    
    "plan complex filter" in {
      plan("select * from foo where bar > 10 and (baz = 'quux' or foop = 'zebra')") must
       beWorkflow(
          PipelineTask(
            ReadTask(Collection("foo")),
            Pipeline(List(
              Match(Selector.And(
                Selector.Doc(BsonField.Name("bar") -> Selector.Gt(Bson.Int64(10))),
                Selector.Or(
                  Selector.Doc(BsonField.Name("baz") -> Selector.Eq(Bson.Text("quux"))),
                  Selector.Doc(BsonField.Name("foop") -> Selector.Eq(Bson.Text("zebra")))
                )
              ))
            ))
          )
        )
    }

    "plan simple sort with field in projection" in {
      plan("select bar from foo order by bar") must
        beWorkflow(
          PipelineTask(
            ReadTask(Collection("foo")),
            Pipeline(List(Project(Reshape.Doc(ListMap(
              BsonField.Name("lEft") -> \/- (Reshape.Doc(ListMap(
                BsonField.Name("bar") -> -\/ (ExprOp.DocField(BsonField.Name("bar")))))), 
              BsonField.Name("rIght") -> \/- (Reshape.Arr(ListMap(
                BsonField.Index(0) -> \/- (Reshape.Doc(ListMap(
                  BsonField.Name("key") -> -\/ (ExprOp.DocField(BsonField.Name("bar")))))))))))), 
            Sort(NonEmptyList(BsonField.Name("rIght") \ BsonField.Index(0) \ BsonField.Name("key") -> Ascending)), 
            Project(Reshape.Doc(ListMap(
              BsonField.Name("bar") -> -\/ (ExprOp.DocField(BsonField.Name("lEft") \ BsonField.Name("bar")))))))))
        )
    }
    
    
    "plan simple sort with wildcard" in {
      plan("select * from zips order by pop") must
        beWorkflow(
          PipelineTask(
            ReadTask(Collection("zips")),
            Pipeline(List(
              // TODO: Simplify to this once we identify sort keys in projection
              // Sort(NonEmptyList(BsonField.Name("pop") -> Ascending))
              Project(Reshape.Doc(ListMap(
                BsonField.Name("lEft") -> \/-(Reshape.Arr(ListMap(
                  BsonField.Index(0) -> \/-(Reshape.Doc(ListMap(
                    BsonField.Name("key") -> -\/(ExprOp.DocField(BsonField.Name("pop"))))))))),
                BsonField.Name("rIght") -> -\/(ExprOp.DocVar(DocVar.ROOT, None))))),
              Sort(NonEmptyList(BsonField.Name("lEft") \ BsonField.Index(0) \ BsonField.Name("key") -> Ascending)),
              Project(Reshape.Doc(ListMap (
                BsonField.Name("value") -> -\/(ExprOp.DocField(BsonField.Name ("rIght"))))))))))
    }

    "plan sort with expression in key" in {
      plan("select baz from foo order by bar/10") must
        beWorkflow(
          PipelineTask(
            ReadTask(Collection("foo")),
            Pipeline(List(
              Project(Reshape.Doc(ListMap(
                BsonField.Name("lEft") -> \/- (Reshape.Doc(ListMap(
                  BsonField.Name("baz") -> -\/(ExprOp.DocField(BsonField.Name("baz")))))), 
                BsonField.Name("rIght") -> \/- (Reshape.Arr(ListMap(
                  BsonField.Index(0) -> \/- (Reshape.Doc(ListMap(
                    BsonField.Name("key") -> -\/ (ExprOp.Divide(ExprOp.DocField(BsonField.Name("bar")), ExprOp.Literal(Bson.Int64(10))))))))))))), 
              Sort(NonEmptyList(BsonField.Name("rIght") \ BsonField.Index(0) \ BsonField.Name("key") -> Ascending)), 
              Project(Reshape.Doc(ListMap(
                BsonField.Name("baz") -> -\/(ExprOp.DocField(BsonField.Name("lEft") \ BsonField.Name("baz")))))))))
        )
    }

    "plan select with wildcard and field" in {
      plan("select *, pop from zips") must
      beWorkflow(
        MapReduceTask(
          PipelineTask(
            ReadTask(Collection("zips")),
            Pipeline(List(
              Project(Reshape.Doc(ListMap(
                BsonField.Name("lEft") -> \/-(Reshape.Doc(ListMap(
                  BsonField.Name("pop") ->
                    -\/(ExprOp.DocField(BsonField.Name("pop")))))),
                BsonField.Name("rIght") ->
                  -\/(ExprOp.DocVar(DocVar.ROOT, None)))))))),
          MapReduce(
            MapOp.mapFn(MapOp.mapMap(
              Js.Call(Js.Select(
                Js.AnonFunDecl(List("rez"),
                  List(
                    Js.ForIn(
                      Js.Ident("attr"),
                      Js.Select(Js.Ident("this"), "rIght"),
                      Js.If(
                        Js.Call(
                          Js.Select(Js.Select(Js.Ident("this"), "rIght"),
                            "hasOwnProperty"),
                          List(Js.Ident("attr"))),
                        Js.BinOp("=",
                          Js.Access(Js.Ident("rez"), Js.Ident("attr")),
                          Js.Access(Js.Select(Js.Ident("this"), "rIght"),
                            Js.Ident("attr"))),
                        None)),
                    Js.BinOp("=",
                      Js.Access(Js.Ident("rez"), Js.Str("pop")),
                      Js.Select(Js.Select(Js.Ident("this"), "lEft"), "pop")),
                    Js.Return(Js.Ident("rez")))), "apply"),
                List(Js.Ident("this"), Js.AnonElem(List(Js.AnonObjDecl(Nil))))))),
            ReduceOp.reduceNOP)))
    }

    "plan sort with wildcard and expression in key" in {
      import Js._

      plan("select * from zips order by pop/10 desc") must
      beWorkflow(
        PipelineTask(
          MapReduceTask(
            PipelineTask(
              ReadTask(Collection("zips")),
              Pipeline(List(
                Project(Reshape.Doc(ListMap(
                  BsonField.Name("lEft") -> \/-(Reshape.Doc(ListMap(
                    BsonField.Name("__sd__0") -> -\/(ExprOp.Divide(
                      ExprOp.DocField(BsonField.Name("pop")),
                      ExprOp.Literal(Bson.Int64(10))))))),
                  BsonField.Name("rIght") -> -\/(ExprOp.DocVar(DocVar.ROOT, None)))))))),
            MapReduce(
              MapOp.mapFn(MapOp.mapMap(
                Call(Select(AnonFunDecl(List("rez"),
                  List(
                    ForIn(Ident("attr"),Select(Ident("this"), "rIght"),If(Call(Select(Select(Ident("this"), "rIght"), "hasOwnProperty"),List(Ident("attr"))),BinOp("=",Access(Ident("rez"),Ident("attr")),Access(Select(Ident("this"), "rIght"),Ident("attr"))),None)),
                    BinOp("=",Access(Ident("rez"),Str("__sd__0")),Select(Select(Ident("this"),"lEft"),"__sd__0")), Return(Ident("rez")))), "apply"),
                  List(Ident("this"), AnonElem(List(AnonObjDecl(List()))))))),
              AnonFunDecl(List("key", "values"),List(Return(Access(Ident("values"),Num(0.0,false))))))),
          Pipeline(List(
            Project(Reshape.Doc(ListMap(
              BsonField.Name("lEft") -> -\/(ExprOp.DocVar(DocVar.ROOT, None)),
              BsonField.Name("rIght") -> \/-(Reshape.Arr(ListMap(
                BsonField.Index(0) -> \/-(Reshape.Doc(ListMap(
                  BsonField.Name("key") -> -\/(ExprOp.DocField(BsonField.Name("value") \ BsonField.Name("__sd__0")))))))))))),
            Sort(NonEmptyList(BsonField.Name("rIght") \ BsonField.Index(0) \ BsonField.Name("key") -> Descending)),
            Project(Reshape.Doc(ListMap(
              BsonField.Name("value") -> -\/(ExprOp.DocField(BsonField.Name("lEft") \ BsonField.Name("value"))))))))))
    }
    
    "plan simple sort with field not in projections" in {
      plan("select name from person order by height") must
        beWorkflow(
          PipelineTask(
            ReadTask(Collection("person")),
            Pipeline(List(
              Project(Reshape.Doc(ListMap(
                BsonField.Name("lEft") -> \/- (Reshape.Doc(ListMap(
                  BsonField.Name("name") -> -\/ (ExprOp.DocField(BsonField.Name("name"))) ))), 
                BsonField.Name("rIght") -> \/- (Reshape.Arr(ListMap(
                  BsonField.Index(0) -> \/- (Reshape.Doc(ListMap(
                    BsonField.Name("key") -> -\/ (ExprOp.DocField(BsonField.Name("height")))))))))))), 
              Sort(NonEmptyList(BsonField.Name("rIght") \ BsonField.Index(0) \ BsonField.Name("key") -> Ascending)), 
              Project(Reshape.Doc(ListMap(
                BsonField.Name("name") -> -\/ (ExprOp.DocField(BsonField.Name("lEft") \ BsonField.Name("name")))))))))
        )
    }
    
    "plan sort with expression and alias" in {
      plan("select pop/1000 as popInK from zips order by popInK") must
        beWorkflow(
          PipelineTask(
            ReadTask(Collection("zips")),
            Pipeline(List(
              Project(Reshape.Doc(ListMap(
                BsonField.Name("lEft") -> \/- (Reshape.Doc(ListMap(
                  BsonField.Name("popInK") -> -\/ (ExprOp.Divide(ExprOp.DocField(BsonField.Name("pop")), ExprOp.Literal(Bson.Int64(1000))))))), 
                BsonField.Name("rIght") -> \/- (Reshape.Arr(ListMap(
                  BsonField.Index(0) -> \/- (Reshape.Doc(ListMap(
                    BsonField.Name("key") -> -\/ (ExprOp.Divide(ExprOp.DocField(
                      BsonField.Name("pop")), ExprOp.Literal(Bson.Int64(1000)))) ))))))))), 
              Sort(NonEmptyList(BsonField.Name("rIght") \ BsonField.Index(0) \ BsonField.Name("key") -> Ascending)), 
              Project(Reshape.Doc(ListMap(
                BsonField.Name("popInK") -> -\/ (ExprOp.DocField(BsonField.Name("lEft") \ BsonField.Name("popInK")))))))))
        )
    }
    
    "plan sort with filter" in {
      plan("select city, pop from zips where pop <= 1000 order by pop desc, city") must
        beWorkflow(
          PipelineTask(
            ReadTask(Collection("zips")),
            Pipeline(List(
              Match(Selector.Doc(BsonField.Name("pop") -> Selector.Lte(Bson.Int64(1000)))), 
              Project(Reshape.Doc(ListMap(
                BsonField.Name("lEft") -> \/-  (Reshape.Doc(ListMap(
                    BsonField.Name("city") -> -\/(ExprOp.DocField(BsonField.Name("city"))),
                    BsonField.Name("pop") -> -\/(ExprOp.DocField(BsonField.Name("pop")))))),
                BsonField.Name("rIght") -> \/- (Reshape.Arr(ListMap(
                  BsonField.Index(0) -> \/- (Reshape.Doc(ListMap(
                    BsonField.Name("key") -> -\/ (ExprOp.DocField(BsonField.Name("pop")))))),
                  BsonField.Index(1) -> \/- (Reshape.Doc(ListMap(
                    BsonField.Name("key") -> -\/ (ExprOp.DocField(BsonField.Name("city")))))))))))),
              Sort(NonEmptyList(
                BsonField.Name("rIght") \ BsonField.Index(0) \ BsonField.Name("key") -> Descending,
                BsonField.Name("rIght") \ BsonField.Index(1) \ BsonField.Name("key") -> Ascending)),
              Project(Reshape.Doc(ListMap(
                BsonField.Name("city") -> -\/ (ExprOp.DocField(BsonField.Name("lEft") \ BsonField.Name("city"))), 
                BsonField.Name("pop") -> -\/ (ExprOp.DocField(BsonField.Name("lEft") \ BsonField.Name("pop")))))))))
        )
    }
    
    "plan sort with expression, alias, and filter" in {
      plan("select pop/1000 as popInK from zips where pop >= 1000 order by popInK") must
        beWorkflow(
          PipelineTask(
            ReadTask(Collection("zips")),
            Pipeline(List(
              Match(Selector.Doc(BsonField.Name("pop") -> Selector.Gte(Bson.Int64(1000)))), 
              Project(Reshape.Doc(ListMap(
                BsonField.Name("lEft") -> \/-(Reshape.Doc(ListMap(BsonField.Name("popInK") -> -\/(ExprOp.Divide(ExprOp.DocField(BsonField.Name("pop")), ExprOp.Literal(Bson.Int64(1000))))))),
                BsonField.Name("rIght") -> \/-(Reshape.Arr(ListMap(BsonField.Index(0) -> \/-(Reshape.Doc(ListMap(BsonField.Name("key") -> -\/(ExprOp.Divide(ExprOp.DocField(BsonField.Name("pop")), ExprOp.Literal(Bson.Int64(1000))))))))))))),
              Sort(NonEmptyList(BsonField.Name("rIght") \ BsonField.Index(0) \ BsonField.Name("key") -> Ascending)),
              Project(Reshape.Doc(ListMap(BsonField.Name("popInK") -> -\/(ExprOp.DocField(BsonField.Name("lEft") \ BsonField.Name("popInK")))))))))  
        )
    }

    "plan multiple column sort with wildcard" in {
      plan("select * from zips order by pop, city desc") must
       beWorkflow(
          PipelineTask(
            ReadTask(Collection("zips")),
            Pipeline(List(
              // TODO: Simplify to this once we identify sort keys in projection
              // Sort(NonEmptyList(BsonField.Name("pop") -> Ascending, 
              //                   BsonField.Name("city") -> Descending
              Project(Reshape.Doc(ListMap(
                BsonField.Name("lEft") -> \/-(Reshape.Arr(ListMap(
                  BsonField.Index(0) -> \/-(Reshape.Doc(ListMap(
                    BsonField.Name("key") -> -\/(ExprOp.DocField(BsonField.Name("pop")))))),
                  BsonField.Index(1) -> \/-(Reshape.Doc(ListMap(
                    BsonField.Name("key") -> -\/(ExprOp.DocField(BsonField.Name("city"))))))))),
                BsonField.Name("rIght") -> -\/(ExprOp.DocVar(DocVar.ROOT, None))))),
              Sort(NonEmptyList(
                BsonField.Name("lEft") \ BsonField.Index(0) \ BsonField.Name("key") -> Ascending,
                BsonField.Name("lEft") \ BsonField.Index(1) \ BsonField.Name("key") -> Descending)),
              Project(Reshape.Doc(ListMap (
                BsonField.Name("value") -> -\/(ExprOp.DocField(BsonField.Name ("rIght"))))))))))
    }
    
    "plan many sort columns" in {
      plan("select * from zips order by pop, state, city, a4, a5, a6") must
       beWorkflow(
          PipelineTask(
            ReadTask(Collection("zips")),
            Pipeline(List(
              // TODO: Simplify to this once we identify sort keys in projection
              // Sort(NonEmptyList(BsonField.Name("pop") -> Ascending, 
              //                   BsonField.Name("state") -> Ascending, 
              //                   BsonField.Name("city") -> Ascending, 
              //                   BsonField.Name("a4") -> Ascending, 
              //                   BsonField.Name("a5") -> Ascending, 
              //                   BsonField.Name("a6") -> Ascending
              Project(Reshape.Doc(ListMap(
                BsonField.Name("lEft") -> \/-(Reshape.Arr(ListMap(
                  BsonField.Index(0) -> \/-(Reshape.Doc(ListMap(
                    BsonField.Name("key") -> -\/(ExprOp.DocField(BsonField.Name("pop")))))),
                  BsonField.Index(1) -> \/-(Reshape.Doc(ListMap(
                    BsonField.Name("key") -> -\/(ExprOp.DocField(BsonField.Name("state")))))),
                  BsonField.Index(2) -> \/-(Reshape.Doc(ListMap(
                    BsonField.Name("key") -> -\/(ExprOp.DocField(BsonField.Name("city")))))),
                  BsonField.Index(3) -> \/-(Reshape.Doc(ListMap(
                    BsonField.Name("key") -> -\/(ExprOp.DocField(BsonField.Name("a4")))))),
                  BsonField.Index(4) -> \/-(Reshape.Doc(ListMap(
                    BsonField.Name("key") -> -\/(ExprOp.DocField(BsonField.Name("a5")))))),
                  BsonField.Index(5) -> \/-(Reshape.Doc(ListMap(
                    BsonField.Name("key") -> -\/(ExprOp.DocField(BsonField.Name("a6"))))))))),
                BsonField.Name("rIght") -> -\/(ExprOp.DocVar(DocVar.ROOT, None))))),
              Sort(NonEmptyList(
                BsonField.Name("lEft") \ BsonField.Index(0) \ BsonField.Name("key") -> Ascending,
                BsonField.Name("lEft") \ BsonField.Index(1) \ BsonField.Name("key") -> Ascending,
                BsonField.Name("lEft") \ BsonField.Index(2) \ BsonField.Name("key") -> Ascending,
                BsonField.Name("lEft") \ BsonField.Index(3) \ BsonField.Name("key") -> Ascending,
                BsonField.Name("lEft") \ BsonField.Index(4) \ BsonField.Name("key") -> Ascending,
                BsonField.Name("lEft") \ BsonField.Index(5) \ BsonField.Name("key") -> Ascending)),
              Project(Reshape.Doc(ListMap (
                BsonField.Name("value") -> -\/(ExprOp.DocField(BsonField.Name ("rIght"))))))))))
    }

    "plan efficient count and field ref" in {
      plan("SELECT city, COUNT(*) AS cnt FROM zips ORDER BY cnt DESC") must
        beWorkflow {
          PipelineTask(
            ReadTask(Collection("zips")),
            Pipeline(List(
              Project(Reshape.Doc(ListMap(
                BsonField.Name("lEft") -> \/-(Reshape.Doc(ListMap(
                  BsonField.Name("city") -> -\/(ExprOp.DocField(BsonField.Name("city"))))))))),
              Group(Grouped(ListMap(
                BsonField.Name("cnt") -> ExprOp.Sum(ExprOp.Literal(Bson.Int32(1))),
                BsonField.Name("__sd_tmp_1") -> ExprOp.Push(ExprOp.DocField(BsonField.Name("lEft"))))),
                -\/ (ExprOp.Literal(Bson.Int32(1)))),
              Unwind(ExprOp.DocField(BsonField.Name("__sd_tmp_1"))), 
              Project(Reshape.Doc(ListMap(
                BsonField.Name("lEft") -> \/- (Reshape.Doc(ListMap(
                  BsonField.Name("city") -> -\/ (ExprOp.DocField(BsonField.Name("__sd_tmp_1") \ BsonField.Name("city"))), 
                  BsonField.Name("cnt") -> -\/ (ExprOp.DocField(BsonField.Name("cnt")))))),
                BsonField.Name("rIght") -> \/- (Reshape.Arr(ListMap(
                  BsonField.Index(0) -> \/- (Reshape.Doc(ListMap(
                    BsonField.Name("key") -> -\/(ExprOp.DocField(BsonField.Name("cnt")))))))))))),
              Sort(NonEmptyList(BsonField.Name("rIght") \ BsonField.Index(0) \ BsonField.Name("key") -> Descending)), 
              Project(Reshape.Doc(ListMap(
                BsonField.Name("city") -> -\/ (ExprOp.DocField(BsonField.Name("lEft") \ BsonField.Name("city"))), 
                BsonField.Name("cnt") -> -\/ (ExprOp.DocField(BsonField.Name("lEft") \ BsonField.Name("cnt"))),
                BsonField.Name("_id") -> -\/ (ExprOp.Exclude)))))))
        }
    }

    "plan trivial group by" in {
      plan("select city from zips group by city") must
      beWorkflow(
        PipelineTask(
          ReadTask(Collection("zips")),
          Pipeline(List(
            Project(Reshape.Doc(ListMap(
              BsonField.Name("lEft") -> \/- (Reshape.Doc(ListMap(
                BsonField.Name("value") -> \/- (Reshape.Doc(ListMap(
                  BsonField.Name("city") -> -\/ (ExprOp.DocField(BsonField.Name("city"))))))))),
              BsonField.Name("rIght") -> \/- (Reshape.Doc(ListMap(
                BsonField.Name("city") -> -\/ (ExprOp.DocField(BsonField.Name("city"))))))))),
          Group(
            Grouped(ListMap(
              BsonField.Name("value") -> ExprOp.First(ExprOp.DocField(BsonField.Name("lEft") \ BsonField.Name("value"))))),
              -\/ (ExprOp.DocField(BsonField.Name("rIght"))))))))
    }.pendingUntilFixed("should group and not unwind; currenly just ignored, effectively")

    "plan count grouped by single field" in {
      plan("select count(*) from bar group by baz") must
        beWorkflow {
          PipelineTask(
            ReadTask(Collection("bar")),
            Pipeline(List(
              Project(Reshape.Doc(ListMap(
                BsonField.Name("lEft") -> \/- (Reshape.Doc(ListMap(
                  BsonField.Name("value") -> -\/ (ExprOp.Literal(Bson.Int32(1)))))), 
                BsonField.Name("rIght") -> \/- (Reshape.Arr(ListMap(
                  BsonField.Index(0) -> -\/ (ExprOp.DocField(BsonField.Name("baz"))))))))), 
              Group(Grouped(ListMap(
                BsonField.Name("0") -> ExprOp.Sum(ExprOp.DocField(BsonField.Name("lEft") \ BsonField.Name("value"))))),
                -\/(ExprOp.DocField(BsonField.Name("rIght")))))))
        }
    }

    "plan count and sum grouped by single field" in {
      plan("select count(*) as cnt, sum(biz) as sm from bar group by baz") must
        beWorkflow {
          PipelineTask(
            ReadTask(Collection("bar")),
            Pipeline(List(
              Group(Grouped(ListMap(BsonField.Name("__sd_tmp_1") -> ExprOp.Sum(ExprOp.Literal(Bson.Int32(1))), BsonField.Name("__sd_tmp_2") -> ExprOp.Sum(ExprOp.DocField(BsonField.Name("biz"))))),\/-(Reshape.Arr(ListMap(BsonField.Index(0) -> \/-(Reshape.Arr(ListMap(BsonField.Index(0) -> -\/(ExprOp.DocField(BsonField.Name("baz")))))), BsonField.Index(1) -> \/-(Reshape.Arr(ListMap(BsonField.Index(0) -> -\/(ExprOp.DocField(BsonField.Name("baz")))))))))), 
              Project(Reshape.Doc(ListMap(
                BsonField.Name("cnt") -> -\/ (ExprOp.DocField(BsonField.Name("__sd_tmp_1"))), 
                BsonField.Name("sm") -> -\/(ExprOp.DocField(BsonField.Name("__sd_tmp_2")))))))))
        }
    }

    "plan count and field when grouped" in {
      // TODO: Technically we need a 'distinct by city' here
      plan("select count(*) as cnt, city from zips group by city") must
        beWorkflow {
          PipelineTask(
            ReadTask(Collection("zips")),
            Pipeline(List(
              Project(Reshape.Doc(ListMap(
                BsonField.Name("lEft") -> \/- (Reshape.Doc(ListMap(
                  BsonField.Name("lEft") -> \/- (Reshape.Doc(ListMap(
                    BsonField.Name("value") -> -\/ (ExprOp.Literal(Bson.Int32(1)))))), 
                  BsonField.Name("rIght") -> \/- (Reshape.Arr(ListMap(
                    BsonField.Index(0) -> -\/ (ExprOp.DocField(BsonField.Name("city"))))))))), 
                BsonField.Name("rIght") -> \/-(Reshape.Doc(ListMap(
                  BsonField.Name("city") -> -\/(ExprOp.DocField(BsonField.Name("city"))))))))), 
              Group(Grouped(ListMap(
                BsonField.Name("cnt") -> ExprOp.Sum(ExprOp.DocField(BsonField.Name("lEft") \ BsonField.Name("lEft") \ BsonField.Name("value"))), 
                BsonField.Name("__sd_tmp_1") -> ExprOp.Push(ExprOp.DocField(BsonField.Name("rIght"))))),
                -\/(ExprOp.DocField(BsonField.Name("lEft") \ BsonField.Name("rIght")))), 
              Unwind(ExprOp.DocField(BsonField.Name("__sd_tmp_1"))), 
              Project(Reshape.Doc(ListMap(
                BsonField.Name("cnt") -> -\/(ExprOp.DocField(BsonField.Name("cnt"))), 
                BsonField.Name("city") -> -\/(ExprOp.DocField(BsonField.Name("__sd_tmp_1") \ BsonField.Name("city"))), 
                BsonField.Name("_id") -> -\/(ExprOp.Exclude)))))))
        }
    }

    "plan object flatten" in {
      plan("select geo{*} from usa_factbook") must
        beWorkflow {
          PipelineTask(
            MapReduceTask(
              PipelineTask(
                ReadTask(Collection("usa_factbook")),
                Pipeline(List(
                  Project(Reshape.Doc(ListMap(BsonField.Name("value") -> -\/ (ExprOp.DocField(BsonField.Name("geo"))))))))),
              MapReduce(
                FlatMapOp.mapFn(
                  Js.AnonFunDecl(List("key"),
                    List(
                      Js.VarDef(List("rez" -> Js.AnonElem(Nil))),
                      Js.ForIn(Js.Ident("attr"), Js.Select(Js.Ident("this"), "value"),
                        Js.Call(
                          Js.Select(Js.Ident("rez"), "push"),
                          List(
                            Js.AnonElem(List(
                              Js.Call(Js.Ident("ObjectId"), Nil),
                              Js.Access(
                                Js.Select(Js.Ident("this"), "value"),
                                Js.Ident("attr"))))))),
                      Js.Return(Js.Ident("rez"))))),
                ReduceOp.reduceNOP)),
            Pipeline(List(
              Project(Reshape.Doc(ListMap(
                BsonField.Name("geo") ->
                  -\/(ExprOp.DocField(BsonField.Name("value")))))))))
        }
    }

    "plan array flatten" in {
      plan("select loc[*] from zips") must
        beWorkflow {
          PipelineTask(
            ReadTask(Collection("zips")),
            Pipeline(List(
              Project(Reshape.Doc(ListMap(BsonField.Name("value") -> -\/ (ExprOp.DocField(BsonField.Name("loc")))))), 
              Unwind(ExprOp.DocField(BsonField.Name("value"))), 
              Project(Reshape.Doc(ListMap(
                BsonField.Name("loc") -> -\/(ExprOp.DocField(BsonField.Name("value"))),
                BsonField.Name("_id") -> -\/ (ExprOp.Exclude)))))))
        }
    }

    "plan limit with offset" in {
      plan("SELECT * FROM zips LIMIT 5 OFFSET 100") must
      beWorkflow(
        PipelineTask(ReadTask(Collection("zips")),
          Pipeline(List(Limit(105), Skip(100)))))
    }

    "plan filter and limit" in {
      plan("SELECT city, pop FROM zips ORDER BY pop DESC LIMIT 5") must
        beWorkflow {
          PipelineTask(
            ReadTask(Collection("zips")),
            Pipeline(List(
              Project(Reshape.Doc(ListMap(
                BsonField.Name("lEft") -> \/-(Reshape.Doc(ListMap(
                  BsonField.Name("city") -> -\/(ExprOp.DocField(BsonField.Name("city"))), 
                  BsonField.Name("pop") -> -\/(ExprOp.DocField(BsonField.Name("pop")))))), 
                BsonField.Name("rIght") -> \/-(Reshape.Arr(ListMap(BsonField.Index(0) -> \/-(Reshape.Doc(ListMap(
                  BsonField.Name("key") -> -\/(ExprOp.DocField(BsonField.Name("pop"))) ))))))))),
              Sort(NonEmptyList(BsonField.Name("rIght") \ BsonField.Index(0) \ BsonField.Name("key") -> Descending)),
              Limit(5),
              Project(Reshape.Doc(ListMap(
                BsonField.Name("city") -> -\/(ExprOp.DocField(BsonField.Name("lEft") \ BsonField.Name("city"))), 
                BsonField.Name("pop") -> -\/(ExprOp.DocField(BsonField.Name("lEft") \ BsonField.Name("pop")))))))))
        }
    }

    "plan simple single field selection and limit" in {
      plan("SELECT city FROM zips LIMIT 5") must
        beWorkflow {
          PipelineTask(
            ReadTask(Collection("zips")),
            Pipeline(List(
              Project(Reshape.Doc(ListMap(BsonField.Name("city") -> -\/ (ExprOp.DocField(BsonField.Name("city")))))),
              Limit(5))))
        }
    }

    "plan complex group by with sorting and limiting" in {
      plan("SELECT city, SUM(pop) AS pop FROM zips GROUP BY city ORDER BY pop") must
        beWorkflow {
          PipelineTask(
            ReadTask(Collection("zips")),
            Pipeline(List()))
        }
    }.pendingUntilFixed

    "plan simple distinct" in {
      plan("select distinct city, state from zips") must 
      beWorkflow(
        PipelineTask(
          ReadTask(Collection("zips")),
          Pipeline(List(
            Group(
              Grouped(ListMap(
                BsonField.Name("city") -> ExprOp.First(ExprOp.DocField(BsonField.Name("city"))),
                BsonField.Name("state") -> ExprOp.First(ExprOp.DocField(BsonField.Name("state"))))),
              \/- (Reshape.Doc(ListMap(
                BsonField.Name("city") -> -\/ (ExprOp.DocField(BsonField.Name("city"))),
                BsonField.Name("state") -> -\/(ExprOp.DocField(BsonField.Name("state")))))))))))
    }

    "plan distinct as expression" in {
      plan("select count(distinct(city)) from zips") must 
        beWorkflow(
          PipelineTask(
            ReadTask(Collection("zips")),
            Pipeline(List(
              Group(
                Grouped(ListMap(
                  BsonField.Name("value") -> ExprOp.First(ExprOp.DocField(BsonField.Name("city"))))),
                -\/ (ExprOp.DocField(BsonField.Name("city")))),
              Group(
                Grouped(ListMap(
                  BsonField.Name("0") -> ExprOp.Sum(ExprOp.Literal(Bson.Int32(1))))),
                -\/ (ExprOp.Literal(Bson.Int32(1))))))))
    }

    "plan distinct of expression as expression" in {
      plan("select count(distinct substring(city, 0, 1)) from zips") must 
        beWorkflow(
          PipelineTask(
            ReadTask(Collection("zips")),
            Pipeline(List(
              Group(
                Grouped(ListMap(
                  BsonField.Name("value") -> ExprOp.First(
                                                ExprOp.Substr(
                                                  ExprOp.DocField(BsonField.Name("city")),
                                                  ExprOp.Literal(Bson.Int64(0)),
                                                  ExprOp.Literal(Bson.Int64(1)))))),
                -\/ (ExprOp.Substr(
                      ExprOp.DocField(BsonField.Name("city")),
                      ExprOp.Literal(Bson.Int64(0)),
                      ExprOp.Literal(Bson.Int64(1))))),
              Group(
                Grouped(ListMap(
                  BsonField.Name("0") -> ExprOp.Sum(ExprOp.Literal(Bson.Int32(1))))),
                -\/ (ExprOp.Literal(Bson.Int32(1))))))))
    }

    "plan distinct of wildcard" in {
      plan("select distinct * from zips") must 
        beWorkflow(
          PipelineTask(
            ReadTask(Collection("zips")),
            Pipeline(List(???))))
    }.pendingUntilFixed("#283")

    "plan distinct of wildcard as expression" in {
      plan("select count(distinct *) from zips") must 
        beWorkflow(
          PipelineTask(
            ReadTask(Collection("zips")),
            Pipeline(List(???))))
    }.pendingUntilFixed("#283")

    "plan distinct with expression and order by" in {
      plan("select distinct city from zips order by pop desc") must 
        beWorkflow(
          PipelineTask(
            ReadTask(Collection("zips")),
            Pipeline(List(???))))
    }.pendingUntilFixed("#284")


    "plan distinct as function with group" in {
      plan("select state, count(distinct(city)) from zips group by state") must 
        beWorkflow(
          PipelineTask(
            ReadTask(Collection("zips")),
            Pipeline(List(???))))
    }.pendingUntilFixed
    
    "plan distinct with sum and group" in {
      plan("SELECT DISTINCT SUM(pop) AS totalPop, city FROM zips GROUP BY city") must
        beWorkflow(
          PipelineTask(
            ReadTask(Collection("zips")),
            Pipeline(List(
              Project(
                Reshape.Doc(ListMap(
                  BsonField.Name("lEft") -> \/-(Reshape.Doc(ListMap(
                    BsonField.Name("lEft") -> \/-(Reshape.Doc(ListMap(
                      BsonField.Name("value") -> -\/(ExprOp.DocField(BsonField.Name("pop")))))), 
                    BsonField.Name("rIght") -> \/-(Reshape.Arr(ListMap(
                      BsonField.Index(0) -> -\/(ExprOp.DocField(BsonField.Name("city"))))))))), 
                  BsonField.Name("rIght") -> \/-(Reshape.Doc(ListMap(
                    BsonField.Name("city") -> -\/(ExprOp.DocField(BsonField.Name("city"))))))))), 
              Group(
                Grouped(ListMap(
                  BsonField.Name("totalPop") -> ExprOp.Sum(ExprOp.DocField(BsonField.Name("lEft") \ BsonField.Name("lEft") \ BsonField.Name("value"))), 
                  BsonField.Name("__sd_tmp_1") -> ExprOp.Push(ExprOp.DocField(BsonField.Name("rIght"))))),
                -\/(ExprOp.DocField(BsonField.Name("lEft") \ BsonField.Name("rIght")))), 
              Unwind(ExprOp.DocField(BsonField.Name("__sd_tmp_1"))), 
              Group(
                Grouped(ListMap(
                  BsonField.Name("totalPop") -> ExprOp.First(ExprOp.DocField(BsonField.Name("totalPop"))), 
                  BsonField.Name("city") -> ExprOp.First(ExprOp.DocField(BsonField.Name("__sd_tmp_1") \ BsonField.Name("city"))))),
                \/-(Reshape.Doc(ListMap(
                  BsonField.Name("totalPop") -> -\/(ExprOp.DocField(BsonField.Name("totalPop"))), 
                  BsonField.Name("city") -> -\/(ExprOp.DocField(BsonField.Name("__sd_tmp_1") \ BsonField.Name("city"))))))), 
              Project(
                Reshape.Doc(ListMap(
                  BsonField.Name("totalPop") -> -\/(ExprOp.DocField(BsonField.Name("totalPop"))), 
                  BsonField.Name("city") -> -\/(ExprOp.DocField(BsonField.Name("city"))), 
                  BsonField.Name("_id") -> -\/(ExprOp.Exclude))))))))
    }
    
    "plan distinct with sum, group, and orderBy" in {
      plan("SELECT DISTINCT SUM(pop) AS totalPop, city FROM zips GROUP BY city ORDER BY totalPop DESC LIMIT 5") must
        beWorkflow(
          PipelineTask(
            ReadTask(Collection("zips")),
            Pipeline(List())))
    }.pendingUntilFixed
    
    "plan combination of two distinct sets" in {
      plan("SELECT (DISTINCT foo.bar) + (DISTINCT foo.baz) FROM foo") must
        beWorkflow(
          PipelineTask(
            ReadTask(Collection("zips")),
            Pipeline(List()))) // TODO
    }.pendingUntilFixed
    
    import Js._

    def joinStructure(
      left: WorkflowTask, right: WorkflowTask,
      leftKey: ExprOp, rightKey: Js.Expr,
      fin: WorkflowTask => WorkflowTask) = {
      val initialPipeOps =
        List(
          Group(
            Grouped(ListMap(
              BsonField.Name("left") ->
                ExprOp.AddToSet(ExprOp.DocVar(DocVar.ROOT, None)))),
            -\/(leftKey)),
          Project(Reshape.Doc(ListMap(
            BsonField.Name("value") -> \/-(Reshape.Doc(ListMap(
              BsonField.Name("left") ->
                -\/(ExprOp.DocField(BsonField.Name("left"))),
              BsonField.Name("right") ->
                -\/(ExprOp.Literal(Bson.Arr(List()))))))))))
      fin(
        FoldLeftTask(NonEmptyList(
          left match {
            case PipelineTask(src, pipe) =>
              PipelineTask(src, Pipeline(pipe.ops ++ initialPipeOps))
            case _ => PipelineTask(left, Pipeline(initialPipeOps))
          },
          MapReduceTask(
            right,
            MapReduce(
              MapOp.mapFn(MapOp.mapKeyVal(
                rightKey,
                AnonObjDecl(List(
                  ("left", AnonElem(List())),
                  ("right", AnonElem(List(Ident("this")))))))),
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
                  Return(Ident("result")))),
              Some(MapReduce.WithAction(MapReduce.Action.Reduce)))))))
    }
            
    "plan simple join" in {
      plan("select zips2.city from zips join zips2 on zips._id = zips2._id") must
        beWorkflow(
          joinStructure(
            ReadTask(Collection("zips")),
            ReadTask(Collection("zips2")),
            ExprOp.DocField(BsonField.Name("_id")),
            Select(Ident("this"), "_id"),
            PipelineTask(_,
              Pipeline(List(
                Match(Selector.Doc(ListMap[BsonField, Selector.SelectorExpr](
                  BsonField.Name("value") \ BsonField.Name("left") -> Selector.NotExpr(Selector.Size(0)),
                  BsonField.Name("value") \ BsonField.Name("right") -> Selector.NotExpr(Selector.Size(0))))),
                Unwind(ExprOp.DocField(BsonField.Name("value") \ BsonField.Name("left"))),
                Unwind(ExprOp.DocField(BsonField.Name("value") \ BsonField.Name("right"))),
                Project(Reshape.Doc(ListMap(
                  BsonField.Name("city") -> -\/(ExprOp.DocField(BsonField.Name("value") \ BsonField.Name("right") \ BsonField.Name("city"))),
                  BsonField.Name("_id") -> -\/(ExprOp.Exclude)))))))))
    }

    "plan simple inner equi-join" in {
      plan(
        "select foo.name, bar.address from foo join bar on foo.id = bar.foo_id") must
      beWorkflow(
        joinStructure(
          ReadTask(Collection("foo")),
          ReadTask(Collection("bar")),
          ExprOp.DocField(BsonField.Name("id")),
          Select(Ident("this"), "foo_id"),
          PipelineTask(_,
            Pipeline(List(
              Match(Selector.Doc(ListMap[BsonField, Selector.SelectorExpr](
                BsonField.Name("value") \ BsonField.Name("left") -> Selector.NotExpr(Selector.Size(0)),
                BsonField.Name("value") \ BsonField.Name("right") -> Selector.NotExpr(Selector.Size(0))))),
              Unwind(ExprOp.DocField(BsonField.Name("value") \ BsonField.Name("left"))),
              Unwind(ExprOp.DocField(BsonField.Name("value") \ BsonField.Name("right"))),
              Project(Reshape.Doc(ListMap(
                BsonField.Name("name") -> -\/(ExprOp.DocField(BsonField.Name("value") \ BsonField.Name("left") \ BsonField.Name("name"))),
                BsonField.Name("address") -> -\/(ExprOp.DocField(BsonField.Name("value") \ BsonField.Name("right") \ BsonField.Name("address"))),
                BsonField.Name("_id") -> -\/(ExprOp.Exclude)))))))))
    }

    "plan simple inner equi-join with wildcard" in {
      plan("select * from foo join bar on foo.id = bar.foo_id") must
      beWorkflow(
        joinStructure(
          ReadTask(Collection("foo")),
          ReadTask(Collection("bar")),
          ExprOp.DocField(BsonField.Name("id")),
          Select(Ident("this"), "foo_id"),
          x => MapReduceTask(PipelineTask(x,
            Pipeline(List(
              Match(Selector.Doc(ListMap[BsonField, Selector.SelectorExpr](
                BsonField.Name("value") \ BsonField.Name("left") -> Selector.NotExpr(Selector.Size(0)),
                BsonField.Name("value") \ BsonField.Name("right") -> Selector.NotExpr(Selector.Size(0))))),
              Unwind(ExprOp.DocField(BsonField.Name("value") \ BsonField.Name("left"))),
              Unwind(ExprOp.DocField(BsonField.Name("value") \ BsonField.Name("right"))),
              Project(Reshape.Doc(ListMap(
                BsonField.Name("left") -> -\/(ExprOp.DocField(BsonField.Name("value") \ BsonField.Name("left"))),
                BsonField.Name("right") -> -\/(ExprOp.DocField(BsonField.Name("value") \ BsonField.Name("right"))))))))),
            MapReduce(
              MapOp.mapFn(MapOp.mapMap(
                Js.Call(Js.Select(Js.AnonFunDecl(List("rez"),
                  List(
                    Js.ForIn(
                      Js.Ident("attr"),
                      Js.Select(Js.Ident("this"), "left"),
                      Js.If(
                        Js.Call(
                          Js.Select(Js.Select(Js.Ident("this"), "left"),
                            "hasOwnProperty"),
                          List(Js.Ident("attr"))),
                        Js.BinOp("=",
                          Js.Access(Js.Ident("rez"), Js.Ident("attr")),
                          Js.Access(Js.Select(Js.Ident("this"), "left"),
                            Js.Ident("attr"))),
                        None)),
                    Js.ForIn(
                      Js.Ident("attr"),
                      Js.Select(Js.Ident("this"), "right"),
                      Js.If(
                        Js.Call(
                          Js.Select(Js.Select(Js.Ident("this"), "right"),
                            "hasOwnProperty"),
                          List(Js.Ident("attr"))),
                        Js.BinOp("=",
                          Js.Access(Js.Ident("rez"), Js.Ident("attr")),
                          Js.Access(Js.Select(Js.Ident("this"), "right"),
                            Js.Ident("attr"))),
                        None)),
                    Js.Return(Js.Ident("rez")))), "apply"),
                  List(Js.Ident("this"), Js.AnonElem(List(Js.AnonObjDecl(Nil))))))),
              ReduceOp.reduceNOP))))
    }

    "plan simple left equi-join" in {
      plan(
        "select foo.name, bar.address " +
          "from foo left join bar on foo.id = bar.foo_id") must
      beWorkflow(
        joinStructure(
          ReadTask(Collection("foo")),
          ReadTask(Collection("bar")),
          ExprOp.DocField(BsonField.Name("id")),
          Select(Ident("this"), "foo_id"),
          PipelineTask(_,
            Pipeline(List(
              Match(Selector.Doc(ListMap[BsonField, Selector.SelectorExpr](
                BsonField.Name("value") \ BsonField.Name("left") -> Selector.NotExpr(Selector.Size(0))))),
              Project(Reshape.Doc(ListMap(
                BsonField.Name("value") -> \/-(Reshape.Doc(ListMap(
                  BsonField.Name("left") -> -\/(ExprOp.DocField(BsonField.Name("value") \ BsonField.Name("left"))),
                  BsonField.Name("right") -> -\/(ExprOp.Cond(
                    ExprOp.Eq(
                      ExprOp.Size(ExprOp.DocField(BsonField.Name("value") \ BsonField.Name("right"))),
                      ExprOp.Literal(Bson.Int32(0))),
                    ExprOp.Literal(Bson.Arr(List(Bson.Doc(ListMap())))),
                    ExprOp.DocField(BsonField.Name("value") \ BsonField.Name("right")))))))))),
              Unwind(ExprOp.DocField(BsonField.Name("value") \ BsonField.Name("left"))),
              Unwind(ExprOp.DocField(BsonField.Name("value") \ BsonField.Name("right"))),
              Project(Reshape.Doc(ListMap(
                BsonField.Name("name") -> -\/(ExprOp.DocField(BsonField.Name("value") \ BsonField.Name("left") \ BsonField.Name("name"))),
                BsonField.Name("address") -> -\/(ExprOp.DocField(BsonField.Name("value") \ BsonField.Name("right") \ BsonField.Name("address"))),
                BsonField.Name("_id") -> -\/(ExprOp.Exclude)))))))))
    }
 
    "plan 3-way equi-join" in {
      plan(
        "select foo.name, bar.address " +
          "from foo join bar on foo.id = bar.foo_id " +
          "join baz on bar.id = baz.bar_id") must
      beWorkflow(
        joinStructure(
          joinStructure(
            ReadTask(Collection("foo")),
            ReadTask(Collection("bar")),
            ExprOp.DocField(BsonField.Name("id")),
            Select(Ident("this"), "foo_id"),
            PipelineTask(_,
              Pipeline(List(
                Match(Selector.Doc(ListMap[BsonField, Selector.SelectorExpr](
                  BsonField.Name("value") \ BsonField.Name("left") -> Selector.NotExpr(Selector.Size(0)),
                  BsonField.Name("value") \ BsonField.Name("right") -> Selector.NotExpr(Selector.Size(0))))),
                Unwind(ExprOp.DocField(BsonField.Name("value") \ BsonField.Name("left"))),
                Unwind(ExprOp.DocField(BsonField.Name("value") \ BsonField.Name("right"))))))),
          ReadTask(Collection("baz")),
          ExprOp.DocField(BsonField.Name("value") \ BsonField.Name("right")\ BsonField.Name("id")),
          Select(Ident("this"), "bar_id"),
          PipelineTask(_,
            Pipeline(List(
              Match(Selector.Doc(ListMap[BsonField, Selector.SelectorExpr](
                BsonField.Name("value") \ BsonField.Name("left") -> Selector.NotExpr(Selector.Size(0)),
                BsonField.Name("value") \ BsonField.Name("right") -> Selector.NotExpr(Selector.Size(0))))),
              Unwind(ExprOp.DocField(BsonField.Name("value") \ BsonField.Name("left"))),
              Unwind(ExprOp.DocField(BsonField.Name("value") \ BsonField.Name("right"))),
              Project(Reshape.Doc(ListMap(
                BsonField.Name("name") -> -\/(ExprOp.DocField(BsonField.Name("value") \ BsonField.Name("left") \ BsonField.Name("left") \ BsonField.Name("name"))),
                BsonField.Name("address") -> -\/(ExprOp.DocField(BsonField.Name("value") \ BsonField.Name("left") \ BsonField.Name("right") \ BsonField.Name("address"))),
                BsonField.Name("_id") -> -\/(ExprOp.Exclude)))))))))
    }

    "plan simple cross" in {
      import Js._
      plan("select zips2.city from zips, zips2 where zips._id = zips2._id") must
      beWorkflow(
        joinStructure(
          ReadTask(Collection("zips")),
          ReadTask(Collection("zips2")),
          ExprOp.Literal(Bson.Int64(1)),
          Num(1, false),
          src => PipelineTask(
            MapReduceTask(
              PipelineTask(src,
                Pipeline(List(
                  Match(Selector.Doc(ListMap[BsonField, Selector.SelectorExpr](
                    BsonField.Name("value") \ BsonField.Name("left") -> Selector.NotExpr(Selector.Size(0)),
                    BsonField.Name("value") \ BsonField.Name("right") -> Selector.NotExpr(Selector.Size(0))))),
                  Unwind(ExprOp.DocField(BsonField.Name("value") \ BsonField.Name("left"))),
                  Unwind(ExprOp.DocField(BsonField.Name("value") \ BsonField.Name("right")))))),
              MapReduce(MapOp.mapFn(MapOp.mapNOP), ReduceOp.reduceNOP, None,
                Some(Selector.Where(BinOp("==",
                  Select(Select(Ident("this"), "left"), "_id"),
                  Select(Select(Ident("this"), "right"), "_id")))))),
            Pipeline(List(
              Project(Reshape.Doc(ListMap(
                BsonField.Name("city") -> -\/(ExprOp.DocField(BsonField.Name("value") \ BsonField.Name("right") \ BsonField.Name("city"))),
                BsonField.Name("_id") -> -\/(ExprOp.Exclude)))))))))
    }
  }

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

      val exp = PipelineTask(
        ReadTask(Collection("foo")),
        Pipeline(List(
          Project(Reshape.Doc(ListMap(
            BsonField.Name("lEft") -> \/- (Reshape.Doc(ListMap(
              BsonField.Name("bar") -> -\/ (ExprOp.DocField(BsonField.Name("bar")))))), 
            BsonField.Name("rIght") -> \/- (Reshape.Arr(ListMap(
              BsonField.Index(0) -> \/- (Reshape.Doc(ListMap(
                BsonField.Name("key") -> -\/ (ExprOp.DocField(
                  BsonField.Name("bar"))), 
                BsonField.Name("order") -> -\/ (ExprOp.Literal(Bson.Text("ASC")))))))))))), 
          Sort(NonEmptyList(BsonField.Name("rIght") \ BsonField.Index(0) \ BsonField.Name("key") -> Ascending)), 
          Project(Reshape.Doc(ListMap(BsonField.Name("bar") -> -\/(ExprOp.DocField(BsonField.Name("lEft") \ BsonField.Name("bar")))))))))

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

      val exp = PipelineTask(
                  ReadTask(Collection("foo")),
                  Pipeline(List(
                    Project(Reshape.Doc(ListMap(
                      BsonField.Name("__sd_tmp_1") ->  \/- (Reshape.Arr(ListMap(
                        BsonField.Index(0) -> -\/ (ExprOp.Divide(
                                                            DocField(BsonField.Name("bar")), 
                                                            Literal(Bson.Dec(10.0))))
                      )))
                    ))),
                    Sort(NonEmptyList(BsonField.Name("__sd_tmp_1") \ BsonField.Index(0) -> Ascending))
                    // We'll want another Project here to remove the temporary field
                  ))
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

      val exp = PipelineTask(
                  ReadTask(Collection("foo")),
                  Pipeline(List(
                    Match(
                      Selector.Doc(
                        BsonField.Name("baz") -> Selector.Eq(Bson.Int64(0))
                      )
                    ),
                    Sort(NonEmptyList(BsonField.Name("bar") -> Ascending))
                  ))
                )

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

      val exp = PipelineTask(
        ReadTask(Collection("foo")),
        Pipeline(List(
          Project(Reshape.Doc(ListMap(BsonField.Name("lEft") -> \/-(Reshape.Doc(ListMap(BsonField.Name("bar") -> -\/(ExprOp.DocField(BsonField.Name("bar")))))), BsonField.Name("rIght") -> \/-(Reshape.Arr(ListMap(BsonField.Index(0) -> \/-(Reshape.Doc(ListMap(BsonField.Name("key") -> -\/(ExprOp.Divide(ExprOp.DocField(BsonField.Name("bar")), ExprOp.Literal(Bson.Dec(10.0)))), BsonField.Name("order") -> -\/(ExprOp.Literal(Bson.Text("ASC")))))))))))), 
          Sort(NonEmptyList(BsonField.Name("rIght") \ BsonField.Index(0) \ BsonField.Name("key") -> Ascending)), 
          Project(Reshape.Doc(ListMap(BsonField.Name("bar") -> -\/(ExprOp.DocField(BsonField.Name("lEft") \ BsonField.Name("bar")))))))))

      plan(lp) must beWorkflow(exp)
    }
  }
  */
}
