package slamdata.engine.physical.mongodb

import slamdata.engine._
import slamdata.engine.fp._
import slamdata.engine.analysis.fixplate._
import slamdata.engine.analysis._
import slamdata.engine.sql.SQLParser
import slamdata.engine.std._

import scalaz._

import org.specs2.mutable._
import org.specs2.matcher.{Matcher, Expectable}

class PlannerSpec extends Specification with CompilerHelpers {
  import StdLib._
  import structural._
  import math._
  import LogicalPlan._
  import SemanticAnalysis._
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

  def plan(query: String): Either[PlannerError, Workflow] = {
    (for {
      logical <- compile(query).leftMap(e => PlannerError.InternalError("query could not be compiled: " + e))
      simplified <- \/-(Optimizer.simplify(logical))
      phys <- MongoDbPlanner.plan(simplified)
    } yield phys).toEither
  }

  def plan(logical: Term[LogicalPlan]): Either[PlannerError, Workflow] =
    (for {
      simplified <- \/-(Optimizer.simplify(logical))
      phys <- MongoDbPlanner.plan(simplified)
    } yield phys).toEither

  def beWorkflow(task: WorkflowTask) = beRight(equalToWorkflow(Workflow(task)))

  "plan from query string" should {

    "plan simple select *" in {
      plan("select * from foo") must beWorkflow(ReadTask(Collection("foo")))
    }

    "plan count(*)" in {
      plan("select count(*) from foo") must beWorkflow( 
        PipelineTask(
          ReadTask(Collection("foo")),
          Pipeline(List(
            Group(
              Grouped(Map(BsonField.Name("0") -> Count)),
              -\/(Literal(Bson.Int32(1))))))))
    }

    "plan simple field projection on single set" in {
      plan("select foo.bar from foo") must
        beWorkflow(
          PipelineTask(
            ReadTask(Collection("foo")),
            Pipeline(List(
              Project(Reshape.Doc(Map(BsonField.Name("bar") -> -\/(DocField(BsonField.Name("bar"))))))
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
              Project(Reshape.Doc(Map(BsonField.Name("bar") -> -\/(DocField(BsonField.Name("bar"))))))
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
              Project(Reshape.Doc(Map(
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
              Project(Reshape.Doc(Map(BsonField.Name("0") -> -\/ (ExprOp.Add(DocField(BsonField.Name("foo")), DocField(BsonField.Name("bar")))))))
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
              Project(Reshape.Doc(Map(
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
              Project(Reshape.Doc(Map(
                BsonField.Name("0") ->
                  -\/(ExprOp.ToLower(DocField(BsonField.Name("bar")))))))))))
    }

    "plan coalesce" in {
      plan("select coalesce(bar, baz) from foo") must
       beWorkflow(
          PipelineTask(
            ReadTask(Collection("foo")),
            Pipeline(List(
              Project(Reshape.Doc(Map(
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
              Project(Reshape.Doc(Map(
                BsonField.Name("0") ->
                  -\/(ExprOp.DayOfMonth(DocField(BsonField.Name("baz")))))))))))
    }

    "plan complex date field extraction" in {
      plan("select date_part('quarter', baz) from foo") must
       beWorkflow(
          PipelineTask(
            ReadTask(Collection("foo")),
            Pipeline(List(
              Project(Reshape.Doc(Map(
                BsonField.Name("0") ->
                  -\/(
                    ExprOp.Add(
                      ExprOp.Divide(
                        ExprOp.DayOfYear(DocField(BsonField.Name("baz"))),
                        ExprOp.Literal(Bson.Int32(92))),
                      ExprOp.Literal(Bson.Int32(1)))))))))))
    }

    "plan array length" in {
      plan("select array_length(bar, 1) from foo") must
       beWorkflow(
          PipelineTask(
            ReadTask(Collection("foo")),
            Pipeline(List(
              Project(Reshape.Doc(Map(
                BsonField.Name("0") ->
                  -\/(ExprOp.Size(DocField(BsonField.Name("bar")))))))))))
    }

    "plan conditional" in {
      plan("select case when pop < 10000 then city else loc end from zips") must
       beWorkflow(
          PipelineTask(
            ReadTask(Collection("zips")),
            Pipeline(List(
              Project(Reshape.Doc(Map(
                BsonField.Name("0") ->
                  -\/(Cond(
                    Lt(
                      DocField(BsonField.Name("pop")),
                      ExprOp.Literal(Bson.Int64(10000))),
                    DocField(BsonField.Name("city")),
                    DocField(BsonField.Name("loc")))))))))))
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
              Project(Reshape.Doc(Map(BsonField.Name("0") -> -\/ (ExprOp.Add(
                                                                    DocField(BsonField.Name("a")), 
                                                                    DocField(BsonField.Name("b"))
                                                                  ))
              )))
            ))
          )
        )
    }.pendingUntilFixed
    
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
    }.pendingUntilFixed // failing during type-checking
    
    "plan simple sort with field in projection" in {
      plan("select bar from foo order by bar") must
       beWorkflow(
          PipelineTask(
            ReadTask(Collection("foo")),
            Pipeline(List(
              Project(Reshape.Doc(Map(
                BsonField.Name("bar")   -> -\/ (DocField(BsonField.Name("bar")))
              ))), 
              Project(Reshape.Doc(Map(
                BsonField.Name("bar")         -> -\/  (DocField(BsonField.Name("bar"))), 
                BsonField.Name("__sd_tmp_1")  ->  \/- (Reshape.Arr(Map(
                                                        BsonField.Index(0) -> -\/ (DocField(BsonField.Name("bar")))
                                                      )))
              ))), 
              Sort(NonEmptyList(
                BsonField.Name("__sd_tmp_1") \ BsonField.Index(0) -> Ascending
              ))
            ))
          )
        )
    }
    
    "plan simple sort with wildcard" in {
      plan("select * from foo order by bar") must
       beWorkflow(
          PipelineTask(
            ReadTask(Collection("foo")),
            Pipeline(List(
              Sort(NonEmptyList(BsonField.Name("bar") -> Ascending))
            ))
          )
        )
    } 

    "plan sort with expression in key" in {
      plan("select baz from foo order by bar/10") must
       beWorkflow(
          PipelineTask(
            ReadTask(Collection("foo")),
            Pipeline(List(
              Project(Reshape.Doc(Map(
                BsonField.Name("baz")    -> -\/ (DocField(BsonField.Name("baz"))),
                BsonField.Name("__sd__0") -> -\/ (ExprOp.Divide(
                                                    DocField(BsonField.Name("bar")),
                                                    Literal(Bson.Int64(10))))
              ))),
              Project(Reshape.Doc(Map(
                BsonField.Name("baz")        -> -\/  (DocField(BsonField.Name("baz"))),
                BsonField.Name("__sd__0")     -> -\/  (DocField(BsonField.Name("__sd__0"))),
                BsonField.Name("__sd_tmp_1")  ->  \/- (Reshape.Arr(Map(
                                                        BsonField.Index(0) -> -\/ (DocField(BsonField.Name("__sd__0")))
                                                       )))
              ))),
              Sort(NonEmptyList(
                BsonField.Name("__sd_tmp_1") \ BsonField.Index(0) -> Ascending
              )),
              Project(Reshape.Doc(Map(
                BsonField.Name("baz") -> -\/ (DocField(BsonField.Name("baz")))
              )))
            ))
          )
        )
    }

    "plan sort with wildcard and expression in key" in {
      plan("select * from foo order by bar/10") must
       beWorkflow(
          PipelineTask(
            ReadTask(Collection("foo")),
            ???  // TODO: Currently cannot deal with logical plan having ObjectConcat with collection as an arg
          )
        )
    }.pendingUntilFixed
    
    "plan simple sort with field not in projections" in {
      plan("select name from person order by height") must
       beWorkflow(
          PipelineTask(
            ReadTask(Collection("person")),
            Pipeline(List(
              Project(Reshape.Doc(Map(
                BsonField.Name("name")    -> -\/ (DocField(BsonField.Name("name"))),
                BsonField.Name("__sd__0") -> -\/ (DocField(BsonField.Name("height")))
              ))),
              Project(Reshape.Doc(Map(
                BsonField.Name("name")        -> -\/  (DocField(BsonField.Name("name"))),
                BsonField.Name("__sd__0")     -> -\/  (DocField(BsonField.Name("__sd__0"))),
                BsonField.Name("__sd_tmp_1")  ->  \/- (Reshape.Arr(Map(
                                                        BsonField.Index(0) -> -\/ (DocField(BsonField.Name("__sd__0")))
                                                       )))
              ))),
              Sort(NonEmptyList(
                BsonField.Name("__sd_tmp_1") \ BsonField.Index(0) -> Ascending
              )),
              Project(Reshape.Doc(Map(
                BsonField.Name("name") -> -\/ (DocField(BsonField.Name("name")))
              )))
            ))
          )
        )
    }
    
    "plan sort with expression and alias" in {
      plan("select pop/1000 as popInK from zips order by popInK") must
       beWorkflow(
          PipelineTask(
            ReadTask(Collection("zips")),
            Pipeline(List(
              Project(Reshape.Doc(Map(
                BsonField.Name("popInK") -> -\/ (ExprOp.Divide(DocField(BsonField.Name("pop")), Literal(Bson.Int64(1000)))) 
              ))),
              Project(Reshape.Doc(Map(
                BsonField.Name("popInK")     -> -\/  (DocField(BsonField.Name("popInK"))),
                BsonField.Name("__sd_tmp_1") ->  \/- (Reshape.Arr(Map(
                                                        BsonField.Index(0) -> -\/ (DocField(BsonField.Name("popInK")))
                                                       )))
              ))),
              Sort(NonEmptyList(
                BsonField.Name("__sd_tmp_1") \ BsonField.Index(0) -> Ascending
              ))
            ))
          )
        )
    }
    
    "plan sort with filter" in {
      plan("select city, pop from zips where pop <= 1000 order by pop desc, city") must
       beWorkflow(
          PipelineTask(
            ReadTask(Collection("zips")),
            Pipeline(List(
              Match(Selector.Doc(
                BsonField.Name("pop") -> Selector.Lte(Bson.Int64(1000))
              )),
              Project(Reshape.Doc(Map(
                BsonField.Name("city") -> -\/ (DocField(BsonField.Name("city"))),
                BsonField.Name("pop")  -> -\/ (DocField(BsonField.Name("pop")))
              ))),
              Project(Reshape.Doc(Map(
                BsonField.Name("city") -> -\/ (DocField(BsonField.Name("city"))),
                BsonField.Name("pop")  -> -\/ (DocField(BsonField.Name("pop"))),
                BsonField.Name("__sd_tmp_1") ->  \/- (Reshape.Arr(Map(
                                                        BsonField.Index(0) -> -\/ (DocField(BsonField.Name("pop"))),
                                                        BsonField.Index(1) -> -\/ (DocField(BsonField.Name("city")))
                                                       )))
              ))),
              Sort(NonEmptyList(
                BsonField.Name("__sd_tmp_1") \ BsonField.Index(0) -> Descending,
                BsonField.Name("__sd_tmp_1") \ BsonField.Index(1) -> Ascending
              ))
              // Note: this would be better... to remove the inserted sort value
              // Project(Reshape.Doc(Map(
              //   BsonField.Name("city") -> -\/ (DocField(BsonField.Name("city"))),
              //   BsonField.Name("pop")  -> -\/ (DocField(BsonField.Name("pop")))
              // )))
            ))
          )
        )
    }
    
    "plan sort with expression, alias, and filter" in {
      plan("select pop/1000 as popInK from zips where pop >= 1000 order by popInK") must
       beWorkflow(
          PipelineTask(
            ReadTask(Collection("zips")),
            Pipeline(List(
              Match(Selector.Doc(
                BsonField.Name("pop") -> Selector.Gte(Bson.Int64(1000))
              )),
              Project(Reshape.Doc(Map(
                BsonField.Name("popInK") -> -\/ (ExprOp.Divide(DocField(BsonField.Name("pop")), Literal(Bson.Int64(1000)))) 
              ))),
              Project(Reshape.Doc(Map(
                BsonField.Name("popInK")     -> -\/  (DocField(BsonField.Name("popInK"))),
                BsonField.Name("__sd_tmp_1") ->  \/- (Reshape.Arr(Map(
                                                        BsonField.Index(0) -> -\/ (DocField(BsonField.Name("popInK")))
                                                       )))
              ))),
              Sort(NonEmptyList(
                BsonField.Name("__sd_tmp_1") \ BsonField.Index(0) -> Ascending
              ))
            ))
          )
        )
    }.pendingUntilFixed

    "plan multiple column sort with wildcard" in {
      plan("select * from foo order by bar, baz desc") must
       beWorkflow(
          PipelineTask(
            ReadTask(Collection("foo")),
            Pipeline(List(
              Sort(NonEmptyList(BsonField.Name("bar") -> Ascending, 
                                BsonField.Name("baz") -> Descending
              ))
            ))
          )
        )
    }
    
    "plan many sort columns" in {
      plan("select * from foo order by a1, a2, a3, a4, a5, a6") must
       beWorkflow(
          PipelineTask(
            ReadTask(Collection("foo")),
            Pipeline(List(
              Sort(NonEmptyList(BsonField.Name("a1") -> Ascending, 
                                BsonField.Name("a2") -> Ascending, 
                                BsonField.Name("a3") -> Ascending, 
                                BsonField.Name("a4") -> Ascending, 
                                BsonField.Name("a5") -> Ascending, 
                                BsonField.Name("a6") -> Ascending
              ))
            ))
          )
        )
    }
    
  }
  
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
                    Project(Reshape.Doc(Map(
                      BsonField.Name("bar") -> -\/ (DocField(BsonField.Name("bar")))
                    ))),
                    Project(Reshape.Doc(Map(
                      BsonField.Name("bar")        -> -\/  (DocField(BsonField.Name("bar"))),
                      BsonField.Name("__sd_tmp_1") ->  \/- (Reshape.Arr(Map(
                        BsonField.Index(0) -> -\/ (DocField(BsonField.Name("bar")))
                      )))
                    ))),
                    Sort(NonEmptyList(BsonField.Name("__sd_tmp_1") \ BsonField.Index(0) -> Ascending))
                  ))
                )

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
                    Project(Reshape.Doc(Map(
                      BsonField.Name("__sd_tmp_1") ->  \/- (Reshape.Arr(Map(
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
    }

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
                    Project(Reshape.Doc(Map(
                      BsonField.Name("bar") -> -\/ (DocField(BsonField.Name("bar")))
                    ))),
                    Project(Reshape.Doc(Map(
                      BsonField.Name("bar") -> -\/ (DocField(BsonField.Name("bar"))),
                      BsonField.Name("__sd_tmp_1") ->  \/- (Reshape.Arr(Map(
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
    }.pendingUntilFixed  // blows up early in the pipeline phase

  }
}
