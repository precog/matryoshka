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
      result(expected == s.value,
             "\n" + Show[Workflow].show(s.value) + "\n is equal to \n" + Show[Workflow].show(expected),
             "\n" + Show[Workflow].show(s.value) + "\n is not equal to \n" + Show[Workflow].show(expected),
             s)
    }
  }

  def testPhysicalPlanCompile(query: String, expected: Workflow) = {
    val phys = for {
      logical <- compile(query)
      simplified <- \/-(Optimizer.simplify(logical))
      phys <- MongoDbPlanner.plan(simplified)
    } yield phys
    phys.toEither must beRight(equalToWorkflow(expected))
  }

  "planner" should {
    "plan simple select *" in {
      testPhysicalPlanCompile(
        "select * from foo", 
        Workflow(ReadTask(Collection("foo"))))
    }

    "plan count(*)" in {
      testPhysicalPlanCompile(
        "select count(*) from foo", 
        Workflow(
          PipelineTask(
            ReadTask(Collection("foo")),
            Pipeline(List(
              Group(
                Grouped(Map(BsonField.Name("0") -> Count)),
                -\/(Literal(Bson.Int32(1)))))))))
    }

    "plan simple field projection on single set" in {
      testPhysicalPlanCompile(
        "select foo.bar from foo",
        Workflow(
          PipelineTask(
            ReadTask(Collection("foo")),
            Pipeline(List(
              Project(Reshape.Doc(Map(BsonField.Name("bar") -> -\/(DocField(BsonField.Name("bar"))))))
            ))
          )
        )
      )
    }

    "plan simple field projection on single set when table name is inferred" in {
      testPhysicalPlanCompile(
        "select bar from foo",
        Workflow(
          PipelineTask(
            ReadTask(Collection("foo")),
            Pipeline(List(
              Project(Reshape.Doc(Map(BsonField.Name("bar") -> -\/(DocField(BsonField.Name("bar"))))))
            ))
          )
        )
      )
    }
    
    "plan multiple field projection on single set when table name is inferred" in {
      testPhysicalPlanCompile(
        "select bar, baz from foo",
        Workflow(
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
      )
    }

    "plan simple addition on two fields" in {
      testPhysicalPlanCompile(
        "select foo + bar from baz",
        Workflow(
          PipelineTask(
            ReadTask(Collection("baz")),
            Pipeline(List(
              Project(Reshape.Doc(Map(BsonField.Name("0") -> -\/ (ExprOp.Add(DocField(BsonField.Name("foo")), DocField(BsonField.Name("bar")))))))
            ))
          )
        )
      )
    }
    
    "plan concat" in {
      testPhysicalPlanCompile(
        "select concat(bar, baz) from foo",
        Workflow(
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
      )
    }
    
    "plan simple filter" in {
      testPhysicalPlanCompile(
        "select * from foo where bar > 10",
        Workflow(
          PipelineTask(
            ReadTask(Collection("foo")),
            Pipeline(List(
              Match(Selector.Doc(BsonField.Name("bar") -> Selector.Gt(Bson.Int64(10))))
            ))
          )
        )
      )
    }
    
    "plan filter with between" in {
      testPhysicalPlanCompile(
        "select * from foo where bar between 10 and 100",
        Workflow(
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
      )
    }
    
    "plan filter with like" in {
      testPhysicalPlanCompile(
        "select * from foo where bar like 'A%'",
        Workflow(
          PipelineTask(
            ReadTask(Collection("foo")),
            Pipeline(List(
              Match(
                Selector.Doc(BsonField.Name("bar") -> Selector.Regex("^A.*$", false, false, false, false))
              )
            ))
          )
        )
      )
    }
    
    "plan complex filter" in {
      testPhysicalPlanCompile(
        "select * from foo where bar > 10 and (baz = 'quux' or foop = 'zebra')",
        Workflow(
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
      )
    }.pendingUntilFixed // failing during type-checking
    
    "plan simple sort with field in projection" in {
      testPhysicalPlanCompile(
        "select bar from foo order by bar",
        Workflow(
          PipelineTask(
            ReadTask(Collection("foo")),
            Pipeline(List(
              Project(Reshape.Doc(Map(
                BsonField.Name("bar")   -> -\/ (DocField(BsonField.Name("bar"))), 
                BsonField.Name("order") -> -\/ (Literal(Bson.Text("ASC")))
              ))), 
              Project(Reshape.Doc(Map(
                BsonField.Name("bar")   -> -\/ (DocField(BsonField.Name("bar"))), 
                BsonField.Name("key")   -> -\/ (DocField(BsonField.Name("bar"))), 
                BsonField.Name("order") -> -\/ (DocField(BsonField.Name("order")))
              ))), 
              Project(Reshape.Doc(Map(
                BsonField.Name("bar") -> -\/  (DocField(BsonField.Name("bar"))), 
                BsonField.Name("0")   ->  \/- (Reshape.Doc(Map(
                                                BsonField.Name("key")   -> -\/ (DocField(BsonField.Name("key"))), 
                                                BsonField.Name("order") -> -\/ (DocField(BsonField.Name("order")))
                                              )))
              ))), 
              Project(Reshape.Doc(Map(
                BsonField.Name("bar")         -> -\/  (DocField(BsonField.Name("bar"))), 
                BsonField.Name("__sd_tmp_1")  ->  \/- (Reshape.Arr(Map(
                                                        BsonField.Index(0) -> \/- (Reshape.Doc(Map(
                                                          BsonField.Name("key") -> -\/ (DocField(BsonField.Index(0) \ BsonField.Name("key"))), 
                                                          BsonField.Name("order") -> -\/ (DocField(BsonField.Index(0) \ BsonField.Name("order")))
                                                        )))
                                                      )))
              ))), 
              Sort(NonEmptyList((BsonField.Name("__sd_tmp_1"), Ascending)))
            ))
          )
        )
      )
    }
    
    "plan simple sort with wildcard" in {
      testPhysicalPlanCompile(
        "select * from foo order by bar",
        Workflow(
          PipelineTask(
            ReadTask(Collection("foo")),
            Pipeline(List(
              Sort(NonEmptyList(BsonField.Name("bar") -> Ascending))
            ))
          )
        )
      )
    } 
    
    "plan simple sort with field not in projections" in {
      testPhysicalPlanCompile(
        "select name from person order by height",
        Workflow(
          PipelineTask(
            ReadTask(Collection("person")),
            Pipeline(List(
              Project(Reshape.Doc(Map(
                BsonField.Name("name")    -> -\/ (DocVar.ROOT(BsonField.Name("name"))), 
                BsonField.Name("__sd__0") -> -\/ (DocVar.ROOT(BsonField.Name("height"))), 
                BsonField.Name("order")   -> -\/ (Literal(Bson.Text("ASC")))
              ))),
              Project(Reshape.Doc(Map(
                BsonField.Name("name")    -> -\/ (DocVar.ROOT(BsonField.Name("name"))), 
                BsonField.Name("__sd__0") -> -\/ (DocVar.ROOT(BsonField.Name("__sd__0"))), 
                BsonField.Name("key")     -> -\/ (DocVar.ROOT(BsonField.Name("__sd__0"))), 
                BsonField.Name("order")   -> -\/ (DocVar.ROOT(BsonField.Name("order")))
              ))),
              Project(Reshape.Doc(Map(
                BsonField.Name("name")    -> -\/  (DocVar.ROOT(BsonField.Name("name"))), 
                BsonField.Name("__sd__0") -> -\/  (DocVar.ROOT(BsonField.Name("__sd__0"))), 
                BsonField.Name("0")       ->  \/- (Reshape.Doc(Map(
                                                    BsonField.Name("key")   -> -\/ (DocVar.ROOT(BsonField.Name("key"))), 
                                                    BsonField.Name("order") -> -\/ (DocVar.ROOT(BsonField.Name("order")))
                                                  )))
              ))), 
              Project(Reshape.Doc(Map(
                BsonField.Name("name")        -> -\/  (DocVar.ROOT(BsonField.Name("name"))), 
                BsonField.Name("__sd__0")     -> -\/  (DocVar.ROOT(BsonField.Name("__sd__0"))), 
                BsonField.Name("__sd_tmp_1")  ->  \/- (Reshape.Arr(Map(
                                                        BsonField.Index(0) -> \/- (Reshape.Doc(Map(
                                                            BsonField.Name("key") -> 
                                                              -\/ (DocVar.ROOT() \ BsonField.Index(0) \ BsonField.Name("key")), 
                                                            BsonField.Name("order") -> 
                                                              -\/ (DocVar.ROOT() \ BsonField.Index(0) \ BsonField.Name("order"))
                                                              )))
                                                        )))
              ))),
              Sort(NonEmptyList(
                BsonField.Name("__sd_tmp_1") -> Ascending
              )), 
              Project(Reshape.Doc(Map(
                BsonField.Name("name") -> -\/ (DocVar.ROOT(BsonField.Name("name")))
              )))
            ))
          )
        )
      )
    }

    "plan multiple column sort with wildcard" in {
      testPhysicalPlanCompile(
        "select * from foo order by bar, baz desc",
        Workflow(
          PipelineTask(
            ReadTask(Collection("foo")),
            Pipeline(List(
              Sort(NonEmptyList(BsonField.Name("bar") -> Ascending, 
                                BsonField.Name("baz") -> Descending
              ))
            ))
          )
        )
      )
    }
    
    "plan many sort columns" in {
      testPhysicalPlanCompile(
        "select * from foo order by a1, a2, a3, a4, a5, a6",
        Workflow(
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
      )
    }
  }
}
