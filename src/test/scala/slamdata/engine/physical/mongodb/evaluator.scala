package slamdata.engine.physical.mongodb

import slamdata.engine._

import scalaz._
import Scalaz._

import org.specs2.mutable._

class EvaluatorSpec extends Specification with DisjunctionMatchers {
  "evaluate" should {
    import WorkflowTask._
    import PipelineOp._
    import fs.Path

    "write trivial workflow to JS" in {
      val wf = Workflow(
        ReadTask(Collection("zips")))

      MongoDbEvaluator.toJS(wf) must beRightDisj(
        "db.zips.find()")
    }
    
    "write simple pipeline workflow to JS" in {
      val wf = Workflow(
        PipelineTask(
          ReadTask(Collection("zips")),
          Pipeline(List(
            Match(Selector.Doc(
              BsonField.Name("pop") -> Selector.Gte(Bson.Int64(1000))
            ))))))
      
      MongoDbEvaluator.toJS(wf) must beRightDisj(
        """db.zips.aggregate([
          |  { "$match" : { "pop" : { "$gte" : 1000}}},
          |  { "$out" : "result"}
          |])
          |db.result.find()""".stripMargin)
    }
    
    "write chained pipeline workflow to JS" in {
      val p1 = PipelineTask(
                ReadTask(Collection("zips")),
                Pipeline(List(
                  Match(Selector.Doc(
                    BsonField.Name("pop") -> Selector.Lte(Bson.Int64(1000))
                  )))))
      val p2 = PipelineTask(
                p1,
                Pipeline(List(
                  Match(Selector.Doc(
                    BsonField.Name("pop") -> Selector.Gte(Bson.Int64(100))
                  )))))
      val p3 = PipelineTask(
                p2,
                Pipeline(List(
                  Sort(NonEmptyList(BsonField.Name("city") -> Ascending))
                )))
      val wf = Workflow(p3)
      
      MongoDbEvaluator.toJS(wf) must beRightDisj(
        """db.zips.aggregate([
          |  { "$match" : { "pop" : { "$lte" : 1000}}},
          |  { "$out" : "tmp_1"}
          |])
          |db.tmp_1.aggregate([
          |  { "$match" : { "pop" : { "$gte" : 100}}},
          |  { "$out" : "tmp_0"}
          |])
          |db.tmp_0.aggregate([
          |  { "$sort" : { "city" : 1}},
          |  { "$out" : "result"}
          |])
          |db.result.find()""".stripMargin)
    }
    
    "write map-reduce Workflow to JS" in {
      val wf = Workflow(
        MapReduceTask(
          ReadTask(Collection("zips")),
          MapReduce(
            Js.AnonFunDecl(Nil, 
              Js.Block(List(
              Js.Call(
                Js.Ident("emit"),
                List(
                  Js.Select(Js.Ident("this"), "city"),
                  Js.Select(Js.Ident("this"), "pop")))))),
            Js.AnonFunDecl("key" :: "values" :: Nil, 
              Js.Block(List(
              Js.Return(Js.Call(
                Js.Select(Js.Ident("Array"), "sum"),
                List(Js.Ident("values"))))))),
            Some(Output.WithAction()),
            None,
            None,
            None,
            None,
            None,
            None,
            None
          )))
          

      MongoDbEvaluator.toJS(wf) must beRightDisj(
        """db.zips.mapReduce(
        |  function () {
        |    emit(this.city, this.pop);
        |  },
        |  function (key, values) {
        |    return Array.sum(values);
        |  },
        |  { "out" : { "replace" : "result"}}
        |)
        |db.result.find()""".stripMargin)
    }

  }
}