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

      MongoDbEvaluator.toJS(wf, Path("result")) must beRightDisj(
        "db.zips.find()")
    }

    "write trivial workflow to JS with fancy collection name" in {
      val wf = Workflow(
        ReadTask(Collection("tmp.123")))

      MongoDbEvaluator.toJS(wf, Path("result")) must beRightDisj(
        "db.getCollection(\"tmp.123\").find()")
    }

    "write simple pipeline workflow to JS" in {
      val wf = Workflow(
        PipelineTask(
          ReadTask(Collection("zips")),
          Pipeline(List(
            Match(Selector.Doc(
              BsonField.Name("pop") -> Selector.Gte(Bson.Int64(1000))
            ))))))
      
      MongoDbEvaluator.toJS(wf, Path("result")) must beRightDisj(
        """db.zips.aggregate([
          |  { "$match" : { "pop" : { "$gte" : 1000}}},
          |  { "$out" : "result"}
          |],
          |  { allowDiskUse: true })
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
      
      MongoDbEvaluator.toJS(wf, Path("result")) must beRightDisj(
        """db.zips.aggregate([
          |  { "$match" : { "pop" : { "$lte" : 1000}}},
          |  { "$out" : "tmp.gen_1"}
          |],
          |  { allowDiskUse: true })
          |db.tmp.gen_1.aggregate([
          |  { "$match" : { "pop" : { "$gte" : 100}}},
          |  { "$out" : "tmp.gen_0"}
          |],
          |  { allowDiskUse: true })
          |db.tmp.gen_0.aggregate([
          |  { "$sort" : { "city" : 1}},
          |  { "$out" : "result"}
          |],
          |  { allowDiskUse: true })
          |db.result.find()""".stripMargin)
    }
    
    "write map-reduce Workflow to JS" in {
      val wf = Workflow(
        MapReduceTask(
          ReadTask(Collection("zips")),
          MapReduce(
            Js.AnonFunDecl(Nil, 
              List(
                Js.Call(
                  Js.Ident("emit"),
                  List(
                    Js.Select(Js.Ident("this"), "city"),
                    Js.Select(Js.Ident("this"), "pop"))))),
            Js.AnonFunDecl("key" :: "values" :: Nil, 
              List(
                Js.Return(Js.Call(
                  Js.Select(Js.Ident("Array"), "sum"),
                  List(Js.Ident("values")))))),
            Some(MapReduce.WithAction()))))
          

      MongoDbEvaluator.toJS(wf, Path("result")) must beRightDisj(
        """db.zips.mapReduce(
        |  function () {
        |    emit(this.city, this.pop);
        |  },
        |  function (key, values) {
        |    return Array.sum(values);
        |  },
        |  { "out" : { "replace" : "result"}})
        |db.result.find()""".stripMargin)
    }

  }

  "JSExecutor.SimpleNamePattern" should {
    import JSExecutor._

    "match identifier" in {
      SimpleNamePattern.unapplySeq("foo") must beSome
    }

    "not match leading _" in {
      SimpleNamePattern.unapplySeq("_foo") must beNone
    }

    "match dot-separated identifiers" in {
      SimpleNamePattern.unapplySeq("foo.bar") must beSome
    }

    "match everything allowed" in {
      SimpleNamePattern.unapplySeq("foo2.BAR_BAZ") must beSome
    }

    "not match leading digit" in {
      SimpleNamePattern.unapplySeq("123") must beNone
    }

    "not match leading digit in second position" in {
      SimpleNamePattern.unapplySeq("foo.123") must beNone
    }
  }
}
