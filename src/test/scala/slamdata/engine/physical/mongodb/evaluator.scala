package slamdata.engine.physical.mongodb

import slamdata.engine._

import scala.collection.immutable.ListMap

import scalaz._
import Scalaz._

import org.specs2.mutable._

class EvaluatorSpec extends Specification with DisjunctionMatchers {
  "evaluate" should {
    import WorkflowTask._
    import PipelineOp._
    import fs.Path

    "write trivial workflow to JS" in {
      val wf = ReadTask(Collection("zips"))

      MongoDbEvaluator.toJS(wf, Path("result")) must beRightDisj(
        "db.zips.find()")
    }

    "write trivial workflow to JS with fancy collection name" in {
      val wf = ReadTask(Collection("tmp.123"))

      MongoDbEvaluator.toJS(wf, Path("result")) must beRightDisj(
        "db.getCollection(\"tmp.123\").find()")
    }

    "write workflow with simple pure value" in {
      val wf = PureTask(Bson.Doc(ListMap("foo" -> Bson.Text("bar"))))

        MongoDbEvaluator.toJS(wf, Path("result")) must beRightDisj(
          """db.tmp.gen_0.insert({ "foo" : "bar"})
            |db.tmp.gen_0.renameCollection("result", true)
            |db.result.find()""".stripMargin)
    }

    "write workflow with multiple pure values" in {
      val wf = PureTask(Bson.Arr(List(
        Bson.Doc(ListMap("foo" -> Bson.Int64(1))),
        Bson.Doc(ListMap("bar" -> Bson.Int64(2))))))

        MongoDbEvaluator.toJS(wf, Path("result")) must beRightDisj(
          """db.tmp.gen_0.insert({ "foo" : 1})
            |db.tmp.gen_0.insert({ "bar" : 2})
            |db.tmp.gen_0.renameCollection("result", true)
            |db.result.find()""".stripMargin)
    }

    "fail with non-doc pure value" in {
      val wf = PureTask(Bson.Text("foo"))

      MongoDbEvaluator.toJS(wf, Path("result")) must beAnyLeftDisj
    }

    "fail with multiple pure values, one not a doc" in {
      val wf = PureTask(Bson.Arr(List(
        Bson.Doc(ListMap("foo" -> Bson.Int64(1))),
        Bson.Int64(2))))

        MongoDbEvaluator.toJS(wf, Path("result")) must beAnyLeftDisj
    }

    "write simple pipeline workflow to JS" in {
      val wf = PipelineTask(ReadTask(Collection("zips")),
        Pipeline(List(
          Match(Selector.Doc(
            BsonField.Name("pop") -> Selector.Gte(Bson.Int64(1000)))))))
      
      MongoDbEvaluator.toJS(wf, Path("result")) must beRightDisj(
        """db.zips.aggregate([
          |    { "$match" : { "pop" : { "$gte" : 1000}}},
          |    { "$out" : "tmp.gen_0"}
          |  ],
          |  { allowDiskUse: true })
          |db.tmp.gen_0.renameCollection("result", true)
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
      val wf = PipelineTask(
                p2,
                Pipeline(List(
                  Sort(NonEmptyList(BsonField.Name("city") -> Ascending))
                )))
      
      MongoDbEvaluator.toJS(wf, Path("result")) must beRightDisj(
        """db.zips.aggregate([
          |    { "$match" : { "pop" : { "$lte" : 1000}}},
          |    { "$out" : "tmp.gen_0"}
          |  ],
          |  { allowDiskUse: true })
          |db.tmp.gen_0.aggregate([
          |    { "$match" : { "pop" : { "$gte" : 100}}},
          |    { "$out" : "tmp.gen_1"}
          |  ],
          |  { allowDiskUse: true })
          |db.tmp.gen_1.aggregate([
          |    { "$sort" : { "city" : 1}},
          |    { "$out" : "tmp.gen_2"}
          |  ],
          |  { allowDiskUse: true })
          |db.tmp.gen_0.drop()
          |db.tmp.gen_1.drop()
          |db.tmp.gen_2.renameCollection("result", true)
          |db.result.find()""".stripMargin)
    }
    
    "write map-reduce Workflow to JS" in {
      val wf = MapReduceTask(ReadTask(Collection("zips")),
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
          Some(MapReduce.WithAction())))

      MongoDbEvaluator.toJS(wf, Path("result")) must beRightDisj(
        """db.zips.mapReduce(
        |  function () {
        |    emit(this.city, this.pop);
        |  },
        |  function (key, values) {
        |    return Array.sum(values);
        |  },
        |  { "out" : { "replace" : "tmp.gen_0"}})
        |db.tmp.gen_0.renameCollection("result", true)
        |db.result.find()""".stripMargin)
    }

    "write join Workflow to JS" in {
      val wf =
        FoldLeftTask(
          PipelineTask(
            ReadTask(Collection("zips1")),
            Pipeline(List(
              Match(Selector.Doc(
                BsonField.Name("city") -> Selector.Eq(Bson.Text("BOULDER"))))))),
          NonEmptyList(MapReduceTask(
            PipelineTask(
              ReadTask(Collection("zips2")),
              Pipeline(List(
                Match(Selector.Doc(
                  BsonField.Name("pop") -> Selector.Lte(Bson.Int64(1000))))))),
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
              Some(MapReduce.WithAction(MapReduce.Action.Reduce))))))

      MongoDbEvaluator.toJS(wf, Path("result")) must beRightDisj(
        """db.zips1.aggregate([
          |    { "$match" : { "city" : "BOULDER"}},
          |    { "$out" : "tmp.gen_0"}
          |  ],
          |  { allowDiskUse: true })
          |db.zips2.aggregate([
          |    { "$match" : { "pop" : { "$lte" : 1000}}},
          |    { "$out" : "tmp.gen_1"}
          |  ],
          |  { allowDiskUse: true })
          |db.tmp.gen_1.mapReduce(
          |  function () {
          |    emit(this.city, this.pop);
          |  },
          |  function (key, values) {
          |    return Array.sum(values);
          |  },
          |  { "out" : { "reduce" : "tmp.gen_0"}})
          |db.tmp.gen_0.renameCollection("result", true)
          |db.tmp.gen_1.drop()
          |db.result.find()""".stripMargin)
    }
    
    "fail with simple read under foldLeft" in {
      val wf = FoldLeftTask(
        ReadTask(Collection("zips1")),
        NonEmptyList(MapReduceTask(
          PipelineTask(
            ReadTask(Collection("zips2")),
            Pipeline(List(
              Match(Selector.Doc(
                BsonField.Name("pop") -> Selector.Lte(Bson.Int64(1000))))))),
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
            Some(MapReduce.WithAction(MapReduce.Action.Reduce))))))

      MongoDbEvaluator.toJS(wf, Path("result")) must beAnyLeftDisj
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
