package slamdata.engine.physical.mongodb

import slamdata.engine._

import scala.collection.immutable.ListMap

import scalaz._
import Scalaz._

import org.specs2.mutable._

class EvaluatorSpec extends Specification with DisjunctionMatchers {
  "evaluate" should {
    import WorkflowOp._
    import PipelineOp._
    import fs.Path

    "write trivial workflow to JS" in {
      val wf = readOp(Collection("zips"))

      MongoDbEvaluator.toJS(wf, Path("result")) must beRightDisj(
        "db.zips.find()")
    }

    "write trivial workflow to JS with fancy collection name" in {
      val wf = readOp(Collection("tmp.123"))

      MongoDbEvaluator.toJS(wf, Path("result")) must beRightDisj(
        "db.getCollection(\"tmp.123\").find()")
    }

    "write workflow with simple pure value" in {
      val wf = pureOp(Bson.Doc(ListMap("foo" -> Bson.Text("bar"))))

        MongoDbEvaluator.toJS(wf, Path("result")) must beRightDisj(
          """db.tmp.gen_0.insert({ "foo" : "bar"})
            |db.tmp.gen_0.renameCollection("result", true)
            |db.result.find()""".stripMargin)
    }

    "write workflow with multiple pure values" in {
      val wf = pureOp(Bson.Arr(List(
        Bson.Doc(ListMap("foo" -> Bson.Int64(1))),
        Bson.Doc(ListMap("bar" -> Bson.Int64(2))))))

        MongoDbEvaluator.toJS(wf, Path("result")) must beRightDisj(
          """db.tmp.gen_0.insert({ "foo" : 1})
            |db.tmp.gen_0.insert({ "bar" : 2})
            |db.tmp.gen_0.renameCollection("result", true)
            |db.result.find()""".stripMargin)
    }

    "fail with non-doc pure value" in {
      val wf = pureOp(Bson.Text("foo"))

      MongoDbEvaluator.toJS(wf, Path("result")) must beAnyLeftDisj
    }

    "fail with multiple pure values, one not a doc" in {
      val wf = pureOp(Bson.Arr(List(
        Bson.Doc(ListMap("foo" -> Bson.Int64(1))),
        Bson.Int64(2))))

        MongoDbEvaluator.toJS(wf, Path("result")) must beAnyLeftDisj
    }

    "write simple pipeline workflow to JS" in {
      val wf = chain(
        readOp(Collection("zips")),
        matchOp(Selector.Doc(
          BsonField.Name("pop") -> Selector.Gte(Bson.Int64(1000)))))
      
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
      val wf = chain(
        readOp(Collection("zips")),
        matchOp(Selector.Doc(
          BsonField.Name("pop") -> Selector.Lte(Bson.Int64(1000)))),
        matchOp(Selector.Doc(
          BsonField.Name("pop") -> Selector.Gte(Bson.Int64(100)))),
        sortOp(NonEmptyList(BsonField.Name("city") -> Ascending)))
      
      MongoDbEvaluator.toJS(wf, Path("result")) must beRightDisj(
        """db.zips.aggregate([
          |    { "$match" : { "$and" : [ { "pop" : { "$lte" : 1000}} , { "pop" : { "$gte" : 100}}]}},
          |    { "$sort" : { "city" : 1}},
          |    { "$out" : "tmp.gen_0"}
          |  ],
          |  { allowDiskUse: true })
          |db.tmp.gen_0.renameCollection("result", true)
          |db.result.find()""".stripMargin)
    }
    
    "write map-reduce Workflow to JS" in {
      val wf = chain(
        readOp(Collection("zips")),
        mapOp(MapOp.mapKeyVal(("key", "value"),
          Js.Select(Js.Ident("value"), "city"),
          Js.Select(Js.Ident("value"), "pop"))),
        reduceOp(Js.AnonFunDecl(List("key", "values"), List(
            Js.Return(Js.Call(
              Js.Select(Js.Ident("Array"), "sum"),
              List(Js.Ident("values"))))))))

      MongoDbEvaluator.toJS(wf, Path("result")) must beRightDisj(
        """db.zips.mapReduce(
          |  function () {
          |    emit.apply(null, (function (key, value) {
          |        return [value.city, value.pop];
          |      })(this._id, this));
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
        foldLeftOp(
          chain(
            readOp(Collection("zips1")),
            matchOp(Selector.Doc(
              BsonField.Name("city") -> Selector.Eq(Bson.Text("BOULDER"))))),
          chain(
            readOp(Collection("zips2")),
            matchOp(Selector.Doc(
              BsonField.Name("pop") -> Selector.Lte(Bson.Int64(1000)))),
            mapOp(MapOp.mapKeyVal(("key", "value"),
              Js.Select(Js.Ident("value"), "city"),
              Js.Select(Js.Ident("value"), "pop"))),
            reduceOp(Js.AnonFunDecl(List("key", "values"), List(
              Js.Return(Js.Call(
                Js.Select(Js.Ident("Array"), "sum"),
                List(Js.Ident("values")))))))))

      MongoDbEvaluator.toJS(wf, Path("result")) must beRightDisj(
        """db.zips1.aggregate([
          |    { "$match" : { "city" : "BOULDER"}},
          |    { "$project" : { "value" : "$$ROOT"}},
          |    { "$out" : "tmp.gen_0"}
          |  ],
          |  { allowDiskUse: true })
          |db.zips2.mapReduce(
          |  function () {
          |    emit.apply(null, (function (key, value) {
          |        return [value.city, value.pop];
          |      })(this._id, this));
          |  },
          |  function (key, values) {
          |    return Array.sum(values);
          |  },
          |  { "out" : { "reduce" : "tmp.gen_0"} , "query" : { "pop" : { "$lte" : 1000}}})
          |db.tmp.gen_0.renameCollection("result", true)
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
