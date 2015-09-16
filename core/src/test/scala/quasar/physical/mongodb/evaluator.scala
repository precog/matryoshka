package quasar.physical.mongodb

import quasar.Predef._
import quasar._
import quasar.javascript._

import scala.collection.immutable.ListMap

import scalaz._

import org.specs2.mutable._
import org.specs2.scalaz._

class EvaluatorSpec extends Specification with DisjunctionMatchers {
  "evaluate" should {
    import Workflow._
    import fs.Path

    "write trivial workflow to JS" in {
      val wf = $read(Collection("db", "zips"))

      MongoDbEvaluator.toJS(crystallize(wf)) must beRightDisjunction(
        s"""db.zips.find();
           |""".stripMargin)
    }

    "write trivial workflow to JS with fancy collection name" in {
      val wf = $read(Collection("db", "tmp.123"))

      MongoDbEvaluator.toJS(crystallize(wf)) must beRightDisjunction(
        s"""db.getCollection(\"tmp.123\").find();
           |""".stripMargin)
    }

    "write workflow with simple pure value" in {
      val wf = $pure(Bson.Doc(ListMap("foo" -> Bson.Text("bar"))))

        MongoDbEvaluator.toJS(crystallize(wf)) must beRightDisjunction(
          """db.tmp.gen_0.insert({ "foo": "bar" });
            |db.tmp.gen_0.find();
            |""".stripMargin)
    }

    "write workflow with multiple pure values" in {
      val wf = $pure(Bson.Arr(List(
        Bson.Doc(ListMap("foo" -> Bson.Int64(1))),
        Bson.Doc(ListMap("bar" -> Bson.Int64(2))))))

        MongoDbEvaluator.toJS(crystallize(wf)) must beRightDisjunction(
          """db.tmp.gen_0.insert({ "foo": NumberLong(1) });
            |db.tmp.gen_0.insert({ "bar": NumberLong(2) });
            |db.tmp.gen_0.find();
            |""".stripMargin)
    }

    "fail with non-doc pure value" in {
      val wf = $pure(Bson.Text("foo"))

      MongoDbEvaluator.toJS(crystallize(wf)) must beLeftDisjunction
    }

    "fail with multiple pure values, one not a doc" in {
      val wf = $pure(Bson.Arr(List(
        Bson.Doc(ListMap("foo" -> Bson.Int64(1))),
        Bson.Int64(2))))

        MongoDbEvaluator.toJS(crystallize(wf)) must beLeftDisjunction
    }

    "write simple pipeline workflow to JS" in {
      val wf = chain(
        $read(Collection("db", "zips")),
        $match(Selector.Doc(
          BsonField.Name("pop") -> Selector.Gte(Bson.Int64(1000)))))

      MongoDbEvaluator.toJS(crystallize(wf)) must beRightDisjunction(
        """db.zips.aggregate(
          |  [
          |    { "$match": { "pop": { "$gte": NumberLong(1000) } } },
          |    { "$out": "tmp.gen_0" }],
          |  { "allowDiskUse": true });
          |db.tmp.gen_0.find();
          |""".stripMargin)
    }

    "write chained pipeline workflow to JS" in {
      val wf = chain(
        $read(Collection("db", "zips")),
        $match(Selector.Doc(
          BsonField.Name("pop") -> Selector.Lte(Bson.Int64(1000)))),
        $match(Selector.Doc(
          BsonField.Name("pop") -> Selector.Gte(Bson.Int64(100)))),
        $sort(NonEmptyList(BsonField.Name("city") -> Ascending)))

      MongoDbEvaluator.toJS(crystallize(wf)) must beRightDisjunction(
        """db.zips.aggregate(
          |  [
          |    {
          |      "$match": {
          |        "$and": [
          |          { "pop": { "$lte": NumberLong(1000) } },
          |          { "pop": { "$gte": NumberLong(100) } }]
          |      }
          |    },
          |    { "$sort": { "city": NumberInt(1) } },
          |    { "$out": "tmp.gen_0" }],
          |  { "allowDiskUse": true });
          |db.tmp.gen_0.find();
          |""".stripMargin)
    }

    "write map-reduce Workflow to JS" in {
      val wf = chain(
        $read(Collection("db", "zips")),
        $map($Map.mapKeyVal(("key", "value"),
          Js.Select(Js.Ident("value"), "city"),
          Js.Select(Js.Ident("value"), "pop")),
          ListMap()),
        $reduce(Js.AnonFunDecl(List("key", "values"), List(
          Js.Return(Js.Call(
            Js.Select(Js.Ident("Array"), "sum"),
            List(Js.Ident("values")))))),
          ListMap()))

      MongoDbEvaluator.toJS(crystallize(wf)) must beRightDisjunction(
        """db.zips.mapReduce(
          |  function () {
          |    emit.apply(
          |      null,
          |      (function (key, value) { return [value.city, value.pop] })(
          |        this._id,
          |        this))
          |  },
          |  function (key, values) { return Array.sum(values) },
          |  { "out": { "replace": "tmp.gen_0" } });
          |db.tmp.gen_0.find();
          |""".stripMargin)
    }

    "write $where condition to JS" in {
      val wf = chain(
        $read(Collection("db", "zips2")),
        $match(Selector.Where(Js.Ident("foo"))))

      MongoDbEvaluator.toJS(crystallize(wf)) must beRightDisjunction(
        """db.zips2.mapReduce(
          |  function () {
          |    emit.apply(
          |      null,
          |      (function (key, value) { return [key, value] })(this._id, this))
          |  },
          |  function (key, values) { return (values != null) ? values[0] : undefined },
          |  {
          |    "out": { "replace": "tmp.gen_0" },
          |    "query": { "$where": function () { return foo } }
          |  });
          |db.tmp.gen_0.find();
          |""".stripMargin)
    }

    "write join Workflow to JS" in {
      val wf =
        $foldLeft(
          chain(
            $read(Collection("db", "zips1")),
            $match(Selector.Doc(
              BsonField.Name("city") -> Selector.Eq(Bson.Text("BOULDER"))))),
          chain(
            $read(Collection("db", "zips2")),
            $match(Selector.Doc(
              BsonField.Name("pop") -> Selector.Lte(Bson.Int64(1000)))),
            $map($Map.mapKeyVal(("key", "value"),
              Js.Select(Js.Ident("value"), "city"),
              Js.Select(Js.Ident("value"), "pop")),
              ListMap()),
            $reduce(Js.AnonFunDecl(List("key", "values"), List(
              Js.Return(Js.Call(
                Js.Select(Js.Ident("Array"), "sum"),
                List(Js.Ident("values")))))),
              ListMap())))

      MongoDbEvaluator.toJS(crystallize(wf)) must beRightDisjunction(
        """db.zips1.aggregate(
          |  [
          |    { "$match": { "city": "BOULDER" } },
          |    { "$project": { "value": "$$ROOT" } },
          |    { "$out": "tmp.gen_0" }],
          |  { "allowDiskUse": true });
          |db.zips2.mapReduce(
          |  function () {
          |    emit.apply(
          |      null,
          |      (function (key, value) { return [value.city, value.pop] })(
          |        this._id,
          |        this))
          |  },
          |  function (key, values) { return Array.sum(values) },
          |  {
          |    "out": { "reduce": "tmp.gen_0", "nonAtomic": true },
          |    "query": { "pop": { "$lte": NumberLong(1000) } }
          |  });
          |db.tmp.gen_0.find();
          |""".stripMargin)
    }
  }

  "SimpleCollectionNamePattern" should {
    import JSExecutor._

    "match identifier" in {
      SimpleCollectionNamePattern.unapplySeq("foo") must beSome
    }

    "not match leading _" in {
      SimpleCollectionNamePattern.unapplySeq("_foo") must beNone
    }

    "match dot-separated identifiers" in {
      SimpleCollectionNamePattern.unapplySeq("foo.bar") must beSome
    }

    "match everything allowed" in {
      SimpleCollectionNamePattern.unapplySeq("foo2.BAR_BAZ") must beSome
    }

    "not match leading digit" in {
      SimpleCollectionNamePattern.unapplySeq("123") must beNone
    }

    "not match leading digit in second position" in {
      SimpleCollectionNamePattern.unapplySeq("foo.123") must beNone
    }
  }
}
