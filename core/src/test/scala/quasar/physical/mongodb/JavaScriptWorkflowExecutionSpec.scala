package quasar
package physical
package mongodb

import quasar.Predef._
import quasar.javascript._

import scala.collection.immutable.ListMap

import scalaz._

import org.specs2.mutable._
import org.specs2.scalaz._

class JavaScriptWorkflowExecutionSpec extends Specification with DisjunctionMatchers {
  import Workflow._

  val testColl = Collection("db", "jswf")

  def toJS(wf: Workflow): WorkflowExecutionError \/ String =
    WorkflowExecutor.toJS(crystallize(wf), testColl)

  "Executing 'Workflow' as JavaScript" should {

    "write trivial workflow to JS" in {
      val wf = $read(Collection("db", "zips"))

      toJS(wf) must beRightDisjunction("db.zips.find();\n")
    }

    "write trivial workflow to JS with fancy collection name" in {
      val wf = $read(Collection("db", "tmp.123"))

      toJS(wf) must beRightDisjunction("db.getCollection(\"tmp.123\").find();\n")
    }

    "write workflow with simple pure value" in {
      val wf = $pure(Bson.Doc(ListMap("foo" -> Bson.Text("bar"))))

      toJS(wf) must beRightDisjunction(
        """db.jswf.insert([{ "foo": "bar" }]);
          |db.jswf.find();
          |""".stripMargin)
    }

    "write workflow with multiple pure values" in {
      val wf = $pure(Bson.Arr(List(
        Bson.Doc(ListMap("foo" -> Bson.Int64(1))),
        Bson.Doc(ListMap("bar" -> Bson.Int64(2))))))

        toJS(wf) must beRightDisjunction(
          """db.jswf.insert([{ "foo": NumberLong(1) }, { "bar": NumberLong(2) }]);
            |db.jswf.find();
            |""".stripMargin)
    }

    "fail with non-doc pure value" in {
      val wf = $pure(Bson.Text("foo"))

      toJS(wf) must beLeftDisjunction
    }

    "fail with multiple pure values, one not a doc" in {
      val wf = $pure(Bson.Arr(List(
        Bson.Doc(ListMap("foo" -> Bson.Int64(1))),
        Bson.Int64(2))))

      toJS(wf) must beLeftDisjunction
    }

    "write simple pipeline workflow to JS" in {
      val wf = chain(
        $read(Collection("db", "zips")),
        $match(Selector.Doc(
          BsonField.Name("pop") -> Selector.Gte(Bson.Int64(1000)))))

      toJS(wf) must beRightDisjunction(
        """db.zips.aggregate(
          |  [{ "$match": { "pop": { "$gte": NumberLong(1000) } } }, { "$out": "jswf" }],
          |  { "allowDiskUse": true });
          |db.jswf.find();
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

      toJS(wf) must beRightDisjunction(
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
          |    { "$out": "jswf" }],
          |  { "allowDiskUse": true });
          |db.jswf.find();
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

      toJS(wf) must beRightDisjunction(
        """db.zips.mapReduce(
          |  function () {
          |    emit.apply(
          |      null,
          |      (function (key, value) { return [value.city, value.pop] })(
          |        this._id,
          |        this))
          |  },
          |  function (key, values) { return Array.sum(values) },
          |  { "out": { "replace": "jswf" } });
          |db.jswf.find();
          |""".stripMargin)
    }

    "write $where condition to JS" in {
      val wf = chain(
        $read(Collection("db", "zips2")),
        $match(Selector.Where(Js.Ident("foo"))))

      toJS(wf) must beRightDisjunction(
        """db.zips2.mapReduce(
          |  function () {
          |    emit.apply(
          |      null,
          |      (function (key, value) { return [key, value] })(this._id, this))
          |  },
          |  function (key, values) { return values[0] },
          |  {
          |    "out": { "replace": "jswf" },
          |    "query": { "$where": function () { return foo } }
          |  });
          |db.jswf.find();
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

      toJS(wf) must beRightDisjunction(
        """db.zips1.aggregate(
          |  [
          |    { "$match": { "city": "BOULDER" } },
          |    { "$project": { "value": "$$ROOT" } },
          |    { "$out": "jswf" }],
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
          |    "out": { "reduce": "jswf", "nonAtomic": true },
          |    "query": { "pop": { "$lte": NumberLong(1000) } }
          |  });
          |db.jswf.find();
          |""".stripMargin)
    }
  }

  "SimpleCollectionNamePattern" should {
    import JavaScriptWorkflowExecutor.SimpleCollectionNamePattern

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
