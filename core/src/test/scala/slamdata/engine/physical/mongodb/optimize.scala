package slamdata.engine.physical.mongodb

import org.specs2.mutable._

import scala.collection.immutable.ListMap

import scalaz._

import slamdata.engine.{TreeMatchers}

class OptimizeSpecs extends Specification with TreeMatchers {
  import ExprOp._; import DSL._
  import AccumOp._
  import Workflow._
  import IdHandling._

  import optimize.pipeline._

  "reorder" should {
    "push $skip before $project" in {
      val op = chain(
       $read(Collection("db", "zips")),
       $project(
         Reshape(ListMap(
           BsonField.Name("0") -> -\/($toLower($var(DocField(BsonField.Name("city"))))))),
         IgnoreId),
       $skip(5))
     val exp = chain(
      $read(Collection("db", "zips")),
      $skip(5),
      $project(
        Reshape(ListMap(
          BsonField.Name("0") -> -\/($toLower($var(DocField(BsonField.Name("city"))))))),
        IgnoreId))

      reorderOps(op) must beTree(exp)
    }

    "push $skip before $simpleMap" in {
      import slamdata.engine.javascript._
      import JsCore._

      val op = chain(
       $read(Collection("db", "zips")),
       $simpleMap(
         NonEmptyList(MapExpr(JsFn(Ident("x"), Obj(ListMap(
           "0" -> Select(Ident("x").fix, "length").fix)).fix))),
         ListMap()),
       $skip(5))
     val exp = chain(
      $read(Collection("db", "zips")),
      $skip(5),
      $simpleMap(
        NonEmptyList(MapExpr(JsFn(Ident("x"), Obj(ListMap(
          "0" -> Select(Ident("x").fix, "length").fix)).fix))),
        ListMap()))

      reorderOps(op) must beTree(exp)
    }

    "not push $skip before flattening $simpleMap" in {
      import slamdata.engine.javascript._
      import JsCore._

      val op = chain(
       $read(Collection("db", "zips")),
       $simpleMap(
         NonEmptyList(
           MapExpr(JsFn(Ident("x"), Obj(ListMap(
             "0" -> Select(Ident("x").fix, "length").fix)).fix)),
           FlatExpr(JsFn(Ident("x"), Select(Ident("x").fix, "loc").fix))),
         ListMap()),
       $skip(5))

      reorderOps(op) must beTree(op)
    }

    "push $limit before $project" in {
      val op = chain(
       $read(Collection("db", "zips")),
       $project(
         Reshape(ListMap(
           BsonField.Name("0") -> -\/($toLower($var(DocField(BsonField.Name("city"))))))),
         IgnoreId),
       $limit(10))
     val exp = chain(
      $read(Collection("db", "zips")),
      $limit(10),
      $project(
        Reshape(ListMap(
          BsonField.Name("0") -> -\/($toLower($var(DocField(BsonField.Name("city"))))))),
        IgnoreId))

      reorderOps(op) must beTree(exp)
    }

    "push $limit before $simpleMap" in {
      import slamdata.engine.javascript._
      import JsCore._

      val op = chain(
       $read(Collection("db", "zips")),
       $simpleMap(
         NonEmptyList(MapExpr(JsFn(Ident("x"), Obj(ListMap(
           "0" -> Select(Ident("x").fix, "length").fix)).fix))),
         ListMap()),
       $limit(10))
     val exp = chain(
      $read(Collection("db", "zips")),
      $limit(10),
      $simpleMap(
        NonEmptyList(MapExpr(JsFn(Ident("x"), Obj(ListMap(
          "0" -> Select(Ident("x").fix, "length").fix)).fix))),
        ListMap()))

      reorderOps(op) must beTree(exp)
    }

    "not push $limit before flattening $simpleMap" in {
      import slamdata.engine.javascript._
      import JsCore._

      val op = chain(
       $read(Collection("db", "zips")),
       $simpleMap(
         NonEmptyList(
           MapExpr(JsFn(Ident("x"), Obj(ListMap(
             "0" -> Select(Ident("x").fix, "length").fix)).fix)),
           FlatExpr(JsFn(Ident("x"), Select(Ident("x").fix, "loc").fix))),
         ListMap()),
       $limit(10))

      reorderOps(op) must beTree(op)
    }

    "push $match before $project" in {
      val op = chain(
       $read(Collection("db", "zips")),
       $project(
         Reshape(ListMap(
           BsonField.Name("city") -> -\/($var(DocField(BsonField.Name("__tmp0") \ BsonField.Name("city")))))),
         IgnoreId),
       $match(Selector.Doc(
         BsonField.Name("city") -> Selector.Eq(Bson.Text("BOULDER")))))
     val exp = chain(
      $read(Collection("db", "zips")),
      $match(Selector.Doc(
        (BsonField.Name("__tmp0") \ BsonField.Name("city")) -> Selector.Eq(Bson.Text("BOULDER")))),
      $project(
        Reshape(ListMap(
          BsonField.Name("city") -> -\/($var(DocField(BsonField.Name("__tmp0") \ BsonField.Name("city")))))),
        IgnoreId))

      reorderOps(op) must beTree(exp)
    }

    "not push $match before $project with dependency" in {
      val op = chain(
       $read(Collection("db", "zips")),
       $project(
         Reshape(ListMap(
           BsonField.Name("city") -> -\/($var(DocField(BsonField.Name("city")))),
           BsonField.Name("__tmp0") -> -\/($toLower($var(DocField(BsonField.Name("city"))))))),
         IgnoreId),
       $match(Selector.Doc(
         BsonField.Name("__tmp0") -> Selector.Eq(Bson.Text("boulder")))))

      reorderOps(op) must beTree(op)
    }

    "push $match before $simpleMap" in {
      import slamdata.engine.javascript._
      import JsCore._

      val op = chain(
       $read(Collection("db", "zips")),
       $simpleMap(
         NonEmptyList(MapExpr(JsFn(Ident("x"), Obj(ListMap(
           "__tmp0" -> Ident("x").fix,
           "city" -> Select(Ident("x").fix, "city").fix)).fix))),
         ListMap()),
       $match(Selector.Doc(
         BsonField.Name("city") -> Selector.Eq(Bson.Text("BOULDER")))))
     val exp = chain(
      $read(Collection("db", "zips")),
      $match(Selector.Doc(
        (BsonField.Name("city")) -> Selector.Eq(Bson.Text("BOULDER")))),
      $simpleMap(
        NonEmptyList(MapExpr(JsFn(Ident("x"), Obj(ListMap(
          "__tmp0" -> Ident("x").fix,
          "city" -> Select(Ident("x").fix, "city").fix)).fix))),
        ListMap()))

      reorderOps(op) must beTree(exp)
    }

    "push $match before splicing $simpleMap" in {
      import slamdata.engine.javascript._
      import JsCore._

      val op = chain(
       $read(Collection("db", "zips")),
       $simpleMap(
         NonEmptyList(
           MapExpr(JsFn(Ident("x"),
             SpliceObjects(List(
               Ident("x").fix,
               Obj(ListMap(
                 "city" -> Select(Ident("x").fix, "city").fix)).fix)).fix))),
         ListMap()),
       $match(Selector.Doc(
         BsonField.Name("city") -> Selector.Eq(Bson.Text("BOULDER")))))
     val exp = chain(
      $read(Collection("db", "zips")),
      $match(Selector.Doc(
        (BsonField.Name("city")) -> Selector.Eq(Bson.Text("BOULDER")))),
      $simpleMap(
        NonEmptyList(MapExpr(JsFn(Ident("x"),
          SpliceObjects(List(
            Ident("x").fix,
            Obj(ListMap(
              "city" -> Select(Ident("x").fix, "city").fix)).fix)).fix))),
        ListMap()))

      reorderOps(op) must beTree(exp)
    }

    "not push $match before $simpleMap with dependency" in {
      import slamdata.engine.javascript._
      import JsCore._

      val op = chain(
       $read(Collection("db", "zips")),
       $simpleMap(
         NonEmptyList(MapExpr(JsFn(Ident("x"), Obj(ListMap(
           "__tmp0" -> Ident("x").fix,
           "city" -> Select(Ident("x").fix, "city").fix,
           "__sd_tmp_0" -> Select(Select(Ident("x").fix, "city").fix, "length").fix)).fix))),
         ListMap()),
       $match(Selector.Doc(
         BsonField.Name("__sd_tmp_0") -> Selector.Lt(Bson.Int32(1000)))))

      reorderOps(op) must beTree(op)
    }

    "not push $match before flattening $simpleMap" in {
      import slamdata.engine.javascript._
      import JsCore._

      val op = chain(
       $read(Collection("db", "zips")),
       $simpleMap(
         NonEmptyList(
           MapExpr(JsFn(Ident("x"), Obj(ListMap(
             "city" -> Select(Select(Ident("x").fix, "__tmp0").fix, "city").fix)).fix)),
           FlatExpr(JsFn(Ident("x"), Select(Ident("x").fix, "loc").fix))),
         ListMap()),
       $match(Selector.Doc(
         BsonField.Name("city") -> Selector.Eq(Bson.Text("BOULDER")))))

      reorderOps(op) must beTree(op)
    }

    "not push $sort up" in {
      val op = chain(
       $read(Collection("db", "zips")),
       $project(
         Reshape(ListMap(
           BsonField.Name("city") -> -\/($var(DocField(BsonField.Name("__tmp0") \ BsonField.Name("city")))))),
         IgnoreId),
       $sort(NonEmptyList(BsonField.Name("city") -> Ascending)))

       reorderOps(op) must beTree(op)
    }
  }

  "inline" should {
    "inline simple project on group" in {
      val inlined = inlineProjectGroup(
        Reshape(ListMap(
          BsonField.Name("foo") -> -\/ ($var(DocField(BsonField.Name("value")))))),
        Grouped(ListMap(BsonField.Name("value") -> $sum($literal(Bson.Int32(1))))))

      inlined must beSome(Grouped(ListMap(BsonField.Name("foo") -> $sum($literal(Bson.Int32(1))))))
    }

    "inline multiple projects on group, dropping extras" in {
        val inlined = inlineProjectGroup(
          Reshape(ListMap(
            BsonField.Name("foo") -> -\/($var(DocField(BsonField.Name("__sd_tmp_1")))),
            BsonField.Name("bar") -> -\/($var(DocField(BsonField.Name("__sd_tmp_2")))))),
          Grouped(ListMap(
            BsonField.Name("__sd_tmp_1") -> $sum($literal(Bson.Int32(1))),
            BsonField.Name("__sd_tmp_2") -> $sum($literal(Bson.Int32(2))),
            BsonField.Name("__sd_tmp_3") -> $sum($literal(Bson.Int32(3))))))

          inlined must beSome(Grouped(ListMap(
            BsonField.Name("foo") -> $sum($literal(Bson.Int32(1))),
            BsonField.Name("bar") -> $sum($literal(Bson.Int32(2))))))
    }

    "inline project on group with nesting" in {
        val inlined = inlineProjectGroup(
          Reshape(ListMap(
            BsonField.Name("bar") -> -\/($var(DocField(BsonField.Name("value") \ BsonField.Name("bar")))),
            BsonField.Name("baz") -> -\/($var(DocField(BsonField.Name("value") \ BsonField.Name("baz")))))),
          Grouped(ListMap(
            BsonField.Name("value") -> $push($var(DocField(BsonField.Name("foo")))))))

          inlined must beNone
    }
  }
}
