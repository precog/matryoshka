package slamdata.engine.physical.mongodb

import org.specs2.mutable._

import scala.collection.immutable.ListMap
import scalaz._, Scalaz._

import slamdata.engine.{RenderTree, Terminal, NonTerminal, TreeMatchers}
import slamdata.engine.fp._
import slamdata.engine.analysis.fixplate._
import slamdata.engine.javascript._

class WorkflowSpec extends Specification with TreeMatchers {
  import Reshape.{merge => _, _}
  import Workflow._
  import IdHandling._

  val readFoo = $read(Collection("foo"))

  "smart constructors" should {
    "put match before sort" in {
      val given = chain(
        readFoo,
        $sort(NonEmptyList(BsonField.Name("city") -> Descending)),
        $match(Selector.Doc(
          BsonField.Name("pop") -> Selector.Gte(Bson.Int64(1000)))))
      val expected = chain(
        readFoo,
        $match(Selector.Doc(
          BsonField.Name("pop") -> Selector.Gte(Bson.Int64(1000)))),
        $sort(NonEmptyList(BsonField.Name("city") -> Descending)))

      given must beTree(expected)
    }

    "choose smallest limit" in {
      val expected = chain(readFoo, $limit(5))
      chain(readFoo, $limit(10), $limit(5)) must_== expected
      chain(readFoo, $limit(5), $limit(10)) must_== expected
    }

    "sum skips" in {
      chain(readFoo, $skip(10), $skip(5)) must beTree(chain(readFoo, $skip(15)))
    }

    "flatten foldLefts when possible" in {
      val given = $foldLeft(
        $foldLeft(
          readFoo,
          $read(Collection("zips"))),
        $read(Collection("olympics")))
      val expected = $foldLeft(
        readFoo,
        $read(Collection("zips")),
        $read(Collection("olympics")))

      given must beTree(expected)
    }

    "flatten project into group/unwind" in {
      val given = chain(
        readFoo,
        $group(
          Grouped(ListMap(
            BsonField.Name("value") -> ExprOp.Push(ExprOp.DocField(BsonField.Name("rIght"))))),
            -\/ (ExprOp.DocField(BsonField.Name("lEft")))),
        $unwind(ExprOp.DocField(BsonField.Name("value"))),
        $project(Reshape(ListMap(
          BsonField.Name("city") -> -\/ (ExprOp.DocField(BsonField.Name("value") \ BsonField.Name("city"))))),
          IncludeId))

      val expected = chain(
        readFoo,
        $group(
          Grouped(ListMap(
            BsonField.Name("city") -> ExprOp.Push(ExprOp.DocField(BsonField.Name("rIght") \ BsonField.Name("city"))))),
            -\/ (ExprOp.DocField(BsonField.Name("lEft")))),
        $unwind(ExprOp.DocField(BsonField.Name("city"))))

      given must beTree(expected: Workflow)
    }.pendingUntilFixed("#536")

    "not flatten project into group/unwind with _id excluded" in {
      val given = chain(
        readFoo,
        $group(
          Grouped(ListMap(
            BsonField.Name("value") -> ExprOp.Push(ExprOp.DocField(BsonField.Name("rIght"))))),
            -\/ (ExprOp.DocField(BsonField.Name("lEft")))),
        $unwind(ExprOp.DocField(BsonField.Name("value"))),
        $project(Reshape(ListMap(
          BsonField.Name("city") -> -\/ (ExprOp.DocField(BsonField.Name("value") \ BsonField.Name("city"))))),
          ExcludeId))

      given must beTree(given: Workflow)
    }

    "resolve `Include`" in {
      chain($read(Collection("zips")),
        $project(Reshape(ListMap(
          BsonField.Name("bar") -> -\/(ExprOp.Include))),
          ExcludeId),
        $project(Reshape(ListMap(
          BsonField.Name("_id") ->
            -\/(ExprOp.DocField(BsonField.Name("bar"))))),
          IncludeId)) must_==
      chain($read(Collection("zips")),
        $project(Reshape(ListMap(
          BsonField.Name("_id") ->
            -\/(ExprOp.DocField(BsonField.Name("bar"))))),
          IncludeId))
    }

    "traverse `Include`" in {
      chain($read(Collection("zips")),
        $project(Reshape(ListMap(
          BsonField.Name("bar") ->
            -\/(ExprOp.Divide(
              ExprOp.DocField(BsonField.Name("baz")),
              ExprOp.Literal(Bson.Int32(92)))))),
          IncludeId),
        $project(Reshape(ListMap(
          BsonField.Name("bar") -> -\/(ExprOp.Include))),
          IncludeId)) must_==
      chain($read(Collection("zips")),
        $project(Reshape(ListMap(
          BsonField.Name("bar") -> -\/(ExprOp.Divide(
            ExprOp.DocField(BsonField.Name("baz")),
            ExprOp.Literal(Bson.Int32(92)))))),
          IncludeId))
    }

    "resolve implied `_id`" in {
      chain($read(Collection("zips")),
        $project(Reshape(ListMap(
          BsonField.Name("bar") -> -\/(ExprOp.DocField(BsonField.Name("bar"))),
          BsonField.Name("_id") ->
            -\/(ExprOp.DocField(BsonField.Name("baz"))))),
          IncludeId),
        $project(Reshape(ListMap(
          BsonField.Name("bar") ->
            -\/(ExprOp.DocField(BsonField.Name("bar"))))),
          IncludeId)) must_==
      chain($read(Collection("zips")),
        $project(Reshape(ListMap(
          BsonField.Name("bar") -> -\/(ExprOp.DocField(BsonField.Name("bar"))),
          BsonField.Name("_id") ->
            -\/(ExprOp.DocField(BsonField.Name("baz"))))),
          IncludeId))
    }

    "not resolve excluded `_id`" in {
      chain($read(Collection("zips")),
        $project(Reshape(ListMap(
          BsonField.Name("bar") -> -\/(ExprOp.DocField(BsonField.Name("bar"))),
          BsonField.Name("_id") ->
            -\/(ExprOp.DocField(BsonField.Name("baz"))))),
          IncludeId),
        $project(Reshape(ListMap(
          BsonField.Name("bar") ->
            -\/(ExprOp.DocField(BsonField.Name("bar"))))),
          ExcludeId)) must_==
      chain($read(Collection("zips")),
        $project(Reshape(ListMap(
          BsonField.Name("bar") ->
            -\/(ExprOp.DocField(BsonField.Name("bar"))))),
          ExcludeId))
    }
  }

  "merge" should {
    "coalesce pure ops" in {
      val left = $pure(Bson.Int32(3))
      val right = $pure(Bson.Int64(-3))

      val ((lb, rb), op) = merge(left, right).evalZero

      lb must_== ExprOp.DocField(BsonField.Name("__tmp0"))
      rb must_== ExprOp.DocField(BsonField.Name("__tmp1"))
      op must beTree(
        $pure(Bson.Doc(ListMap(
          "__tmp0"  -> Bson.Int32(3),
          "__tmp1" -> Bson.Int64(-3)))))
    }

    "unify trivial reads" in {
      val ((lb, rb), op) = merge(readFoo, readFoo).evalZero

      lb must_== ExprOp.DocVar.ROOT()
      rb must_== ExprOp.DocVar.ROOT()
      op must_== readFoo
    }

    "fold different reads" in {
      val left = readFoo
      val right = $read(Collection("zips"))

      val ((lb, rb), op) = merge(left, right).evalZero

      lb must_== ExprOp.DocField(BsonField.Name("__tmp0"))
      rb must_== ExprOp.DocField(BsonField.Name("__tmp1"))
      op must beTree(
        $foldLeft(
          chain(
            readFoo,
            $project(Reshape(ListMap(
              BsonField.Name("__tmp0") -> -\/(ExprOp.DocVar.ROOT()))),
              IncludeId)),
          chain(
            $read(Collection("zips")),
            $project(Reshape(ListMap(
              BsonField.Name("__tmp1") -> -\/(ExprOp.DocVar.ROOT()))),
              IncludeId))))
    }

    "put shape-preserving before non-" in {
      val left = chain(
        readFoo,
        $project(Reshape(ListMap(
          BsonField.Name("city") ->
            -\/(ExprOp.DocField(BsonField.Name("city"))))),
          IncludeId))
      val right = chain(
        readFoo,
        $match(Selector.Doc(
          BsonField.Name("bar") -> Selector.Gt(Bson.Int64(10)))))

      val ((lb, rb), op) = merge(left, right).evalZero

      lb must_== ExprOp.DocVar.ROOT()
      rb must_== ExprOp.DocVar.ROOT()
      op must beTree(
        chain(
          readFoo,
          $match(Selector.Doc(
            BsonField.Name("bar") -> Selector.Gt(Bson.Int64(10)))),
          $project(Reshape(ListMap(
            BsonField.Name("city") ->
              -\/(ExprOp.DocField(BsonField.Name("city"))))),
            IncludeId)))
    }

    "coalesce unwinds on same field" in {
      val left = chain(
        readFoo,
        $unwind(ExprOp.DocField(BsonField.Name("city"))))
      val right = chain(
        readFoo,
        $unwind(ExprOp.DocField(BsonField.Name("city"))))

      val ((lb, rb), op) = merge(left, right).evalZero

      lb must_== ExprOp.DocVar.ROOT()
      rb must_== ExprOp.DocVar.ROOT()
      op must beTree(
        chain(readFoo, $unwind(ExprOp.DocField(BsonField.Name("city")))))
    }

    "maintain unwinds on separate fields" in {
      val left = chain(
        readFoo,
        $unwind(ExprOp.DocField(BsonField.Name("city"))))
      val right = chain(
        readFoo,
        $unwind(ExprOp.DocField(BsonField.Name("loc"))))

      val ((lb, rb), op) = merge(left, right).evalZero

      lb must_== ExprOp.DocVar.ROOT()
      rb must_== ExprOp.DocVar.ROOT()
      op must beTree(
        chain(
          readFoo,
          $unwind(ExprOp.DocField(BsonField.Name("city"))),
          $unwind(ExprOp.DocField(BsonField.Name("loc")))))
    }

    "don’t coalesce unwinds on same _named_ field with different values" in {
      val left = chain(
        readFoo,
        $unwind(ExprOp.DocField(BsonField.Name("city"))))
      val right = chain(
        readFoo,
        $project(Reshape(ListMap(
          BsonField.Name("city") ->
            -\/(ExprOp.DocField(BsonField.Name("_id"))),
          BsonField.Name("loc") ->
            -\/(ExprOp.DocField(BsonField.Name("loc"))))),
          IncludeId),
        $unwind(ExprOp.DocField(BsonField.Name("city"))))

      val ((lb, rb), op) = merge(left, right).evalZero

      lb must_== ExprOp.DocField(BsonField.Name("__tmp1"))
      rb must_== ExprOp.DocField(BsonField.Name("__tmp0"))
      op must beTree(
        chain(
          readFoo,
          $project(Reshape(ListMap(
            BsonField.Name("__tmp0") -> \/-(Reshape(ListMap(
              BsonField.Name("city") ->
                -\/(ExprOp.DocField(BsonField.Name("_id"))),
              BsonField.Name("loc") ->
                -\/(ExprOp.DocField(BsonField.Name("loc")))))),
            BsonField.Name("__tmp1") -> -\/(ExprOp.DocVar.ROOT()))),
            IncludeId),
          $unwind(ExprOp.DocField(BsonField.Name("__tmp1") \ BsonField.Name("city"))),
          $unwind(ExprOp.DocField(BsonField.Name("__tmp0") \ BsonField.Name("city")))))
    }

    "coalesce non-conflicting projections" in {
      val left = chain(
        readFoo,
        $project(Reshape(ListMap(
          BsonField.Name("city") ->
            -\/(ExprOp.DocField(BsonField.Name("city"))))),
          IncludeId),
        $unwind(ExprOp.DocField(BsonField.Name("city"))))
      val right = chain(
        readFoo,
        $project(Reshape(ListMap(
          BsonField.Name("city") ->
            -\/(ExprOp.DocField(BsonField.Name("city"))),
          BsonField.Name("loc") ->
            -\/(ExprOp.DocField(BsonField.Name("loc"))))),
          IncludeId),
        $unwind(ExprOp.DocField(BsonField.Name("city"))))
      val ((lb, rb), op) = merge(left, right).evalZero

      lb must_== ExprOp.DocVar.ROOT()
      rb must_== ExprOp.DocVar.ROOT()
      op must beTree(
        chain(
          readFoo,
          $project(Reshape(ListMap(
            BsonField.Name("city") ->
              -\/(ExprOp.DocField(BsonField.Name("city"))),
            BsonField.Name("loc") ->
              -\/(ExprOp.DocField(BsonField.Name("loc"))))),
            IncludeId),
          $unwind(ExprOp.DocField(BsonField.Name("city")))))
    }

    "merge groups" in {
      val left = chain(readFoo,
                  $group(
                    Grouped(ListMap(
                      BsonField.Name("value") -> ExprOp.Sum(ExprOp.Literal(Bson.Int32(1))))),
                    -\/ (ExprOp.Literal(Bson.Int32(1)))))
      val right = chain(readFoo,
                  $group(
                    Grouped(ListMap(
                      BsonField.Name("value") -> ExprOp.Sum(ExprOp.DocField(BsonField.Name("bar"))))),
                    -\/ (ExprOp.Literal(Bson.Int32(1)))))

      val ((lb, rb), op) = merge(left, right).evalZero

      lb must_== ExprOp.DocField(BsonField.Name("__tmp0"))
      rb must_== ExprOp.DocField(BsonField.Name("__tmp1"))
      op must beTree(
          chain(readFoo,
            $group(
              Grouped(ListMap(
                 BsonField.Name("__sd_tmp_1") -> ExprOp.Sum(ExprOp.Literal(Bson.Int32(1))),
                 BsonField.Name("__sd_tmp_2") -> ExprOp.Sum(ExprOp.DocField(BsonField.Name("bar"))))),
              -\/ (ExprOp.Literal(Bson.Int32(1)))),
            $project(Reshape(ListMap(
              BsonField.Name("__tmp0") -> \/- (Reshape(ListMap(
                BsonField.Name("value") -> -\/ (ExprOp.DocField(BsonField.Name("__sd_tmp_1")))))),
              BsonField.Name("__tmp1") -> \/- (Reshape(ListMap(
                BsonField.Name("value") -> -\/ (ExprOp.DocField(BsonField.Name("__sd_tmp_2")))))))),
              IgnoreId)))
    }

    "merge groups under unwind" in {
      val left = chain(readFoo,
        $group(
          Grouped(ListMap(
            BsonField.Name("city") -> ExprOp.Push(ExprOp.DocField(BsonField.Name("city"))))),
          -\/(ExprOp.Literal(Bson.Int32(1)))),
        $unwind(ExprOp.DocField(BsonField.Name("city"))))
      val right = chain(readFoo,
        $group(
          Grouped(ListMap(
            BsonField.Name("total") -> ExprOp.Sum(ExprOp.Literal(Bson.Int32(1))))),
          -\/(ExprOp.Literal(Bson.Int32(1)))))

      val ((lb, rb), op) = merge(left, right).evalZero

      lb must_== ExprOp.DocField(BsonField.Name("__tmp3"))
      rb must_== ExprOp.DocField(BsonField.Name("__tmp2"))
      op must beTree(
          chain(readFoo,
            $group(
              Grouped(ListMap(
                 BsonField.Name("total") -> ExprOp.Sum(ExprOp.Literal(Bson.Int32(1))),
                 BsonField.Name("city") -> ExprOp.Push(ExprOp.DocField(BsonField.Name("city"))))),
              -\/ (ExprOp.Literal(Bson.Int32(1)))),
            $project(Reshape(ListMap(
              BsonField.Name("__tmp2") -> -\/(ExprOp.DocVar.ROOT()),
              BsonField.Name("__tmp3") -> -\/(ExprOp.DocVar.ROOT()))),
              IgnoreId),
            $unwind(ExprOp.DocField(BsonField.Name("__tmp3") \ BsonField.Name("city")))))
    }

    "merge unwind and project on same group" in {
      val left = chain(readFoo,
        $group(
          Grouped(ListMap(
            BsonField.Name("sumA") -> ExprOp.Sum(ExprOp.DocField(BsonField.Name("a"))))),
          -\/(ExprOp.DocField(BsonField.Name("key")))),
        $project(Reshape(ListMap(
          BsonField.Name("0") ->
            -\/(ExprOp.Subtract(
              ExprOp.DocField(BsonField.Name("sumA")),
              ExprOp.Literal(Bson.Int64(1)))))),
          IncludeId))

      val right = chain(readFoo,
        $group(
          Grouped(ListMap(
            BsonField.Name("b") -> ExprOp.Push(ExprOp.DocField(BsonField.Name("b"))))),
          -\/ (ExprOp.DocField(BsonField.Name("key")))),
        $unwind(ExprOp.DocField(BsonField.Name("b"))))


      val ((lb, rb), op) = merge(left, right).evalZero

      op must beTree(chain(
        readFoo,
        $group(
          Grouped(ListMap(
            BsonField.Name("sumA") -> ExprOp.Sum(ExprOp.DocField(BsonField.Name("a"))),
            BsonField.Name("b") -> ExprOp.Push(ExprOp.DocField(BsonField.Name("b"))))),
          -\/ (ExprOp.DocField(BsonField.Name("key")))),
        $project(Reshape(ListMap(
          BsonField.Name("__tmp4") -> -\/(ExprOp.DocVar.ROOT()),
          BsonField.Name("__tmp5") -> -\/(ExprOp.DocVar.ROOT()))),
          IgnoreId),
        $unwind(ExprOp.DocField(BsonField.Name("__tmp5") \ BsonField.Name("b"))),
        $project(Reshape(ListMap(
          BsonField.Name("__tmp0") -> \/-(Reshape(ListMap(
            BsonField.Name("0") -> -\/(ExprOp.Subtract(
              ExprOp.DocField(BsonField.Name("__tmp4") \ BsonField.Name("sumA")),
              ExprOp.Literal(Bson.Int64(1))))))),
          BsonField.Name("__tmp1") -> -\/(ExprOp.DocVar.ROOT()))),
          IncludeId)))
    }

    "merge simpleMaps on same src" in {
      import JsCore._

      val left = chain(readFoo,
        $simpleMap(JsMacro(value => Select(value, "a").fix), Nil))
      val right = chain(readFoo,
        $simpleMap(JsMacro(value => Select(value, "b").fix), Nil))

      val ((lb, rb), op) = merge(left, right).evalZero

      lb must_== ExprOp.DocField(BsonField.Name("__tmp0"))
      rb must_== ExprOp.DocField(BsonField.Name("__tmp1"))

      op must beTree(chain(
        readFoo,
        $simpleMap(
          JsMacro(value => Obj(ListMap(
            "__tmp0" -> Select(value, "a").fix,
            "__tmp1" -> Select(value, "b").fix)).fix),
          Nil)))
    }

    "merge simpleMap sequence and project" in {
      import JsCore._

      val left = chain(readFoo,
        $project(
          Reshape(ListMap(
            BsonField.Name("value") -> -\/(ExprOp.DocField(BsonField.Name("a"))))),
            IgnoreId),
        $simpleMap(JsMacro(Select(_, "length").fix), Nil),
        $project(
          Reshape(ListMap(
            BsonField.Name("1") -> -\/(ExprOp.DocField(BsonField.Name("value"))))),
          IgnoreId))
      val right = chain(readFoo,
        $project(
          Reshape(ListMap(
            BsonField.Name("b") -> -\/(ExprOp.DocField(BsonField.Name("b"))))),
          IgnoreId))

      val ((lb, rb), op) = merge(left, right).evalZero

      op must beTree(chain(
        readFoo,
        $project(
          Reshape(ListMap(
            BsonField.Name("__tmp2") -> \/-(Reshape(ListMap(
              BsonField.Name("value") -> -\/(ExprOp.DocField(BsonField.Name("a")))))),
            BsonField.Name("__tmp3") -> -\/(ExprOp.DocVar.ROOT()))),
          IncludeId),
        $simpleMap(
          JsMacro(value => Obj(ListMap(
            "__tmp0" -> Select(Select(value, "__tmp2").fix, "length").fix,
            "__tmp1" -> Select(value, "__tmp3").fix)).fix),
          Nil),
        $project(
          Reshape(ListMap(
            BsonField.Name("1") -> -\/(ExprOp.DocField(BsonField.Name("__tmp0") \ BsonField.Name("value"))),
            BsonField.Name("b") -> -\/(ExprOp.DocField(BsonField.Name("__tmp1") \ BsonField.Name("b"))))),
          IgnoreId)))
    }

    "merge simpleMap sequence and read" in {
      import JsCore._

      val left = chain(readFoo,
        $project(
          Reshape(ListMap(
            BsonField.Name("value") -> -\/(ExprOp.DocField(BsonField.Name("a"))))),
            IgnoreId),
        $simpleMap(JsMacro(Select(_, "length").fix), Nil),
        $project(
          Reshape(ListMap(
            BsonField.Name("1") -> -\/(ExprOp.DocField(BsonField.Name("value"))))),
          IgnoreId))
      val right = readFoo

      val ((lb, rb), op) = merge(left, right).evalZero

      op must beTree(chain(
        readFoo,
        $project(
          Reshape(ListMap(
            BsonField.Name("__tmp2") -> \/-(Reshape(ListMap(
              BsonField.Name("value") -> -\/(ExprOp.DocField(BsonField.Name("a")))))),
            BsonField.Name("__tmp3") -> -\/(ExprOp.DocVar.ROOT()))),
          IncludeId),
        $simpleMap(
          JsMacro(value => Obj(ListMap(
            "__tmp0" -> Select(Select(value, "__tmp2").fix, "length").fix,
            "__tmp1" -> Select(value, "__tmp3").fix)).fix),
          Nil),
        $project(
          Reshape(ListMap(
            BsonField.Name("__tmp4") -> \/-(Reshape(ListMap(
              BsonField.Name("1") -> -\/(ExprOp.DocField(BsonField.Name("__tmp0") \ BsonField.Name("value")))))),
            BsonField.Name("__tmp5") -> -\/(ExprOp.DocField(BsonField.Name("__tmp1"))))),
          IncludeId)))

      lb must_== ExprOp.DocField(BsonField.Name("__tmp4"))
      rb must_== ExprOp.DocField(BsonField.Name("__tmp5"))
    }
  }

  "finalize" should {
    import JsCore._

    "coalesce previous projection into a map" in {
      val readZips = $read(Collection("zips"))
      val given = chain(
        readZips,
        $project(Reshape(ListMap(
          BsonField.Name("value") -> -\/(ExprOp.DocVar.ROOT()))),
          IncludeId),
        $map($Map.mapNOP, ListMap()))

      val expected = chain(
        readZips,
        $map($Map.compose(
          $Map.mapNOP,
          $Map.mapMap("value",
            Obj(ListMap("value" -> Ident("value").fix)).fix.toJs)),
          ListMap()))

      Workflow.finalize(given) must beTree(expected)
    }

    "coalesce previous projection into a flatMap" in {
      val readZips = $read(Collection("zips"))
      val given = chain(
        readZips,
        $project(Reshape(ListMap(
          BsonField.Name("value") -> -\/(ExprOp.DocVar.ROOT()))),
          IncludeId),
        $flatMap(
          Js.AnonFunDecl(List("key", "value"), List(
            Js.VarDef(List("rez" -> Js.AnonElem(Nil))),
            Js.ForIn(
              Js.Ident("attr"),
              Select(Ident("value").fix, "value").fix.toJs,
              Call(
                Select(Ident("rez").fix, "push").fix,
                List(
                  Arr(List(
                    Call(Ident("ObjectId").fix, Nil).fix,
                    Access(
                      Select(Ident("value").fix, "value").fix,
                      Ident("attr").fix).fix)).fix)).fix.toJs),
            Js.Return(Js.Ident("rez")))),
          ListMap()))

      val expected = chain(
        readZips,
        $flatMap($Map.compose(
          Js.AnonFunDecl(List("key", "value"), List(
            Js.VarDef(List("rez" -> Js.AnonElem(Nil))),
            Js.ForIn(
              Js.Ident("attr"),
              Select(Ident("value").fix, "value").fix.toJs,
              Call(
                Select(Ident("rez").fix, "push").fix,
                List(
                  Arr(List(
                    Call(Ident("ObjectId").fix, Nil).fix,
                    Access(
                      Select(Ident("value").fix, "value").fix,
                      Ident("attr").fix).fix)).fix)).fix.toJs),
            Js.Return(Js.Ident("rez")))),
          $Map.mapMap("value",
            Obj(ListMap("value" -> Ident("value").fix)).fix.toJs)),
          ListMap()))

      Workflow.finalize(given) must beTree(expected)
    }

    "convert previous projection before a reduce" in {
      val readZips = $read(Collection("zips"))
      val given = chain(
        readZips,
        $project(Reshape(ListMap(
          BsonField.Name("value") -> -\/(ExprOp.DocVar.ROOT()))),
          IncludeId),
        $reduce($Reduce.reduceNOP, ListMap()))

      val expected = chain(
        readZips,
        $map($Map.mapMap("value",
          Obj(ListMap("value" -> Ident("value").fix)).fix.toJs),
          ListMap()),
        $reduce($Reduce.reduceNOP, ListMap()))

      Workflow.finalize(given) must beTree(expected)
    }

    "coalesce previous unwind into a map" in {
      val readZips = $read(Collection("zips"))
      val given = chain(
        readZips,
        $unwind(ExprOp.DocVar.ROOT(BsonField.Name("loc"))),
        $map($Map.mapNOP, ListMap()))

      val expected = chain(
        readZips,
        $flatMap($FlatMap.mapCompose(
          $Map.mapNOP,
          Js.AnonFunDecl(List("key", "value"), List(
            Js.VarDef(List("rez" -> Js.AnonElem(Nil))),
            Js.ForIn(Js.Ident("elem"), Select(Ident("value").fix, "loc").fix.toJs,
              Js.Block(List(
                Js.VarDef(List(
                  "each" -> Js.Call(Js.Ident("clone"), List(Js.Ident("value"))))),
                safeAssign(Select(Ident("each").fix, "loc").fix, Access(Select(Ident("value").fix, "loc").fix, Ident("elem").fix).fix),
                Call(Select(Ident("rez").fix, "push").fix,
                  List(
                    Arr(List(
                      Call(Ident("ObjectId").fix, Nil).fix,
                      Ident("each").fix)).fix)).fix.toJs))),
            Js.Return(Js.Ident("rez"))))),
          ListMap("clone" -> Bson.JavaScript($SimpleMap.jsClone))))
      Workflow.finalize(given) must beTree(expected)
    }

    "coalesce previous unwind into a flatMap" in {
      val readZips = $read(Collection("zips"))
      val given = chain(
        readZips,
        $unwind(ExprOp.DocVar.ROOT(BsonField.Name("loc"))),
        $flatMap(
          Js.AnonFunDecl(List("key", "value"), List(
            Js.VarDef(List("rez" -> Js.AnonElem(Nil))),
            Js.ForIn(
              Js.Ident("attr"),
              Select(Ident("value").fix, "value").fix.toJs,
              Call(Select(Ident("rez").fix, "push").fix,
                List(
                  Arr(List(
                    Call(Ident("ObjectId").fix, Nil).fix,
                    Access(
                      Select(Ident("value").fix, "value").fix,
                      Ident("attr").fix).fix)).fix)).fix.toJs),
            Js.Return(Js.Ident("rez")))),
          ListMap()))

      val expected = chain(
        readZips,
        $flatMap($FlatMap.kleisliCompose(
          Js.AnonFunDecl(List("key", "value"), List(
            Js.VarDef(List("rez" -> Js.AnonElem(Nil))),
            Js.ForIn(
              Js.Ident("attr"),
              Select(Ident("value").fix, "value").fix.toJs,
              Call(Select(Ident("rez").fix, "push").fix,
                List(
                  Arr(List(
                    Call(Ident("ObjectId").fix, Nil).fix,
                    Access(
                      Select(Ident("value").fix, "value").fix,
                      Ident("attr").fix).fix)).fix)).fix.toJs),
            Js.Return(Js.Ident("rez")))),
          Js.AnonFunDecl(List("key", "value"), List(
            Js.VarDef(List("rez" -> Js.AnonElem(Nil))),
            Js.ForIn(Js.Ident("elem"), Select(Ident("value").fix, "loc").fix.toJs,
              Js.Block(List(
                Js.VarDef(List(
                  "each" -> Js.Call(Js.Ident("clone"), List(Js.Ident("value"))))),
                safeAssign(Select(Ident("each").fix, "loc").fix, Access(Select(Ident("value").fix, "loc").fix, Ident("elem").fix).fix),
                Call(Select(Ident("rez").fix, "push").fix,
                  List(
                    Arr(List(
                      Call(Ident("ObjectId").fix, Nil).fix,
                      Ident("each").fix)).fix)).fix.toJs))),
            Js.Return(Js.Ident("rez"))))),
          ListMap("clone" -> Bson.JavaScript($SimpleMap.jsClone))))
      Workflow.finalize(given) must beTree(expected)
    }

    "convert previous unwind before a reduce" in {
      val readZips = $read(Collection("zips"))
      val given = chain(
        readZips,
        $unwind(ExprOp.DocVar.ROOT(BsonField.Name("loc"))),
        $reduce($Reduce.reduceNOP, ListMap()))

      val expected = chain(
        readZips,
        $flatMap(
          Js.AnonFunDecl(List("key", "value"), List(
            Js.VarDef(List("rez" -> Js.AnonElem(Nil))),
            Js.ForIn(Js.Ident("elem"), Select(Ident("value").fix, "loc").fix.toJs,
              Js.Block(List(
                Js.VarDef(List(
                  "each" -> Js.Call(Js.Ident("clone"), List(Js.Ident("value"))))),
                safeAssign(Select(Ident("each").fix, "loc").fix, Access(Select(Ident("value").fix, "loc").fix, Ident("elem").fix).fix),
                Call(Select(Ident("rez").fix, "push").fix,
                  List(
                    Arr(List(
                      Call(Ident("ObjectId").fix, Nil).fix,
                      Ident("each").fix)).fix)).fix.toJs))),
            Js.Return(Js.Ident("rez")))),
          ListMap("clone" -> Bson.JavaScript($SimpleMap.jsClone))),
        $reduce($Reduce.reduceNOP, ListMap()))
      Workflow.finalize(given) must beTree(expected)
    }

    "patch $FoldLeft" in {
      val readZips = $read(Collection("zips"))
      val given = $foldLeft(readZips, readZips)

      val expected = $foldLeft(
        chain(readZips, $project(Reshape(ListMap(
          BsonField.Name("value") -> -\/(ExprOp.DocVar.ROOT()))),
          IncludeId)),
        chain(readZips, $reduce($Reduce.reduceFoldLeft, ListMap())))

      Workflow.finalize(given) must beTree(expected)
    }

    "patch $FoldLeft with existing reduce" in {
      val readZips = $read(Collection("zips"))
      val given = $foldLeft(
        readZips,
        chain(readZips, $reduce($Reduce.reduceNOP, ListMap())))

      val expected = $foldLeft(
        chain(
          readZips,
          $project(Reshape(ListMap(
            BsonField.Name("value") -> -\/(ExprOp.DocVar.ROOT()))),
            IncludeId)),
        chain(readZips, $reduce($Reduce.reduceNOP, ListMap())))

      Workflow.finalize(given) must beTree(expected)
    }

    "avoid dangling map with known shape" in {
      Workflow.finalize(chain(
        $read(Collection("zips")),
        $simpleMap(
          JsMacro(base => JsCore.Obj(ListMap(
            "first" -> JsCore.Select(base, "pop").fix,
            "second" -> JsCore.Select(base, "city").fix)).fix),
          Nil))) must
      beTree(chain(
        $read(Collection("zips")),
        $simpleMap(
          JsMacro(base => JsCore.Obj(ListMap(
            "first" -> JsCore.Select(base, "pop").fix,
            "second" -> JsCore.Select(base, "city").fix)).fix),
          Nil),
        $project(Reshape(ListMap(
          BsonField.Name("first") -> -\/(ExprOp.Include),
          BsonField.Name("second") -> -\/(ExprOp.Include))),
          IgnoreId)))
    }

    "avoid dangling flatMap with known shape" in {
      Workflow.finalize(chain(
        $read(Collection("zips")),
        $simpleMap(
          JsMacro(base => JsCore.Obj(ListMap(
            "first"  -> JsCore.Select(base, "loc").fix,
            "second" -> base)).fix),
          List(JsMacro(base => JsCore.Select(base, "city").fix))))) must
      beTree(chain(
        $read(Collection("zips")),
        $simpleMap(
          JsMacro(base => JsCore.Obj(ListMap(
            "first"  -> JsCore.Select(base, "loc").fix,
            "second" -> base)).fix),
          List(JsMacro(base => JsCore.Select(base, "city").fix))),
        $project(Reshape(ListMap(
          BsonField.Name("first") -> -\/(ExprOp.Include),
          BsonField.Name("second") -> -\/(ExprOp.Include))),
          IgnoreId)))
    }

    "fold unwind into SimpleMap" in {
      import JsCore._

      Workflow.finalize(chain(
        $read(Collection("zips")),
        $unwind(ExprOp.DocField(BsonField.Name("loc"))),
        $simpleMap(JsMacro(x =>
          Obj(ListMap(
            "0" -> Select(x, "loc").fix)).fix), Nil))) must
      beTree(chain(
        $read(Collection("zips")),
        $simpleMap(
          JsMacro(x => Obj(ListMap("0" -> Select(x, "loc").fix)).fix),
          List(JsMacro(Select(_, "loc").fix))),
        $project(
          Reshape(ListMap(
            BsonField.Name("0") -> -\/(ExprOp.Include))),
          IgnoreId)))
    }

    "fold multiple unwinds into SimpleMap" in {
      import JsCore._

      Workflow.finalize(chain(
        $read(Collection("foo")),
        $unwind(ExprOp.DocField(BsonField.Name("bar"))),
        $unwind(ExprOp.DocField(BsonField.Name("baz"))),
        $simpleMap(JsMacro(x =>
          Obj(ListMap(
            "0" -> Select(x, "bar").fix,
            "1" -> Select(x, "baz").fix)).fix), Nil))) must
      beTree(chain(
        $read(Collection("foo")),
        $simpleMap(
          JsMacro(x => Obj(ListMap(
            "0" -> Select(x, "bar").fix,
            "1" -> Select(x, "baz").fix)).fix),
          List(
            JsMacro(Select(_, "bar").fix),
            JsMacro(Select(_, "baz").fix))),
        $project(
          Reshape(ListMap(
            BsonField.Name("0") -> -\/(ExprOp.Include),
            BsonField.Name("1") -> -\/(ExprOp.Include))),
          IgnoreId)))
    }
  }

  "task" should {
    import WorkflowTask._

    "convert $match with $where into map/reduce" in {
      task(chain(
        $read(Collection("zips")),
        $match(Selector.Where(Js.BinOp("<",
          Js.Select(Js.Select(Js.Ident("this"), "city"), "length"),
          Js.Num(4, false)))))) must
      beTree[WorkflowTask](
        MapReduceTask(ReadTask(Collection("zips")),
          MapReduce($Map.mapFn($Map.mapNOP), $Reduce.reduceNOP,
            selection = Some(Selector.Where(Js.BinOp("<",
              Js.Select(Js.Select(Js.Ident("this"), "city"), "length"),
              Js.Num(4, false)))))))
    }

    "always pipeline unconverted aggregation ops" in {
      task(chain(
        $read(Collection("zips")),
        $group(
          Grouped(ListMap(
            BsonField.Name("__sd_tmp_1") ->
              ExprOp.Push(ExprOp.DocField(BsonField.Name("lEft"))))),
          -\/(ExprOp.Literal(Bson.Int32(1)))),
        $project(Reshape(ListMap(
          BsonField.Name("a") -> -\/(ExprOp.Include),
          BsonField.Name("b") -> -\/(ExprOp.Include),
          BsonField.Name("equal?") -> -\/(ExprOp.Eq(
            ExprOp.DocField(BsonField.Name("a")),
            ExprOp.DocField(BsonField.Name("b")))))),
          IncludeId),
        $match(Selector.Doc(
          BsonField.Name("equal?") -> Selector.Eq(Bson.Bool(true)))),
        $sort(NonEmptyList(BsonField.Name("a") -> Descending)),
        $limit(100),
        $skip(5),
        $project(Reshape(ListMap(
          BsonField.Name("a") -> -\/(ExprOp.Include),
          BsonField.Name("b") -> -\/(ExprOp.Include))),
          IncludeId))) must
      beTree[WorkflowTask](
        PipelineTask(ReadTask(Collection("zips")),
          List(
            $Group((), Grouped(ListMap()), -\/(ExprOp.Literal(Bson.Null))),
            $Project((),
              Reshape(ListMap(
                BsonField.Name("a") -> -\/(ExprOp.DocField(BsonField.Name("a"))),
                BsonField.Name("b") -> -\/(ExprOp.DocField(BsonField.Name("b"))),
                BsonField.Name("equal?") -> -\/(ExprOp.Eq(
                  ExprOp.DocField(BsonField.Name("a")),
                  ExprOp.DocField(BsonField.Name("b")))))),
              IncludeId),
            $Match((),
              Selector.Doc(
                BsonField.Name("equal?") -> Selector.Eq(Bson.Bool(true)))),
            $Sort((), NonEmptyList(BsonField.Name("a") -> Descending)),
            $Limit((), 100),
            $Skip((), 5),
            $Project((),
              Reshape(ListMap(
                BsonField.Name("a") -> -\/(ExprOp.DocField(BsonField.Name("a"))),
                BsonField.Name("b") -> -\/(ExprOp.DocField(BsonField.Name("b"))))),
              IncludeId))))
    }

    "create maximal map/reduce" in {
      task(chain(
        $read(Collection("zips")),
        $match(Selector.Doc(
          BsonField.Name("loc") \ BsonField.Index(0) ->
            Selector.Lt(Bson.Int64(-73)))),
        $sort(NonEmptyList(BsonField.Name("city") -> Descending)),
        $limit(100),
        $map($Map.mapMap("value",
          Js.Access(Js.Ident("value"), Js.Num(0, false))),
          ListMap()),
        $reduce($Reduce.reduceFoldLeft, ListMap()),
        $map($Map.mapMap("value", Js.Ident("value")), ListMap()))) must
      beTree[WorkflowTask](
        MapReduceTask(ReadTask(Collection("zips")),
          MapReduce(
            $Map.mapFn($Map.mapMap("value",
              Js.Access(Js.Ident("value"), Js.Num(0, false)))),
            $Reduce.reduceFoldLeft,
            selection = Some(Selector.Doc(
              BsonField.Name("loc") \ BsonField.Index(0) ->
                Selector.Lt(Bson.Int64(-73)))),
            inputSort =
              Some(NonEmptyList(BsonField.Name("city") -> Descending)),
            limit = Some(100),
            finalizer = Some($Map.finalizerFn($Map.mapMap("value",
              Js.Ident("value")))))))
    }

    "create maximal map/reduce with flatMap" in {
      task(chain(
        $read(Collection("zips")),
        $match(Selector.Doc(
          BsonField.Name("loc") \ BsonField.Index(0) ->
            Selector.Lt(Bson.Int64(-73)))),
        $sort(NonEmptyList(BsonField.Name("city") -> Descending)),
        $limit(100),
        $flatMap(Js.AnonFunDecl(List("key", "value"), List(
          Js.AnonElem(List(
            Js.AnonElem(List(Js.Ident("key"), Js.Ident("value"))))))),
          ListMap()),
        $reduce($Reduce.reduceFoldLeft, ListMap()),
        $map($Map.mapMap("value", Js.Ident("value")), ListMap()))) must
      beTree[WorkflowTask](
        MapReduceTask(ReadTask(Collection("zips")),
          MapReduce(
            $FlatMap.mapFn(Js.AnonFunDecl(List("key", "value"), List(
              Js.AnonElem(List(
                Js.AnonElem(List(Js.Ident("key"), Js.Ident("value")))))))),
            $Reduce.reduceFoldLeft,
            selection = Some(Selector.Doc(
              BsonField.Name("loc") \ BsonField.Index(0) ->
                Selector.Lt(Bson.Int64(-73)))),
            inputSort =
              Some(NonEmptyList(BsonField.Name("city") -> Descending)),
            limit = Some(100),
            finalizer = Some($Map.finalizerFn($Map.mapMap("value",
              Js.Ident("value")))))))
    }

    "create map/reduce without map" in {
      task(chain(
        $read(Collection("zips")),
        $match(Selector.Doc(
          BsonField.Name("loc") \ BsonField.Index(0) ->
            Selector.Lt(Bson.Int64(-73)))),
        $sort(NonEmptyList(BsonField.Name("city") -> Descending)),
        $limit(100),
        $reduce($Reduce.reduceFoldLeft, ListMap()),
        $map($Map.mapMap("value", Js.Ident("value")), ListMap()))) must
      beTree[WorkflowTask](
        MapReduceTask(ReadTask(Collection("zips")),
          MapReduce(
            $Map.mapFn($Map.mapNOP),
            $Reduce.reduceFoldLeft,
            selection = Some(Selector.Doc(
              BsonField.Name("loc") \ BsonField.Index(0) ->
                Selector.Lt(Bson.Int64(-73)))),
            inputSort =
              Some(NonEmptyList(BsonField.Name("city") -> Descending)),
            limit = Some(100),
            finalizer = Some($Map.finalizerFn($Map.mapMap("value",
              Js.Ident("value")))))))
    }

    "fold unwind into SimpleMap" in {
      import JsCore._

      task(chain(
        $read(Collection("zips")),
        $unwind(ExprOp.DocField(BsonField.Name("loc"))),
        $simpleMap(JsMacro(x =>
          Obj(ListMap(
            "0" -> Select(x, "loc").fix)).fix), Nil))) must
      beTree[WorkflowTask](
        PipelineTask(
          MapReduceTask(
            ReadTask(Collection("zips")),
            MapReduce(
              Js.AnonFunDecl(Nil, List(
                Js.Call(
                  Js.Select(
                    Js.Call(
                      Js.AnonFunDecl(List("key", "value"), List(
                        Js.VarDef(List("rez" -> Js.AnonElem(Nil))),
                        Js.ForIn(Js.Ident("elem"), JsCore.Select(Ident("value").fix, "loc").fix.toJs,
                          Js.Block(List(
                            Js.VarDef(List("each" -> Js.Call(Js.Ident("clone"), List(Js.Ident("value"))))),
                            JsCore.safeAssign(Select(Ident("each").fix, "loc").fix, Access(Select(Ident("value").fix, "loc").fix, Ident("elem").fix).fix),
                            JsCore.Call(JsCore.Select(Ident("rez").fix, "push").fix, List(
                              Arr(List(
                                Call(Ident("ObjectId").fix, List[Term[JsCore]]()).fix,
                                Obj(ListMap("0" -> Select(Ident("each").fix, "loc").fix)).fix)).fix)).fix.toJs))),
                        Js.Return(Js.Ident("rez")))),
                      List(Js.Select(Js.This, IdLabel), Js.This)),
                    "map"),
                  List(
                    Js.AnonFunDecl(List("__rez"), List(
                      Js.Call(Js.Select(Js.Ident("emit"), "apply"), List(Js.Null, Js.Ident("__rez"))))))))),
              $Reduce.reduceNOP,
              scope = ListMap(
                "clone" -> Bson.JavaScript($SimpleMap.jsClone)))),
          List(
            $Project((),
              Reshape(ListMap(
                BsonField.Name("0") -> -\/(ExprOp.DocField(BsonField.Name("value") \ BsonField.Name("0"))))),
              IgnoreId))))
    }

    "fold multiple unwinds into SimpleMap" in {
      import JsCore._

      task(chain(
        $read(Collection("foo")),
        $unwind(ExprOp.DocField(BsonField.Name("bar"))),
        $unwind(ExprOp.DocField(BsonField.Name("baz"))),
        $simpleMap(JsMacro(x =>
          Obj(ListMap(
            "0" -> Select(x, "bar").fix,
            "1" -> Select(x, "baz").fix)).fix), Nil))) must
      beTree[WorkflowTask](
        PipelineTask(
          MapReduceTask(
            ReadTask(Collection("foo")),
            MapReduce(
              Js.AnonFunDecl(Nil, List(
                Js.Call(
                  Js.Select(
                    Js.Call(
                      Js.AnonFunDecl(List("key", "value"), List(
                        Js.VarDef(List("rez" -> Js.AnonElem(Nil))),
                        Js.ForIn(Js.Ident("elem1"), JsCore.Select(Ident("value").fix, "baz").fix.toJs,
                          Js.ForIn(Js.Ident("elem0"), JsCore.Select(Ident("value").fix, "bar").fix.toJs,
                            Js.Block(List(
                              Js.VarDef(List("each" -> Js.Call(Js.Ident("clone"), List(Js.Ident("value"))))),
                              JsCore.safeAssign(Select(Ident("each").fix, "bar").fix, Access(Select(Ident("value").fix, "bar").fix, Ident("elem0").fix).fix),
                              JsCore.safeAssign(Select(Ident("each").fix, "baz").fix, Access(Select(Ident("value").fix, "baz").fix, Ident("elem1").fix).fix),
                              JsCore.Call(JsCore.Select(Ident("rez").fix, "push").fix, List(
                                Arr(List(
                                  Call(Ident("ObjectId").fix, List[Term[JsCore]]()).fix,
                                  Obj(ListMap(
                                    "0" -> Select(Ident("each").fix, "bar").fix,
                                    "1" -> Select(Ident("each").fix, "baz").fix)).fix)).fix)).fix.toJs)))),
                        Js.Return(Js.Ident("rez")))),
                      List(Js.Select(Js.This, IdLabel), Js.This)),
                    "map"),
                  List(
                    Js.AnonFunDecl(List("__rez"), List(
                      Js.Call(Js.Select(Js.Ident("emit"), "apply"), List(Js.Null, Js.Ident("__rez"))))))))),
              $Reduce.reduceNOP,
              scope = ListMap(
                "clone" -> Bson.JavaScript($SimpleMap.jsClone)))),
          List(
            $Project((),
              Reshape(ListMap(
                BsonField.Name("0") -> -\/(ExprOp.DocField(BsonField.Name("value") \ BsonField.Name("0"))),
                BsonField.Name("1") -> -\/(ExprOp.DocField(BsonField.Name("value") \ BsonField.Name("1"))))),
              IgnoreId))))
    }
  }

  "RenderTree[Workflow]" should {
    def render(op: Workflow)(implicit RO: RenderTree[Workflow]): String = RO.render(op).draw.mkString("\n")

    "render read" in {
      render(readFoo) must_== "$Read(foo)"
    }

    "render simple project" in {
      val op = chain(readFoo,
        $project(
          Reshape(ListMap(
            BsonField.Name("bar") -> -\/ (ExprOp.DocField(BsonField.Name("baz"))))),
          IncludeId))

      render(op) must_==
        """Chain
          |├─ $Read(foo)
          |╰─ $Project
          |   ├─ Name(bar -> $baz)
          |   ╰─ IncludeId""".stripMargin
    }

    "render nested project" in {
      val op = chain(readFoo,
        $project(
          Reshape(ListMap(
            BsonField.Name("bar") -> \/- (Reshape(ListMap(
              BsonField.Name("0") -> -\/ (ExprOp.DocField(BsonField.Name("baz")))))))),
          IncludeId))

      render(op) must_==
        """Chain
          |├─ $Read(foo)
          |╰─ $Project
          |   ├─ Name(bar)
          |   │  ╰─ Name(0 -> $baz)
          |   ╰─ IncludeId""".stripMargin
    }

    "render map/reduce ops" in {
      val op = chain(readFoo,
        $map(Js.AnonFunDecl(List("key"), Nil), ListMap()),
        $project(Reshape(ListMap(
          BsonField.Name("bar") -> -\/(ExprOp.DocField(BsonField.Name("baz"))))),
          IncludeId),
        $flatMap(Js.AnonFunDecl(List("key"), Nil), ListMap()),
        $reduce(
          Js.AnonFunDecl(List("key", "values"),
            List(Js.Return(Js.Access(Js.Ident("values"), Js.Num(1, false))))),
          ListMap()))

      render(op) must_==
        """Chain
          |├─ $Read(foo)
          |├─ $Map
          |│  ├─ JavaScript(function (key) {})
          |│  ╰─ Scope(Map())
          |├─ $Project
          |│  ├─ Name(bar -> $baz)
          |│  ╰─ IncludeId
          |├─ $FlatMap
          |│  ├─ JavaScript(function (key) {})
          |│  ╰─ Scope(Map())
          |╰─ $Reduce
          |   ├─ JavaScript(function (key, values) { return values[1] })
          |   ╰─ Scope(Map())""".stripMargin
    }

    "render unchained" in {
      val op =
        $foldLeft(
          chain(readFoo,
            $project(Reshape(ListMap(
              BsonField.Name("bar") -> -\/(ExprOp.DocField(BsonField.Name("baz"))))),
              IncludeId)),
          chain(readFoo,
            $map(Js.AnonFunDecl(List("key"), Nil), ListMap()),
            $reduce($Reduce.reduceNOP, ListMap())))

      render(op) must_==
      """$FoldLeft
        |├─ Chain
        |│  ├─ $Read(foo)
        |│  ╰─ $Project
        |│     ├─ Name(bar -> $baz)
        |│     ╰─ IncludeId
        |╰─ Chain
        |   ├─ $Read(foo)
        |   ├─ $Map
        |   │  ├─ JavaScript(function (key) {})
        |   │  ╰─ Scope(Map())
        |   ╰─ $Reduce
        |      ├─ JavaScript(function (key, values) { return (values != null) ? values[0] : undefined })
        |      ╰─ Scope(Map())""".stripMargin
    }
  }
}
