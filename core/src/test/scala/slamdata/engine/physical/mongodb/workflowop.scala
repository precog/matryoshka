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
        $project(Reshape.Doc(ListMap(
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
    }
    
    "not flatten project into group/unwind with _id excluded" in {
      val given = chain(
        readFoo,
        $group(
          Grouped(ListMap(
            BsonField.Name("value") -> ExprOp.Push(ExprOp.DocField(BsonField.Name("rIght"))))),
            -\/ (ExprOp.DocField(BsonField.Name("lEft")))),
        $unwind(ExprOp.DocField(BsonField.Name("value"))),
        $project(Reshape.Doc(ListMap(
          BsonField.Name("city") -> -\/ (ExprOp.DocField(BsonField.Name("value") \ BsonField.Name("city"))))),
          ExcludeId))
      
      given must beTree(given: Workflow)
    }

    "resolve `Include`" in {
      chain($read(Collection("zips")),
        $project(Reshape.Doc(ListMap(
          BsonField.Name("bar") -> -\/(ExprOp.Include))),
          ExcludeId),
        $project(Reshape.Doc(ListMap(
          BsonField.Name("_id") ->
            -\/(ExprOp.DocField(BsonField.Name("bar"))))),
          IncludeId)) must_==
      chain($read(Collection("zips")),
        $project(Reshape.Doc(ListMap(
          BsonField.Name("_id") ->
            -\/(ExprOp.DocField(BsonField.Name("bar"))))),
          IncludeId))
    }.pendingUntilFixed("#385")

    "traverse `Include`" in {
      chain($read(Collection("zips")),
        $project(Reshape.Doc(ListMap(
          BsonField.Name("bar") ->
            -\/(ExprOp.Divide(
              ExprOp.DocField(BsonField.Name("baz")),
              ExprOp.Literal(Bson.Int32(92)))))),
          IncludeId),
        $project(Reshape.Doc(ListMap(
          BsonField.Name("bar") -> -\/(ExprOp.Include))),
          IncludeId)) must_==
      chain($read(Collection("zips")),
        $project(Reshape.Doc(ListMap(
          BsonField.Name("bar") -> -\/(ExprOp.Divide(
            ExprOp.DocField(BsonField.Name("baz")),
            ExprOp.Literal(Bson.Int32(92)))))),
          IncludeId))
    }.pendingUntilFixed("#385")

    "resolve implied `_id`" in {
      chain($read(Collection("zips")),
        $project(Reshape.Doc(ListMap(
          BsonField.Name("bar") -> -\/(ExprOp.DocField(BsonField.Name("bar"))),
          BsonField.Name("_id") ->
            -\/(ExprOp.DocField(BsonField.Name("baz"))))),
          IncludeId),
        $project(Reshape.Doc(ListMap(
          BsonField.Name("bar") ->
            -\/(ExprOp.DocField(BsonField.Name("bar"))))),
          IncludeId)) must_==
      chain($read(Collection("zips")),
        $project(Reshape.Doc(ListMap(
          BsonField.Name("bar") -> -\/(ExprOp.DocField(BsonField.Name("bar"))),
          BsonField.Name("_id") ->
            -\/(ExprOp.DocField(BsonField.Name("baz"))))),
          IncludeId))
    }.pendingUntilFixed("#386")

    "not resolve excluded `_id`" in {
      chain($read(Collection("zips")),
        $project(Reshape.Doc(ListMap(
          BsonField.Name("bar") -> -\/(ExprOp.DocField(BsonField.Name("bar"))),
          BsonField.Name("_id") ->
            -\/(ExprOp.DocField(BsonField.Name("baz"))))),
          IncludeId),
        $project(Reshape.Doc(ListMap(
          BsonField.Name("bar") ->
            -\/(ExprOp.DocField(BsonField.Name("bar"))))),
          ExcludeId)) must_==
      chain($read(Collection("zips")),
        $project(Reshape.Doc(ListMap(
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
            $project(Reshape.Doc(ListMap(
              BsonField.Name("__tmp0") -> -\/(ExprOp.DocVar.ROOT()))),
              IncludeId)),
          chain(
            $read(Collection("zips")),
            $project(Reshape.Doc(ListMap(
              BsonField.Name("__tmp1") -> -\/(ExprOp.DocVar.ROOT()))),
              IncludeId))))
    }

    "put shape-preserving before non-" in {
      val left = chain(
        readFoo,
        $project(Reshape.Doc(ListMap(
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
          $project(Reshape.Doc(ListMap(
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
        $project(Reshape.Doc(ListMap(
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
          $project(Reshape.Doc(ListMap(
            BsonField.Name("__tmp0") -> \/-(Reshape.Doc(ListMap(
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
        $project(Reshape.Doc(ListMap(
          BsonField.Name("city") ->
            -\/(ExprOp.DocField(BsonField.Name("city"))))),
          IncludeId),
        $unwind(ExprOp.DocField(BsonField.Name("city"))))
      val right = chain(
        readFoo,
        $project(Reshape.Doc(ListMap(
          BsonField.Name("city") ->
            -\/(ExprOp.DocField(BsonField.Name("city"))),
          BsonField.Name("loc") ->
            -\/(ExprOp.DocField(BsonField.Name("loc"))))),
          IncludeId),
        $unwind(ExprOp.DocField(BsonField.Name("city"))))
      merge(left, right) must_==
      (ExprOp.DocField(BsonField.Name("rIght")),
        ExprOp.DocField(BsonField.Name("lEft"))) ->
        chain(
          readFoo,
          $project(Reshape.Doc(ListMap(
            BsonField.Name("city") ->
              -\/(ExprOp.DocField(BsonField.Name("city"))),
            BsonField.Name("loc") ->
              -\/(ExprOp.DocField(BsonField.Name("loc"))))),
            IncludeId),
          $unwind(ExprOp.DocField(BsonField.Name("city"))))
    }.pendingUntilFixed("#388")

    "merge group by constant with project" in {
      val left = chain(readFoo, 
                  $group(
                    Grouped(ListMap()),
                    -\/ (ExprOp.Literal(Bson.Int32(1)))))
      val right = chain(readFoo,
                    $project(Reshape.Doc(ListMap(
                      BsonField.Name("city") -> -\/ (ExprOp.DocField(BsonField.Name("city"))))),
                      IncludeId))
          
      val ((lb, rb), op) = merge(left, right).evalZero
      
      lb must_== ExprOp.DocVar.ROOT()
      rb must_== ExprOp.DocField(BsonField.Name("__sd_tmp_1"))
      op must beTree(
          chain(readFoo,
            $project(Reshape.Doc(ListMap(
              BsonField.Name("__tmp0") -> \/-(Reshape.Doc(ListMap(
                BsonField.Name("city") -> -\/(ExprOp.DocField(BsonField.Name("city")))))),
              BsonField.Name("__tmp1") -> -\/ (ExprOp.DocVar.ROOT()))),
              IncludeId),
            $group(
              Grouped(ListMap(
                 BsonField.Name("__sd_tmp_1") -> ExprOp.Push(ExprOp.DocField(BsonField.Name("__tmp0"))))),
              -\/ (ExprOp.Literal(Bson.Int32(1)))),
            $unwind(
              ExprOp.DocField(BsonField.Name("__sd_tmp_1")))))
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
            $project(Reshape.Doc(ListMap(
              BsonField.Name("__tmp0") -> \/- (Reshape.Doc(ListMap(
                BsonField.Name("value") -> -\/ (ExprOp.DocField(BsonField.Name("__sd_tmp_1")))))),
              BsonField.Name("__tmp1") -> \/- (Reshape.Doc(ListMap(
                BsonField.Name("value") -> -\/ (ExprOp.DocField(BsonField.Name("__sd_tmp_2")))))))),
              IgnoreId)))
    }

    "merge groups under unwind" in {
      val left = chain(readFoo, 
                  $group(
                    Grouped(ListMap(
                      BsonField.Name("city") -> ExprOp.Push(ExprOp.DocField(BsonField.Name("city"))))),
                    -\/ (ExprOp.Literal(Bson.Int32(1)))),
                    $unwind(ExprOp.DocField(BsonField.Name("city"))))
      val right = chain(readFoo, 
                  $group(
                    Grouped(ListMap(
                      BsonField.Name("total") -> ExprOp.Sum(ExprOp.Literal(Bson.Int32(1))))),
                    -\/ (ExprOp.Literal(Bson.Int32(1)))))
          
      val ((lb, rb), op) = merge(left, right).evalZero
      
      lb must_== ExprOp.DocField(BsonField.Name("__tmp0"))
      rb must_== ExprOp.DocField(BsonField.Name("__tmp1"))
      op must beTree( 
          chain(readFoo,
            $group(
              Grouped(ListMap(
                 BsonField.Name("__sd_tmp_1") -> ExprOp.Push(ExprOp.DocField(BsonField.Name("city"))),
                 BsonField.Name("__sd_tmp_2") -> ExprOp.Sum(ExprOp.Literal(Bson.Int32(1))))),
              -\/ (ExprOp.Literal(Bson.Int32(1)))),
            $project(Reshape.Doc(ListMap(
              BsonField.Name("__tmp0") -> \/- (Reshape.Doc(ListMap(
                BsonField.Name("city") -> -\/ (ExprOp.DocField(BsonField.Name("__sd_tmp_1")))))),
              BsonField.Name("__tmp1") -> \/- (Reshape.Doc(ListMap(
                BsonField.Name("total") -> -\/ (ExprOp.DocField(BsonField.Name("__sd_tmp_2")))))))),
              IgnoreId),
            $unwind(ExprOp.DocField(BsonField.Name("__tmp0") \ BsonField.Name("city")))))
    }
    
    "merge unwind and project on same group" in {
      val left = chain(readFoo,
        $group(
          Grouped(ListMap(
            BsonField.Name("sumA") -> ExprOp.Sum(ExprOp.DocField(BsonField.Name("a"))))),
          -\/ (ExprOp.DocField(BsonField.Name("key")))),
        $project(Reshape.Arr(ListMap(
          BsonField.Index(0) -> -\/ (ExprOp.DocField(BsonField.Name("sumA"))))),
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
            BsonField.Name("__sd_tmp_1") -> ExprOp.Push(ExprOp.DocField(BsonField.Name("b"))),
            BsonField.Name("__sd_tmp_2") -> ExprOp.Sum(ExprOp.DocField(BsonField.Name("a"))))),
          -\/ (ExprOp.DocField(BsonField.Name("key")))),
        $project(Reshape.Doc(ListMap(
          BsonField.Name("__tmp2") -> \/- (Reshape.Doc(ListMap(
            BsonField.Name("b") -> -\/ (ExprOp.DocField(BsonField.Name("__sd_tmp_1")))))),
          BsonField.Name("__tmp3") -> \/- (Reshape.Doc(ListMap(
            BsonField.Name("sumA") -> -\/ (ExprOp.DocField(BsonField.Name("__sd_tmp_2")))))))),
          IgnoreId),
        $unwind(ExprOp.DocField(BsonField.Name("__tmp2") \ BsonField.Name("b"))),
        $project(Reshape.Doc(ListMap(
          BsonField.Name("__tmp0") -> \/- (Reshape.Arr(ListMap(
            BsonField.Index(0) -> -\/ (ExprOp.DocField(BsonField.Name("__tmp3") \ BsonField.Name("sumA")))))),
          BsonField.Name("__tmp1") -> -\/ (ExprOp.DocVar.ROOT()))),
          IncludeId)))
    }

    "merge simpleMaps on same src" in {
      import JsCore._

      val left = chain(readFoo,
        $simpleMap(JsMacro(value => Select(value, "a").fix)))
      val right = chain(readFoo,
        $simpleMap(JsMacro(value => Select(value, "b").fix)))

      val ((lb, rb), op) = merge(left, right).evalZero

      lb must_== ExprOp.DocField(BsonField.Name("__tmp0"))
      rb must_== ExprOp.DocField(BsonField.Name("__tmp1"))

      op must beTree(chain(
        readFoo,
        $simpleMap(JsMacro(value => Obj(ListMap(
          "__tmp0" -> Select(value, "a").fix,
          "__tmp1" -> Select(value, "b").fix)).fix))))
    }

    "merge simpleMap sequence and project" in {
      import JsCore._

      val left = chain(readFoo,
        $project(
          Reshape.Doc(ListMap(
            BsonField.Name("value") -> -\/(ExprOp.DocField(BsonField.Name("a"))))),
            IgnoreId),
        $simpleMap(JsMacro(Select(_, "length").fix)),
        $project(
          Reshape.Doc(ListMap(
            BsonField.Name("1") -> -\/(ExprOp.DocField(BsonField.Name("value"))))),
          IgnoreId))
      val right = chain(readFoo,
        $project(
          Reshape.Doc(ListMap(
            BsonField.Name("b") -> -\/(ExprOp.DocField(BsonField.Name("b"))))),
          IgnoreId))

      val ((lb, rb), op) = merge(left, right).evalZero

      op must beTree(chain(
        readFoo,
        $project(
          Reshape.Doc(ListMap(
            BsonField.Name("__tmp2") -> \/-(Reshape.Doc(ListMap(
              BsonField.Name("value") -> -\/(ExprOp.DocField(BsonField.Name("a")))))),
            BsonField.Name("__tmp3") -> -\/(ExprOp.DocVar.ROOT()))),
          IncludeId),
        $simpleMap(JsMacro(value => Obj(ListMap(
          "__tmp0" -> Select(Select(value, "__tmp2").fix, "length").fix,
          "__tmp1" -> Select(value, "__tmp3").fix)).fix)),
        $project(
          Reshape.Doc(ListMap(
            BsonField.Name("1") -> -\/(ExprOp.DocField(BsonField.Name("__tmp0") \ BsonField.Name("value"))),
            BsonField.Name("b") -> -\/(ExprOp.DocField(BsonField.Name("__tmp1") \ BsonField.Name("b"))))),
          IgnoreId)))
    }

    "merge simpleMap sequence and read" in {
      import JsCore._

      val left = chain(readFoo,
        $project(
          Reshape.Doc(ListMap(
            BsonField.Name("value") -> -\/(ExprOp.DocField(BsonField.Name("a"))))),
            IgnoreId),
        $simpleMap(JsMacro(Select(_, "length").fix)),
        $project(
          Reshape.Doc(ListMap(
            BsonField.Name("1") -> -\/(ExprOp.DocField(BsonField.Name("value"))))),
          IgnoreId))
      val right = readFoo

      val ((lb, rb), op) = merge(left, right).evalZero

      op must beTree(chain(
        readFoo,
        $project(
          Reshape.Doc(ListMap(
            BsonField.Name("__tmp2") -> \/-(Reshape.Doc(ListMap(
              BsonField.Name("value") -> -\/(ExprOp.DocField(BsonField.Name("a")))))),
            BsonField.Name("__tmp3") -> -\/(ExprOp.DocVar.ROOT()))),
          IncludeId),
        $simpleMap(JsMacro(value => Obj(ListMap(
          "__tmp0" -> Select(Select(value, "__tmp2").fix, "length").fix,
          "__tmp1" -> Select(value, "__tmp3").fix)).fix)),
        $project(
          Reshape.Doc(ListMap(
            BsonField.Name("__tmp4") -> \/-(Reshape.Doc(ListMap(
              BsonField.Name("1") -> -\/(ExprOp.DocField(BsonField.Name("__tmp0") \ BsonField.Name("value")))))),
            BsonField.Name("__tmp5") -> -\/(ExprOp.DocField(BsonField.Name("__tmp1"))))),
          IncludeId)))

      lb must_== ExprOp.DocField(BsonField.Name("__tmp4"))
      rb must_== ExprOp.DocField(BsonField.Name("__tmp5"))
    }
  }

  "finalize" should {
    import Js._

    "coalesce previous projection into a map" in {
      val readZips = $read(Collection("zips"))
      val given = chain(
        readZips,
        $project(Reshape.Doc(ListMap(
          BsonField.Name("value") -> -\/(ExprOp.DocVar.ROOT()))),
          IncludeId),
        $map($Map.mapNOP))

      val expected = chain(
        readZips,
        $map($Map.compose(
          $Map.mapNOP,
          $Map.mapMap("value",
            AnonObjDecl(List(
              "value" -> Ident("value")))))))

      Workflow.finalize(given) must beTree(expected)
    }

    "coalesce previous projection into a flatMap" in {
      val readZips = $read(Collection("zips"))
      val given = chain(
        readZips,
        $project(Reshape.Doc(ListMap(
          BsonField.Name("value") -> -\/(ExprOp.DocVar.ROOT()))),
          IncludeId),
        $flatMap(
          AnonFunDecl(List("key", "value"), List(
            VarDef(List("rez" -> AnonElem(Nil))),
            ForIn(
              Ident("attr"),
              Access(Ident("value"), Str("value")),
              Call(
                Select(Ident("rez"), "push"),
                List(
                  AnonElem(List(
                    Call(Ident("ObjectId"), Nil),
                    Access(
                      Access(Ident("value"), Str("value")),
                      Ident("attr"))))))),
            Return(Ident("rez"))))))

      val expected = chain(
        readZips,
        $flatMap($Map.compose(
          AnonFunDecl(List("key", "value"), List(
            VarDef(List("rez" -> AnonElem(Nil))),
            ForIn(
              Ident("attr"),
              Access(Ident("value"), Str("value")),
              Call(
                Select(Ident("rez"), "push"),
                List(
                  AnonElem(List(
                    Call(Ident("ObjectId"), Nil),
                    Access(
                      Access(Ident("value"), Str("value")),
                      Ident("attr"))))))),
            Return(Ident("rez")))),
          $Map.mapMap("value",
            AnonObjDecl(List(
              "value" -> Ident("value")))))))

      Workflow.finalize(given) must beTree(expected)
    }

    "convert previous projection before a reduce" in {
      val readZips = $read(Collection("zips"))
      val given = chain(
        readZips,
        $project(Reshape.Doc(ListMap(
          BsonField.Name("value") -> -\/(ExprOp.DocVar.ROOT()))),
          IncludeId),
        $reduce($Reduce.reduceNOP))

      val expected = chain(
        readZips,
        $map($Map.mapMap("value",
          AnonObjDecl(List(
            "value" -> Ident("value"))))),
        $reduce($Reduce.reduceNOP))

      Workflow.finalize(given) must beTree(expected)
    }

    "coalesce previous unwind into a map" in {
      val readZips = $read(Collection("zips"))
      val given = chain(
        readZips,
        $unwind(ExprOp.DocVar.ROOT(BsonField.Name("loc"))),
        $map($Map.mapNOP))

      val expected = chain(
        readZips,
        $flatMap($FlatMap.mapCompose(
          $Map.mapNOP,
          AnonFunDecl(List("key", "value"), List(
            VarDef(List("each" -> AnonObjDecl(Nil))),
            ForIn(Ident("attr"), Ident("value"),
              If(
                Call(Select(Ident("value"), "hasOwnProperty"), List(
                  Ident("attr"))),
                BinOp("=",
                  Access(Ident("each"), Ident("attr")),
                  Access(Ident("value"), Ident("attr"))),
                None)),
            Return(
              Call(Select(Select(Js.Ident("value"), "loc"), "map"), List(
                AnonFunDecl(List("elem"), List(
                  BinOp("=", Select(Ident("each"), "loc"), Ident("elem")),
                  Return(
                    AnonElem(List(
                      Call(Ident("ObjectId"), Nil),
                      Ident("each"))))))))))))))
      Workflow.finalize(given) must beTree(expected)
    }

    "coalesce previous unwind into a flatMap" in {
      val readZips = $read(Collection("zips"))
      val given = chain(
        readZips,
        $unwind(ExprOp.DocVar.ROOT(BsonField.Name("loc"))),
        $flatMap(
          AnonFunDecl(List("key", "value"), List(
            VarDef(List("rez" -> AnonElem(Nil))),
            ForIn(
              Ident("attr"),
              Access(Ident("value"), Str("value")),
              Call(
                Select(Ident("rez"), "push"),
                List(
                  AnonElem(List(
                    Call(Ident("ObjectId"), Nil),
                    Access(
                      Access(Ident("value"), Str("value")),
                      Ident("attr"))))))),
            Return(Ident("rez"))))))

      val expected = chain(
        readZips,
        $flatMap($FlatMap.kleisliCompose(
          AnonFunDecl(List("key", "value"), List(
            VarDef(List("rez" -> AnonElem(Nil))),
            ForIn(
              Ident("attr"),
              Access(Ident("value"), Str("value")),
              Call(
                Select(Ident("rez"), "push"),
                List(
                  AnonElem(List(
                    Call(Ident("ObjectId"), Nil),
                    Access(
                      Access(Ident("value"), Str("value")),
                      Ident("attr"))))))),
            Return(Ident("rez")))),
          AnonFunDecl(List("key", "value"), List(
            VarDef(List("each" -> AnonObjDecl(Nil))),
            ForIn(Ident("attr"), Ident("value"),
              If(
                Call(Select(Ident("value"), "hasOwnProperty"), List(
                  Ident("attr"))),
                BinOp("=",
                  Access(Ident("each"), Ident("attr")),
                  Access(Ident("value"), Ident("attr"))),
                None)),
            Return(
              Call(Select(Select(Js.Ident("value"), "loc"), "map"), List(
                AnonFunDecl(List("elem"), List(
                  BinOp("=", Select(Ident("each"), "loc"), Ident("elem")),
                  Return(
                    AnonElem(List(
                      Call(Ident("ObjectId"), Nil),
                      Ident("each"))))))))))))))
      Workflow.finalize(given) must beTree(expected)
    }

    "convert previous unwind before a reduce" in {
      val readZips = $read(Collection("zips"))
      val given = chain(
        readZips,
        $unwind(ExprOp.DocVar.ROOT(BsonField.Name("loc"))),
        $reduce($Reduce.reduceNOP))

      val expected = chain(
        readZips,
        $flatMap(
          AnonFunDecl(List("key", "value"), List(
            VarDef(List("each" -> AnonObjDecl(Nil))),
            ForIn(Ident("attr"), Ident("value"),
              If(
                Call(Select(Ident("value"), "hasOwnProperty"), List(
                  Ident("attr"))),
                BinOp("=",
                  Access(Ident("each"), Ident("attr")),
                  Access(Ident("value"), Ident("attr"))),
                None)),
            Return(
              Call(Select(Select(Js.Ident("value"), "loc"), "map"), List(
                AnonFunDecl(List("elem"), List(
                  BinOp("=", Select(Ident("each"), "loc"), Ident("elem")),
                  Return(
                    AnonElem(List(
                      Call(Ident("ObjectId"), Nil),
                      Ident("each")))))))))))),
        $reduce($Reduce.reduceNOP))
      Workflow.finalize(given) must beTree(expected)
    }

    "patch $FoldLeft" in {
      val readZips = $read(Collection("zips"))
      val given = $foldLeft(readZips, readZips)

      val expected = $foldLeft(
        chain(readZips, $project(Reshape.Doc(ListMap(
          BsonField.Name("value") -> -\/(ExprOp.DocVar.ROOT()))),
          IncludeId)),
        chain(readZips, $reduce($Reduce.reduceFoldLeft)))

      Workflow.finalize(given) must beTree(expected)
    }

    "patch $FoldLeft with existing reduce" in {
      val readZips = $read(Collection("zips"))
      val given = $foldLeft(
        readZips,
        chain(readZips, $reduce($Reduce.reduceNOP)))

      val expected = $foldLeft(
        chain(
          readZips,
          $project(Reshape.Doc(ListMap(
            BsonField.Name("value") -> -\/(ExprOp.DocVar.ROOT()))),
            IncludeId)),
        chain(readZips, $reduce($Reduce.reduceNOP)))

      Workflow.finalize(given) must beTree(expected)
    }
  }
  
  "finish" should {
    "preserve field in array shape referenced by index" in {
      val given = chain(
        $read(Collection("zips")),
        $project(Reshape.Doc(ListMap(
          BsonField.Name("lEft") -> \/- (Reshape.Arr(ListMap(
            BsonField.Index(0) -> -\/ (ExprOp.DocField(BsonField.Name("foo")))))),
          BsonField.Name("rIght") -> -\/ (ExprOp.DocField(BsonField.Name("bar"))))),
          IncludeId),
        $sort(NonEmptyList(
          BsonField.Name("lEft") \ BsonField.Index(0) -> Ascending)),
        $project(Reshape.Doc(ListMap(
          BsonField.Name("bar") -> -\/ (ExprOp.DocField(BsonField.Name("rIght"))))),
          IncludeId))
          
      finish(given) must beTree(given)
    }
        
    "preserve field in Doc shape referenced by index" in {
      val given = chain(
        $read(Collection("zips")),
        $project(Reshape.Doc(ListMap(
          BsonField.Name("lEft") -> \/- (Reshape.Doc(ListMap(
            BsonField.Name("0") -> -\/ (ExprOp.DocField(BsonField.Name("foo")))))),
          BsonField.Name("rIght") -> -\/ (ExprOp.DocField(BsonField.Name("bar"))))),
          IncludeId),
        $sort(NonEmptyList(
          BsonField.Name("lEft") \ BsonField.Index(0) -> Ascending)),  // possibly a questionable reference
        $project(Reshape.Doc(ListMap(
          BsonField.Name("bar") -> -\/ (ExprOp.DocField(BsonField.Name("rIght"))))),
          IncludeId))
          
      finish(given) must beTree(given)
    }.pendingUntilFixed("it's not clear that this is actually wrong")
  }

  "crush" should {
    import WorkflowTask._

    "convert $match with $where into map/reduce" in {
      crush(chain(
        $read(Collection("zips")),
        $match(Selector.Where(Js.BinOp("<",
          Js.Select(Js.Select(Js.Ident("this"), "city"), "length"),
          Js.Num(4, false)))))) must_==
      ((ExprOp.DocField(BsonField.Name("value")),
        MapReduceTask(ReadTask(Collection("zips")),
          MapReduce($Map.mapFn($Map.mapNOP), $Reduce.reduceNOP,
            selection = Some(Selector.Where(Js.BinOp("<",
              Js.Select(Js.Select(Js.Ident("this"), "city"), "length"),
              Js.Num(4, false))))))))
    }

    "always pipeline unconverted aggregation ops" in {
      crush(chain(
        $read(Collection("zips")),
        $group(
          Grouped(ListMap(
            BsonField.Name("__sd_tmp_1") ->
              ExprOp.Push(ExprOp.DocField(BsonField.Name("lEft"))))),
          -\/ (ExprOp.Literal(Bson.Int32(1)))),
        $project(Reshape.Doc(ListMap(
          BsonField.Name("a") -> -\/(ExprOp.Include),
          BsonField.Name("b") -> -\/(ExprOp.Include),
          BsonField.Name("equal?") -> -\/(ExprOp.Eq(
            ExprOp.DocField(BsonField.Name("a")),
            ExprOp.DocField(BsonField.Name("b")))))),
          IncludeId),
        $match(Selector.Doc(
          BsonField.Name("equal") ->
            Selector.Literal(Bson.Bool(true)))),
        $sort(NonEmptyList(BsonField.Name("a") -> Descending)),
        $limit(100),
        $skip(5),
        $project(Reshape.Doc(ListMap(
          BsonField.Name("a") -> -\/(ExprOp.Include),
          BsonField.Name("b") -> -\/(ExprOp.Include))),
          IncludeId))) must_==
      ((ExprOp.DocVar.ROOT(),
        PipelineTask(ReadTask(Collection("zips")),
          List(
            $Group((),
              Grouped(ListMap(
                BsonField.Name("__sd_tmp_1") ->
                  ExprOp.Push(ExprOp.DocField(BsonField.Name("lEft"))))),
              -\/ (ExprOp.Literal(Bson.Null))),
            $Project((),
              Reshape.Doc(ListMap(
                BsonField.Name("a") -> -\/(ExprOp.Include),
                BsonField.Name("b") -> -\/(ExprOp.Include),
                BsonField.Name("equal?") -> -\/(ExprOp.Eq(
                  ExprOp.DocField(BsonField.Name("a")),
                  ExprOp.DocField(BsonField.Name("b")))))),
              IncludeId),
            $Match((),
              Selector.Doc(
                BsonField.Name("equal") -> Selector.Literal(Bson.Bool(true)))),
            $Sort((), NonEmptyList(BsonField.Name("a") -> Descending)),
            $Limit((), 100),
            $Skip((), 5),
            $Project((),
              Reshape.Doc(ListMap(
                BsonField.Name("a") -> -\/(ExprOp.Include),
                BsonField.Name("b") -> -\/(ExprOp.Include))),
              IncludeId)))))
    }

    "create maximal map/reduce" in {
      crush(chain(
        $read(Collection("zips")),
        $match(Selector.Doc(
          BsonField.Name("loc") \ BsonField.Index(0) ->
            Selector.Lt(Bson.Int64(-73)))),
        $sort(NonEmptyList(BsonField.Name("city") -> Descending)),
        $limit(100),
        $map($Map.mapMap("value",
          Js.Access(Js.Ident("value"), Js.Num(0, false)))),
        $reduce($Reduce.reduceFoldLeft),
        $map($Map.mapMap("value", Js.Ident("value"))))) must_==
      ((ExprOp.DocField(BsonField.Name("value")),
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
              Js.Ident("value"))))))))
    }

    "create maximal map/reduce with flatMap" in {
      crush(chain(
        $read(Collection("zips")),
        $match(Selector.Doc(
          BsonField.Name("loc") \ BsonField.Index(0) ->
            Selector.Lt(Bson.Int64(-73)))),
        $sort(NonEmptyList(BsonField.Name("city") -> Descending)),
        $limit(100),
        $flatMap(Js.AnonFunDecl(List("key", "value"), List(
          Js.AnonElem(List(
            Js.AnonElem(List(Js.Ident("key"), Js.Ident("value")))))))),
        $reduce($Reduce.reduceFoldLeft),
        $map($Map.mapMap("value", Js.Ident("value"))))) must_==
      ((ExprOp.DocField(BsonField.Name("value")),
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
              Js.Ident("value"))))))))
    }

    "create map/reduce without map" in {
      crush(chain(
        $read(Collection("zips")),
        $match(Selector.Doc(
          BsonField.Name("loc") \ BsonField.Index(0) ->
            Selector.Lt(Bson.Int64(-73)))),
        $sort(NonEmptyList(BsonField.Name("city") -> Descending)),
        $limit(100),
        $reduce($Reduce.reduceFoldLeft),
        $map($Map.mapMap("value", Js.Ident("value"))))) must_==
      ((ExprOp.DocField(BsonField.Name("value")),
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
              Js.Ident("value"))))))))
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
          Reshape.Doc(ListMap(
            BsonField.Name("bar") -> -\/ (ExprOp.DocField(BsonField.Name("baz"))))),
          IncludeId))

      render(op) must_==
        """Chain
          |├─ $Read(foo)
          |╰─ $Project
          |   ├─ Name(bar -> $baz)
          |   ╰─ IncludeId""".stripMargin
    }

    "render array project" in {
      val op = chain(readFoo,
        $project(
          Reshape.Arr(ListMap(
            BsonField.Index(0) -> -\/ (ExprOp.DocField(BsonField.Name("baz"))))),
          IncludeId))

      render(op) must_==
        """Chain
          |├─ $Read(foo)
          |╰─ $Project
          |   ├─ Index(0 -> $baz)
          |   ╰─ IncludeId""".stripMargin
    }

    "render nested project" in {
      val op = chain(readFoo,
        $project(
          Reshape.Doc(ListMap(
            BsonField.Name("bar") -> \/- (Reshape.Arr(ListMap(
              BsonField.Index(0) -> -\/ (ExprOp.DocField(BsonField.Name("baz")))))))),
          IncludeId))

      render(op) must_==
        """Chain
          |├─ $Read(foo)
          |╰─ $Project
          |   ├─ Name(bar)
          |   │  ╰─ Index(0 -> $baz)
          |   ╰─ IncludeId""".stripMargin
    }

    "render map/reduce ops" in {
      val op = chain(readFoo,
        $map(
          Js.AnonFunDecl(List("key"), Nil)),
        $project( 
          Reshape.Doc(ListMap(
            BsonField.Name("bar") -> -\/ (ExprOp.DocField(BsonField.Name("baz"))))),
          IncludeId),
        $flatMap(
          Js.AnonFunDecl(List("key"), Nil)),
        $reduce(
          Js.AnonFunDecl(List("key", "values"),
            List(Js.Return(Js.Access(Js.Ident("values"), Js.Num(1, false)))))))

      render(op) must_==
        """Chain
          |├─ $Read(foo)
          |├─ $Map
          |│  ╰─ JavaScript(function (key) {})
          |├─ $Project
          |│  ├─ Name(bar -> $baz)
          |│  ╰─ IncludeId
          |├─ $FlatMap
          |│  ╰─ JavaScript(function (key) {})
          |╰─ $Reduce
          |   ╰─ JavaScript(function (key, values) {
          |                   return values[1];
          |                 })""".stripMargin
    }

    "render unchained" in {
      val op = 
        $foldLeft(
          chain(readFoo,
            $project( 
              Reshape.Doc(ListMap(
                BsonField.Name("bar") -> -\/ (ExprOp.DocField(BsonField.Name("baz"))))),
              IncludeId)),
          chain(readFoo,
            $map(
              Js.AnonFunDecl(List("key"), Nil)),
            $reduce($Reduce.reduceNOP)))

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
        |   │  ╰─ JavaScript(function (key) {})
        |   ╰─ $Reduce
        |      ╰─ JavaScript(function (key, values) {
        |                      return values[0];
        |                    })""".stripMargin
    }
  }
}
