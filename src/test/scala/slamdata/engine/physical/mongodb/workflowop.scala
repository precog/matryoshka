package slamdata.engine.physical.mongodb

import org.specs2.mutable._

import scala.collection.immutable.ListMap
import scalaz._, Scalaz._

import slamdata.engine.{RenderTree, Terminal, NonTerminal, TreeMatchers}
import slamdata.engine.fp._

class WorkflowOpSpec extends Specification with TreeMatchers {
  import WorkflowOp._
  import PipelineOp._
  import IdHandling._

  val readFoo = ReadOp(Collection("foo"))

  "smart constructors" should {
    "put match before sort" in {
      val given = chain(
        readFoo,
        sortOp(NonEmptyList(BsonField.Name("city") -> Descending)),
        matchOp(Selector.Doc(
          BsonField.Name("pop") -> Selector.Gte(Bson.Int64(1000)))))
      val expected = chain(
        readFoo,
        matchOp(Selector.Doc(
          BsonField.Name("pop") -> Selector.Gte(Bson.Int64(1000)))),
        sortOp(NonEmptyList(BsonField.Name("city") -> Descending)))

      given must_== expected
    }

    "choose smallest limit" in {
      val expected = chain(readFoo, limitOp(5))
      chain(readFoo, limitOp(10), limitOp(5)) must_== expected
      chain(readFoo, limitOp(5), limitOp(10)) must_== expected
    }

    "sum skips" in {
      chain(readFoo, skipOp(10), skipOp(5)) must_== chain(readFoo, skipOp(15))
    }

    "flatten foldLefts when possible" in {
      val given = foldLeftOp(
        foldLeftOp(
          readFoo,
          readOp(Collection("zips"))),
        readOp(Collection("olympics")))
      val expected = foldLeftOp(
        readFoo,
        readOp(Collection("zips")),
        readOp(Collection("olympics")))

      given must_== expected
    }
    
    "flatten project into group/unwind" in {
      val given = chain(
        readFoo,
        groupOp(
          Grouped(ListMap(
            BsonField.Name("value") -> ExprOp.Push(ExprOp.DocField(BsonField.Name("rIght"))))),
            -\/ (ExprOp.DocField(BsonField.Name("lEft")))),
        unwindOp(ExprOp.DocField(BsonField.Name("value"))),
        projectOp(Reshape.Doc(ListMap(
          BsonField.Name("city") -> -\/ (ExprOp.DocField(BsonField.Name("value") \ BsonField.Name("city"))))),
          IncludeId))
        
      val expected = chain(
        readFoo,
        groupOp(
          Grouped(ListMap(
            BsonField.Name("city") -> ExprOp.Push(ExprOp.DocField(BsonField.Name("rIght") \ BsonField.Name("city"))))),
            -\/ (ExprOp.DocField(BsonField.Name("lEft")))),
        unwindOp(ExprOp.DocField(BsonField.Name("city"))))
      
      given must beTree(expected: WorkflowOp)
    }
    
    "not flatten project into group/unwind with _id excluded" in {
      val given = chain(
        readFoo,
        groupOp(
          Grouped(ListMap(
            BsonField.Name("value") -> ExprOp.Push(ExprOp.DocField(BsonField.Name("rIght"))))),
            -\/ (ExprOp.DocField(BsonField.Name("lEft")))),
        unwindOp(ExprOp.DocField(BsonField.Name("value"))),
        projectOp(Reshape.Doc(ListMap(
          BsonField.Name("city") -> -\/ (ExprOp.DocField(BsonField.Name("value") \ BsonField.Name("city"))))),
          ExcludeId))
      
      given must beTree(given: WorkflowOp)
    }

    "resolve `Include`" in {
      chain(readOp(Collection("zips")),
        projectOp(Reshape.Doc(ListMap(
          BsonField.Name("bar") -> -\/(ExprOp.Include))),
          ExcludeId),
        projectOp(Reshape.Doc(ListMap(
          BsonField.Name("_id") ->
            -\/(ExprOp.DocField(BsonField.Name("bar"))))),
          IncludeId)) must_==
      chain(readOp(Collection("zips")),
        projectOp(Reshape.Doc(ListMap(
          BsonField.Name("_id") ->
            -\/(ExprOp.DocField(BsonField.Name("bar"))))),
          IncludeId))
    }.pendingUntilFixed("#385")

    "traverse `Include`" in {
      chain(readOp(Collection("zips")),
        projectOp(Reshape.Doc(ListMap(
          BsonField.Name("bar") ->
            -\/(ExprOp.Divide(
              ExprOp.DocField(BsonField.Name("baz")),
              ExprOp.Literal(Bson.Int32(92)))))),
          IncludeId),
        projectOp(Reshape.Doc(ListMap(
          BsonField.Name("bar") -> -\/(ExprOp.Include))),
          IncludeId)) must_==
      chain(readOp(Collection("zips")),
        projectOp(Reshape.Doc(ListMap(
          BsonField.Name("bar") -> -\/(ExprOp.Divide(
            ExprOp.DocField(BsonField.Name("baz")),
            ExprOp.Literal(Bson.Int32(92)))))),
          IncludeId))
    }.pendingUntilFixed("#385")

    "resolve implied `_id`" in {
      chain(readOp(Collection("zips")),
        projectOp(Reshape.Doc(ListMap(
          BsonField.Name("bar") -> -\/(ExprOp.DocField(BsonField.Name("bar"))),
          BsonField.Name("_id") ->
            -\/(ExprOp.DocField(BsonField.Name("baz"))))),
          IncludeId),
        projectOp(Reshape.Doc(ListMap(
          BsonField.Name("bar") ->
            -\/(ExprOp.DocField(BsonField.Name("bar"))))),
          IncludeId)) must_==
      chain(readOp(Collection("zips")),
        projectOp(Reshape.Doc(ListMap(
          BsonField.Name("bar") -> -\/(ExprOp.DocField(BsonField.Name("bar"))),
          BsonField.Name("_id") ->
            -\/(ExprOp.DocField(BsonField.Name("baz"))))),
          IncludeId))
    }.pendingUntilFixed("#386")

    "not resolve excluded `_id`" in {
      chain(readOp(Collection("zips")),
        projectOp(Reshape.Doc(ListMap(
          BsonField.Name("bar") -> -\/(ExprOp.DocField(BsonField.Name("bar"))),
          BsonField.Name("_id") ->
            -\/(ExprOp.DocField(BsonField.Name("baz"))))),
          IncludeId),
        projectOp(Reshape.Doc(ListMap(
          BsonField.Name("bar") ->
            -\/(ExprOp.DocField(BsonField.Name("bar"))))),
          ExcludeId)) must_==
      chain(readOp(Collection("zips")),
        projectOp(Reshape.Doc(ListMap(
          BsonField.Name("bar") ->
            -\/(ExprOp.DocField(BsonField.Name("bar"))))),
          ExcludeId))
    }
  }

  "merge" should {
    "coalesce pure ops" in {
      pureOp(Bson.Int32(3)) merge pureOp(Bson.Int64(-3)) must_==
      (ExprOp.DocField(BsonField.Name("lEft")),
        ExprOp.DocField(BsonField.Name("rIght"))) ->
      pureOp(Bson.Doc(ListMap(
        "lEft"  -> Bson.Int32(3),
        "rIght" -> Bson.Int64(-3))))
    }

    "unify trivial reads" in {
      readFoo merge readFoo must_==
        (ExprOp.DocVar.ROOT(), ExprOp.DocVar.ROOT()) -> readFoo
    }

    "fold different reads" in {
      readFoo merge readOp(Collection("zips")) must_==
        (ExprOp.DocField(BsonField.Name("lEft")),
          ExprOp.DocField(BsonField.Name("rIght"))) ->
        foldLeftOp(
          chain(
            readFoo,
            projectOp(Reshape.Doc(ListMap(
              BsonField.Name("lEft") -> -\/(ExprOp.DocVar.ROOT()))),
              IncludeId)),
          chain(
            readOp(Collection("zips")),
            projectOp(Reshape.Doc(ListMap(
              BsonField.Name("rIght") -> -\/(ExprOp.DocVar.ROOT()))),
              IncludeId)))
    }

    "put shape-preserving before non-" in {
      val left = chain(
        readFoo,
        projectOp(Reshape.Doc(ListMap(
          BsonField.Name("city") ->
            -\/(ExprOp.DocField(BsonField.Name("city"))))),
          IncludeId))
      val right = chain(
        readFoo,
        matchOp(Selector.Doc(
          BsonField.Name("bar") -> Selector.Gt(Bson.Int64(10)))))
      left merge right must_==
      (ExprOp.DocVar.ROOT(), ExprOp.DocVar.ROOT()) ->
        chain(
          readFoo,
          matchOp(Selector.Doc(
            BsonField.Name("bar") -> Selector.Gt(Bson.Int64(10)))),
          projectOp(Reshape.Doc(ListMap(
            BsonField.Name("city") ->
              -\/(ExprOp.DocField(BsonField.Name("city"))))),
            IncludeId))
    }

    "coalesce unwinds on same field" in {
      val left = chain(
        readFoo,
        unwindOp(ExprOp.DocField(BsonField.Name("city"))))
      val right = chain(
        readFoo,
        unwindOp(ExprOp.DocField(BsonField.Name("city"))))
      left merge right must_==
      (ExprOp.DocVar.ROOT(), ExprOp.DocVar.ROOT()) ->
        chain(readFoo, unwindOp(ExprOp.DocField(BsonField.Name("city"))))
    }

    "maintain unwinds on separate fields" in {
      val left = chain(
        readFoo,
        unwindOp(ExprOp.DocField(BsonField.Name("city"))))
      val right = chain(
        readFoo,
        unwindOp(ExprOp.DocField(BsonField.Name("loc"))))
      left merge right must_==
      (ExprOp.DocVar.ROOT(), ExprOp.DocVar.ROOT()) ->
        chain(
          readFoo,
          unwindOp(ExprOp.DocField(BsonField.Name("city"))),
          unwindOp(ExprOp.DocField(BsonField.Name("loc"))))
    }
    
    "don’t coalesce unwinds on same _named_ field with different values" in {
      val left = chain(
        readFoo,
        unwindOp(ExprOp.DocField(BsonField.Name("city"))))
      val right = chain(
        readFoo,
        projectOp(Reshape.Doc(ListMap(
          BsonField.Name("city") ->
            -\/(ExprOp.DocField(BsonField.Name("_id"))),
          BsonField.Name("loc") ->
            -\/(ExprOp.DocField(BsonField.Name("loc"))))),
          IncludeId),
        unwindOp(ExprOp.DocField(BsonField.Name("city"))))
      left merge right must_==
      (ExprOp.DocField(BsonField.Name("rIght")),
        ExprOp.DocField(BsonField.Name("lEft"))) ->
        chain(
          readFoo,
          projectOp(Reshape.Doc(ListMap(
            BsonField.Name("lEft") -> \/-(Reshape.Doc(ListMap(
              BsonField.Name("city") ->
                -\/(ExprOp.DocField(BsonField.Name("_id"))),
              BsonField.Name("loc") ->
                -\/(ExprOp.DocField(BsonField.Name("loc")))))),
            BsonField.Name("rIght") -> -\/(ExprOp.DocVar.ROOT()))),
            IncludeId),
          unwindOp(ExprOp.DocField(BsonField.Name("rIght") \ BsonField.Name("city"))),
          unwindOp(ExprOp.DocField(BsonField.Name("lEft") \ BsonField.Name("city"))))
    }

    "coalesce non-conflicting projections" in {
      val left = chain(
        readFoo,
        projectOp(Reshape.Doc(ListMap(
          BsonField.Name("city") ->
            -\/(ExprOp.DocField(BsonField.Name("city"))))),
          IncludeId),
        unwindOp(ExprOp.DocField(BsonField.Name("city"))))
      val right = chain(
        readFoo,
        projectOp(Reshape.Doc(ListMap(
          BsonField.Name("city") ->
            -\/(ExprOp.DocField(BsonField.Name("city"))),
          BsonField.Name("loc") ->
            -\/(ExprOp.DocField(BsonField.Name("loc"))))),
          IncludeId),
        unwindOp(ExprOp.DocField(BsonField.Name("city"))))
      left merge right must_==
      (ExprOp.DocField(BsonField.Name("rIght")),
        ExprOp.DocField(BsonField.Name("lEft"))) ->
        chain(
          readFoo,
          projectOp(Reshape.Doc(ListMap(
            BsonField.Name("city") ->
              -\/(ExprOp.DocField(BsonField.Name("city"))),
            BsonField.Name("loc") ->
              -\/(ExprOp.DocField(BsonField.Name("loc"))))),
            IncludeId),
          unwindOp(ExprOp.DocField(BsonField.Name("city"))))
    }.pendingUntilFixed("#388")

    "merge group by constant with project" in {
      val left = chain(readFoo, 
                  groupOp(
                    Grouped(ListMap()),
                    -\/ (ExprOp.Literal(Bson.Int32(1)))))
      val right = chain(readFoo,
                    projectOp(Reshape.Doc(ListMap(
                      BsonField.Name("city") -> -\/ (ExprOp.DocField(BsonField.Name("city"))))),
                      IncludeId))
          
      val ((lb, rb), op) = left merge right
      
      lb must_== ExprOp.DocVar.ROOT()
      rb must_== ExprOp.DocField(BsonField.Name("__sd_tmp_1"))
      op must_== 
          chain(readFoo,
            projectOp(Reshape.Doc(ListMap(
              BsonField.Name("lEft") -> \/-(Reshape.Doc(ListMap(
                BsonField.Name("city") -> -\/(ExprOp.DocField(BsonField.Name("city")))))),
              BsonField.Name("rIght") -> -\/ (ExprOp.DocVar.ROOT()))),
              IncludeId),
            groupOp(
              Grouped(ListMap(
                 BsonField.Name("__sd_tmp_1") -> ExprOp.Push(ExprOp.DocField(BsonField.Name("lEft"))))),
              -\/ (ExprOp.Literal(Bson.Int32(1)))),
            unwindOp(
              ExprOp.DocField(BsonField.Name("__sd_tmp_1"))))
    }

    "merge groups" in {
      val left = chain(readFoo, 
                  groupOp(
                    Grouped(ListMap(
                      BsonField.Name("value") -> ExprOp.Sum(ExprOp.Literal(Bson.Int32(1))))),
                    -\/ (ExprOp.Literal(Bson.Int32(1)))))
      val right = chain(readFoo, 
                  groupOp(
                    Grouped(ListMap(
                      BsonField.Name("value") -> ExprOp.Sum(ExprOp.DocField(BsonField.Name("bar"))))),
                    -\/ (ExprOp.Literal(Bson.Int32(1)))))
          
      val ((lb, rb), op) = left merge right
      
      lb must_== ExprOp.DocField(BsonField.Name("lEft"))
      rb must_== ExprOp.DocField(BsonField.Name("rIght"))
      op must beTree(
          chain(readFoo,
            groupOp(
              Grouped(ListMap(
                 BsonField.Name("__sd_tmp_1") -> ExprOp.Sum(ExprOp.Literal(Bson.Int32(1))),
                 BsonField.Name("__sd_tmp_2") -> ExprOp.Sum(ExprOp.DocField(BsonField.Name("bar"))))),
              -\/ (ExprOp.Literal(Bson.Int32(1)))),
            projectOp(Reshape.Doc(ListMap(
              BsonField.Name("lEft") -> \/- (Reshape.Doc(ListMap(
                BsonField.Name("value") -> -\/ (ExprOp.DocField(BsonField.Name("__sd_tmp_1")))))),
              BsonField.Name("rIght") -> \/- (Reshape.Doc(ListMap(
                BsonField.Name("value") -> -\/ (ExprOp.DocField(BsonField.Name("__sd_tmp_2")))))))),
              IgnoreId)))
    }

    "merge groups under unwind" in {
      val left = chain(readFoo, 
                  groupOp(
                    Grouped(ListMap(
                      BsonField.Name("city") -> ExprOp.Push(ExprOp.DocField(BsonField.Name("city"))))),
                    -\/ (ExprOp.Literal(Bson.Int32(1)))),
                    unwindOp(ExprOp.DocField(BsonField.Name("city"))))
      val right = chain(readFoo, 
                  groupOp(
                    Grouped(ListMap(
                      BsonField.Name("total") -> ExprOp.Sum(ExprOp.Literal(Bson.Int32(1))))),
                    -\/ (ExprOp.Literal(Bson.Int32(1)))))
          
      val ((lb, rb), op) = left merge right
      
      lb must_== ExprOp.DocField(BsonField.Name("lEft"))
      rb must_== ExprOp.DocField(BsonField.Name("rIght"))
      op must beTree( 
          chain(readFoo,
            groupOp(
              Grouped(ListMap(
                 BsonField.Name("__sd_tmp_1") -> ExprOp.Push(ExprOp.DocField(BsonField.Name("city"))),
                 BsonField.Name("__sd_tmp_2") -> ExprOp.Sum(ExprOp.Literal(Bson.Int32(1))))),
              -\/ (ExprOp.Literal(Bson.Int32(1)))),
            projectOp(Reshape.Doc(ListMap(
              BsonField.Name("lEft") -> \/- (Reshape.Doc(ListMap(
                BsonField.Name("city") -> -\/ (ExprOp.DocField(BsonField.Name("__sd_tmp_1")))))),
              BsonField.Name("rIght") -> \/- (Reshape.Doc(ListMap(
                BsonField.Name("total") -> -\/ (ExprOp.DocField(BsonField.Name("__sd_tmp_2")))))))),
              IgnoreId),
            unwindOp(ExprOp.DocField(BsonField.Name("lEft") \ BsonField.Name("city")))))
    }
    
    "merge unwind and project on same group" in {
      val left = chain(readFoo,
        groupOp(
          Grouped(ListMap(
            BsonField.Name("sumA") -> ExprOp.Sum(ExprOp.DocField(BsonField.Name("a"))))),
          -\/ (ExprOp.DocField(BsonField.Name("key")))),
        projectOp(Reshape.Arr(ListMap(
          BsonField.Index(0) -> -\/ (ExprOp.DocField(BsonField.Name("sumA"))))),
          IncludeId))
        
      val right = chain(readFoo,
        groupOp(
          Grouped(ListMap(
            BsonField.Name("b") -> ExprOp.Push(ExprOp.DocField(BsonField.Name("b"))))),
          -\/ (ExprOp.DocField(BsonField.Name("key")))),
        unwindOp(ExprOp.DocField(BsonField.Name("b"))))
        
      
      val ((lb, rb), op) = left merge right
        
      op must beTree(chain(
        readFoo,
        groupOp(
          Grouped(ListMap(
            BsonField.Name("__sd_tmp_1") -> ExprOp.Push(ExprOp.DocField(BsonField.Name("b"))),
            BsonField.Name("__sd_tmp_2") -> ExprOp.Sum(ExprOp.DocField(BsonField.Name("a"))))),
          -\/ (ExprOp.DocField(BsonField.Name("key")))),
        projectOp(Reshape.Doc(ListMap(
          BsonField.Name("lEft") -> \/- (Reshape.Doc(ListMap(
            BsonField.Name("b") -> -\/ (ExprOp.DocField(BsonField.Name("__sd_tmp_1")))))),
          BsonField.Name("rIght") -> \/- (Reshape.Doc(ListMap(
            BsonField.Name("sumA") -> -\/ (ExprOp.DocField(BsonField.Name("__sd_tmp_2")))))))),
          IgnoreId),
        unwindOp(ExprOp.DocField(BsonField.Name("lEft") \ BsonField.Name("b"))),
        projectOp(Reshape.Doc(ListMap(
          BsonField.Name("lEft") -> \/- (Reshape.Arr(ListMap(
            BsonField.Index(0) -> -\/ (ExprOp.DocField(BsonField.Name("rIght") \ BsonField.Name("sumA")))))),
          BsonField.Name("rIght") -> -\/ (ExprOp.DocVar.ROOT()))),
          IncludeId)))
    }
  }

  "finalize" should {
    import Js._

    "coalesce previous projection into a map" in {
      val readZips = readOp(Collection("zips"))
      val given = chain(
        readZips,
        projectOp(Reshape.Doc(ListMap(
          BsonField.Name("value") -> -\/(ExprOp.DocVar.ROOT()))),
          IncludeId),
        mapOp(MapOp.mapNOP))

      val expected = chain(
        readZips,
        mapOp(MapOp.compose(
          MapOp.mapNOP,
          MapOp.mapMap("value",
            Call(
              AnonFunDecl(Nil, List(
                VarDef(List("rez" -> AnonObjDecl(Nil))),
                BinOp("=", Access(Ident("rez"),Str("value")), Ident("value")),
                Return(Ident("rez")))),
              Nil)))))
      WorkflowOp.finalize(given) must_== expected
    }

    "coalesce previous projection into a flatMap" in {
      val readZips = readOp(Collection("zips"))
      val given = chain(
        readZips,
        projectOp(Reshape.Doc(ListMap(
          BsonField.Name("value") -> -\/(ExprOp.DocVar.ROOT()))),
          IncludeId),
        flatMapOp(
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
        flatMapOp(MapOp.compose(
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
          MapOp.mapMap("value",
            Call(
              AnonFunDecl(Nil, List(
                VarDef(List("rez" -> AnonObjDecl(Nil))),
                BinOp("=", Access(Ident("rez"),Str("value")), Ident("value")),
                Return(Ident("rez")))),
              Nil)))))
      WorkflowOp.finalize(given) must_== expected
    }

    "convert previous projection before a reduce" in {
      val readZips = readOp(Collection("zips"))
      val given = chain(
        readZips,
        projectOp(Reshape.Doc(ListMap(
          BsonField.Name("value") -> -\/(ExprOp.DocVar.ROOT()))),
          IncludeId),
        reduceOp(ReduceOp.reduceNOP))

      val expected = chain(
        readZips,
        mapOp(MapOp.mapMap("value",
          Call(
            AnonFunDecl(Nil, List(
              VarDef(List("rez" -> AnonObjDecl(Nil))),
              BinOp("=", Access(Ident("rez"),Str("value")), Ident("value")),
              Return(Ident("rez")))),
            Nil))),
        reduceOp(ReduceOp.reduceNOP))
      WorkflowOp.finalize(given) must_== expected
    }

    "coalesce previous unwind into a map" in {
      val readZips = readOp(Collection("zips"))
      val given = chain(
        readZips,
        unwindOp(ExprOp.DocVar.ROOT(BsonField.Name("loc"))),
        mapOp(MapOp.mapNOP))

      val expected = chain(
        readZips,
        flatMapOp(FlatMapOp.mapCompose(
          MapOp.mapNOP,
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
              Call(Select(Access(Js.Ident("value"), Str("loc")), "map"), List(
                AnonFunDecl(List("elem"), List(
                  BinOp("=", Access(Ident("each"), Str("loc")), Ident("elem")),
                  Return(
                    AnonElem(List(
                      Call(Ident("ObjectId"), Nil),
                      Ident("each"))))))))))))))
      WorkflowOp.finalize(given) must_== expected
    }

    "coalesce previous unwind into a flatMap" in {
      val readZips = readOp(Collection("zips"))
      val given = chain(
        readZips,
        unwindOp(ExprOp.DocVar.ROOT(BsonField.Name("loc"))),
        flatMapOp(
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
        flatMapOp(FlatMapOp.kleisliCompose(
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
              Call(Select(Access(Js.Ident("value"), Str("loc")), "map"), List(
                AnonFunDecl(List("elem"), List(
                  BinOp("=", Access(Ident("each"), Str("loc")), Ident("elem")),
                  Return(
                    AnonElem(List(
                      Call(Ident("ObjectId"), Nil),
                      Ident("each"))))))))))))))
      WorkflowOp.finalize(given) must_== expected
    }

    "convert previous unwind before a reduce" in {
      val readZips = readOp(Collection("zips"))
      val given = chain(
        readZips,
        unwindOp(ExprOp.DocVar.ROOT(BsonField.Name("loc"))),
        reduceOp(ReduceOp.reduceNOP))

      val expected = chain(
        readZips,
        flatMapOp(
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
              Call(Select(Access(Js.Ident("value"), Str("loc")), "map"), List(
                AnonFunDecl(List("elem"), List(
                  BinOp("=", Access(Ident("each"), Str("loc")), Ident("elem")),
                  Return(
                    AnonElem(List(
                      Call(Ident("ObjectId"), Nil),
                      Ident("each")))))))))))),
        reduceOp(ReduceOp.reduceNOP))
      WorkflowOp.finalize(given) must_== expected
    }

    "patch FoldLeftOp" in {
      val readZips = readOp(Collection("zips"))
      val given = foldLeftOp(readZips, readZips)

      val expected = foldLeftOp(
        chain(readZips, projectOp(Reshape.Doc(ListMap(
          BsonField.Name("value") -> -\/(ExprOp.DocVar.ROOT()))),
          IncludeId)),
        chain(readZips, reduceOp(ReduceOp.reduceFoldLeft)))

      WorkflowOp.finalize(given) must_== expected
    }

    "patch FoldLeftOp with existing reduce" in {
      val readZips = readOp(Collection("zips"))
      val given = foldLeftOp(
        readZips,
        chain(readZips, reduceOp(ReduceOp.reduceNOP)))

      val expected = foldLeftOp(
        chain(
          readZips,
          projectOp(Reshape.Doc(ListMap(
            BsonField.Name("value") -> -\/(ExprOp.DocVar.ROOT()))),
            IncludeId)),
        chain(readZips, reduceOp(ReduceOp.reduceNOP)))

      WorkflowOp.finalize(given) must_== expected
    }
  }
  
  "finish" should {
    "preserve field in array shape referenced by index" in {
      val given = chain(
        readOp(Collection("zips")),
        projectOp(Reshape.Doc(ListMap(
          BsonField.Name("lEft") -> \/- (Reshape.Arr(ListMap(
            BsonField.Index(0) -> -\/ (ExprOp.DocField(BsonField.Name("foo")))))),
          BsonField.Name("rIght") -> -\/ (ExprOp.DocField(BsonField.Name("bar"))))),
          IncludeId),
        sortOp(NonEmptyList(
          BsonField.Name("lEft") \ BsonField.Index(0) -> Ascending)),
        projectOp(Reshape.Doc(ListMap(
          BsonField.Name("bar") -> -\/ (ExprOp.DocField(BsonField.Name("rIght"))))),
          IncludeId))
          
      given.finish must beTree(given)
    }
        
    "preserve field in Doc shape referenced by index" in {
      val given = chain(
        readOp(Collection("zips")),
        projectOp(Reshape.Doc(ListMap(
          BsonField.Name("lEft") -> \/- (Reshape.Doc(ListMap(
            BsonField.Name("0") -> -\/ (ExprOp.DocField(BsonField.Name("foo")))))),
          BsonField.Name("rIght") -> -\/ (ExprOp.DocField(BsonField.Name("bar"))))),
          IncludeId),
        sortOp(NonEmptyList(
          BsonField.Name("lEft") \ BsonField.Index(0) -> Ascending)),  // possibly a questionable reference
        projectOp(Reshape.Doc(ListMap(
          BsonField.Name("bar") -> -\/ (ExprOp.DocField(BsonField.Name("rIght"))))),
          IncludeId))
          
      given.finish must beTree(given)
    }.pendingUntilFixed("it's not clear that this is actually wrong")
  }

  "crush" should {
    import WorkflowTask._

    "convert $match with $where into map/reduce" in {
      chain(
        readOp(Collection("zips")),
        matchOp(Selector.Where(Js.BinOp("<",
          Js.Select(Js.Select(Js.Ident("this"), "city"), "length"),
          Js.Num(4, false))))).crush must_==
      ((ExprOp.DocField(BsonField.Name("value")),
        MapReduceTask(ReadTask(Collection("zips")),
          MapReduce(MapOp.mapFn(MapOp.mapNOP), ReduceOp.reduceNOP,
            selection = Some(Selector.Where(Js.BinOp("<",
              Js.Select(Js.Select(Js.Ident("this"), "city"), "length"),
              Js.Num(4, false))))))))
    }

    "always pipeline unconverted aggregation ops" in {
      chain(
        readOp(Collection("zips")),
        groupOp(
          Grouped(ListMap(
            BsonField.Name("__sd_tmp_1") ->
              ExprOp.Push(ExprOp.DocField(BsonField.Name("lEft"))))),
          -\/ (ExprOp.Literal(Bson.Int32(1)))),
        projectOp(Reshape.Doc(ListMap(
          BsonField.Name("a") -> -\/(ExprOp.Include),
          BsonField.Name("b") -> -\/(ExprOp.Include),
          BsonField.Name("equal?") -> -\/(ExprOp.Eq(
            ExprOp.DocField(BsonField.Name("a")),
            ExprOp.DocField(BsonField.Name("b")))))),
          IncludeId),
        matchOp(Selector.Doc(
          BsonField.Name("equal") ->
            Selector.Literal(Bson.Bool(true)))),
        sortOp(NonEmptyList(BsonField.Name("a") -> Descending)),
        limitOp(100),
        skipOp(5),
        projectOp(Reshape.Doc(ListMap(
          BsonField.Name("a") -> -\/(ExprOp.Include),
          BsonField.Name("b") -> -\/(ExprOp.Include))),
          IncludeId)).crush must_==
      ((ExprOp.DocVar.ROOT(),
        PipelineTask(ReadTask(Collection("zips")),
          List(
            Group(Grouped(ListMap(
              BsonField.Name("__sd_tmp_1") ->
                ExprOp.Push(ExprOp.DocField(BsonField.Name("lEft"))))),
              -\/ (ExprOp.Literal(Bson.Int32(1)))),
            Project(Reshape.Doc(ListMap(
              BsonField.Name("a") -> -\/(ExprOp.Include),
              BsonField.Name("b") -> -\/(ExprOp.Include),
              BsonField.Name("equal?") -> -\/(ExprOp.Eq(
                ExprOp.DocField(BsonField.Name("a")),
                ExprOp.DocField(BsonField.Name("b")))))),
              IncludeId),
            Match(Selector.Doc(
              BsonField.Name("equal") ->
                Selector.Literal(Bson.Bool(true)))),
            Sort(NonEmptyList(BsonField.Name("a") -> Descending)),
            Limit(100),
            Skip(5),
            Project(Reshape.Doc(ListMap(
              BsonField.Name("a") -> -\/(ExprOp.Include),
              BsonField.Name("b") -> -\/(ExprOp.Include))),
              IncludeId)))))
    }

    "create maximal map/reduce" in {
      chain(
        readOp(Collection("zips")),
        matchOp(Selector.Doc(
          BsonField.Name("loc") \ BsonField.Index(0) ->
            Selector.Lt(Bson.Int64(-73)))),
        sortOp(NonEmptyList(BsonField.Name("city") -> Descending)),
        limitOp(100),
        mapOp(MapOp.mapMap("value",
          Js.Access(Js.Ident("value"), Js.Num(0, false)))),
        reduceOp(ReduceOp.reduceFoldLeft),
        mapOp(MapOp.mapMap("value", Js.Ident("value")))).crush must_==
      ((ExprOp.DocField(BsonField.Name("value")),
        MapReduceTask(ReadTask(Collection("zips")),
          MapReduce(
            MapOp.mapFn(MapOp.mapMap("value",
              Js.Access(Js.Ident("value"), Js.Num(0, false)))),
            ReduceOp.reduceFoldLeft,
            selection = Some(Selector.Doc(
              BsonField.Name("loc") \ BsonField.Index(0) ->
                Selector.Lt(Bson.Int64(-73)))),
            inputSort =
              Some(NonEmptyList(BsonField.Name("city") -> Descending)),
            limit = Some(100),
            finalizer = Some(MapOp.finalizerFn(MapOp.mapMap("value",
              Js.Ident("value"))))))))
    }

    "create maximal map/reduce with flatMap" in {
      chain(
        readOp(Collection("zips")),
        matchOp(Selector.Doc(
          BsonField.Name("loc") \ BsonField.Index(0) ->
            Selector.Lt(Bson.Int64(-73)))),
        sortOp(NonEmptyList(BsonField.Name("city") -> Descending)),
        limitOp(100),
        flatMapOp(Js.AnonFunDecl(List("key", "value"), List(
          Js.AnonElem(List(
            Js.AnonElem(List(Js.Ident("key"), Js.Ident("value")))))))),
        reduceOp(ReduceOp.reduceFoldLeft),
        mapOp(MapOp.mapMap("value", Js.Ident("value")))).crush must_==
      ((ExprOp.DocField(BsonField.Name("value")),
        MapReduceTask(ReadTask(Collection("zips")),
          MapReduce(
            FlatMapOp.mapFn(Js.AnonFunDecl(List("key", "value"), List(
          Js.AnonElem(List(
            Js.AnonElem(List(Js.Ident("key"), Js.Ident("value")))))))),
            ReduceOp.reduceFoldLeft,
            selection = Some(Selector.Doc(
              BsonField.Name("loc") \ BsonField.Index(0) ->
                Selector.Lt(Bson.Int64(-73)))),
            inputSort =
              Some(NonEmptyList(BsonField.Name("city") -> Descending)),
            limit = Some(100),
            finalizer = Some(MapOp.finalizerFn(MapOp.mapMap("value",
              Js.Ident("value"))))))))
    }

    "create map/reduce without map" in {
      chain(
        readOp(Collection("zips")),
        matchOp(Selector.Doc(
          BsonField.Name("loc") \ BsonField.Index(0) ->
            Selector.Lt(Bson.Int64(-73)))),
        sortOp(NonEmptyList(BsonField.Name("city") -> Descending)),
        limitOp(100),
        reduceOp(ReduceOp.reduceFoldLeft),
        mapOp(MapOp.mapMap("value", Js.Ident("value")))).crush must_==
      ((ExprOp.DocField(BsonField.Name("value")),
        MapReduceTask(ReadTask(Collection("zips")),
          MapReduce(
            MapOp.mapFn(MapOp.mapNOP),
            ReduceOp.reduceFoldLeft,
            selection = Some(Selector.Doc(
              BsonField.Name("loc") \ BsonField.Index(0) ->
                Selector.Lt(Bson.Int64(-73)))),
            inputSort =
              Some(NonEmptyList(BsonField.Name("city") -> Descending)),
            limit = Some(100),
            finalizer = Some(MapOp.finalizerFn(MapOp.mapMap("value",
              Js.Ident("value"))))))))
    }
  }

  "RenderTree[WorkflowOp]" should {
    def render(op: WorkflowOp)(implicit RO: RenderTree[WorkflowOp]): String = RO.render(op).draw.mkString("\n")
    
    "render read" in {
      render(readFoo) must_== "ReadOp(foo)"
    }

    "render simple project" in {
      val op = chain(readFoo,
        projectOp( 
          Reshape.Doc(ListMap(
            BsonField.Name("bar") -> -\/ (ExprOp.DocField(BsonField.Name("baz"))))),
          IncludeId))

      render(op) must_==
        """Chain
          |├─ ReadOp(foo)
          |╰─ ProjectOp
          |   ├─ Name(bar -> $baz)
          |   ╰─ IncludeId""".stripMargin
    }

    "render array project" in {
      val op = chain(readFoo,
        projectOp(
          Reshape.Arr(ListMap(
            BsonField.Index(0) -> -\/ (ExprOp.DocField(BsonField.Name("baz"))))),
          IncludeId))

      render(op) must_==
        """Chain
          |├─ ReadOp(foo)
          |╰─ ProjectOp
          |   ├─ Index(0 -> $baz)
          |   ╰─ IncludeId""".stripMargin
    }

    "render nested project" in {
      val op = chain(readFoo,
        projectOp(
          Reshape.Doc(ListMap(
            BsonField.Name("bar") -> \/- (Reshape.Arr(ListMap(
              BsonField.Index(0) -> -\/ (ExprOp.DocField(BsonField.Name("baz")))))))),
          IncludeId))

      render(op) must_==
        """Chain
          |├─ ReadOp(foo)
          |╰─ ProjectOp
          |   ├─ Name(bar)
          |   │  ╰─ Index(0 -> $baz)
          |   ╰─ IncludeId""".stripMargin
    }

    "render map/reduce ops" in {
      val op = chain(readFoo,
        mapOp(
          Js.AnonFunDecl(List("key"), Nil)),
        projectOp( 
          Reshape.Doc(ListMap(
            BsonField.Name("bar") -> -\/ (ExprOp.DocField(BsonField.Name("baz"))))),
          IncludeId),
        flatMapOp(
          Js.AnonFunDecl(List("key"), Nil)),
        reduceOp(
          Js.AnonFunDecl(List("key", "values"),
            List(Js.Return(Js.Access(Js.Ident("values"), Js.Num(1, false)))))))

      render(op) must_==
        """Chain
          |├─ ReadOp(foo)
          |├─ MapOp
          |│  ╰─ JavaScript(function (key) {})
          |├─ ProjectOp
          |│  ├─ Name(bar -> $baz)
          |│  ╰─ IncludeId
          |├─ FlatMapOp
          |│  ╰─ JavaScript(function (key) {})
          |╰─ ReduceOp
          |   ╰─ JavaScript(function (key, values) {
          |                   return values[1];
          |                 })""".stripMargin
    }

    "render unchained" in {
      val op = 
        foldLeftOp(
          chain(readFoo,
            projectOp( 
              Reshape.Doc(ListMap(
                BsonField.Name("bar") -> -\/ (ExprOp.DocField(BsonField.Name("baz"))))),
              IncludeId)),
          chain(readFoo,
            mapOp(
              Js.AnonFunDecl(List("key"), Nil)),
            reduceOp(ReduceOp.reduceNOP)))

      render(op) must_==
      """FoldLeftOp
        |├─ Chain
        |│  ├─ ReadOp(foo)
        |│  ╰─ ProjectOp
        |│     ├─ Name(bar -> $baz)
        |│     ╰─ IncludeId
        |╰─ Chain
        |   ├─ ReadOp(foo)
        |   ├─ MapOp
        |   │  ╰─ JavaScript(function (key) {})
        |   ╰─ ReduceOp
        |      ╰─ JavaScript(function (key, values) {
        |                      return values[0];
        |                    })""".stripMargin
    }
  }
}
