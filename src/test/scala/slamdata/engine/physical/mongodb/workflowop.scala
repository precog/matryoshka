package slamdata.engine.physical.mongodb

import org.specs2.mutable._

import scala.collection.immutable.ListMap
import scalaz._, Scalaz._

import slamdata.engine.{RenderTree, Terminal, NonTerminal}
import slamdata.engine.fp._

class WorkflowOpSpec extends Specification {
  import WorkflowOp._
  import PipelineOp._

  val readFoo = ReadOp(Collection("foo"))
  
  "merge" should {
    "merge trivial reads" in {
      readFoo merge readFoo must_==
        (ExprOp.DocVar.ROOT(), ExprOp.DocVar.ROOT()) -> readFoo
    }
    
    "merge group by constant with project" in {
      val left = chain(readFoo, 
                  groupOp(
                    Grouped(ListMap()),
                    -\/ (ExprOp.Literal(Bson.Int32(1)))))
      val right = chain(readFoo,
                    projectOp(Reshape.Doc(ListMap(
                      BsonField.Name("city") -> -\/ (ExprOp.DocField(BsonField.Name("city")))))))
          
      val ((lb, rb), op) = left merge right
      
      lb must_== ExprOp.DocVar.ROOT()
      rb must_== ExprOp.DocField(BsonField.Name("__sd_tmp_1"))
      op must_== 
          chain(readFoo,
            projectOp(Reshape.Doc(ListMap(
              BsonField.Name("lEft") -> \/-(Reshape.Doc(ListMap(
                BsonField.Name("city") -> -\/(ExprOp.DocField(BsonField.Name("city")))))),
              BsonField.Name("rIght") -> -\/ (ExprOp.DocVar.ROOT())))), 
            groupOp(
              Grouped(ListMap(
                 BsonField.Name("__sd_tmp_1") -> ExprOp.Push(ExprOp.DocField(BsonField.Name("lEft"))))),
              -\/ (ExprOp.Literal(Bson.Int32(1)))),
            unwindOp(
              ExprOp.DocField(BsonField.Name("__sd_tmp_1"))))
    }
  }

  "finalize" should {
    import Js._

    "convert previous projection into a map" in {
      val readZips = readOp(Collection("zips"))
      val given = chain(
        readZips,
        projectOp(Reshape.Doc(ListMap(
          BsonField.Name("value") -> -\/(ExprOp.DocVar.ROOT())))),
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

    "convert previous unwind into a flatMap" in {
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

    "patch FoldLeftOp" in {
      val readZips = readOp(Collection("zips"))
      val given = foldLeftOp(readZips, readZips)

      val expected = foldLeftOp(
        chain(readZips, projectOp(Reshape.Doc(ListMap(
          BsonField.Name("value") -> -\/(ExprOp.DocVar.ROOT()))))),
        chain(readZips, reduceOp(ReduceOp.reduceFoldLeft)))

      WorkflowOp.finalize(given) must_== expected
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
            BsonField.Name("bar") -> -\/ (ExprOp.DocField(BsonField.Name("baz")))))))

      render(op) must_==
        """Chain
          |├─ ReadOp(foo)
          |╰─ ProjectOp
          |   ╰─ Name(bar -> $baz)""".stripMargin
    }

    "render array project" in {
      val op = chain(readFoo,
        projectOp(
          Reshape.Arr(ListMap(
            BsonField.Index(0) -> -\/ (ExprOp.DocField(BsonField.Name("baz")))))))

      render(op) must_==
        """Chain
          |├─ ReadOp(foo)
          |╰─ ProjectOp
          |   ╰─ Index(0 -> $baz)""".stripMargin
    }

    "render nested project" in {
      val op = chain(readFoo,
        projectOp(
          Reshape.Doc(ListMap(
            BsonField.Name("bar") -> \/- (Reshape.Arr(ListMap(
              BsonField.Index(0) -> -\/ (ExprOp.DocField(BsonField.Name("baz"))))))))))

      render(op) must_==
        """Chain
          |├─ ReadOp(foo)
          |╰─ ProjectOp
          |   ╰─ Name(bar)
          |      ╰─ Index(0 -> $baz)""".stripMargin
    }

    "render map/reduce ops" in {
      val op = chain(readFoo,
        mapOp(
          Js.AnonFunDecl(List("key"), Nil)),
        projectOp( 
          Reshape.Doc(ListMap(
            BsonField.Name("bar") -> -\/ (ExprOp.DocField(BsonField.Name("baz")))))),
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
          |│  ╰─ Name(bar -> $baz)
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
                BsonField.Name("bar") -> -\/ (ExprOp.DocField(BsonField.Name("baz"))))))),
          chain(readFoo,
            mapOp(
              Js.AnonFunDecl(List("key"), Nil)),
            reduceOp(ReduceOp.reduceNOP)))

      render(op) must_==
      """FoldLeftOp
        |├─ Chain
        |│  ├─ ReadOp(foo)
        |│  ╰─ ProjectOp
        |│     ╰─ Name(bar -> $baz)
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
