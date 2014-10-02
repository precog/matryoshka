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
  
  "WorkflowOp.++" should {
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
          |   ╰─ Shape
          |      ╰─ Name(bar -> $baz)""".stripMargin
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
          |   ╰─ Shape
          |      ╰─ Index(0 -> $baz)""".stripMargin
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
          |   ╰─ Shape
          |      ╰─ Shape(bar)
          |         ╰─ Index(0 -> $baz)""".stripMargin
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
          |│  ╰─ Shape
          |│     ╰─ Name(bar -> $baz)
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
        |│     ╰─ Shape
        |│        ╰─ Name(bar -> $baz)
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