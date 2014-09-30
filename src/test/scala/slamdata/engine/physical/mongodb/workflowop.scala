package slamdata.engine.physical.mongodb

import org.specs2.mutable._

import scala.collection.immutable.ListMap
import scalaz._, Scalaz._

import slamdata.engine.fp._

class WorkflowOpSpec extends Specification {
  import WorkflowOp._
  import PipelineOp._
  
  "WorkflowOp.++" should {
    val readFoo = ReadOp(Collection("foo"))
    
    "merge trivial reads" in {
      readFoo merge readFoo must_==
        (ExprOp.DocVar.ROOT(), ExprOp.DocVar.ROOT()) -> readFoo
    }
    
    "merge group by constant with project" in {
      val left = GroupOp(readFoo, 
                    Grouped(ListMap()),
                    -\/ (ExprOp.Literal(Bson.Int32(1))))
      val right = ProjectOp(readFoo,
                    Reshape.Doc(ListMap(
                      BsonField.Name("city") -> -\/ (ExprOp.DocField(BsonField.Name("city"))))))
          
      val ((lb, rb), op) = left merge right
      
      lb must_== ExprOp.DocVar.ROOT()
      rb must_== ExprOp.DocField(BsonField.Name("__sd_tmp_1"))
      op must_== 
          chain(readFoo,
            ProjectOp(_,
              Reshape.Doc(ListMap(
                BsonField.Name("lEft") -> \/-(Reshape.Doc(ListMap(
                  BsonField.Name("city") -> -\/(ExprOp.DocField(BsonField.Name("city")))))),
                BsonField.Name("rIght") -> -\/ (ExprOp.DocVar.ROOT())))), 
            GroupOp(_,
              Grouped(ListMap(
                 BsonField.Name("__sd_tmp_1") -> ExprOp.Push(ExprOp.DocField(BsonField.Name("lEft"))))),
              -\/ (ExprOp.Literal(Bson.Int32(1)))),
            UnwindOp(_,
              ExprOp.DocField(BsonField.Name("__sd_tmp_1"))))
    }
  }
}