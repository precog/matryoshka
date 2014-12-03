package slamdata.engine.physical.mongodb

import org.specs2.mutable._

import scala.collection.immutable.ListMap

import scalaz._

import slamdata.engine.fp._
import slamdata.engine.{TreeMatchers}

class OptimizeSpecs extends Specification with TreeMatchers {
  import Reshape._
  import Workflow._
  
  "inline" should {
    import optimize.pipeline._
    
    "inline simple project on group" in {
        val inlined = inlineProjectGroup(
          Reshape.Doc(ListMap(
            BsonField.Name("foo") -> -\/ (ExprOp.DocField(BsonField.Name("value"))))),
          Grouped(ListMap(BsonField.Name("value") -> ExprOp.Sum(ExprOp.Literal(Bson.Int32(1))))))
          
          inlined must beSome(Grouped(ListMap(BsonField.Name("foo") -> ExprOp.Sum(ExprOp.Literal(Bson.Int32(1))))))
    }
    
    "inline multiple projects on group, dropping extras" in {
        val inlined = inlineProjectGroup(
          Reshape.Doc(ListMap(
            BsonField.Name("foo") -> -\/ (ExprOp.DocField(BsonField.Name("__sd_tmp_1"))),
            BsonField.Name("bar") -> -\/ (ExprOp.DocField(BsonField.Name("__sd_tmp_2"))))),
          Grouped(ListMap(
            BsonField.Name("__sd_tmp_1") -> ExprOp.Sum(ExprOp.Literal(Bson.Int32(1))),
            BsonField.Name("__sd_tmp_2") -> ExprOp.Sum(ExprOp.Literal(Bson.Int32(2))),
            BsonField.Name("__sd_tmp_3") -> ExprOp.Sum(ExprOp.Literal(Bson.Int32(3))))))
          
          inlined must beSome(Grouped(ListMap(
            BsonField.Name("foo") -> ExprOp.Sum(ExprOp.Literal(Bson.Int32(1))),
            BsonField.Name("bar") -> ExprOp.Sum(ExprOp.Literal(Bson.Int32(2))))))
    }
    
    "inline project on group with nesting" in {
        val inlined = inlineProjectGroup(
          Reshape.Doc(ListMap(
            BsonField.Name("bar") -> -\/ (ExprOp.DocField(BsonField.Name("value") \ BsonField.Name("bar"))),
            BsonField.Name("baz") -> -\/ (ExprOp.DocField(BsonField.Name("value") \ BsonField.Name("baz"))))),
          Grouped(ListMap(
            BsonField.Name("value") -> ExprOp.Push(ExprOp.DocField(BsonField.Name("foo"))))))

          inlined must beSome(beTree(Grouped(ListMap(
            BsonField.Name("bar") -> ExprOp.Push(ExprOp.DocField(BsonField.Name("foo") \ BsonField.Name("bar"))),
            BsonField.Name("baz") -> ExprOp.Push(ExprOp.DocField(BsonField.Name("foo") \ BsonField.Name("baz")))))))
    }
  }
}
