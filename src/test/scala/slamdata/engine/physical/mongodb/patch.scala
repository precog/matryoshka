package slamdata.engine.physical.mongodb

import slamdata.engine._
import slamdata.engine.fp._
import slamdata.engine.DisjunctionMatchers 

import scalaz._
import Scalaz._

import org.specs2.mutable._
import org.specs2.ScalaCheck

import org.scalacheck._
import Gen._

class MergePatchSpec extends Specification with ScalaCheck with DisjunctionMatchers {
  def p(ops: PipelineOp*) = Pipeline(ops.toList)

  val empty = p()

  import PipelineOp._
  import ExprOp._

  "MergePatch.Id" should {
    "do nothing with pipeline op" in {
      MergePatch.Id(Skip(10)).map(_._1) must (beRightDisj(List[PipelineOp](Skip(10))))
    }

    "return Id for successor patch" in {
      MergePatch.Id(Skip(10)).map(_._2) must (beRightDisj(MergePatch.Id))
    }
  }

  "MergePatch.Rename" should {
    "rename top-level field" in {
      val init = Project(Reshape.Doc(Map(
        BsonField.Name("bar") -> -\/(DocField(BsonField.Name("baz")))
      )))

      val expect = List[PipelineOp](Project(Reshape.Doc(Map(
        BsonField.Name("bar") -> -\/(DocField(BsonField.Name("buz")))
      ))))

      val applied = MergePatch.Rename(DocField(BsonField.Name("baz")), DocField(BsonField.Name("buz")))(init)

      applied.map(_._1) must (beRightDisj(expect))
      applied.map(_._2) must (beRightDisj(MergePatch.Id))
    }

    "rename top-level field defined by ROOT doc var" in {
      val init = Project(Reshape.Doc(Map(
        BsonField.Name("bar") -> -\/(DocVar.ROOT(BsonField.Name("baz")))
      )))

      val expect = List[PipelineOp](Project(Reshape.Doc(Map(
        BsonField.Name("bar") -> -\/(DocVar.ROOT(BsonField.Name("buz")))
      ))))

      val applied = MergePatch.Rename(DocField(BsonField.Name("baz")), DocField(BsonField.Name("buz")))(init)

      applied.map(_._1) must (beRightDisj(expect))
      applied.map(_._2) must (beRightDisj(MergePatch.Id))
    }

    "rename top-level field defined by CURRENT doc var" in {
      val init = Project(Reshape.Doc(Map(
        BsonField.Name("bar") -> -\/(DocVar.CURRENT(BsonField.Name("baz")))
      )))

      val expect = List[PipelineOp](Project(Reshape.Doc(Map(
        BsonField.Name("bar") -> -\/(DocVar.CURRENT(BsonField.Name("buz")))
      ))))

      val applied = MergePatch.Rename(DocVar.CURRENT(BsonField.Name("baz")), DocVar.CURRENT(BsonField.Name("buz")))(init)

      applied.map(_._1) must (beRightDisj(expect))
      applied.map(_._2) must (beRightDisj(MergePatch.Id))
    }

    "rename even root fields when ROOT is renamed" in {
      val init = Project(Reshape.Doc(Map(
        BsonField.Name("bar") -> -\/ (DocField(BsonField.Name("baz")))
      )))

      val expect = List[PipelineOp](Project(Reshape.Doc(Map(
        BsonField.Name("bar") -> -\/ (DocField(BsonField.Name("buz") \ BsonField.Name("baz")))
      ))))

      val applied = MergePatch.Rename(DocVar.ROOT(), DocField(BsonField.Name("buz")))(init)

      applied.map(_._1) must (beRightDisj(expect))
      applied.map(_._2) must (beRightDisj(MergePatch.Id))
    }
    
    "not rename nested" in {
      val init = Project(Reshape.Doc(Map(
        BsonField.Name("bar") -> -\/(DocField(BsonField.Name("foo") \ BsonField.Name("baz")))
      )))

      val applied = MergePatch.Rename(DocField(BsonField.Name("baz")), DocField(BsonField.Name("buz")))(init)

      applied.map(_._1) must (beRightDisj(List[PipelineOp](init)))
      applied.map(_._2) must (beRightDisj(MergePatch.Id))
    }

    "rename Then sequentially" in {
      import Selector._

      val op = Match(
        Selector.Doc(Map[BsonField, SelectorExpr](
          BsonField.Name("name")      -> Selector.Expr(Selector.Eq(Bson.Text("Steve"))), 
          BsonField.Name("age")       -> Selector.Expr(Selector.Gt(Bson.Int32(18))), 
          BsonField.Name("length")    -> Selector.Expr(Selector.Lte(Bson.Dec(8.5))), 
          BsonField.Name("publisher") -> Selector.Expr(Selector.Neq(Bson.Text("Amazon")))
        ))
      )

      val patch = 
        MergePatch.Rename(DocVar.ROOT(BsonField.Name("name")), DocVar.ROOT(BsonField.Name("__sd_tmp_1"))) >>
        MergePatch.Rename(DocVar.ROOT(BsonField.Name("length")), DocVar.ROOT(BsonField.Name("__sd_tmp_2")))

      val expect = Match(
        Selector.Doc(Map[BsonField, SelectorExpr](
          BsonField.Name("__sd_tmp_1")  -> Selector.Expr(Selector.Eq(Bson.Text("Steve"))), 
          BsonField.Name("age")         -> Selector.Expr(Selector.Gt(Bson.Int32(18))), 
          BsonField.Name("__sd_tmp_2")  -> Selector.Expr(Selector.Lte(Bson.Dec(8.5))), 
          BsonField.Name("publisher")   -> Selector.Expr(Selector.Neq(Bson.Text("Amazon")))
        ))
      )

      patch(op) must (beRightDisj((expect :: Nil) -> patch))
    }
  }

}
