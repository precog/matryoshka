package slamdata.engine.physical.mongodb

import slamdata.engine._
import slamdata.engine.DisjunctionMatchers 

import scalaz._
import Scalaz._

import org.specs2.mutable._

class PipelineSpec extends Specification with DisjunctionMatchers {
  def p(ops: PipelineOp*) = Pipeline(ops.toList)

  val empty = p()

  import PipelineOp._
  import ExprOp._

  "MergePatch.Id" should {
    "do nothing with pipeline op" in {
      MergePatch.Id(Skip(10))._1 must_== Skip(10)
    }

    "return Id for successor patch" in {
      MergePatch.Id(Skip(10))._2 must_== MergePatch.Id
    }
  }

  "MergePatch.Nest" should {
    "nest with project op" in {
      val init = Project(Reshape(Map(
        "bar" -> -\/(DocField(BsonField.Name("baz")))
      )))

      val expect = Project(Reshape(Map(
        "bar" -> -\/(DocField(BsonField.Name("foo") :+ BsonField.Name("baz")))
      )))

      MergePatch.Nest(BsonField.Name("foo"))(init)._1 must_== expect
    }
  }

  "Pipeline.merge" should {
    "return left when right is empty" in {
      val l = p(Skip(10), Limit(10))

      l.merge(empty) must (beRightDisj(l))
    }

    "return right when left is empty" in {
      val r = p(Skip(10), Limit(10))

      empty.merge(r) must (beRightDisj(r))
    }

    "return empty when both empty" in {
      empty.merge(empty) must (beRightDisj(empty))
    }

    "return left when left and right are equal" in {
      val v = p(Skip(10), Limit(10))      

      v.merge(v) must (beRightDisj(v))
    }.pendingUntilFixed

    "merge two simple projections" in {
      val p1 = Project(Reshape(Map(
        "foo" -> -\/ (Literal(Bson.Int32(1)))
      )))

      val p2 = Project(Reshape(Map(
        "bar" -> -\/ (Literal(Bson.Int32(2)))
      )))   

      val r = Project(Reshape(Map(
        "foo" -> -\/ (Literal(Bson.Int32(1))),
        "bar" -> -\/ (Literal(Bson.Int32(2)))
      ))) 

      p(p1).merge(p(p2)) must (beRightDisj(p(r)))
    }

     "merge two simple nested projections sharing top-level field name" in {
      val p1 = Project(Reshape(Map(
        "foo" -> \/- (Reshape(Map(
          "bar" -> -\/ (Literal(Bson.Int32(9)))
        )))
      )))

      val p2 = Project(Reshape(Map(
        "foo" -> \/- (Reshape(Map(
          "baz" -> -\/ (Literal(Bson.Int32(2)))
        )))
      )))

      val r = Project(Reshape(Map(
        "foo" -> \/- (Reshape(Map(
          "bar" -> -\/ (Literal(Bson.Int32(9))),
          "baz" -> -\/ (Literal(Bson.Int32(2)))
        )))
      )))

      p(p1).merge(p(p2)) must (beRightDisj(p(r)))
    }

    "put match before project" in {
      val p1 = Project(Reshape(Map(
        "foo" -> \/- (Reshape(Map(
          "bar" -> -\/ (Literal(Bson.Int32(9)))
        )))
      )))

      val p2 = Match(Selector.Doc(Map(BsonField.Name("foo") -> Selector.Eq(Bson.Int32(-1)))))

      p(p1).merge(p(p2)) must (beRightDisj(p(p2, p1)))
      p(p2).merge(p(p1)) must (beRightDisj(p(p2, p1)))
    }

    "put redact before project" in {
      val p1 = Project(Reshape(Map(
        "foo" -> \/- (Reshape(Map(
          "bar" -> -\/ (Literal(Bson.Int32(9)))
        )))
      )))

      val p2 = Redact(DocVar(BsonField.Name("KEEP")))

      p(p1).merge(p(p2)) must (beRightDisj(p(p2, p1)))
      p(p2).merge(p(p1)) must (beRightDisj(p(p2, p1)))
    }

    "put limit before project" in {
      val p1 = Project(Reshape(Map(
        "foo" -> \/- (Reshape(Map(
          "bar" -> -\/ (Literal(Bson.Int32(9)))
        )))
      )))

      val p2 = Limit(10L)

      p(p1).merge(p(p2)) must (beRightDisj(p(p2, p1)))
      p(p2).merge(p(p1)) must (beRightDisj(p(p2, p1)))
    }

    "put skip before project" in {
      val p1 = Project(Reshape(Map(
        "foo" -> \/- (Reshape(Map(
          "bar" -> -\/ (Literal(Bson.Int32(9)))
        )))
      )))

      val p2 = Skip(10L)

      p(p1).merge(p(p2)) must (beRightDisj(p(p2, p1)))
      p(p2).merge(p(p1)) must (beRightDisj(p(p2, p1)))
    }

    "put unwind before project" in {
      val p1 = Project(Reshape(Map(
        "foo" -> \/- (Reshape(Map(
          "bar" -> -\/ (Literal(Bson.Int32(9)))
        )))
      )))

      val p2 = Unwind(BsonField.Name("foo"))

      p(p1).merge(p(p2)) must (beRightDisj(p(p2, p1)))
      p(p2).merge(p(p1)) must (beRightDisj(p(p2, p1)))
    }
  }
}