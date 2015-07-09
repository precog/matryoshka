package slamdata.engine.physical.mongodb

import collection.immutable.ListMap

import org.scalacheck._
import org.scalacheck.Arbitrary
import org.specs2.mutable._
import org.specs2.scalaz._
import scalaz._
import Scalaz._
import scalaz.scalacheck.ScalazProperties._
import shapeless.contrib.scalaz.instances.{deriveShow => _, _}

import slamdata.engine.{DisjunctionMatchers}
import slamdata.engine.fp._

object ArbitraryExprOp {
  import ExprOp._; import DSL._

  lazy val genExpr: Gen[Expression] = Gen.const($literal(Bson.Int32(1)))
}

class GroupOpSpec extends Spec {
  import GroupOp._

  implicit val arbGroupOp: Arbitrary ~> λ[α => Arbitrary[GroupOp[α]]] =
    new (Arbitrary ~> λ[α => Arbitrary[GroupOp[α]]]) {
      def apply[α](arb: Arbitrary[α]): Arbitrary[GroupOp[α]] =
        Arbitrary(arb.arbitrary.flatMap(a =>
          Gen.oneOf(
            $addToSet(a),
            $push(a),
            $first(a),
            $last(a),
            $max(a),
            $min(a),
            $avg(a),
            $sum(a))))
    }

  implicit val arbIntGroupOp = arbGroupOp(Arbitrary.arbInt)

  checkAll(traverse.laws[GroupOp])
}


class ExprOpSpec extends Specification with DisjunctionMatchers {
  import ExprOp._; import DSL._

  "ExprOp" should {

    "escape literal string with $" in {
      val x = Bson.Text("$1")
      $literal(x).cata(bsonƒ) must_== Bson.Doc(ListMap("$literal" -> x))
    }

    "escape literal string with no leading '$'" in {
      val x = Bson.Text("abc")
      $literal(x).cata(bsonƒ) must_== Bson.Doc(ListMap("$literal" -> x))
    }

    "escape simple integer literal" in {
      val x = Bson.Int32(0)
      $literal(x).cata(bsonƒ) must_== Bson.Doc(ListMap("$literal" -> x))
    }

    "escape simple array literal" in {
      val x = Bson.Arr(Bson.Text("abc") :: Bson.Int32(0) :: Nil)
      $literal(x).cata(bsonƒ) must_== Bson.Doc(ListMap("$literal" -> x))
    }

    "escape string nested in array" in {
      val x = Bson.Arr(Bson.Text("$1") :: Nil)
      $literal(x).cata(bsonƒ) must_== Bson.Doc(ListMap("$literal" -> x))
    }

    "escape simple doc literal" in {
      val x = Bson.Doc(ListMap("a" -> Bson.Text("b")))
      $literal(x).cata(bsonƒ) must_== Bson.Doc(ListMap("$literal" -> x))
    }

    "escape string nested in doc" in {
      val x = Bson.Doc(ListMap("a" -> Bson.Text("$1")))
      $literal(x).cata(bsonƒ) must_== Bson.Doc(ListMap("$literal" -> x))
    }

    "render $$ROOT" in {
      DocVar.ROOT().bson.repr must_== "$$ROOT"
    }

    "treat DocField as alias for DocVar.ROOT()" in {
      DocField(BsonField.Name("foo")) must_== DocVar.ROOT(BsonField.Name("foo"))
    }

    "render $foo under $$ROOT" in {
      DocVar.ROOT(BsonField.Name("foo")).bson.repr must_== "$foo"
    }

    "render $foo.bar under $$CURRENT" in {
      DocVar.CURRENT(BsonField.Name("foo") \ BsonField.Name("bar")).bson.repr must_== "$$CURRENT.foo.bar"
    }

    "render $redact result variables" in {
      Workflow.$Redact.DESCEND.bson.repr must_== "$$DESCEND"
      Workflow.$Redact.PRUNE.bson.repr   must_== "$$PRUNE"
      Workflow.$Redact.KEEP.bson.repr    must_== "$$KEEP"
    }
  }

  "toJs" should {
    import org.threeten.bp._
    import slamdata.engine.javascript.JsFn
    import slamdata.engine.javascript.JsCore._

    "handle addition with epoch date literal" in {
      toJs(
        $add(
          $literal(Bson.Date(Instant.ofEpochMilli(0))),
          $(DocField(BsonField.Name("epoch"))))) must beRightDisj(
        JsFn(JsFn.base, New("Date", List(Select(JsFn.base.fix, "epoch").fix)).fix))
    }
  }
}
