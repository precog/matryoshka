package slamdata.engine.physical.mongodb

import scalaz._
import Scalaz._

import org.specs2.mutable._

class ExprOpSpec extends Specification {
  import PipelineOp._
  import ExprOp._

  "ExprOp" should {

    "escape literal string with $" in {
      Literal(Bson.Text("$1")).bson must_== Bson.Doc(Map("$literal" -> Bson.Text("$1")))
    }

    "not escape literal string with no leading '$'" in {
      val x = Bson.Text("abc")
      Literal(x).bson must_== x
    }

    "not escape simple integer literal" in {
      val x = Bson.Int32(0)
      Literal(x).bson must_== x
    }

    "not escape simple array literal" in {
      val x = Bson.Arr(Bson.Text("abc") :: Bson.Int32(0) :: Nil)
      Literal(x).bson must_== x
    }

    "escape string nested in array" in {
      val x = Bson.Arr(Bson.Text("$1") :: Nil)
      val exp = Bson.Arr(Bson.Doc(Map("$literal" -> Bson.Text("$1"))) :: Nil)
      Literal(x).bson must_== exp
    }

    "not escape simple doc literal" in {
      val x = Bson.Doc(Map("a" -> Bson.Text("b")))
      Literal(x).bson must_== x
    }

    "escape string nested in doc" in {
      val x = Bson.Doc(Map("a" -> Bson.Text("$1")))
      val exp = Bson.Doc(Map("a" -> Bson.Doc(Map("$literal" -> Bson.Text("$1")))))
      Literal(x).bson must_== exp
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
      Redact.DESCEND.bson.repr must_== "$$DESCEND"
      Redact.PRUNE.bson.repr   must_== "$$PRUNE"
      Redact.KEEP.bson.repr    must_== "$$KEEP"
    }

  }
}
