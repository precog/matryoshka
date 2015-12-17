package quasar.physical.mongodb.expression

import quasar.Predef._
import quasar.recursionschemes.Recursive.ops._

import org.scalacheck._
import org.scalacheck.Arbitrary
import org.specs2.mutable._
import org.specs2.scalaz._

import quasar.physical.mongodb.{Bson, BsonField}

object ArbitraryExprOp {

  lazy val genExpr: Gen[Expression] = Gen.const($literal(Bson.Int32(1)))
}

class ExpressionSpec extends Specification with DisjunctionMatchers {

  "Expression" should {

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
      DocVar.ROOT().bson must_== Bson.Text("$$ROOT")
    }

    "treat DocField as alias for DocVar.ROOT()" in {
      DocField(BsonField.Name("foo")) must_== DocVar.ROOT(BsonField.Name("foo"))
    }

    "render $foo under $$ROOT" in {
      DocVar.ROOT(BsonField.Name("foo")).bson must_== Bson.Text("$foo")
    }

    "render $foo.bar under $$CURRENT" in {
      DocVar.CURRENT(BsonField.Name("foo") \ BsonField.Name("bar")).bson must_== Bson.Text("$$CURRENT.foo.bar")
    }
  }

  "toJs" should {
    import org.threeten.bp._
    import quasar.jscore._

    "handle addition with epoch date literal" in {
      toJs(
        $add(
          $literal(Bson.Date(Instant.ofEpochMilli(0))),
          $var(DocField(BsonField.Name("epoch"))))) must beRightDisjunction(
        JsFn(JsFn.defaultName, New(Name("Date"), List(Select(Ident(JsFn.defaultName), "epoch")))))
    }
  }
}
