package quasar.physical.mongodb

import quasar.Predef._

import org.specs2.mutable._

class FindQuerySpec extends Specification  {

  implicit def toBson(x: Int) = Bson.Int32(x)
  implicit def toField(name: String) = BsonField.Name(name)

  "Selector" should {
    import Selector._

    "bson" should {
      "render simple expr" in {
        Expr(Lt(10)).bson must_== Bson.Doc(ListMap("$lt" -> 10))
      }

      "render $not expr" in {
        NotExpr(Lt(10)).bson must_== Bson.Doc(ListMap("$not" -> Bson.Doc(ListMap("$lt" -> 10))))
      }

      "render simple selector" in {
        val sel = Doc(BsonField.Name("foo") -> Gt(10))

        sel.bson must_== Bson.Doc(ListMap("foo" -> Bson.Doc(ListMap("$gt" -> 10))))
      }

      "render simple selector with path" in {
        val sel = Doc(
          BsonField.Name("foo") \ BsonField.Index(3) \ BsonField.Name("bar") -> Gt(10)
        )

        sel.bson must_== Bson.Doc(ListMap("foo.3.bar" -> Bson.Doc(ListMap("$gt" -> 10))))
      }

      "render flattened $and" in {
        val cs = And(
          Doc(BsonField.Name("foo") -> Gt(10)),
          And(
            Doc(BsonField.Name("foo") -> Lt(20)),
            Doc(BsonField.Name("foo") -> Neq(15))
          )
        )
        cs.bson must_==
          Bson.Doc(ListMap("$and" -> Bson.Arr(List(
            Bson.Doc(ListMap("foo" -> Bson.Doc(ListMap("$gt" -> 10)))),
            Bson.Doc(ListMap("foo" -> Bson.Doc(ListMap("$lt" -> 20)))),
            Bson.Doc(ListMap("foo" -> Bson.Doc(ListMap("$ne" -> 15))))
          ))))
      }

      "render not(eq(...))" in {
        val cond = NotExpr(Eq(10))
        cond.bson must_== Bson.Doc(ListMap("$ne" -> 10))
      }
    }

    "negate" should {
      "rewrite singleton Doc" in {
        val sel = Doc(
          BsonField.Name("x") -> Lt(10),
          BsonField.Name("y") -> Gt(10))
        sel.negate must_== Or(
          Doc(ListMap(("x": BsonField) -> NotExpr(Lt(10)))),
          Doc(ListMap(("y": BsonField) -> NotExpr(Gt(10)))))
      }

      "rewrite And" in {
        val sel =
          And(
            Doc(BsonField.Name("x") -> Lt(10)),
            Doc(BsonField.Name("y") -> Gt(10)))
        sel.negate must_== Or(
          Doc(ListMap(("x": BsonField) -> NotExpr(Lt(10)))),
          Doc(ListMap(("y": BsonField) -> NotExpr(Gt(10)))))
      }

      "rewrite Doc" in {
        val sel = Doc(
          BsonField.Name("x") -> Lt(10),
          BsonField.Name("y") -> Gt(10))
        sel.negate must_== Or(
          Doc(ListMap(("x": BsonField) -> NotExpr(Lt(10)))),
          Doc(ListMap(("y": BsonField) -> NotExpr(Gt(10)))))
      }
    }

    "constructors" should {
      "define nested $and and $or" in {
        val cs =
          Or(
            And(
              Doc(BsonField.Name("foo") -> Gt(10)),
              Doc(BsonField.Name("foo") -> Lt(20))
            ),
            And(
              Doc(BsonField.Name("bar") -> Gte(1)),
              Doc(BsonField.Name("bar") -> Lte(5))
            )
          )

        1 must_== 1
      }
    }
  }
}
