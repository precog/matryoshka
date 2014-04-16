package slamdata.engine

import org.specs2.mutable._

class ProvenanceSpec extends Specification {
  import SemanticAnalysis._
  import Provenance._
  import sql._

  val Rel1 = Relation(TableRelationAST("foo", None))
  val Rel2 = Relation(TableRelationAST("bar", None))

  "flatten" should {
    "flatten singleton in" in {
      Value.flatten must_== (Set(Value))
    }

    "flatten 2-way product in" in {
      (Value & Empty).flatten must_== (Set(Value, Empty))
    }

    "flatten 3-way product in" in {
      (Value & Empty & Rel1).flatten must_== (Set(Value, Empty, Rel1))
    }

    "flatten 2-way coproduct in" in {
      (Value | Empty).flatten must_== (Set(Value, Empty))
    }

    "flatten 3-way coproduct in" in {
      (Value | Empty | Rel1).flatten must_== (Set(Value, Empty, Rel1))
    }
  }

  "simplify" should {
    "simplify 3-way twice duplicated coproduct" in {
      (Value | Value | Empty | Empty | Rel1 | Rel1).simplify.flatten must_== (Set(Value, Rel1))
    }
  }

  "equality" should {
    "be order independent for 3-way products" in {
      (Value & Rel1 & Rel2) must_== (Rel1 & Value & Rel2)
    }
  }
}