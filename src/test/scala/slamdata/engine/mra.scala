package slamdata.engine

import slamdata.engine.fs._

import org.specs2.mutable._

class MRASpecs extends Specification {
  import MRA._

  val FooPath = Path("foo.json")

  val DimIdFooPath = DimId.Source(FooPath)

  val DimsFooPath = Dims.set(FooPath)

  "Dims.size" should {
    "report size 0 for value" in {
      Dims.Value.size must_== 0
    }

    "report size 1 for set" in {
      Dims.set(Path("foo")).size must_== 1
    }

    "report size 3 for set that has been grouped and expanded" in {
      Dims.set(Path("foo")).contract.expand.size must_== 3
    }

    "report size 2 for set that has been grouped and flattened" in {
      Dims.set(Path("foo")).contract.flatten.size must_== 2
    }
  }

  "Dims.value" should {
    "report true for value" in {
      Dims.Value.value must beTrue
    }

    "report true for value that has been grouped and flattened" in {
      Dims.Value.contract.flatten.value must beTrue
    }

    "report false for set" in {
      Dims.set(Path("foo.json")).value must beFalse
    }
  }

  "Dims.contract" should {
    "increase size by 1" in {
      Dims.Value.contract.size must_== 1
    }

    "add to contracted dims" in {
      Dims.Value.contract.contracts.length must_== 1
    }
  }

  "Dims.flatten" should {
    "not change size of value" in {
      Dims.Value.flatten.size must_== 0
    }

    "not change size of set" in {
      Dims.set(Path("foo.json")).flatten.size must_== 1
    }
  }

  "Dims.expand" should {
    "change size of value" in {
      Dims.Value.expand.size must_== 1
    }

    "change size of set" in {
      Dims.set(FooPath).expand.size must_== 2
    }
  }

  "Dims.contracted" should {
    "return true for contracted value" in {
      Dims.Value.contract.contracted must beTrue
    }

    "return true for contracted set" in {
      Dims.set(FooPath).contract.contracted must beTrue
    }
  }

  "Dims.squash" should {
    "destroy array index information" in {
      Dims.set(FooPath).index(10L).index(20L).squash must_== (Dims.set(FooPath))
    }

    "destroy field name information" in {
      Dims.set(FooPath).field("foo").field("bar").squash must_== (Dims.set(FooPath))
    }
  }

  "Dims.squashed" should {
    "return false for unsquashed" in {
      Dims.set(FooPath).field("foo").index(10L).squashed must beFalse
    }

    "return true for squashed" in {
      Dims.set(FooPath).field("foo").index(10L).squash.squashed must beTrue
    }
  }

  "Dims.identified" should {
    "return false for value" in {
      Dims.Value.identified must beFalse
    }

    "return true for set" in {
      Dims.set(FooPath).identified must beTrue
    }
  }

  "Dims.expanded" should {
    "return false for unexpanded set" in {
      Dims.set(FooPath).expanded must beFalse
    }

    "return true for expanded set" in {
      Dims.set(FooPath).expand.expanded must beTrue
    }
  }

  "Dims.(++)" should {
    "combine value and set" in {
      (Dims.Value ++ Dims.set(FooPath)).id must_== Set(DimId.Value, DimId.Source(FooPath))
    }
  }

  "DimId.subsumes" should {
    "return false for Value / Set" in {
      DimId.Value.subsumes(DimId.Source(FooPath)) must beFalse
    }

    "return false for Set / Value" in {
      DimId.Source(FooPath).subsumes(DimId.Value) must beFalse
    }
  }

  "DimId.simplify" should {
    "not change value and set" in {
      DimId.simplify(Set(DimId.Value, DimId.Source(FooPath))) must_== Set(DimId.Value, DimId.Source(FooPath))
    }

    "simplify set and array dereferenced set" in {
      DimId.simplify(Set(DimIdFooPath, DimIdFooPath.index(12L))) must_== Set(DimIdFooPath.index(12L))
    }

    "simplify set and object dereferenced set" in {
      DimId.simplify(Set(DimIdFooPath, DimIdFooPath.field("foo"))) must_== Set(DimIdFooPath.field("foo"))
    }
  }

  "Dims.aggregate" should {
    "remove last expanded dimension" in {
      Dims.Value.expand.aggregate.expands.length must_== 0
    }

    "remove last identified dimension" in {
      DimsFooPath.aggregate.value must beTrue
    }

    "remove last identified and multiply dereferenced dimension" in {
      DimsFooPath.index(10L).field("foo").aggregate.value must beTrue
    }
  }
}