package quasar.std

import quasar.Predef._

import org.specs2.mutable._
import org.specs2.scalaz._
import org.specs2.ScalaCheck

import quasar._

class StructuralSpecs extends Specification with ScalaCheck with ValidationMatchers {
  import quasar.Type._

  import StructuralLib._

  import DataGen._
  import TypeGen._

  "ConcatOp" should {
    // NB: the func's domain is the type assigned to any argument if nothing
    // else is known about it.
    val unknown = AnyArray | Str

    "type combination of arbitrary strs as str" ! (arbStrType, arbStrType) { (st1: Type, st2: Type) =>
      ConcatOp(st1, st2).map(Str contains _) must beSuccessful(true)
      ConcatOp(st2, st1).map(Str contains _) must beSuccessful(true)
    }

    "type arbitrary str || unknown as Str" ! arbStrType { (st: Type) =>
      ConcatOp(st, unknown) must beSuccessful(Str)
      ConcatOp(unknown, st) must beSuccessful(Str)
    }

    "fold constant Strings" ! prop { (s1: String, s2: String) =>
      ConcatOp(Const(Data.Str(s1)), Const(Data.Str(s2))) must
        beSuccessful(Const(Data.Str(s1 + s2)))
    }

    "type combination of arbitrary arrays as array" ! (arbArrayType, arbArrayType) { (at1: Type, at2: Type) =>
      ConcatOp(at1, at2).map(_.arrayLike) must beSuccessful(true)
      ConcatOp(at2, at1).map(_.arrayLike) must beSuccessful(true)
    }

    "type arbitrary array || unknown as array" ! arbArrayType { (at: Type) =>
      ConcatOp(at, unknown).map(_.arrayLike) must beSuccessful(true)
      ConcatOp(unknown, at).map(_.arrayLike) must beSuccessful(true)
    }

    "fold constant arrays" ! prop { (ds1: List[Data], ds2: List[Data]) =>
      ConcatOp(Const(Data.Arr(ds1)), Const(Data.Arr(ds2))) must
        beSuccessful(Const(Data.Arr(ds1 ++ ds2)))
    }

    "fail with mixed Str and array args" ! (arbStrType, arbArrayType) { (st: Type, at: Type) =>
      ConcatOp(st, at) must beFailing
      ConcatOp(at, st) must beFailing
    }

    "propagate unknown types" in {
      ConcatOp(unknown, unknown) must beSuccessful(unknown)
    }

    import org.scalacheck.Gen, Gen._
    import org.scalacheck.Arbitrary, Arbitrary._
    lazy val arbStrType = Arbitrary(Gen.oneOf(
      const(Str),
      arbitrary[String].map(s => Const(Data.Str(s)))))

    lazy val arbArrayType = Arbitrary(simpleArrayGen)
    lazy val simpleArrayGen = Gen.oneOf(
      for {
        i <- arbitrary[Int]
        n <- arbitrary[Option[Int]]
        t <- arbitrary[Type]
      } yield FlexArr(i.abs, n.map(i.abs max _.abs), t),
      for {
        t <- arbitrary[List[Type]]
      } yield Arr(t),
      for {
        ds <- resize(5, arbitrary[List[Data]])
      } yield Const(Data.Arr(ds)))
  }
}
