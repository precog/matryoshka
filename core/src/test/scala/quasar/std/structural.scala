package quasar.std

import quasar.Predef._
import quasar._

import org.specs2.mutable._
import org.specs2.scalaz._
import org.specs2.ScalaCheck
import scalaz._, Scalaz._


class StructuralSpecs extends Specification with ScalaCheck with ValidationMatchers {
  import quasar.Type._

  import StructuralLib._

  import DataGen._
  import TypeGen._

  "ConcatOp" should {
    // NB: the func's domain is the type assigned to any argument if nothing
    // else is known about it.
    val unknown = AnyArray ⨿ Str

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
  }

  "FlattenMap" should {
    "only accept maps" ! arbArrayType { (nonMap: Type) =>
      FlattenMap(nonMap) must beFailing
    }

    "convert from a map type to the type of its values" in {
      FlattenMap(Obj(Map(), Str.some)) must beSuccessful(Str)
    }

    "untype to a map type from some value type" in {
      FlattenMap.untype(Str) must beSuccessful(List(Obj(Map(), Str.some)))
    }
  }

  "FlattenMapKeys" should {
    "only accept maps" ! arbArrayType { (nonMap: Type) =>
      FlattenMapKeys(nonMap) must beFailing
    }

    "convert from a map type to the type of its keys" in {
      FlattenMapKeys(Obj(Map(), Int.some)) must beSuccessful(Str)
    }

    "untype to a map type from some key type" in {
      FlattenMapKeys.untype(Str) must beSuccessful(List(Obj(Map(), Top.some)))
    }
  }

  "FlattenArray" should {
    "only accept arrays" ! arbStrType { (nonArr: Type) =>
      FlattenArray(nonArr) must beFailing
    }

    "convert from an array type to the type of its values" in {
      FlattenArray(FlexArr(0, None, Str)) must beSuccessful(Str)
    }

    "untype to an array type from some value type" in {
      FlattenArray.untype(Str) must beSuccessful(List(FlexArr(0, None, Str)))
    }
  }

  "FlattenArrayIndices" should {
    "only accept arrays" ! arbStrType { (nonArr: Type) =>
      FlattenArrayIndices(nonArr) must beFailing
    }

    "convert from an array type to int" in {
      FlattenArrayIndices(FlexArr(0, None, Str)) must beSuccessful(Int)
    }

    "untype to an array type from int" in {
      FlattenArrayIndices.untype(Int) must
        beSuccessful(List(FlexArr(0, None, Top)))
    }

    "only untype from ints" ! arbStrType { (nonInt: Type) =>
      FlattenArrayIndices.untype(nonInt) must beFailing
    }
  }

  "ShiftMap" should {
    "only accept maps" ! arbArrayType { (nonMap: Type) =>
      ShiftMap(nonMap) must beFailing
    }

    "convert from a map type to the type of its values" in {
      ShiftMap(Obj(Map(), Str.some)) must beSuccessful(Str)
    }

    "untype to a map type from some value type" in {
      ShiftMap.untype(Str) must beSuccessful(List(Obj(Map(), Str.some)))
    }
  }

  "ShiftMapKeys" should {
    "only accept maps" ! arbArrayType { (nonMap: Type) =>
      ShiftMapKeys(nonMap) must beFailing
    }

    "convert from a map type to the type of its keys" in {
      ShiftMapKeys(Obj(Map(), Int.some)) must beSuccessful(Str)
    }

    "untype to a map type from some key type" in {
      ShiftMapKeys.untype(Str) must beSuccessful(List(Obj(Map(), Top.some)))
    }
  }

  "ShiftArray" should {
    "only accept arrays" ! arbStrType { (nonArr: Type) =>
      ShiftArray(nonArr) must beFailing
    }

    "convert from an array type to the type of its values" in {
      ShiftArray(FlexArr(0, None, Str)) must beSuccessful(Str)
    }

    "untype to an array type from some value type" in {
      ShiftArray.untype(Str) must beSuccessful(List(FlexArr(0, None, Str)))
    }
  }

  "ShiftArrayIndices" should {
    "only accept arrays" ! arbStrType { (nonArr: Type) =>
      ShiftArrayIndices(nonArr) must beFailing
    }

    "convert from an array type to int" in {
      ShiftArrayIndices(FlexArr(0, None, Str)) must beSuccessful(Int)
    }

    "untype to an array type from int" in {
      ShiftArrayIndices.untype(Int) must
        beSuccessful(List(FlexArr(0, None, Top)))
    }

    "only untype from ints" ! arbStrType { (nonInt: Type) =>
      ShiftArrayIndices.untype(nonInt) must beFailing
    }
  }

  "UnshiftMap" should {
    "convert to a map type from some value type" in {
      UnshiftMap(Str) must beSuccessful(Obj(Map(), Str.some))
    }

    "untype from a map type to the type of its values" in {
      UnshiftMap.untype(Obj(Map(), Str.some)) must beSuccessful(List(Str))
    }
  }

  "UnshiftArray" should {
    "convert to an array type from some value type" in {
      UnshiftArray(Str) must beSuccessful(FlexArr(0, None, Str))
    }

    "untype from an array type to the type of its values" in {
      UnshiftArray.untype(FlexArr(0, None, Str)) must beSuccessful(List(Str))
    }
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
