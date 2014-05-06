package slamdata.engine

import org.specs2.mutable._

class TypesSpec extends Specification {
  import Type._

  val LatLong = NamedField("lat", Dec) & NamedField("long", Dec)
  val Azim = NamedField("az", Dec)


  "typecheck" should {
    "succeed with int/int" in {
      typecheck(Int, Int).toOption should beSome
    }

    "succeed with int/(int|int)" in {
      typecheck(Int, Int | Int).toOption should beSome
    }

    "succeed with (int|int)/int" in {
      typecheck(Int | Int, Int).toOption should beSome
    }

    "succeed with (int|str)/int" in {
      typecheck(Int | Str, Int).toOption should beSome
    }

    "succeed with simple object widening" in {
      typecheck(LatLong, LatLong & Azim).toOption should beSome
    }

    "fails with simple object narrowing" in {
      typecheck(LatLong & Azim, LatLong).toOption should beNone
    }

    "succeed with coproduct(int|dec)/coproduct(int|dec)" in {
      typecheck(Int | Dec, Dec | Int).toOption should beSome
    }
  }

  "objectField" should {
    "descend into singleton type" in {
      Const(Data.Obj(Map("foo" -> Data.Str("bar")))).objectField(Const(Data.Str("foo"))).toOption should beSome(Const(Data.Str("bar")))
    }

    "descend into singleton type with Str field and return lub of field values" in {
      Const(Data.Obj(Map("foo" -> Data.Str("bar")))).objectField(Str).toOption should beSome(Str)
    }

    "descend into obj field type with const field" in {
      NamedField("foo", Str).objectField(Const(Data.Str("foo"))).toOption should beSome(Str)
    }
  }

  "coproduct" should {
    "have order-independent equality" in {
      (Int | Str) must_== (Str | Int)
    }
  }
}
