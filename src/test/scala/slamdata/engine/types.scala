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
  }

/*
  "check" should {
    "succeed with str|int" in {
      check(Str | Int).toOption should beSome
    }

    "succeed with int&int" in {
      check(Int & Int).toOption should beSome
    }

    "find contradiction with int&str" in {
      check(Str & Int).toOption should beNone
    }
  }
*/
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
}
