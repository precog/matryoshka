package slamdata.engine

import org.specs2.mutable._

class TypesSpec extends Specification {
  import Type._

  val LatLong = ObjField("lat", Dec) & ObjField("long", Dec)


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
      typecheck(LatLong, LatLong & ObjField("az", Dec)).toOption should beSome
    }

    "fails with simple object narrowing" in {
      typecheck(LatLong & ObjField("az", Dec), LatLong).toOption should beNone
    }
  }

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
}
