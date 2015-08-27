package quasar

import quasar.Predef._
import quasar.fp._
import quasar.specs2._

import org.specs2.mutable._
import org.specs2.ScalaCheck
import scalaz.Validation.{success, failure}
import scalaz.Monad

class TypesSpec extends Specification with ScalaCheck with ValidationMatchers with PendingWithAccurateCoverage {
  import Type._

  import TypeGen._

  val LatLong = Obj(Map("lat" -> Dec, "long" -> Dec), Some(Top))
  val Azim = Obj(Map("az" -> Dec), Some(Top))

  def const(s: String): Type = Const(Data.Str(s))
  def const(elems: (String, Data)*): Type = Const(Data.Obj(Map(elems: _*)))

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

    "fail with (int|str)/bool" in {
      typecheck(Int | Str, Bool).toOption should beNone
    }

    "succeed with simple object widening" in {
      typecheck(LatLong, LatLong & Azim).toOption should beSome
    }

    "fail with simple object narrowing" in {
      typecheck(LatLong & Azim, LatLong).toOption should beNone
    }

    "succeed with coproduct(int|dec)/coproduct(int|dec)" in {
      typecheck(Int | Dec, Dec | Int).toOption should beSome
    }

    "succeed with (int&int)/int" in {
      typecheck(Int & Int, Int).toOption should beSome
    }

    "fails with (int&str)/int" in {
      typecheck(Int & Str, Int).toOption should beNone
    }

    "succeed with type and matching constant" in {
      typecheck(Str, Const(Data.Str("a"))).toOption should beSome
    }

    "fail with type and matching constant (reversed)" in {
      typecheck(Const(Data.Str("a")), Str).toOption should beNone
    }

    "fail with type and non-matching constant" in {
      typecheck(Str, Const(Data.Int(0))).toOption should beNone
    }

    "fail with type and non-matching constant (reversed)" in {
      typecheck(Const(Data.Int(0)), Str) should beFailing
    }

    // Properties:
    "succeed with same arbitrary type" ! prop { (t: Type) =>
      typecheck(t, t).toOption should beSome
    }

    "succeed with Top/arbitrary type" ! prop { (t: Type) =>
      typecheck(Top, t).toOption should beSome
    }

    "succeed with widening of product" ! (arbitrarySimpleType, arbitrarySimpleType) { (t1: Type, t2: Type) =>
      typecheck(t1, t1 & t2).toOption should beSome
    }

    "fail with narrowing to product" ! (arbitraryNonnestedType, arbitraryNonnestedType) { (t1: Type, t2: Type) =>
      // Note: using only non-product/coproduct input types here because otherwise this test
      // rejects many large inputs and the test is very slow.
      typecheck(t2, t1).isFailure ==> {
        typecheck(t1 & t2, t1).toOption should beNone
      }
    }

    "succeed with widening to coproduct" ! (arbitrarySimpleType, arbitrarySimpleType) { (t1: Type, t2: Type) =>
      typecheck(t1 | t2, t1).toOption should beSome
    }

    "fail with narrowing from coproduct" ! (arbitraryNonnestedType, arbitraryNonnestedType) { (t1: Type, t2: Type) =>
      // Note: using only non-product/coproduct input types here because otherwise this test
      // rejects many large inputs and the test is very slow.
      typecheck(t1, t2).isFailure ==> {
        typecheck(t1, t1 | t2).toOption should beNone
      }
    }

    "match under Obj with matching field name" ! prop { (t1: Type, t2: Type) =>
      typecheck(Obj(Map("a" -> t1), None), Obj(Map("a" -> t2), None)) must
        beEqualIfSuccess(typecheck(t1, t2))
    }

    "fail under Obj with non-matching field name and arbitrary types" ! prop { (t1: Type, t2: Type) =>
      typecheck(Obj(Map("a" -> t1), None), Obj(Map("b" -> t2), None)).toOption should beNone
    }

    "match unknowns under Obj" ! prop { (t1: Type, t2: Type) =>
      typecheck(Obj(Map(), Some(t1)), Obj(Map(), Some(t2))) must
        beEqualIfSuccess(typecheck(t1, t2))
    }

    "match under unknown/known Objs" ! prop { (t1: Type, t2: Type) =>
      typecheck(Obj(Map(), Some(t1)), Obj(Map("a" -> t2), None)) must
        beEqualIfSuccess(typecheck(t1, t2))
    }

    "match under Arr with matching index" ! prop { (t1: Type, t2: Type) =>
      typecheck(Arr(List(t1)), Arr(List(t2))) must
        beEqualIfSuccess(typecheck(t1, t2))
    }

    "match under FlexArr" ! prop { (t1: Type, t2: Type) =>
      typecheck(FlexArr(0, None, t1), FlexArr(0, None, t2)) must
        beEqualIfSuccess(typecheck(t1, t2))
    }

    "match under FlexArr/Arr" ! prop { (t1: Type, t2: Type) =>
      typecheck(FlexArr(0, None, t1), Arr(List(t2))) must
        beEqualIfSuccess(typecheck(t1, t2))
    }

    "match under Set" ! prop { (t1: Type, t2: Type) =>
      typecheck(Set(t1), Set(t2)) must beEqualIfSuccess(typecheck(t1, t2))
    }
  }

  "objectField" should {
    "reject arbitrary simple type" ! arbitrarySimpleType { (t: Type) =>
      t.objectField(const("a")).toOption should beNone
    }

    "reject simple type" in {
      Int.objectField(const("a")).toOption should beNone
    }

    "descend into singleton type" in {
      val obj = const("foo" -> Data.Str("bar"))
      obj.objectField(const("foo")).toOption should beSome(const("bar"))
    }

    "descend into singleton type with missing field" in {
      val obj = const("foo" -> Data.Str("bar"))
      obj.objectField(const("baz")).toOption should beNone
    }

    "descend into singleton type with Str field and return type of field value" in {
      val obj = const("foo" -> Data.Str("bar"))
      obj.objectField(Str).toOption should beSome(const("bar"))
    }

    "descend into singleton type with multiple fields and return coproduct of field values" in {
      val obj = const("foo" -> Data.Str("abc"), "bar" -> Data.Int(0))
      obj.objectField(Str).toOption should
        beSome(Coproduct(Const(Data.Str("abc")), Const(Data.Int(0))))
    }

    "descend into obj field type with const field" in {
      val field = Obj(Map("foo" -> Str), None)
      field.objectField(const("foo")).toOption should beSome(Str)
    }

    "descend into obj field type with missing field" in {
      val field = Obj(Map("foo" -> Str), None)
      field.objectField(const("bar")).toOption should beNone
    }

    "descend into product with const field" in {
      val obj = Obj(Map("foo" -> Str, "bar" -> Int), None)
      obj.objectField(const("bar")).toOption should beSome(Int)
    }

    "descend into product with Str" in {
      val obj = Obj(Map("foo" -> Str, "bar" -> Int), None)
      // TODO: result needs simplification? That would just produce Top at the moment
      obj.objectField(Str).toOption should beSome(Str | Int)
    }

    // JAD: Decide if this is correct or not
    "descend into coproduct with const field" in {
      val obj = Obj(Map("foo" -> Str), None) | Obj(Map("bar" -> Int), None)
      obj.objectField(Const(Data.Str("foo"))).toOption should beSome(Str)
    }

    "descend into coproduct with Str" in {
      val obj = Obj(Map("foo" -> Str), None) | Obj(Map("bar" -> Int), None)
      obj.objectField(Str).toOption should beSome(Str | Int)
    }
  }

  "children" should {
    "be Nil for arbitrary simple type" ! arbitraryTerminal { (t: Type) =>
      children(t) should_== Nil
    }

    "be list of one for arbitrary const type" ! arbitraryConst { (t: Type) =>
      children(t).length should_== 1
    }

    "be flattened for |" in {
      children(Int | Int | Str) should_== List(Int, Str)
    }
  }

  "foldMap" should {
    def intToStr(t: Type): Type = t match {
      case Int => Str
      case t => t
    }

    implicit val or = TypeOrMonoid

    "cast int to str" in {
      foldMap(intToStr)(Int) should_== Str
    }

    "ignore non-int" in {
      foldMap(intToStr)(Bool) should_== Bool
    }

    "cast const int to str" in {
      foldMap(intToStr)(Const(Data.Int(0))) should_== Const(Data.Int(0)) | Str
    }

    "ignore other const" in {
      foldMap(intToStr)(Const(Data.True)) should_== Const(Data.True) | Bool
    }

    "cast int to str in set" in {
      foldMap(intToStr)(Set(Int)) should_== Set(Int) | Str
    }

    "cast int to str in unknown Obj" in {
      foldMap(intToStr)(Obj(Map(), Some(Int))) should_== Obj(Map(), Some(Int)) | Str
    }

    "cast int to str in FlexArr" in {
      foldMap(intToStr)(FlexArr(0, None, Int)) should_== FlexArr(0, None, Int) | Str
    }

    implicit def list[T] = new scalaz.Monoid[List[T]] {
      def zero = Nil
      def append(l1: List[T], l2: => List[T]) = l1 ++ l2
    }

    def skipInt(t: Type): List[Type] = t match {
      case Int => Nil
      case _ => t :: Nil
    }

    "skip int" in {
      foldMap(skipInt)(Int) should_== Nil
    }

    "collect non-int" in {
      foldMap(skipInt)(Bool) should_== Bool :: Nil
    }

    "collect set and its child" in {
      foldMap(skipInt)(Set(Str)) should_== Set(Str) :: Str :: Nil
    }

    "collect set but skip child" in {
      foldMap(skipInt)(Set(Int)) should_== Set(Int) :: Nil
    }
  }

  "mapUp" should {
    val idp: PartialFunction[Type, Type] = { case t => t }

    "preserve arbitrary types" ! prop { (t: Type) =>
      mapUp(t)(idp) should_== t
    }

    val intToStr: PartialFunction[Type, Type] = {
      case Int => Str
    }

    "cast int to str" in {
      mapUp(Int)(intToStr) should_== Str
    }

    "cast const int to str" in {
      mapUp(Const(Data.Int(0)))(intToStr) should_== Str
    }

    "preserve other const" in {
      mapUp(Const(Data.True))(intToStr) should_== Const(Data.True)
    }

    "cast int to str in set" in {
      mapUp(Set(Int))(intToStr) should_== Set(Str)
    }

    "cast int to str in product/coproduct" in {
      mapUp(Int | (Dec & Int))(intToStr) should_== Str | (Dec & Str)
    }

    "cast int to str in unknown Obj" in {
      mapUp(Obj(Map(), Some(Int)))(intToStr) should_== Obj(Map(), Some(Str))
    }

    "cast int to str in FlexArr" in {
      mapUp(FlexArr(0, None, Int))(intToStr) should_== FlexArr(0, None, Str)
    }
  }

  "mapUpM" should {
    import scalaz.Id._

    "preserve arbitrary types" ! prop { (t: Type) =>
      mapUpM[Id](t)(ι) should_== t
    }

    def intToStr(t: Type): Type =
      t match {
        case Int => Str
        case _ => t
      }

    "cast int to str" in {
      mapUpM[Id](Int)(intToStr) should_== Str
    }

    "cast const int to str" in {
      mapUpM[Id](Const(Data.Int(0)))(intToStr) should_== Str
    }

    "preserve other const" in {
      mapUpM[Id](Const(Data.True))(intToStr) should_== Const(Data.True)
    }

    "cast int to str in set" in {
      mapUpM[Id](Set(Int))(intToStr) should_== Set(Str)
    }

    "cast int to str in product/coproduct" in {
      mapUpM[Id](Int | (Dec & Int))(intToStr) should_== Str | (Dec & Str)
    }

    "cast int to str in unknown Obj" in {
      mapUpM[Id](Obj(Map(), Some(Int)))(intToStr) should_== Obj(Map(), Some(Str))
    }

    "cast int to str in FlexArr" in {
      mapUpM[Id](FlexArr(0, None, Int))(intToStr) should_== FlexArr(0, None, Str)
    }

    import scalaz.std.list._

    def intAndStr(t: Type) = t match {
      case Int => t :: Str :: Nil
      case _ => t :: Nil
    }

    "yield int and str" in {
      mapUpM(Int)(intAndStr) should_== Int :: Str :: Nil
    }

    "yield int and str permutations" in {
      val t = Obj(Map("i" -> Int), Some(Int))
      mapUpM(t)(intAndStr) should_== List(
        Obj(Map("i" -> Int), Some(Int)),
        Obj(Map("i" -> Int), Some(Str)),
        Obj(Map("i" -> Str), Some(Int)),
        Obj(Map("i" -> Str), Some(Str)))
    }
  }

  "product" should {
    "have order-independent equality for arbitrary types" ! prop { (t1: Type, t2: Type) =>
      (t1 & t2) must_== (t2 & t1)
    }
  }

  "coproduct" should {
    "have order-independent equality" in {
      (Int | Str) must_== (Str | Int)
    }

    "have order-independent equality for arbitrary types" ! prop { (t1: Type, t2: Type) =>
      (t1 | t2) must_== (t2 | t1)
    }

    "be Bottom with no args" in {
      Coproduct(Nil) should_== Bottom
    }

    "return single arg" in {
      Coproduct(Int :: Nil) should_== Int
    }

    "wrap two args" in {
      Coproduct(Int :: Str :: Nil) should_== Int | Str
    }

    "have lub of Top for mixed types" in {
      (Int | Str).lub should_== Top
    }

    "have glb of Bottom for mixed types" in {
      (Int | Str).glb should_== Bottom
    }

    "have lub of Str for constant and Str" in {
      (Str | Const(Data.Str("b"))).lub should_== Str
    }

    "have lub of Str for different constants" in {
      (Const(Data.Str("a")) | Const(Data.Str("b"))).lub should_== Str
    }

    "have lub of Const for same constant" in {
      (Const(Data.Str("a")) | Const(Data.Str("a"))).lub should_== Const(Data.Str("a"))
    }

    // TODO: property:
    // cp.simplify.lub = cp.lub
  }

  "type" should {
    // Properties:
    "have t == t for arbitrary type" ! prop { (t: Type) =>
      t must_== t
    }

    "simplify int|int to int" in {
      simplify(Int | Int) should_== Int
    }

    "simplify int&int to int" in {
      simplify(Int & Int) should_== Int
    }

    "simplify nested product/coproduct to flat" in {
      simplify((Int | Str) & (Int | Str)) should_== Int | Str
    }

    "simplify nested coproduct" in {
      simplify((Int | Str) | (Int | Str)) should_== Int | Str
    }

    "simplify nested coproduct/product to flat" in {
      simplify((Int & Str) | (Int & Str)) should_== Int & Str
    }

    "simplify nested product" in {
      simplify((Int & Str) & (Int & Str)) should_== Int & Str
    }

    "simplify int&top to int" in {
      simplify(Int & Top) should_== Int
    }

    "simplify int&bottom to Bottom" in {
      simplify(Int & Bottom) should_== Bottom
    }

    "simplify int|top to Top" in {
      simplify(Int | Top) should_== Top
    }

    "simplify int|bottom to int" in {
      simplify(Int | Bottom) should_== Int
    }


    // Properties for product:
    "simplify t & t to t" ! prop { (t: Type) =>
      simplify(t & t) must_== simplify(t)
    }

    "simplify Top & t to t" ! prop { (t: Type) =>
      simplify(Top & t) must_== simplify(t)
    }

    "simplify Bottom & t to Bottom" ! prop { (t: Type) =>
      simplify(Bottom & t) must_== Bottom
    }

    // Properties for coproduct:
    "simplify t | t to t" ! prop { (t: Type) =>
      simplify(t | t) must_== simplify(t)
    }

    "simplify Top | t to Top" ! prop { (t: Type) =>
      simplify(Top | t) must_== Top
    }

    "simplify Bottom | t to t" ! prop { (t: Type) =>
      simplify(Bottom | t) must_== simplify(t)
    }



    "lub simple match" in {
      lub(Int, Int) should_== Int
    }

    "glb simple match" in {
      glb(Int, Int) should_== Int
    }

    "lub int|str/int" in {
      lub(Int | Str, Int) should_== (Int | Str)
    }

    "lub int|str/int with args reversed" in {
      lub(Int, Int | Str) should_== (Int | Str)
    }

    "glb int|str/int" in {
      glb(Int | Str, Int) should_== Int
    }

    "glb int|str/int with args reversed" in {
      glb(Int, Int | Str) should_== Int
    }

    "lub with no match" in {
      lub(Int, Str) should_== Top
    }

    "glb with no match" in {
      glb(Int, Str) should_== Bottom
    }

    "lub with different constants of same type" in {
      lub(const("a"), const("b")) should_== Str
    }

    "lub with constants of different types" in {
      lub(const("a"), Const(Data.Int(0))) should_== Top
    }

    "lub with constant and same type" in {
      lub(Const(Data.Str("a")), Str) should_== Str
    }

    "lub with constant and same type (reversed)" in {
      lub(Str, Const(Data.Str("a"))) should_== Str
    }

    "glb with different constants" in {
      glb(const("a"), const("b")) should_== Bottom
    }

    "glb with constant and same type" in {
      glb(Const(Data.Str("a")), Str) should_== Const(Data.Str("a"))
    }

    "glb with constant and same type (reversed)" in {
      glb(Str, Const(Data.Str("a"))) should_== Const(Data.Str("a"))
    }

    "lub with Top" ! prop { (t: Type) =>
      lub(t, Top) should_== Top
    }

    "lub with Bottom" ! prop { (t: Type) =>
      lub(t, Bottom) should_== t
    }

    "lub symmetric for nonnested" ! (arbitraryNonnestedType, arbitraryNonnestedType) { (t1: Type, t2: Type) =>
      lub(t1, t2) should_== lub(t2, t1)
    }

    "lub symmetric" ! prop { (t1: Type, t2: Type) =>
      lub(t1, t2) should_== lub(t2, t1)
    }

    "glb with Top" ! prop { (t: Type) =>
      glb(t, Top) should_== t
    }

    "glb with Bottom" ! prop { (t: Type) =>
      glb(t, Bottom) should_== Bottom
    }

    "glb symmetric for non-nested" ! (arbitraryNonnestedType, arbitraryNonnestedType) { (t1: Type, t2: Type) =>
      glb(t1, t2) should_== glb(t2, t1)
    }

    "glb symmetric" ! prop { (t1: Type, t2: Type) =>
      glb(t1, t2) should_== glb(t2, t1)
    }

    val exField = Obj(Map(), Some(Int))
    val exNamed = Obj(Map("i" -> Int), None)
    val exConstObj = Const(Data.Obj(Map("a" -> Data.Int(0))))
    val exElem = FlexArr(0, None, Int)
    val exIndexed = Arr(List(Int))
    val exSet = Set(Int)

    val examples =
      List(Top, Bottom, Null, Str, Int, Dec, Bool, Binary, Timestamp, Date, Time, Interval,
          Const(Data.Int(0)),
          Int & Str, Int | Str,
          exField, exNamed, exConstObj, exElem, exIndexed, exSet)

    "only fields and objects are objectLike" in {
      examples.filter(_.objectLike) should_== List(exField, exNamed, exConstObj)
    }

    "only elems are arrayLike" in {
      examples.filter(_.arrayLike) should_== List(exElem, exIndexed)
    }

    "only sets are setLike" in {
      examples.filter(_.setLike) should_== List(exSet)
    }

    "empty array constant is arrayLike" in {
      Const(Data.Arr(List())).arrayLike must_== true
    }

    "arrayType for simple type" in {
      Int.arrayType should beNone
    }

    "arrayType for FlexArr" in {
      FlexArr(0, None, Int).arrayType should beSome(Int)
    }

    "arrayType for Arr" in {
      Arr(List(Int)).arrayType should beSome(Int)
    }

    "arrayType for product" in {
      (FlexArr(0, None, Int) & FlexArr(0, None, Int)).arrayType should beSome(Int)
    }

    "arrayType for product with mixed types" in {
      (FlexArr(0, None, Int) & FlexArr(0, None, Str)).arrayType should beSome(Int | Str)
    }
  }

  "arrayElem" should {
    "fail for non-array type" ! arbitrarySimpleType { (t: Type) =>
      t.arrayElem(Const(Data.Int(0))) should beFailing//WithClass[TypeError]
    }

    "fail for non-int index"  ! arbitrarySimpleType { (t: Type) =>
      // TODO: this occasionally get stuck... maybe lub() is diverging?
      lub(t, Int) != Int ==> {
        val arr = Const(Data.Arr(Nil))
        arr.arrayElem(t) should beFailing
      }
    }

    "descend into const array with const index" in {
      val arr = Const(Data.Arr(List(Data.Int(0), Data.Str("a"), Data.True)))
      arr.arrayElem(Const(Data.Int(1))) should beSuccessful(Const(Data.Str("a")))
    }

    "descend into const array with unspecified index" in {
      val arr = Const(Data.Arr(List(Data.Int(0), Data.Str("a"), Data.True)))
      arr.arrayElem(Int) should
        beSuccessful(Const(Data.Int(0)) | Const(Data.Str("a")) | Const(Data.True))
    }

    "descend into FlexArr with const index" in {
      FlexArr(0, None, Str).arrayElem(Const(Data.Int(0))) should beSuccessful(Str)
    }

    "descend into FlexArr with unspecified index" in {
      FlexArr(0, None, Str).arrayElem(Int) should beSuccessful(Str)
    }

    "descend into product of FlexArrs with const index" in {
      val arr = FlexArr(0, None, Int) & FlexArr(0, None, Str)
          arr.arrayElem(Const(Data.Int(0))) should beSuccessful(Int | Str)
    }

    "descend into product of FlexArrss with unspecified index" in {
      val arr = FlexArr(0, None, Int) & FlexArr(0, None, Str)
      arr.arrayElem(Int) should beSuccessful(Int | Str)
    }

    "descend into FlexArr with non-int" in {
      FlexArr(0, None, Str).arrayElem(Str) should beFailing
    }

    "descend into Arr" in {
      Arr(List(Int, Top, Bottom, Str)).arrayElem(Const(Data.Int(3))) should beSuccessful(Str)
    }

    "descend into Arr with wrong index" in {
      Arr(List(Int, Top, Bottom, Str)).arrayElem(Const(Data.Int(5))) should beFailing
    }

    "descend into multiple Arr" in {
      val arr = Arr(List(Int, Str))
      arr.arrayElem(Const(Data.Int(1))) should beSuccessful(Str)
    }

    "descend into multi-element Arr with unspecified index" in {
      val arr = Arr(List(Int, Str))
      arr.arrayElem(Int) should beSuccessful(Int | Str)
    }

    // TODO: tests for coproducts
  }
}
