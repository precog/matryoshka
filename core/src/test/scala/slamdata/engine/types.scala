package slamdata.engine

import org.specs2.mutable._
import org.specs2.ScalaCheck
import ValidationMatchers._
import scalaz.Validation.{success, failure}
import scalaz.Monad
import slamdata.specs2._

@org.junit.runner.RunWith(classOf[org.specs2.runner.JUnitRunner])
class TypesSpec extends Specification with ScalaCheck with PendingWithAccurateCoverage {
  import Type._
  import SemanticError._
  
  import TypeGen._
    
  val LatLong = NamedField("lat", Dec) & NamedField("long", Dec)
  val Azim = NamedField("az", Dec)

  def const(s: String): Type = Const(Data.Str(s))
  def const(elems: (String, Data)*): Type = Const(Data.Obj(Map(elems: _*)))
  
  "typecheck" should {
    "succeed with int/int" in {
      typecheck(Int, Int) should beSuccess
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

    "fail with simple object narrowing" in {
      typecheck(LatLong & Azim, LatLong) should beFailureWithClass[TypeError, Unit]
    }

    "succeed with coproduct(int|dec)/coproduct(int|dec)" in {
      typecheck(Int | Dec, Dec | Int).toOption should beSome
    }

    "succeed with (int&int)/int" in {
      typecheck(Int & Int, Int).toOption should beSome
    }
   
    "fails with (int&str)/int" in {
      typecheck(Int & Str, Int) should beFailureWithClass[TypeError, Unit]
    }
    
    
    // Properties:    
    "succeed with same arbitrary type" ! prop { (t: Type) =>
      typecheck(t, t) should beSuccess
    }
    
    "succeed with Top/arbitrary type" ! prop { (t: Type) =>
      typecheck(Top, t) should beSuccess
    }
    
    "succeed with widening of product" ! (arbitrarySimpleType, arbitrarySimpleType) { (t1: Type, t2: Type) =>
      typecheck(t1, t1 & t2) should beSuccess
    }
        
    "fail with narrowing to product" ! (arbitraryNonnestedType, arbitraryNonnestedType) { (t1: Type, t2: Type) =>
      // Note: using only non-product/coproduct input types here because otherwise this test
      // rejects many large inputs and the test is very slow.
      typecheck(t2, t1).isFailure ==> {
        typecheck(t1 & t2, t1) should beFailureWithClass[TypeError, Unit]
      }
    }
    
    "succeed with widening to coproduct" ! (arbitrarySimpleType, arbitrarySimpleType) { (t1: Type, t2: Type) =>
      typecheck(t1 | t2, t1) should beSuccess
    }
    
    "fail with narrowing from coproduct" ! (arbitraryNonnestedType, arbitraryNonnestedType) { (t1: Type, t2: Type) =>
      // Note: using only non-product/coproduct input types here because otherwise this test
      // rejects many large inputs and the test is very slow.
      typecheck(t1, t2).isFailure ==> {
        typecheck(t1, t1 | t2) should beFailureWithClass[TypeError, Unit]
      }
    }
    
    "match under NamedField with matching field name" ! prop { (t1: Type, t2: Type) =>
      typecheck(NamedField("a", t1), NamedField("a", t2)) should_== typecheck(t1, t2)
    }
    
    "fail under NamedField with non-matching field name and arbitrary types" ! prop { (t1: Type, t2: Type) =>
      typecheck(NamedField("a", t1), NamedField("b", t2)) should beFailure
    }
    
    "match under AnonField" ! prop { (t1: Type, t2: Type) =>
      typecheck(AnonField(t1), AnonField(t2)) should_== typecheck(t1, t2)
    }
    
    "match under AnonField/NamedField" ! prop { (t1: Type, t2: Type) =>
      typecheck(AnonField(t1), NamedField("a", t2)) should_== typecheck(t1, t2)
    }
    
    "match under NamedField/AnonField" ! prop { (t1: Type, t2: Type) =>
      typecheck(NamedField("a", t1), AnonField(t2)) should_== typecheck(t1, t2)
    }
    

    "match under IndexedElem with matching index" ! prop { (i: Int, t1: Type, t2: Type) =>
      typecheck(IndexedElem(i, t1), IndexedElem(i, t2)) should_== typecheck(t1, t2)
    }
    
    "fail under IndexedElem with non-matching index and arbitrary types" ! prop { (i: Int, j: Int, t1: Type, t2: Type) =>
      i != j ==> { 
        typecheck(IndexedElem(i, t1), IndexedElem(j, t2)) should beFailure 
      }
    }
    
    "match under AnonElem" ! prop { (t1: Type, t2: Type) =>
      typecheck(AnonElem(t1), AnonElem(t2)) should_== typecheck(t1, t2)
    }
    
    "match under AnonElem/IndexedElem" ! prop { (t1: Type, i: Int, t2: Type) =>
      typecheck(AnonElem(t1), IndexedElem(i, t2)) should_== typecheck(t1, t2)
    }
    
    "match under NamedField/AnonField" ! prop { (i: Int, t1: Type, t2: Type) =>
      typecheck(IndexedElem(i, t1), AnonElem(t2)) should_== typecheck(t1, t2)
    }
    
    
    "match under Set" ! prop { (t1: Type, t2: Type) =>
      typecheck(Set(t1), Set(t2)) should_== typecheck(t1, t2)
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
      obj.objectField(Str).toOption should beSome(Str)
    }

    "descend into singleton type with multiple fields and return lub of field values" in {
      val obj = const("foo" -> Data.Str("abc"), "bar" -> Data.Int(0))
      obj.objectField(Str).toOption should beSome(Top)
    }

    "descend into obj field type with const field" in {
      val field = NamedField("foo", Str)
      field.objectField(const("foo")).toOption should beSome(Str)
    }
    
    "descend into obj field type with missing field" in {
      val field = NamedField("foo", Str)
      field.objectField(const("bar")).toOption should beSome(Top | Bottom)
    }
    
    "descend into product with const field" in {
      val obj = NamedField("foo", Str) & NamedField("bar", Int)
      obj.objectField(const("bar")).toOption should beSome(Int)
    }
    
    "descend into product with Str" in {
      val obj = NamedField("foo", Str) & NamedField("bar", Str)
      // TODO: result needs simplification? That would just produce Top at the moment
      obj.objectField(Str).toOption should beSome((Str | Top | Bottom) & (Str | Top | Bottom))
    }

    // JAD: Decide if this is correct or not
    "descend into coproduct with const field" in {
      val obj = NamedField("foo", Str) | NamedField("bar", Int)
      obj.objectField(Const(Data.Str("foo"))).toOption should beSome(Str | Top | Bottom)
    }
    
    "descend into coproduct with Str" in {
      val obj = NamedField("foo", Str) | NamedField("bar", Int)
      obj.objectField(Str).toOption should beSome(Str | Int | Top | Bottom)
    }
  }

  "children" should {
    "be Nil for arbitrary simple type" ! arbitraryTerminal { (t: Type) =>
      children(t) should_== Nil
    }
    
    "be list of one for arbitrary const type" ! arbitraryConst { (t: Type) =>
      children(t).length should_== 1
    }
    
    "be flattened for &" in {
      children(Int & Int & Str) should_== List(Int, Int, Str)
    }
    
    "be flattened for |" in {
      children(Int | Int | Str) should_== List(Int, Int, Str)
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

    "cast int to str in AnonField" in {
      foldMap(intToStr)(AnonField(Int)) should_== AnonField(Int) | Str
    } 

    "cast int to str in AnonElem" in {
      foldMap(intToStr)(AnonElem(Int)) should_== AnonElem(Int) | Str
    } 
    
    "cast int to str in product" in {
      foldMap(intToStr)(Int & Bool & Dec & Null) should_== 
        (Int & Bool & Dec & Null) | (Str | Bool | Dec | Null)
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

    "cast int to str in AnonField" in {
      mapUp(AnonField(Int))(intToStr) should_== AnonField(Str)
    } 

    "cast int to str in AnonElem" in {
      mapUp(AnonElem(Int))(intToStr) should_== AnonElem(Str)
    } 
  }
  
  "mapUpM" should {
    import scalaz.Id._
    
    "preserve arbitrary types" ! prop { (t: Type) =>
      mapUpM[Id](t)(identity) should_== t
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

    "cast int to str in AnonField" in {
      mapUpM[Id](AnonField(Int))(intToStr) should_== AnonField(Str)
    } 

    "cast int to str in AnonElem" in {
      mapUpM[Id](AnonElem(Int))(intToStr) should_== AnonElem(Str)
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
      val t = AnonField(Int) & NamedField("i", Int)
      mapUpM(t)(intAndStr) should_== List(
            AnonField(Int) & NamedField("i", Int),
            AnonField(Int) & NamedField("i", Str),
            AnonField(Str) & NamedField("i", Int),
            AnonField(Str) & NamedField("i", Str))
    }
  }
  
  "product" should {
    "have order-independent equality" in {
      (Int & Str) must_== (Str & Int)
    }

    "have order-independent equality for arbitrary types" ! prop { (t1: Type, t2: Type) =>
      (t1 & t2) must_== (t2 & t1)
    }
    
    "be Top with no args" in {
      Product(Nil) should_== Top
    }
    
    "return single arg" in {
      Product(Int :: Nil) should_== Int
    }
    
    "wrap two args" in {
      Product(Int :: Str :: Nil) should_== Int & Str
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
      lub(Int | Str, Int) should_== Int
    }
    
    "lub int|str/int with args reversed" in {
      lub(Int, Int | Str) should_== Int
    }
    
    "glb int|str/int" in {
      glb(Int | Str, Int) should_== (Int | Str)
    }
    
    "glb int|str/int with args reversed" in {
      glb(Int, Int | Str) should_== (Int | Str)
    }
    
    "lub with no match" in {
      lub(Int, Str) should_== Top
    }
    
    "glb with no match" in {
      glb(Int, Str) should_== Bottom
    }
    
    val exField = AnonField(Int)
    val exNamed = NamedField("i", Int)
    val exConstObj = Const(Data.Obj(Map("a" -> Data.Int(0))))
    val exElem = AnonElem(Int)
    val exIndexed = IndexedElem(0, Int)
    val exSet = Set(Int)
    
    val examples = 
      List(Top, Bottom, Null, Str, Int, Dec, Bool, Binary, DateTime, Interval,  
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
    
    "arrayType for simple type" in {
      Int.arrayType should beNone
    }
    
    "arrayType for AnonElem" in {
      AnonElem(Int).arrayType should beSome(Int)
    }
    
    "arrayType for IndexedElem" in {
      IndexedElem(0, Int).arrayType should beSome(Int)
    }
    
    "arrayType for product" in {
      (AnonElem(Int) & AnonElem(Int)).arrayType should beSome(Int)
    }
    
    "arrayType for product with mixed types" in {
      (AnonElem(Int) & AnonElem(Str)).arrayType should beSome(Top)
    }
  }
  
  "arrayElem" should {
    "fail for non-array type" ! arbitrarySimpleType { (t: Type) =>
      t.arrayElem(Const(Data.Int(0))) should beFailure//WithClass[TypeError]
    }
    
    "fail for non-int index"  ! arbitrarySimpleType { (t: Type) => 
      // TODO: this occasionally get stuck... maybe lub() is diverging?
      lub(t, Int) != Int ==> {
        val arr = Const(Data.Arr(Nil))
        arr.arrayElem(t) should beFailure
      }
    }
    
    "descend into const array with const index" in {
      val arr = Const(Data.Arr(List(Data.Int(0), Data.Str("a"), Data.True)))
      arr.arrayElem(Const(Data.Int(1))) should beSuccess(Const(Data.Str("a")))
    }

    "descend into const array with unspecified index" in {
      val arr = Const(Data.Arr(List(Data.Int(0), Data.Str("a"), Data.True)))
      arr.arrayElem(Int) should beSuccess(Int | Str | Bool)
    }

    "descend into AnonElem with const index" in {
      AnonElem(Str).arrayElem(Const(Data.Int(0))) should beSuccess(Str)
    }  

    "descend into AnonElem with unspecified index" in {
      AnonElem(Str).arrayElem(Int) should beSuccess(Str)
    }  

    "descend into product of AnonElems with const index" in {
      val arr = AnonElem(Int) & AnonElem(Str) 
          arr.arrayElem(Const(Data.Int(0))) should beSuccess(Int | Str)
    }
    
    "descend into product of AnonElems with unspecified index" in {
      val arr = AnonElem(Int) & AnonElem(Str) 
      arr.arrayElem(Int) should beSuccess(Int | Str)
    }

    "descend into AnonElem with non-int" in {
      AnonElem(Str).arrayElem(Str) should beFailure
    }  

    "descend into IndexedElem" in {
      IndexedElem(3, Str).arrayElem(Const(Data.Int(3))) should beSuccess(Str)
    }  
    
    "descend into IndexedElem with wrong index" in {
      IndexedElem(3, Str).arrayElem(Const(Data.Int(5))) should beFailure
    }  

    "descend into multiple IndexedElem" in {
      val arr = IndexedElem(0, Int) & IndexedElem(1, Str)
      arr.arrayElem(Const(Data.Int(1))) should beSuccess(Str)
    }.pendingUntilFixed

    "descend into multiple IndexedElem with unspecified index" in {
      val arr = IndexedElem(0, Int) & IndexedElem(1, Str)
      arr.arrayElem(Int) should beSuccess(Int | Str)
    }
    
    // TODO: tests for coproducts
  }  
  
  "makeObject" should {
    "make product" in {
      makeObject(("i", Int) :: ("s", Str) :: Nil) should_==
        NamedField("i", Int) & NamedField("s", Str)
    }
  }
  
  "makeArray" should {
    "make const array" in {
      makeArray(Const(Data.Int(0)) :: Nil) should_== 
        Const(Data.Arr(Data.Int(0) :: Nil))
    }
    
    "make product of indexed elems for types" in {
      makeArray(Bool :: Str :: Nil) should_== 
        IndexedElem(0, Bool) & IndexedElem(1, Str)
    }
    
    "make product for mixed const and type" in {
      makeArray(Dec :: Const(Data.True) :: Nil) should_== 
        IndexedElem(0, Dec) & IndexedElem(1, Const(Data.True))
    }
  }
}
