package slamdata.engine

import org.specs2.mutable._
import org.specs2.ScalaCheck
import scalaz.Validation.{success, failure}

@org.junit.runner.RunWith(classOf[org.specs2.runner.JUnitRunner])
class TypesSpec extends Specification with ScalaCheck {
  import Type._
  import TypeGen._
    
  val LatLong = NamedField("lat", Dec) & NamedField("long", Dec)
  val Azim = NamedField("az", Dec)

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

    "succeed with simple object widening" in {
      typecheck(LatLong, LatLong & Azim).toOption should beSome
    }

    "fails with simple object narrowing" in {
      typecheck(LatLong & Azim, LatLong).toOption should beNone
    }

    "succeed with coproduct(int|dec)/coproduct(int|dec)" in {
      typecheck(Int | Dec, Dec | Int).toOption should beSome
    }

   "fail with (int&int)/int" in {
     typecheck(Int & Int, Int).toOption should beSome
   }
   
   "fail with (int&str)/int" in {
     typecheck(Int & Str, Int).toOption should beNone
   }
  }
  
  "objectField" should {
    "reject arbitrary simple type" ! prop { (t: Type) => 
      t match {
//        case NamedField(_, _) => 1 must_== 1 // HACK
        case Int => t.objectField(const("a")).toOption should beNone
        case _ => 1 must_== 1
      }
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
	"be Nil for Top" in {
	  children(Top) should_== Nil
	}
	
	"be Nil for Bottom" in {
	  children(Bottom) should_== Nil
	}
	
	// Note: there are many more cases, but nearly all trivial
	
	"be flattened for &" in {
	  children(Int & Int & Str) should_== List(Int, Int, Str)
	}
	
	"be flattened for |" in {
	  children(Int | Int | Str) should_== List(Int, Int, Str)
	}
  }
  
  "foldMap" should {
//    "or" in {
//      foldMap(t => TypeOrMonoid)(Int) should_== Int
//    }
  }
  
  "mapUp" should {
     val intToStr: PartialFunction[Type, Type] = {
      case Int => Str
    }

    "cast int to str" in {
      mapUp(Int)(intToStr) should_== Str
    }

    "cast const int to str" in {
      mapUp(Const(Data.Int(0)))(intToStr) should_== Str
    }

    "ignore other const" in {
      mapUp(Const(Data.True))(intToStr) should_== Const(Data.True)
      // required a fix in types.scala to retain Const-ness
    }
    
    "cast int to str in set" in {
      mapUp(Set(Int))(intToStr) should_== Set(Str)
    }

    // TODO: AnonElem, IndexedElem, AnonField, NamedField
    
    "cast int to str in product/coproduct" in {
      mapUp(Int | (Dec & Int))(intToStr) should_== Str | (Dec & Str)
    } 
  }
  
  // TODO: "mapUpM", analagous to "mapUp" 
  
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

    // TODO: is there any Const with an array type? Data.Arr ends up with a Product type
//    "arrayType for array Const" in {
//      Const(Data.Arr(???)).arrayType should beSome(???)
//    }
    
    "arrayType for product" in {
      (AnonElem(Int) & AnonElem(Int)).arrayType should beSome(Int)
    }
    
    "arrayType for product with mixed types" in {
      (AnonElem(Int) & AnonElem(Str)).arrayType should beSome(Top)
    }
  }
  
  "arrayElem" should {
    "descend into const array with const index" in {
      val arr = Const(Data.Arr(List(Data.Int(0), Data.Str("a"), Data.True)))
      arr.arrayElem(Const(Data.Int(1))).toOption should beSome(Const(Data.Str("a")))
    }  

    "descend into const array" in {
      val arr = Const(Data.Arr(List(Data.Int(0), Data.Str("a"), Data.True)))
      arr.arrayElem(Int).toOption should beSome(Top)
      // Arguably should be Int | Str | Bool but it looks like the code intends to produce 
      // the lub, which would be Top.
      // Anyway it currently fails producing Some(Bool)
    }.pendingUntilFixed

    "descend into AnonElem with const int" in {
      AnonElem(Str).arrayElem(Const(Data.Int(0))).toOption should beSome(Str)
    }  

    "descend into AnonElem with int" in {
      AnonElem(Str).arrayElem(Int).toOption should beSome(Str)
    }  

    "descend into AnonElem with non-int" in {
      AnonElem(Str).arrayElem(Str).toOption should beNone
    }  

    "descend into IndexedElem" in {
      IndexedElem(3, Str).arrayElem(Const(Data.Int(3))).toOption should beSome(Str)
    }  

    "descend into IndexedElem with wrong index" in {
      IndexedElem(3, Str).arrayElem(Const(Data.Int(5))).toOption should beNone
    }  
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
