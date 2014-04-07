package slamdata.engine.std

import org.specs2.mutable._
import org.specs2.ScalaCheck
import org.scalacheck.Arbitrary
import org.scalacheck.Arbitrary.arbitrary
import org.scalacheck.Gen
import org.specs2.matcher.Matcher
import scalaz.Validation
import scalaz.Success
import scalaz.Failure

class LibrarySpec extends Specification with ScalaCheck {
  import MathLib._
  import RelationsLib._
  import scalaz.ValidationNel
  import slamdata.engine.Type
  import slamdata.engine.Type.Const
  import slamdata.engine.Type.NamedField
//  import slamdata.engine.Type.Numeric
  import slamdata.engine.Data.Bool
  import slamdata.engine.Data.Dec
  import slamdata.engine.Data.Int
  import slamdata.engine.Data.Number
  import slamdata.engine.Data.Str
  import slamdata.engine.SemanticError
  
  val zero = Const(Int(0))
  
  "MathLib" should {
    "type simple add with ints" in {
    	val expr = Add(Type.Int, Type.Int)
    	expr should beSuccess(Type.Int)
    }
    
    "type simple add with decs" in {
    	val expr = Add(Type.Dec, Type.Dec)
    	expr should beSuccess(Type.Dec)
    }
    
    "type simple add with promotion" in {
    	val expr = Add(Type.Int, Type.Dec)
    	expr should beSuccess(Type.Dec)
    }
    
    "fold simple add with int constants" in {
    	val expr = Add(Const(Int(1)), Const(Int(2)))
    	expr should beSuccess(Const(Int(3)))
    }
    
    "fold simple add with decimal constants" in {
    	val expr = Add(Const(Dec(1.0)), Const(Dec(2.0)))
    	expr should beSuccess(Const(Dec(3)))
    }
    
    "fold simple add with promotion" in {
    	val expr = Add(Const(Int(1)), Const(Dec(2.0)))
    	expr should beSuccess(Const(Dec(3)))
    }
    
    "eliminate multiply by zero (on the right)" ! prop { (c : Const) => 
      val expr = Multiply(c, zero)
      expr should beSuccess(zero) 
    }
    
    "eliminate multiply by zero (on the left)" ! prop { (c : Const) => 
      val expr = Multiply(zero, c)
      expr should beSuccess(zero) 
    }
    
    "fold simple division" in { 
      val expr = Divide(Const(Int(6)), Const(Int(3)))
      expr should beSuccess(Const(Int(2))) 
    }
    
    "fold truncating division" in { 
      val expr = Divide(Const(Int(5)), Const(Int(2)))
      expr should beSuccess(Const(Int(2))) 
    }
    
    "fold simple division (dec)" in { 
      val expr = Divide(Const(Int(6)), Const(Dec(3.0)))
      expr should beSuccess(Const(Dec(2.0))) 
    }
    
    "fold division (dec)" in { 
      val expr = Divide(Const(Int(5)), Const(Dec(2)))
      expr should beSuccess(Const(Dec(2.5))) 
    }
    
    "divide by zero" in { 
      val expr = Divide(Const(Int(1)), zero)
      expr must beFailure  // Currently Success(Const(Int(1))) !?
    }
    
    "divide by zero (dec)" in { 
      val expr = Divide(Const(Dec(1.0)), Const(Dec(0.0)))
      expr must beFailure  // Currently Success(Const(Dec(1.0))) !?
    }
    
    "fold a complex expression (10-4)/3 + (5*8)" in {
      // use a for comprehension to capture the Success values
      val expr = for {
    	x1 <- Subtract(Const(Int(10)),
    				   Const(Int(4)));
    	x2 <- Divide(x1,
    				 Const(Int(3)));
    	x3 <- Multiply(Const(Int(5)),
    		           Const(Int(8)));
    	x4 <- Add(x2, x3)
      } yield x4
      expr should beSuccess(Const(Int(42)))
    }
    
    "widen to named field(?)" in {
      val expr = Add(NamedField("x", Type.Int), Const(Int(1)))
      println("x + 1: " + expr)
      expr should beSuccess
    }
    
    // TODO: tests for unapply() in general
  }
  
  "RelationsLib" should {
    
    "type eq with matching arguments" ! prop { (t : Type) =>
      val expr = Eq(t, t)
      t match {
        case Const(_) => expr should beSuccess(Const(Bool(true)))
        case _ => expr should beSuccess(Type.Bool)
      }
    }
    
    "fold integer eq" in {
      val expr = Eq(Const(Int(1)), Const(Int(1)))
      expr should beSuccess(Const(Bool(true)))
    }
    
    "fold eq with mixed numeric type" in {
      val expr = Eq(Const(Int(1)), Const(Dec(1.0)))
      expr should beSuccess(Const(Bool(true)))
    }
    
    "fold eq with mixed type" in {
      val expr = Eq(Const(Int(1)), Const(Str("a")))
      expr should beSuccess(Const(Bool(false)))
    }
    
    // TODO: 
    
    // TODO: similar for the rest of the simple relations

    "fold cond with true" ! prop { (t1 : Type, t2 : Type) => 
      val expr = Cond(Const(Bool(true)), t1, t2)
      expr must beSuccess(t1) 
    }
    
    "fold cond with false" ! prop { (t1 : Type, t2 : Type) => 
      val expr = Cond(Const(Bool(false)), t1, t2)
      expr must beSuccess(t2) 
    }
    
    "find lub for cond with int" in { 
      val expr = Cond(Type.Bool, Type.Int, Type.Int)
      expr must beSuccess(Type.Int)
    }
    
    "find lub for cond with arbitrary args" ! prop { (t1 : Type, t2 : Type) => 
      val expr = Cond(Type.Bool, t1, t2)
      expr must beSuccess(Type.lub(t1, t2))
    }
    
    // TODO: 
  }
  
  "AggLib" should {
    // TODO: ...     
  }
  
  
  
  implicit def genConst : Arbitrary[Const] = Arbitrary { 
    for { i <- Arbitrary.arbitrary[scala.Int] } 
      yield Const(Int(i)) 
  }
  
  implicit def genType : Arbitrary[Type] = Arbitrary {
    Gen.oneOf(Type.Null, Type.Bool, Type.Int, Type.Dec, 
                Type.Binary, Type.Str, Type.DateTime, Type.Interval, 
    			Type.Const(Int(0)),
    			Type.Const(Dec(0.0)),
    			Type.Const(Bool(false)),
    			Type.Const(Str("abc")))
  }
  
//  def genAdd : Arbitrary[Type] = Arbitrary {
//    for { left <- arbitrary[Type]; right <- arbitrary[Type] }
////      yield Add(left, right)
//      yield match Add(left, right) {
//        case Success(t) => t
//        }
//  }
//  
//  implicit def genExpr : Arbitrary[Type] = 
//    Gen.oneOf(genConst, genAdd)//, genSubtract, genMultiply, genDivide)
  
  // Note: does not produce a nice description when the match fails
  def beFailure : Matcher[ValidationNel[SemanticError, Type]] = 
    { (t : Any) =>
      t match {
        case Failure(_) => true 
      }
    }

  // Note: does not produce a nice description when the match fails
  def beSuccess : Matcher[ValidationNel[SemanticError, Type]] = 
    { (t : Any) =>
      t match {
        case Success(_) => true 
      }
    }

  // Note: does not produce a nice description when the match fails
  def beSuccess(expected : Type) : Matcher[ValidationNel[SemanticError, Type]] =
    { (t : Any) =>
      t match {
        case Success(v) if (v == expected) => true 
      }
    }
}
