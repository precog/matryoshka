package slamdata.engine.std

import org.specs2.mutable._
import org.specs2.ScalaCheck
import org.scalacheck.Arbitrary
import org.scalacheck.Arbitrary.arbitrary
import org.scalacheck.Gen
import org.specs2.matcher.Matcher
import slamdata.specs2._

import scalaz.Validation
import scalaz.Validation.FlatMap._
import scalaz.Success
import scalaz.Failure

import slamdata.engine.ValidationMatchers

class LibrarySpec extends Specification with ScalaCheck with ValidationMatchers with PendingWithAccurateCoverage {
  import MathLib._
  // import scalaz.ValidationNel
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
    }.pendingUntilFixed
    
    "divide by zero (dec)" in { 
      val expr = Divide(Const(Dec(1.0)), Const(Dec(0.0)))
      expr must beFailure  // Currently Success(Const(Dec(1.0))) !?
    }.pendingUntilFixed

    "fold simple modulo" in {
      val expr = Modulo(Const(Int(6)), Const(Int(3)))
      expr should beSuccess(Const(Int(0))) 
    }
    
    "fold non-zero modulo" in { 
      val expr = Modulo(Const(Int(5)), Const(Int(2)))
      expr should beSuccess(Const(Int(1))) 
    }
    
    "fold simple modulo (dec)" in { 
      val expr = Modulo(Const(Int(6)), Const(Dec(3.0)))
      expr should beSuccess(Const(Dec(0.0))) 
    }
    
    "fold non-zero modulo (dec)" in { 
      val expr = Modulo(Const(Int(5)), Const(Dec(2.2)))
      expr should beSuccess(Const(Dec(0.6))) 
    }
    
    "modulo by zero" in { 
      val expr = Modulo(Const(Int(1)), zero)
      expr must beFailure  // Currently Success(Const(Int(1))) !?
    }.pendingUntilFixed
    
    "modulo by zero (dec)" in { 
      val expr = Modulo(Const(Dec(1.0)), Const(Dec(0.0)))
      expr must beFailure  // Currently Success(Const(Dec(1.0))) !?
    }.pendingUntilFixed

    "fold a complex expression (10-4)/3 + (5*8)" in {
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
      expr should beSuccess
    }
    
    // TODO: tests for unapply() in general
  }
  
  implicit def genConst : Arbitrary[Const] = Arbitrary { 
    for { i <- Arbitrary.arbitrary[scala.Int] } 
      yield Const(Int(i)) 
  }
}
