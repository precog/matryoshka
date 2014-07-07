package slamdata.engine.std

import org.specs2.mutable._
import org.specs2.ScalaCheck
import org.scalacheck.Arbitrary
import org.scalacheck.Arbitrary.arbitrary
import org.scalacheck.Gen
import org.specs2.matcher.Matcher
import scalaz.Validation
import scalaz.Validation.FlatMap._
import scalaz.Success
import scalaz.Failure

import slamdata.engine.ValidationMatchers

class RelationsSpec extends Specification with ScalaCheck with ValidationMatchers {
  import RelationsLib._
  import slamdata.engine.Type
  import slamdata.engine.Type.Const
  import slamdata.engine.Data.Bool
  import slamdata.engine.Data.Dec
  import slamdata.engine.Data.Int
  import slamdata.engine.Data.Str
  
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
    }.pendingUntilFixed
    
    "find lub for cond with arbitrary args" ! prop { (t1 : Type, t2 : Type) => 
      val expr = Cond(Type.Bool, t1, t2)
      expr must beSuccess(Type.lub(t1, t2))
    }.pendingUntilFixed
    
    // TODO: 
  }

  implicit def genType : Arbitrary[Type] = Arbitrary {
    Gen.oneOf(Type.Null, Type.Bool, Type.Int, Type.Dec,
                Type.Binary, Type.Str, Type.DateTime, Type.Interval,
                Type.Const(Int(0)),
                Type.Const(Dec(0.0)),
                Type.Const(Bool(false)),
                Type.Const(Str("abc")))
  }
}
