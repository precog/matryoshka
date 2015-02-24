package slamdata.engine.std

import org.specs2.mutable._
import org.specs2.ScalaCheck
import org.scalacheck.Arbitrary
import org.scalacheck.Arbitrary.arbitrary
import org.scalacheck.Gen
import org.scalacheck.Prop
import org.specs2.matcher.Matcher
import slamdata.specs2._

import scalaz.Validation
import scalaz.Validation.FlatMap._
import scalaz.Success
import scalaz.Failure

import slamdata.engine.{TypeGen, ValidationMatchers}

class RelationsSpec extends Specification with ScalaCheck with TypeGen with ValidationMatchers with PendingWithAccurateCoverage {
  import RelationsLib._
  import slamdata.engine.Type
  import slamdata.engine.Type.Const
  import slamdata.engine.Data.Bool
  import slamdata.engine.Data.Date
  import slamdata.engine.Data.Dec
  import slamdata.engine.Data.Int
  import slamdata.engine.Data.Null
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

    "type Eq with Top" ! prop { (t : Type) =>
      Eq(Type.Top, t) should beSuccess(Type.Bool)
      Eq(t, Type.Top) should beSuccess(Type.Bool)
    }

    "type Neq with Top" ! prop { (t : Type) =>
      Neq(Type.Top, t) should beSuccess(Type.Bool)
      Neq(t, Type.Top) should beSuccess(Type.Bool)
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

    "fold coalesce with left null type" ! prop { (t2 : Type) =>
      val expr = Coalesce(Type.Null, t2)
      expr must beSuccess(t2)
    }

    "fold coalesce with left null value" ! prop { (t2 : Type) =>
      val expr = Coalesce(Const(Null), t2)
      expr must beSuccess(t2)
    }

    "fold coalesce with left value" ! prop { (t2 : Type) =>
      val expr = Coalesce(Const(Int(3)), t2)
      expr must beSuccess(Const(Int(3)))
    }

    "find lub for coalesce with int" in {
      val expr = Coalesce(Type.Int, Type.Int)
      expr must beSuccess(Type.Int)
    }

    "find lub for coalesce with arbitrary args" ! prop { (t1 : Type, t2 : Type) =>
      val expr = Cond(t1, t2)
      if (t1 == Type.Null || t1 == Const(Null))
        expr must beSuccess(t2)
      else
        expr must beSuccess(Type.lub(t1, t2))
    }.pendingUntilFixed // When t1 is Const, we need to match that

    val comparisonOps = Gen.oneOf(Eq, Neq, Lt, Lte, Gt, Gte)

    "flip comparison ops" !
      Prop.forAll(comparisonOps, arbitrary[BigInt], arbitrary[BigInt]) {
        case (func, left, right) =>
          flip(func).map(
            _(Type.Const(Int(right)), Type.Const(Int(left)))) must
            beSome(func(Type.Const(Int(left)), Type.Const(Int(right))))
    }

    "negate comparison ops" !
      Prop.forAll(comparisonOps, arbitrary[BigInt], arbitrary[BigInt]) {
        case (func, left, right) =>
          RelationsLib.negate(func).map(
            _(Type.Const(Int(left)), Type.Const(Int(right)))) must
          beSome(func(Type.Const(Int(left)), Type.Const(Int(right))).flatMap(Not(_)))
    }

    // TODO:
  }
}
