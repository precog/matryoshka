package quasar.std

import quasar.Predef._

import org.specs2.mutable._
import org.specs2.scalaz._
import org.specs2.ScalaCheck
import org.scalacheck.Arbitrary
import org.scalacheck.Arbitrary.arbitrary
import org.scalacheck.Gen
import org.scalacheck.Prop
import org.specs2.matcher.Matcher
import quasar.specs2.PendingWithAccurateCoverage

import scalaz.Validation
import scalaz.Validation.FlatMap._
import scalaz.Success
import scalaz.Failure

import quasar.TypeGen

class RelationsSpec extends Specification with ScalaCheck with TypeGen with ValidationMatchers with PendingWithAccurateCoverage {
  import RelationsLib._
  import quasar.Type
  import quasar.Type.Const
  import quasar.Data.Bool
  import quasar.Data.Date
  import quasar.Data.Dec
  import quasar.Data.Int
  import quasar.Data.Null
  import quasar.Data.Str

  "RelationsLib" should {

    "type eq with matching arguments" ! prop { (t : Type) =>
      val expr = Eq(t, t)
      t match {
        case Const(_) => expr should beSuccessful(Const(Bool(true)))
        case _ => expr should beSuccessful(Type.Bool)
      }
    }

    "fold integer eq" in {
      val expr = Eq(Const(Int(1)), Const(Int(1)))
      expr should beSuccessful(Const(Bool(true)))
    }

    "fold eq with mixed numeric type" in {
      val expr = Eq(Const(Int(1)), Const(Dec(1.0)))
      expr should beSuccessful(Const(Bool(true)))
    }

    "fold eq with mixed type" in {
      val expr = Eq(Const(Int(1)), Const(Str("a")))
      expr should beSuccessful(Const(Bool(false)))
    }

    "type Eq with Top" ! prop { (t : Type) =>
      Eq(Type.Top, t) should beSuccessful(Type.Bool)
      Eq(t, Type.Top) should beSuccessful(Type.Bool)
    }

    "type Neq with Top" ! prop { (t : Type) =>
      Neq(Type.Top, t) should beSuccessful(Type.Bool)
      Neq(t, Type.Top) should beSuccessful(Type.Bool)
    }

    "fold neq with mixed type" in {
      val expr = Neq(Const(Int(1)), Const(Str("a")))
      expr should beSuccessful(Const(Bool(true)))
    }

    "fold isNull with null" in {
      val expr = IsNull(Const(Null))
      expr should beSuccessful(Const(Bool(true)))
    }

    "fold isNull" ! prop { (t1 : Type) =>
      val expr = IsNull(t1)
      expr must beSuccessful(t1 match {
        case Const(Null) => Const(Bool(true))
        case Const(_)    => Const(Bool(false))
        case _           => Type.Bool
      })
    }

    // TODO: similar for the rest of the simple relations

    "fold cond with true" ! prop { (t1 : Type, t2 : Type) =>
      val expr = Cond(Const(Bool(true)), t1, t2)
      expr must beSuccessful(t1)
    }

    "fold cond with false" ! prop { (t1 : Type, t2 : Type) =>
      val expr = Cond(Const(Bool(false)), t1, t2)
      expr must beSuccessful(t2)
    }

    "find lub for cond with int" in {
      val expr = Cond(Type.Bool, Type.Int, Type.Int)
      expr must beSuccessful(Type.Int)
    }

    "find lub for cond with arbitrary args" ! prop { (t1 : Type, t2 : Type) =>
      val expr = Cond(Type.Bool, t1, t2)
      expr must beSuccessful(Type.lub(t1, t2))
    }

    "fold coalesce with right null type" ! prop { (t1 : Type) =>
      val expr = Coalesce(t1, Type.Null)
      expr must beSuccessful(t1 match {
        case Const(Null) => Type.Null
        case _           => t1
      })
    }

    "fold coalesce with left null type" ! prop { (t2 : Type) =>
      val expr = Coalesce(Type.Null, t2)
      expr must beSuccessful(t2)
    }

    "fold coalesce with right null value" ! prop { (t1 : Type) =>
      val expr = Coalesce(t1, Const(Null))
      expr must beSuccessful(t1 match {
        case Type.Null => Const(Null)
        case _         => t1
      })
    }

    "fold coalesce with left null value" ! prop { (t2 : Type) =>
      val expr = Coalesce(Const(Null), t2)
      expr must beSuccessful(t2)
    }

    "fold coalesce with left value" ! prop { (t2 : Type) =>
      val expr = Coalesce(Const(Int(3)), t2)
      expr must beSuccessful(Const(Int(3)))
    }

    "find lub for coalesce with int" in {
      val expr = Coalesce(Type.Int, Type.Int)
      expr must beSuccessful(Type.Int)
    }

    "find lub for coalesce with arbitrary args" ! prop { (t1 : Type, t2 : Type) =>
      val expr = Cond(t1, t2)
      if (t1 == Type.Null || t1 == Const(Null))
        expr must beSuccessful(t2)
      else
        expr must beSuccessful(Type.lub(t1, t2))
    }.pendingUntilFixed // When t1 is Const, we need to match that

    "maintain first type for constantly" ! prop { (t1 : Type, t2 : Type) =>
      val expr = Constantly(t1, t2)
      expr must beSuccessful(t1)
    }

    val comparisonOps = Gen.oneOf(Eq, Neq, Lt, Lte, Gt, Gte)

    "flip comparison ops" !
      Prop.forAll(comparisonOps, arbitrary[BigInt], arbitrary[BigInt]) {
        case (func, left, right) =>
          flip(func).map(
            _(Type.Const(Int(right)), Type.Const(Int(left)))) must
            beSome(func(Type.Const(Int(left)), Type.Const(Int(right))))
    }

    "flip boolean ops" !
      Prop.forAll(Gen.oneOf(And, Or), arbitrary[Boolean], arbitrary[Boolean]) {
        case (func, left, right) =>
          flip(func).map(
            _(Type.Const(Bool(right)), Type.Const(Bool(left)))) must
            beSome(func(Type.Const(Bool(left)), Type.Const(Bool(right))))
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
