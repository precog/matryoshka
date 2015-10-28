package quasar.std

import quasar.Predef._

import org.specs2.mutable._
import org.specs2.scalaz._
import org.specs2.ScalaCheck
import org.scalacheck.Arbitrary
import org.scalacheck.Arbitrary.arbitrary
import org.scalacheck.Gen
import org.specs2.matcher.Matcher
import quasar.specs2.PendingWithAccurateCoverage

import org.threeten.bp.{Instant, LocalDate, LocalTime, Duration}
import scalaz.{Validation, ValidationNel, Success, Failure}
import scalaz.Validation.FlatMap._

class MathSpec extends Specification with ScalaCheck with ValidationMatchers with PendingWithAccurateCoverage {
  import MathLib._
  import quasar.Type
  import quasar.Type.Const
  import quasar.Type.Obj
  import quasar.Data._
  import quasar.SemanticError

  val zero = Const(Int(0))

  "MathLib" should {
    "type simple add with ints" in {
      val expr = Add(Type.Int, Type.Int)
      expr should beSuccessful(Type.Int)
    }

    "type simple add with decs" in {
      val expr = Add(Type.Dec, Type.Dec)
      expr should beSuccessful(Type.Dec)
    }

    "type simple add with promotion" in {
      val expr = Add(Type.Int, Type.Dec)
      expr should beSuccessful(Type.Dec)
    }

    "type simple add with Int, Top" in {
      val expr = Add(Type.Int, Type.Top)
      expr should beSuccessful(Type.Numeric)
    }.pendingUntilFixed

    "type simple add with int constant, Top" in {
      val expr = Add(Type.Const(Int(0)), Type.Top)
      expr should beSuccessful(Type.Numeric)
    }.pendingUntilFixed

    "type simple add with zero" in {
      val expr = Add(Type.Numeric, Const(Int(0)))
      expr should beSuccessful(Type.Numeric)
    }

    "fold simple add with int constants" in {
      val expr = Add(Const(Int(1)), Const(Int(2)))
      expr should beSuccessful(Const(Int(3)))
    }

    "fold simple add with decimal constants" in {
      val expr = Add(Const(Dec(1.0)), Const(Dec(2.0)))
      expr should beSuccessful(Const(Dec(3)))
    }

    "fold simple add with promotion" in {
      val expr = Add(Const(Int(1)), Const(Dec(2.0)))
      expr should beSuccessful(Const(Dec(3)))
    }

    "eliminate multiply by zero (on the right)" ! prop { (c : Const) =>
      val expr = Multiply(c, zero)
      expr should beSuccessful(zero)
    }

    "eliminate multiply by zero (on the left)" ! prop { (c : Const) =>
      val expr = Multiply(zero, c)
      expr should beSuccessful(zero)
    }

    "fold simple division" in {
      val expr = Divide(Const(Int(6)), Const(Int(3)))
      expr should beSuccessful(Const(Int(2)))
    }

    "fold truncating division" in {
      val expr = Divide(Const(Int(5)), Const(Int(2)))
      expr should beSuccessful(Const(Int(2)))
    }

    "fold simple division (dec)" in {
      val expr = Divide(Const(Int(6)), Const(Dec(3.0)))
      expr should beSuccessful(Const(Dec(2.0)))
    }

    "fold division (dec)" in {
      val expr = Divide(Const(Int(5)), Const(Dec(2)))
      expr should beSuccessful(Const(Dec(2.5)))
    }

    "divide by zero" in {
      val expr = Divide(Const(Int(1)), zero)
      expr must beFailing
    }

    "divide by zero (dec)" in {
      val expr = Divide(Const(Dec(1.0)), Const(Dec(0.0)))
      expr must beFailing
    }

    "fold simple modulo" in {
      val expr = Modulo(Const(Int(6)), Const(Int(3)))
      expr should beSuccessful(Const(Int(0)))
    }

    "fold non-zero modulo" in {
      val expr = Modulo(Const(Int(5)), Const(Int(2)))
      expr should beSuccessful(Const(Int(1)))
    }

    "fold simple modulo (dec)" in {
      val expr = Modulo(Const(Int(6)), Const(Dec(3.0)))
      expr should beSuccessful(Const(Dec(0.0)))
    }

    "fold non-zero modulo (dec)" in {
      val expr = Modulo(Const(Int(5)), Const(Dec(2.2)))
      expr should beSuccessful(Const(Dec(0.6)))
    }

    "modulo by zero" in {
      val expr = Modulo(Const(Int(1)), zero)
      expr must beFailing
    }

    "modulo by zero (dec)" in {
      val expr = Modulo(Const(Dec(1.0)), Const(Dec(0.0)))
      expr must beFailing
    }

    "fold a complex expression (10-4)/3 + (5*8)" in {
      val expr = for {
        x1 <- Subtract(
                Const(Int(10)),
                Const(Int(4)));
        x2 <- Divide(x1,
                Const(Int(3)));
        x3 <- Multiply(
                Const(Int(5)),
                Const(Int(8)))
        x4 <- Add(x2, x3)
      } yield x4
      expr should beSuccessful(Const(Int(42)))
    }

    "fail with mismatched constants" in {
      val expr = Add(Const(Int(1)), Const(Str("abc")))
      expr should beFailing
    }

    "fail with object and int constant" in {
      val expr = Add(Obj(Map("x" -> Type.Int), None), Const(Int(1)))
      expr should beFailing
    }

    "add timestamp and interval" in {
      val expr = Add(
        Type.Const(Timestamp(Instant.parse("2015-01-21T00:00:00Z"))),
        Type.Const(Interval(Duration.ofHours(9))))
      expr should beSuccessful(Type.Const(Timestamp(Instant.parse("2015-01-21T09:00:00Z"))))
    }

    "add with const and non-const Ints" in {
      permute(Add(_), Const(Int(1)), Const(Int(2)))(Const(Int(3)), Type.Int)
    }

    "add with const and non-const Int and Dec" in {
      permute(Add(_), Const(Int(1)), Const(Dec(2.0)))(Const(Dec(3.0)), Type.Dec)
    }

    def permute(f: List[Type] => ValidationNel[SemanticError, Type], t1: Const, t2: Const)(exp1: Const, exp2: Type) = {
      f(t1 :: t2 :: Nil) should beSuccessful(exp1)
      f(t1 :: t2.value.dataType :: Nil) should beSuccessful(exp2)
      f(t1.value.dataType :: t2 :: Nil) should beSuccessful(exp2)
      f(t1.value.dataType :: t2.value.dataType :: Nil) should beSuccessful(exp2)

      f(t2 :: t1 :: Nil) should beSuccessful(exp1)
      f(t2.value.dataType :: t1 :: Nil) should beSuccessful(exp2)
      f(t2 :: t1.value.dataType :: Nil) should beSuccessful(exp2)
      f(t2.value.dataType :: t1.value.dataType :: Nil) should beSuccessful(exp2)
    }

    // TODO: tests for unapply() in general
  }

  implicit def genConst : Arbitrary[Const] = Arbitrary {
    for { i <- Arbitrary.arbitrary[scala.Int] }
      yield Const(Int(i))
  }
}
