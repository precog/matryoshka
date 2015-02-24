package slamdata.engine.std

import org.specs2.mutable._
import org.specs2.ScalaCheck
import org.scalacheck.Arbitrary
import org.scalacheck.Arbitrary.arbitrary
import org.scalacheck.Gen
import org.specs2.matcher.Matcher
import slamdata.specs2._

import org.threeten.bp.{Instant, LocalDate, LocalTime, Duration}
import scalaz.{Validation, ValidationNel, Success, Failure}
import scalaz.Validation.FlatMap._

import slamdata.engine.ValidationMatchers

class MathSpec extends Specification with ScalaCheck with ValidationMatchers with PendingWithAccurateCoverage {
  import MathLib._
  import slamdata.engine.Type
  import slamdata.engine.Type.Const
  import slamdata.engine.Type.NamedField
  import slamdata.engine.Data._
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

    "type simple add with Numeric" in {
      val expr = Divide(Type.Numeric, Const(Int(0)))
      expr should beSuccess(Type.Numeric)
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
      expr must beFailure
    }

    "divide by zero (dec)" in {
      val expr = Divide(Const(Dec(1.0)), Const(Dec(0.0)))
      expr must beFailure
    }

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
      expr must beFailure
    }

    "modulo by zero (dec)" in {
      val expr = Modulo(Const(Dec(1.0)), Const(Dec(0.0)))
      expr must beFailure
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
      expr should beSuccess(Const(Int(42)))
    }

    "fail with mismatched constants" in {
      val expr = Add(Const(Int(1)), Const(Str("abc")))
      expr should beFailure
    }

    "fail with object and int constant" in {
      val expr = Add(NamedField("x", Type.Int), Const(Int(1)))
      expr should beFailure
    }

    "add timestamp and interval" in {
      val expr = Add(
        Type.Const(Timestamp(Instant.parse("2015-01-21T00:00:00Z"))),
        Type.Const(Interval(Duration.ofHours(9))))
      expr should beSuccess(Type.Const(Timestamp(Instant.parse("2015-01-21T09:00:00Z"))))
    }

    "add timestamp and numeric" in {
      val expr = Add(
        Type.Const(Timestamp(Instant.parse("2015-01-21T00:00:00Z"))),
        Type.Numeric)
      expr should beSuccess(Type.Timestamp)
    }

    "add with const and non-const Ints" in {
      permute(Add(_), Const(Int(1)), Const(Int(2)))(Const(Int(3)), Type.Int)
    }

    "add with const and non-const Int and Dec" in {
      permute(Add(_), Const(Int(1)), Const(Dec(2.0)))(Const(Dec(3.0)), Type.Dec)
    }

    def permute(f: List[Type] => ValidationNel[SemanticError, Type], t1: Const, t2: Const)(exp1: Const, exp2: Type) = {
      f(t1 :: t2 :: Nil) should beSuccess(exp1)
      f(t1 :: t2.value.dataType :: Nil) should beSuccess(exp2)
      f(t1.value.dataType :: t2 :: Nil) should beSuccess(exp2)
      f(t1.value.dataType :: t2.value.dataType :: Nil) should beSuccess(exp2)

      f(t2 :: t1 :: Nil) should beSuccess(exp1)
      f(t2.value.dataType :: t1 :: Nil) should beSuccess(exp2)
      f(t2 :: t1.value.dataType :: Nil) should beSuccess(exp2)
      f(t2.value.dataType :: t1.value.dataType :: Nil) should beSuccess(exp2)
    }

    // TODO: tests for unapply() in general
  }

  implicit def genConst : Arbitrary[Const] = Arbitrary {
    for { i <- Arbitrary.arbitrary[scala.Int] }
      yield Const(Int(i))
  }
}
