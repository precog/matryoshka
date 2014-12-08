package slamdata.engine

import slamdata.engine.sql._

import org.specs2.mutable._
import org.specs2.ScalaCheck
import ValidationMatchers._

import scalaz.{Node => _, Tree => _, _}
import Scalaz._

class VariablesSpec extends Specification with ScalaCheck {
  def c(v: String, t: Type) = Variables.coerce(t, VarValue(v))

  "Variables.coerce" should {
    "coerce int / Int to IntLiteral" in {
      c("123", Type.Int) must beSome(IntLiteral(123))
    }

    "coerce float / Float to FloatLiteral" in {
      c("123.25", Type.Dec) must beSome(FloatLiteral(123.25))
    }

    "coerce int / String to StringLiteral" in {
      c("123", Type.Str) must beSome(StringLiteral("123"))
    }

    "coerce SQL string / Top to StringLiteral" in {
      c("'123'", Type.Top) must beSome(StringLiteral("123"))
    }

    "coerce SQL string / String to StringLiteral without quotes" in {
      c("'123'", Type.Str) must beSome(StringLiteral("123"))
    }

    "coerce int / Top to IntLiteral" in {
      c("123", Type.Top) must beSome(IntLiteral(123))
    }

    "coerce float / Top to FloatLiteral" in {
      c("123.25", Type.Top) must beSome(FloatLiteral(123.25))
    }

    "coerce null / Top to null" in {
      c("null", Type.Top) must beSome(NullLiteral())
    }

    "coerce true / Top to Bool" in {
      c("true", Type.Top) must beSome(BoolLiteral(true))
    }

    "coerce false / Top to Bool" in {
      c("false", Type.Top) must beSome(BoolLiteral(false))
    }

    "coerce null / Null to null" in {
      c("null", Type.Null) must beSome(NullLiteral())
    }

    "coerce true / Bool to Bool" in {
      c("true", Type.Bool) must beSome(BoolLiteral(true))
    }

    "coerce false / Bool to Bool" in {
      c("false", Type.Bool) must beSome(BoolLiteral(false))
    }

    "fail with nonsense / Top" in {
      c("all work and no play makes jack a dull boy", Type.Top) must beNone
    }
  }
}