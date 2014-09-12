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
    "coerce number / Int to IntLiteral" in {
      c("123", Type.Int) must beSome(IntLiteral(123))
    }

    "coerce number / Float to FloatLiteral" in {
      c("123.25", Type.Dec) must beSome(FloatLiteral(123.25))
    }

    "coerce number / String to StringLiteral" in {
      c("123", Type.Str) must beSome(StringLiteral("123"))
    }

    "coerce SQL string / Top to StringLiteral" in {
      c("'123'", Type.Top) must beSome(StringLiteral("123"))
    }

    "coerce SQL string / String to StringLiteral without quotes" in {
      c("'123'", Type.Str) must beSome(StringLiteral("123"))
    }
  }
}