package quasar.std

import quasar.Predef._
import quasar.TypeGen
import quasar.specs2.PendingWithAccurateCoverage

import org.scalacheck.Arbitrary
import org.scalacheck.Arbitrary.arbitrary
import org.scalacheck.Gen
import org.scalacheck.Prop
import org.specs2.ScalaCheck
import org.specs2.matcher.Matcher
import org.specs2.mutable._
import org.specs2.scalaz._
import scalaz.{Validation, Success, Failure}

class SetSpec extends Specification with ScalaCheck with TypeGen with ValidationMatchers with PendingWithAccurateCoverage {
  import SetLib._
  import quasar.Type
  import quasar.Type.Const
  import quasar.Data.{Bool, Date, Dec, Int, Null, Str}

  "SetLib" should {
    "maintain first type for constantly" ! prop { (t1 : Type, t2 : Type) =>
      val expr = Constantly(t1, t2)
      expr must beSuccessful(Type.Set(t1))
    }
  }
}
