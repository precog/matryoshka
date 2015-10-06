package quasar.std

import quasar.Predef._
import quasar.specs2.PendingWithAccurateCoverage

import org.specs2.mutable._
import org.specs2.scalaz._
import org.specs2.ScalaCheck
import org.scalacheck.Arbitrary
import org.scalacheck.Arbitrary.arbitrary
import org.scalacheck.Gen
import org.specs2.matcher.Matcher
import org.threeten.bp.{Instant, LocalDate, LocalTime, Duration}
import scalaz.{Validation, Success, Failure}

class SetSpec extends Specification with ScalaCheck with ValidationMatchers with PendingWithAccurateCoverage {
  import SetLib._
  import quasar.Type
  import quasar.Data
  import quasar.SemanticError

  "SetLib" should {
    "type taking no results" in {
      val expr = Take(Type.Set(Type.Int), Type.Const(Data.Int(0)))
      expr should beSuccessful(Type.Const(Data.Set(Nil)))
    }

    "type filtering by false" in {
      val expr = Filter(Type.Set(Type.Int), Type.Const(Data.Bool(false)))
      expr should beSuccessful(Type.Const(Data.Set(Nil)))
    }

    "type inner join on false" in {
      val expr = InnerJoin(Type.Set(Type.Int), Type.Set(Type.Int), Type.Const(Data.Bool(false)))
      expr should beSuccessful(Type.Const(Data.Set(Nil)))
    }

    "type inner join with empty left" in {
      val expr = InnerJoin(Type.Const(Data.Set(Nil)), Type.Set(Type.Int), Type.Bool)
      expr should beSuccessful(Type.Const(Data.Set(Nil)))
    }

    "type inner join with empty right" in {
      val expr = InnerJoin(Type.Set(Type.Int), Type.Const(Data.Set(Nil)), Type.Bool)
      expr should beSuccessful(Type.Const(Data.Set(Nil)))
    }

    "type left outer join with empty left" in {
      val expr = LeftOuterJoin(Type.Const(Data.Set(Nil)), Type.Set(Type.Int), Type.Bool)
      expr should beSuccessful(Type.Const(Data.Set(Nil)))
    }

    "type right outer join with empty right" in {
      val expr = RightOuterJoin(Type.Set(Type.Int), Type.Const(Data.Set(Nil)), Type.Bool)
      expr should beSuccessful(Type.Const(Data.Set(Nil)))
    }
  }
}
