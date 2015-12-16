package quasar.std

import quasar.Predef._
import quasar.TypeGen
import quasar.fp._
import quasar.specs2.PendingWithAccurateCoverage

import org.scalacheck.Arbitrary
import org.scalacheck.Arbitrary.arbitrary
import org.scalacheck.Gen
import org.scalacheck.Prop
import org.specs2.ScalaCheck
import org.specs2.matcher.Matcher
import org.specs2.mutable._
import org.specs2.scalaz._
import org.threeten.bp.{Instant, LocalDate, LocalTime, Duration}

class SetSpec extends Specification with ScalaCheck with TypeGen with ValidationMatchers with PendingWithAccurateCoverage {
  import SetLib._
  import quasar.Data
  import quasar.Data.{Bool, Date, Dec, Int, Null, Str}
  import quasar.SemanticError
  import quasar.Type
  import quasar.Type.Const

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

    "maintain first type for constantly" ! prop { (t1 : Type, t2 : Type) =>
      val expr = Constantly(t1, t2)
      (t1, t2) match {
        case (Const(r), Const(Data.Set(l))) =>
           expr must beSuccessful(Const(Data.Set(l.map(κ(r)))))
        case (_, _) => expr must beSuccessful(Type.Set(t1))
      }
    }
  }
}
