package quasar.std

import quasar.Predef._

import org.specs2.mutable._

import org.threeten.bp._
import scalaz._

import quasar._, SemanticError._

class DateSpecs extends Specification {
  import DateLib._

  "parseInterval" should {
    def fromMillis(millis: Long) = \/-(Data.Interval(Duration.ofMillis(millis)))

    def hms(hours: Int, minutes: Int, seconds: Int, millis: Int) =
      fromMillis((((hours.toLong*60) + minutes)*60 + seconds)*1000 + millis)

    "parse millis" in {
      parseInterval("PT0.001S") must_== fromMillis(1)
    }

    "parse negative parts" in {
      parseInterval("PT-1H-1M-1S") must_== hms(-1, -1, -1, 0)
    }

    "parse fractional parts" in {
      // The spec says "the smallest value may have a decimal fraction"
      parseInterval("PT1.5H") must_== hms(1, 30, 0, 0)
      parseInterval("PT5H1.5M") must_== hms(5, 1, 30, 0)
    }.pendingUntilFixed("#718")

    "parse days" in {
      parseInterval("P1D") must_== hms(24, 0, 0, 0)
    }

    "parse ymd" in {
      val msg = parseInterval("P1Y1M1D") match {
        case -\/(DateFormatError(_, _, hint)) => hint
        case _ => None
      }
      msg must beSome.which(_ contains "year/month not currently supported")
    }
  }
}
