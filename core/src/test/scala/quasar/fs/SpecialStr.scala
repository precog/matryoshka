package quasar
package fs

import quasar.Predef._

import org.scalacheck.{Arbitrary, Gen}
import scalaz.Show
import scalaz.syntax.functor._
import scalaz.std.list._

/** A random string that favors special characters with more frequency. */
final case class SpecialStr(str: String) extends scala.AnyVal

object SpecialStr {
  implicit val specialStrArbitrary: Arbitrary[SpecialStr] =
    Arbitrary {
      val specialFreqs = "$./\\_~ *+-".toList map (Gen.const) strengthL 10

      Gen.nonEmptyListOf(Gen.frequency(
        (100, Arbitrary.arbitrary[Char]) :: specialFreqs : _*
      )) map (cs => SpecialStr(cs.mkString))
    }

  implicit val specialStrShow: Show[SpecialStr] =
    Show.shows(_.str)
}
