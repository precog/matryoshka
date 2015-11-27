package quasar
package regression

import quasar.Predef._

import argonaut._, Argonaut._

sealed trait SkipDirective

object SkipDirective {
  final case object Skip    extends SkipDirective
  final case object Pending extends SkipDirective

  import DecodeResult.{ok, fail}

  implicit val SkipDirectiveDecodeJson: DecodeJson[SkipDirective] =
    DecodeJson(c => c.as[String].flatMap {
      case "skip"     => ok(Skip)
      case "pending"  => ok(Pending)
      case str        => fail("skip, pending; found: \"" + str + "\"", c.history)
    })
}
