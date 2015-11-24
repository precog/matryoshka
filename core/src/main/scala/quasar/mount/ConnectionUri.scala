package quasar.mount

import quasar.Predef._

import argonaut._
import scalaz._
import scalaz.std.string._

final case class ConnectionUri(value: String) extends scala.AnyVal

object ConnectionUri {
  implicit val connectionUriShow: Show[ConnectionUri] =
    Show.showFromToString

  implicit val connectionUriOrder: Order[ConnectionUri] =
    Order.orderBy(_.value)

  implicit val connectionUriCodecJson: CodecJson[ConnectionUri] =
    CodecJson.derived[String].xmap(ConnectionUri(_))(_.value)
}
