package quasar.mount

import quasar.Predef._

import argonaut._, Argonaut._
import scalaz._, Scalaz._

final case class FileSystemType(value: String) extends scala.AnyVal

object FileSystemType {
  implicit val fileSystemTypeOrder: Order[FileSystemType] =
    Order.orderBy(_.value)

  implicit val fileSystemTypeShow: Show[FileSystemType] =
    Show.showFromToString

  implicit val fileSystemTypeCodecJson: CodecJson[FileSystemType] =
    CodecJson.derived[String].xmap(FileSystemType(_))(_.value)
}
