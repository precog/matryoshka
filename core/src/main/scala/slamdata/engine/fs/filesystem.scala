package slamdata.engine.fs

import argonaut._, Argonaut._

import slamdata.engine.{Data, DataCodec}

final case class WriteError(value: Data, hint: Option[String]) {
  def message = hint.getOrElse("failed to write ") ++ value.toString
}
object WriteError {
  implicit val Encode = EncodeJson[WriteError]( e =>
    Json("data"   := DataCodec.Precise.encode(e.value),
         "detail" := e.hint.getOrElse("")))
}
