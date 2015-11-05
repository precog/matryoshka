package quasar.config

import quasar.Predef._
import quasar.config.implicits._

import argonaut._, Argonaut._
import monocle._, macros.Lenses

final case class ServerConfig(port0: Option[Int]) {
  val port = port0.getOrElse(ServerConfig.DefaultPort)
}

object ServerConfig {
   val DefaultPort = 20223

   val port = Lens[ServerConfig, Int](_.port)(p => c => c.copy(port0 = Some(p)))

   implicit def Codec: CodecJson[ServerConfig] =
     casecodec1(ServerConfig.apply, ServerConfig.unapply)("port")
}

@Lenses final case class WebConfig(server: ServerConfig, mountings: MountingsConfig)

object WebConfig extends ConfigOps[WebConfig] {
  def mountingsLens = WebConfig.mountings

  implicit val codecJson: CodecJson[WebConfig] =
    casecodec2(WebConfig.apply, WebConfig.unapply)("server", "mountings")
}

case class WebConfigLens[WC, SC](
  server: Lens[WC, SC],
  mountings: Lens[WC, MountingsConfig],
  wcPort: Lens[WC, Int],
  scPort: Lens[SC, Int])
