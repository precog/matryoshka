package quasar.config

import quasar.Predef._
import quasar.config.implicits._

import argonaut._, Argonaut._
import monocle._, macros.Lenses

@Lenses final case class ServerConfig(port: Int)

object ServerConfig {
   val DefaultPort = 20223

   implicit def Codec = CodecJson(
    (s: ServerConfig) => ("port" := s.port) ->: jEmptyObject,
    c => ((c --\ "port").as[Option[Int]]).map(p => ServerConfig(p.getOrElse(DefaultPort))))

   implicit object empty extends Empty[ServerConfig] { def empty = ServerConfig(DefaultPort) }
}

@Lenses final case class WebConfig(server: ServerConfig, mountings: MountingsConfig)

object WebConfig extends ConfigOps[WebConfig] {

  def mountingsLens = WebConfig.mountings

  implicit val codecJson: CodecJson[WebConfig] =
    casecodec2(WebConfig.apply, WebConfig.unapply)("server", "mountings")

  implicit object empty extends Empty[WebConfig] {
    def empty = WebConfig(Empty[ServerConfig].empty, Empty[MountingsConfig].empty)
  }

}

case class WebConfigLens[WC, SC](
  server: Lens[WC, SC],
  mountings: Lens[WC, MountingsConfig],
  wcPort: Lens[WC, Int],
  scPort: Lens[SC, Int])
