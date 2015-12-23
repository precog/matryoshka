/*
 * Copyright 2014 - 2015 SlamData Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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

final case class WebConfigLens[WC, SC](
  server: Lens[WC, SC],
  mountings: Lens[WC, MountingsConfig],
  wcPort: Lens[WC, Int],
  scPort: Lens[SC, Int])
