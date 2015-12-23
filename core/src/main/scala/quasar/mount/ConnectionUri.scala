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
