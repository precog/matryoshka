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
import quasar.Variables
import quasar.config
import quasar.fp.prism._
import quasar.fs.FileSystemType
import quasar.sql._

import argonaut._, Argonaut._
import monocle.Prism
import scalaz._

/** Configuration for a mount, currently either a view or a filesystem. */
sealed trait MountConfig2

object MountConfig2 {
  final case class ViewConfig private[mount] (query: Expr, vars: Variables)
    extends MountConfig2

  final case class FileSystemConfig private[mount] (typ: FileSystemType, uri: ConnectionUri)
    extends MountConfig2

  val viewConfig: Prism[MountConfig2, (Expr, Variables)] =
    Prism[MountConfig2, (Expr, Variables)] {
      case ViewConfig(query, vars) => Some((query, vars))
      case _                       => None
    } ((ViewConfig(_, _)).tupled)

  val fileSystemConfig: Prism[MountConfig2, (FileSystemType, ConnectionUri)] =
    Prism[MountConfig2, (FileSystemType, ConnectionUri)] {
      case FileSystemConfig(typ, uri) => Some((typ, uri))
      case _                          => None
    } ((FileSystemConfig(_, _)).tupled)

  implicit val mountConfigShow: Show[MountConfig2] =
    Show.showFromToString

/** TODO: Equal[sql.Expr]
  implicit val mountConfigEqual: Equal[MountConfig2] =
    Equal.equalBy[MountConfig2, Expr \/ (FileSystemType, Json)] {
      case ViewConfig(query)           => query.left
      case FileSystemConfig(typ, json) => (typ, json).right
    }
*/

  implicit val mountConfigCodecJson: CodecJson[MountConfig2] =
    CodecJson({
      case ViewConfig(query, vars) =>
        Json("view" := config.ViewConfig(query, vars))

      case FileSystemConfig(typ, uri) =>
        Json(typ.value := Json("connectionUri" := uri))
    }, c => c.fields match {
      case Some("view" :: Nil) =>
        (c --\ "view").as[config.ViewConfig]
          .map(vc => viewConfig(vc.query, vc.variables))

      case Some(t :: Nil) =>
        (c --\ t --\ "connectionUri").as[ConnectionUri]
          .map(fileSystemConfig(FileSystemType(t), _))

      case _ =>
        DecodeResult.fail(s"invalid config: ${c.focus}", c.history)
    })
}
