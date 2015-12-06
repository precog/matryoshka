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

package quasar.fs

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
