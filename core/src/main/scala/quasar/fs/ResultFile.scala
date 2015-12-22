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

package quasar

import quasar.Predef._
import quasar.fs._

import monocle.{Lens, Prism}

sealed trait ResultFile

object ResultFile {
  object Case {
    final case class User(file: AFile) extends ResultFile
    final case class Temp(file: AFile) extends ResultFile
  }

  /** Path to a result which names an unaltered source file or the requested
    * destination.
    */
  val User: AFile => ResultFile =
    Case.User(_)

  /** Path to a result which names a new temporary file created during plan
    * execution.
    */
  val Temp: AFile => ResultFile =
    Case.Temp(_)

  val user: Prism[ResultFile, AFile] =
    Prism[ResultFile, AFile] {
      case Case.User(f) => Some(f)
      case _ => None
    } (User)

  val temp: Prism[ResultFile, AFile] =
    Prism[ResultFile, AFile] {
      case Case.Temp(f) => Some(f)
      case _ => None
    } (Temp)

  val resultFile: Lens[ResultFile, AFile] =
    Lens[ResultFile, AFile] {
      case Case.User(f) => f
      case Case.Temp(f) => f
    } { f => {
      case Case.User(_) => User(f)
      case Case.Temp(_) => Temp(f)
    }}
}
