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

import scala.util.Properties._
import scalaz.concurrent.Task

sealed trait OS {
  import OS._

  def fold[X](win: => X, mac: => X, posix: => X): X =
    this match {
      case Windows => win
      case Mac     => mac
      case Posix   => posix
    }

  def isWin: Boolean = fold(true, false, false)

  def isMac: Boolean = fold(false, true, false)

  def isPosix: Boolean = fold(false, false, true)
}

object OS {
  private case object Windows extends OS
  private case object Mac extends OS
  private case object Posix extends OS

  val windows: OS = Windows
  val mac: OS = Mac
  val posix: OS = Posix

  // NB: We lump everything that isn't Windows or Mac into posix, this may need
  //     to change.
  def currentOS: Task[OS] =
    Task.delay(if (isWin) windows else if (isMac) mac else posix)
}

