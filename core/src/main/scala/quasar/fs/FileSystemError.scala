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
package fs

import quasar.Predef._
import quasar.fp._
import quasar.recursionschemes._
import quasar.Planner.{PlannerError => PlannerErr}

import monocle.Prism
import pathy.Path.posixCodec
import scalaz._
import scalaz.syntax.show._

import QueryFile.ResultHandle
import ReadFile.ReadHandle
import WriteFile.WriteHandle

sealed trait FileSystemError

object FileSystemError {
  object Case {
    final case class PathError(e: PathError2)
      extends FileSystemError
    final case class PlannerError(lp: Fix[LogicalPlan], err: PlannerErr)
      extends FileSystemError
    final case class UnknownResultHandle(h: ResultHandle)
      extends FileSystemError
    final case class UnknownReadHandle(h: ReadHandle)
      extends FileSystemError
    final case class UnknownWriteHandle(h: WriteHandle)
      extends FileSystemError
    final case class PartialWrite(numFailed: Int)
      extends FileSystemError
    final case class WriteFailed(data: Data, reason: String)
      extends FileSystemError
  }

  val PathError: PathError2 => FileSystemError =
    Case.PathError(_)

  val PlannerError: (Fix[LogicalPlan], PlannerErr) => FileSystemError =
    Case.PlannerError(_, _)

  val UnknownResultHandle: ResultHandle => FileSystemError =
    Case.UnknownResultHandle(_)

  val UnknownReadHandle: ReadHandle => FileSystemError =
    Case.UnknownReadHandle(_)

  val UnknownWriteHandle: WriteHandle => FileSystemError =
    Case.UnknownWriteHandle(_)

  val PartialWrite: Int => FileSystemError =
    Case.PartialWrite(_)

  val WriteFailed: (Data, String) => FileSystemError =
    Case.WriteFailed(_, _)

  val pathError: Prism[FileSystemError, PathError2] =
    Prism[FileSystemError, PathError2] {
      case Case.PathError(err) => Some(err)
      case _ => None
    } (PathError)

  val plannerError: Prism[FileSystemError, (Fix[LogicalPlan], PlannerErr)] =
    Prism[FileSystemError, (Fix[LogicalPlan], PlannerErr)] {
      case Case.PlannerError(lp, e) => Some((lp, e))
      case _ => None
    } (PlannerError.tupled)

  val unknownResultHandle: Prism[FileSystemError, ResultHandle] =
    Prism[FileSystemError, ResultHandle] {
      case Case.UnknownResultHandle(h) => Some(h)
      case _ => None
    } (UnknownResultHandle)

  val unknownReadHandle: Prism[FileSystemError, ReadHandle] =
    Prism[FileSystemError, ReadHandle] {
      case Case.UnknownReadHandle(h) => Some(h)
      case _ => None
    } (UnknownReadHandle)

  val unknownWriteHandle: Prism[FileSystemError, WriteHandle] =
    Prism[FileSystemError, WriteHandle] {
      case Case.UnknownWriteHandle(h) => Some(h)
      case _ => None
    } (UnknownWriteHandle)

  val partialWrite: Prism[FileSystemError, Int] =
    Prism[FileSystemError, Int] {
      case Case.PartialWrite(n) => Some(n)
      case _ => None
    } (PartialWrite)

  val writeFailed: Prism[FileSystemError, (Data, String)] =
    Prism[FileSystemError, (Data, String)] {
      case Case.WriteFailed(d, r) => Some((d, r))
      case _ => None
    } (WriteFailed.tupled)

  implicit def fileSystemErrorShow: Show[FileSystemError] =
    Show.shows {
      case Case.PathError(e) =>
        e.shows
      case Case.PlannerError(_, e) =>
        e.shows
      case Case.UnknownResultHandle(h) =>
        s"Attempted to get results from an unknown or closed handle: ${h.run}"
      case Case.UnknownReadHandle(h) =>
        s"Attempted to read from '${posixCodec.printPath(h.file)}' using an unknown or closed handle: ${h.id}"
      case Case.UnknownWriteHandle(h) =>
        s"Attempted to write to '${posixCodec.printPath(h.file)}' using an unknown or closed handle: ${h.id}"
      case Case.PartialWrite(n) =>
        s"Failed to write $n data."
      case Case.WriteFailed(d, r) =>
        s"Failed to write datum: reason='$r', datum=${d.shows}"
    }
}
