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
import quasar.{Data, LogicalPlan}
import quasar.Planner.{PlannerError => PlannerErr}
import quasar.fp._
import quasar.recursionschemes._

import monocle.Prism
import pathy.Path.posixCodec
import scalaz._
import scalaz.syntax.show._

sealed trait FileSystemError

object FileSystemError {
  import QueryFile.ResultHandle
  import ReadFile.ReadHandle
  import WriteFile.WriteHandle

  final case class PathError private[fs] (e: PathError2)
    extends FileSystemError
  final case class PlannerError private[fs] (lp: Fix[LogicalPlan], err: PlannerErr)
    extends FileSystemError
  final case class UnknownResultHandle private[fs] (h: ResultHandle)
    extends FileSystemError
  final case class UnknownReadHandle private[fs] (h: ReadHandle)
    extends FileSystemError
  final case class UnknownWriteHandle private[fs] (h: WriteHandle)
    extends FileSystemError
  final case class PartialWrite private[fs] (numFailed: Int)
    extends FileSystemError
  final case class WriteFailed private[fs] (data: Data, reason: String)
    extends FileSystemError

  val pathError: Prism[FileSystemError, PathError2] =
    Prism[FileSystemError, PathError2] {
      case PathError(err) => Some(err)
      case _ => None
    } (PathError(_))

  val plannerError: Prism[FileSystemError, (Fix[LogicalPlan], PlannerErr)] =
    Prism[FileSystemError, (Fix[LogicalPlan], PlannerErr)] {
      case PlannerError(lp, e) => Some((lp, e))
      case _ => None
    } ((PlannerError(_, _)).tupled)

  val unknownResultHandle: Prism[FileSystemError, ResultHandle] =
    Prism[FileSystemError, ResultHandle] {
      case UnknownResultHandle(h) => Some(h)
      case _ => None
    } (UnknownResultHandle(_))

  val unknownReadHandle: Prism[FileSystemError, ReadHandle] =
    Prism[FileSystemError, ReadHandle] {
      case UnknownReadHandle(h) => Some(h)
      case _ => None
    } (UnknownReadHandle(_))

  val unknownWriteHandle: Prism[FileSystemError, WriteHandle] =
    Prism[FileSystemError, WriteHandle] {
      case UnknownWriteHandle(h) => Some(h)
      case _ => None
    } (UnknownWriteHandle(_))

  val partialWrite: Prism[FileSystemError, Int] =
    Prism[FileSystemError, Int] {
      case PartialWrite(n) => Some(n)
      case _ => None
    } (PartialWrite(_))

  val writeFailed: Prism[FileSystemError, (Data, String)] =
    Prism[FileSystemError, (Data, String)] {
      case WriteFailed(d, r) => Some((d, r))
      case _ => None
    } ((WriteFailed(_, _)).tupled)

  implicit def fileSystemErrorShow: Show[FileSystemError] =
    Show.shows {
      case PathError(e) =>
        e.shows
      case PlannerError(_, e) =>
        e.shows
      case UnknownResultHandle(h) =>
        s"Attempted to get results from an unknown or closed handle: ${h.run}"
      case UnknownReadHandle(h) =>
        s"Attempted to read from '${posixCodec.printPath(h.file)}' using an unknown or closed handle: ${h.id}"
      case UnknownWriteHandle(h) =>
        s"Attempted to write to '${posixCodec.printPath(h.file)}' using an unknown or closed handle: ${h.id}"
      case PartialWrite(n) =>
        s"Failed to write $n data."
      case WriteFailed(d, r) =>
        s"Failed to write datum: reason='$r', datum=${d.shows}"
    }
}
