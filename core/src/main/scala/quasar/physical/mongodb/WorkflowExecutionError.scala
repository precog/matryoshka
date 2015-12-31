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

package quasar.physical.mongodb

import quasar.Predef._
import quasar.RenderTree
import quasar.fp._
import quasar.physical.mongodb.workflowtask._

import monocle.Prism
import scalaz._
import scalaz.syntax.show._

/** Error conditions possible during `Workflow` execution. */
sealed trait WorkflowExecutionError

object WorkflowExecutionError {
  object Case {
    final case class InvalidTask(task: WorkflowTask, reason: String)
      extends WorkflowExecutionError

    final case class InsertFailed(bson: Bson, reason: String)
      extends WorkflowExecutionError

    final case object NoDatabase
      extends WorkflowExecutionError
  }

  val InvalidTask: (WorkflowTask, String) => WorkflowExecutionError =
    Case.InvalidTask(_, _)

  val InsertFailed: (Bson, String) => WorkflowExecutionError =
    Case.InsertFailed(_, _)

  val NoDatabase: WorkflowExecutionError =
    Case.NoDatabase

  val invalidTask: Prism[WorkflowExecutionError, (WorkflowTask, String)] =
    Prism[WorkflowExecutionError, (WorkflowTask, String)] {
      case Case.InvalidTask(t, r) => Some((t, r))
      case _ => None
    } (InvalidTask.tupled)

  val insertFailed: Prism[WorkflowExecutionError, (Bson, String)] =
    Prism[WorkflowExecutionError, (Bson, String)] {
      case Case.InsertFailed(b, r) => Some((b, r))
      case _ => None
    } (InsertFailed.tupled)

  val noDatabase: Prism[WorkflowExecutionError, Unit] =
    Prism[WorkflowExecutionError, Unit] {
      case Case.NoDatabase => Some(())
      case _ => None
    } (κ(NoDatabase))

  implicit val workflowExecutionErrorShow: Show[WorkflowExecutionError] =
    Show.shows { err =>
      val msg = err match {
        case Case.InvalidTask(t, r) =>
          s"Invalid task, $r\n\n" + RenderTree[WorkflowTask].render(t).shows
        case Case.InsertFailed(b, r) =>
          s"Failed to insert BSON, `$b`, $r"
        case Case.NoDatabase =>
          "No database"
      }

      s"Error executing workflow: $msg"
    }
}
