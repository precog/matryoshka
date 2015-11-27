package quasar
package physical
package mongodb

import quasar.Predef._
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
  }

  val InvalidTask: (WorkflowTask, String) => WorkflowExecutionError =
    Case.InvalidTask(_, _)

  val InsertFailed: (Bson, String) => WorkflowExecutionError =
    Case.InsertFailed(_, _)

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

  implicit val workflowExecutionErrorShow: Show[WorkflowExecutionError] =
    Show.shows { err =>
      val msg = err match {
        case Case.InvalidTask(t, r) =>
          s"Invalid task, $r\n\n" + RenderTree[WorkflowTask].render(t).shows
        case Case.InsertFailed(b, r) =>
          s"Failed to insert BSON, `$b`, $r"
      }

      s"Error executing workflow: $msg"
    }
}
