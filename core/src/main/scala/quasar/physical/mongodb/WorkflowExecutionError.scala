package quasar
package physical
package mongodb

import quasar.Predef._
import quasar.fp._
import quasar.physical.mongodb.workflowtask._

import monocle.Prism

import scalaz._
import scalaz.syntax.show._
import scalaz.syntax.std.option._
import scalaz.std.option._

/** Error conditions possible during `Workflow` execution. */
sealed trait WorkflowExecutionError {
  import WorkflowExecutionError._

  def fold[X](
    invalidTask: (WorkflowTask, String) => X,
    insertFailed: (Bson, String) => X
  ): X =
    this match {
      case InvalidTask0(t, r)  => invalidTask(t, r)
      case InsertFailed0(b, r) => insertFailed(b, r)
    }
}

object WorkflowExecutionError {
  private final case class InvalidTask0(task: WorkflowTask, reason: String)
    extends WorkflowExecutionError

  private final case class InsertFailed0(bson: Bson, reason: String)
    extends WorkflowExecutionError

  val InvalidTask: (WorkflowTask, String) => WorkflowExecutionError =
    InvalidTask0(_, _)

  val InsertFailed: (Bson, String) => WorkflowExecutionError =
    InsertFailed0(_, _)

  val invalidTask: Prism[WorkflowExecutionError, (WorkflowTask, String)] =
    Prism((_: WorkflowExecutionError).fold((t, r) => (t, r).some, κ(none)))(InvalidTask.tupled)

  val insertFailed: Prism[WorkflowExecutionError, (Bson, String)] =
    Prism((_: WorkflowExecutionError).fold(κ(none), (b, r) => (b, r).some))(InsertFailed.tupled)

  implicit val workflowExecutionErrorShow: Show[WorkflowExecutionError] =
    Show.shows(err => "Error executing Workflow: " + err.fold(
      (t, r) => s"Invalid task, $r\n\n" + RenderTree[WorkflowTask].render(t).shows,
      (b, r) => s"Failed to insert BSON, `$b`, $r"))
}
