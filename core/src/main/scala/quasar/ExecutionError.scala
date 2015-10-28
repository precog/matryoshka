package quasar

import quasar.Predef._
import quasar.fp._
import quasar.fs.PathError2
import quasar.recursionschemes._
import quasar.Planner.{PlannerError => PlannerErr}

import monocle.{Lens, Prism}

import pathy.Path._

import scalaz._
import scalaz.syntax.std.option._
import scalaz.std.option._
import scalaz.syntax.show._

/** TODO: The implementation-agnostic PlannerErrors should be incorporated as
  *       cases in [[ExecutionError]] directly once we've finished converting
  *       to pathy everywhere.
  */
sealed trait ExecutionError {
  import ExecutionError._

  def fold[X](
    pathError: (Fix[LogicalPlan], PathError2) => X,
    plannerError: (Fix[LogicalPlan], PlannerErr) => X,
    failedToStore: (Fix[LogicalPlan], Option[AbsFile[Sandboxed]], String) => X
  ): X =
    this match {
      case PathError0(lp, e)        => pathError(lp, e)
      case PlannerError0(lp, e)     => plannerError(lp, e)
      case FailedToStore0(lp, f, r) => failedToStore(lp, f, r)
    }
}

object ExecutionError {
  private final case class PathError0(lp: Fix[LogicalPlan], err: PathError2)
    extends ExecutionError
  private final case class PlannerError0(lp: Fix[LogicalPlan], err: PlannerErr)
    extends ExecutionError
  private final case class FailedToStore0(lp: Fix[LogicalPlan], loc: Option[AbsFile[Sandboxed]], reason: String)
    extends ExecutionError

  val PathError: (Fix[LogicalPlan], PathError2) => ExecutionError =
    PathError0(_, _)

  val PlannerError: (Fix[LogicalPlan], PlannerErr) => ExecutionError =
    PlannerError0(_, _)

  val FailedToStore: (Fix[LogicalPlan], Option[AbsFile[Sandboxed]], String) => ExecutionError =
    FailedToStore0(_, _, _)

  val pathError: Prism[ExecutionError, (Fix[LogicalPlan], PathError2)] =
    Prism((_: ExecutionError).fold((lp, e) => (lp, e).some, κ(none), κ(none)))(PathError.tupled)

  val plannerError: Prism[ExecutionError, (Fix[LogicalPlan], PlannerErr)] =
    Prism((_: ExecutionError).fold(κ(none), (lp, e) => (lp, e).some, κ(none)))(PlannerError.tupled)

  val failedToStore: Prism[ExecutionError, (Fix[LogicalPlan], Option[AbsFile[Sandboxed]], String)] =
    Prism((_: ExecutionError).fold(κ(none), κ(none), (lp, f, r) => (lp, f, r).some))(FailedToStore.tupled)

  val logicalPlan: Lens[ExecutionError, Fix[LogicalPlan]] =
    Lens((_: ExecutionError).fold((lp, _) => lp, (lp, _) => lp, (lp, _, _) => lp))(
      lp => _.fold(
        (_, e)    => PathError(lp, e),
        (_, e)    => PlannerError(lp, e),
        (_, f, r) => FailedToStore(lp, f, r)))

  implicit val executionErrorShow: Show[ExecutionError] =
    Show.shows(err => "Execution failed: " + err.fold(
      (_, err)  => err.shows,
      (_, err)  => err.shows,
      (_, f, r) => "Failed to store results " +
                   f.fold("")(p => s"at ${posixCodec.printPath(p)}, ") +
                   s"because $r"))
}
