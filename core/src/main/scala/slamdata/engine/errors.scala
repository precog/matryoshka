package slamdata.engine

import scalaz._
import scalaz.concurrent._

import slamdata.engine.fp._

// TODO: seal this trait
trait EnvironmentError {
  def message: String
}
final case class MissingBackend(message: String) extends EnvironmentError
final case class EnvPathError(error: slamdata.engine.fs.PathError)
    extends EnvironmentError {
  def message = error.message
}
final case class EnvEvalError(error: EvaluationError) extends EnvironmentError {
  def message = error.message
}

sealed trait CompilationError {
  def message: String
}
final case class CompilePathError(error: slamdata.engine.fs.PathError) extends CompilationError
final case class ESemanticError(error: SemanticError) extends CompilationError
final case class EPlannerError(error: PlannerError) extends CompilationError
final case class ManyErrors(errors: NonEmptyList[CompilationError]) extends CompilationError {
  def message = errors.map(_.message).list.mkString("[", "\n", "]")
}
final case class PhaseError(phases: Vector[PhaseResult], causedBy: CompilationError) extends CompilationError {
  def message = phases.mkString("\n\n")
}

sealed trait ProcessingError {
  def message: String
}
final case class PEvalError(error: EvaluationError) extends ProcessingError
final case class PResultError(error: ResultError) extends ProcessingError
final case class PWriteError(error: slamdata.engine.fs.WriteError) extends ProcessingError
final case class PPathError(error: slamdata.engine.fs.PathError) extends ProcessingError

// TODO: seal this trait
trait ResultError {
  def message: String
}
final case class ResultPathError(error: slamdata.engine.fs.PathError) extends ResultError


object Errors {
  import scalaz.stream.Process

  type ETask[E, X] = EitherT[Task, E, X]

  implicit class PrOpsETask[E, O](self: Process[ETask[E, ?], O])
      extends PrOps[ETask[E, ?], O](self)

  implicit def ETaskCatchable[E] = new Catchable[ETask[E, ?]] {
    def attempt[A](f: ETask[E, A]) =
      EitherT(f.run.attempt.map(_.fold(
        e => \/-(-\/(e)),
        _.fold(-\/(_), x => \/-(\/-(x))))))

    def fail[A](err: Throwable) = EitherT.right(Task.fail(err))
  }

  def liftE[E] = new (Task ~> ETask[E, ?]) {
    def apply[T](t: Task[T]): ETask[E, T] = EitherT.right(t)
  }

  def convertError[E, F](f: E => F) = new (ETask[E, ?] ~> ETask[F, ?]) {
    def apply[A](t: ETask[E, A]) = t.leftMap(f)
  }
}
