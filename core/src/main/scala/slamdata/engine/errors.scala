package slamdata.engine

import scalaz._
import scalaz.concurrent._

import slamdata.engine.fp._

// TODO: perhaps toggle `extends Throwable` for debugging purposes
sealed trait Error {
  def message: String

  override def getMessage = message

  // val stackTrace = getStackTrace

  def fullMessage = message // + "\n" + slamdata.java.JavaUtil.abbrev(stackTrace)

  override def toString = fullMessage
}

object Error {
  implicit def ShowError[A <: Error] = new Show[A] {
    override def show(v: A): Cord = Cord(v.fullMessage)
  }

  implicit def ErrorRenderTree[A <: Error] = RenderTree.fromToString[A]("Error")
}

// TODO: seal this trait
trait EnvironmentError {
  def message: String
}
final case class MissingBackend(message: String) extends EnvironmentError

final case class EParseError(error: slamdata.engine.sql.ParsingError) extends Error
final case class CompileError(error: CompilationError) extends Error

sealed trait CompilationError {
  def message: String
}
final case class CompilePathError(error: slamdata.engine.fs.PathError) extends CompilationError
final case class ESemanticError(error: SemanticError) extends CompilationError
final case class EPlannerError(error: PlannerError) extends CompilationError
final case class ManyErrors(errors: NonEmptyList[CompilationError]) extends CompilationError {
  def message = errors.map(_.message).list.mkString("[", "\n", "]")

  // override val stackTrace = errors.head.stackTrace

  // override def fullMessage = errors.head.fullMessage
}
final case class PhaseError(phases: Vector[PhaseResult], causedBy: CompilationError) extends CompilationError {
  def message = phases.mkString("\n\n")

  // override val stackTrace = causedBy.stackTrace

  override def fullMessage = phases.mkString("\n\n")
}

sealed trait ProcessingError {
  def message: String
}
final case class PEvaluationError(error: EvaluationError)
    extends ProcessingError
final case class PResultError(error: ResultError) extends ProcessingError
final case class PWriteError(error: WriteError) extends ProcessingError

// TODO: seal this trait
trait ResultError {
  def message: String
}
final case class ResultPathError(error: PathError) extends ResultError


object Errors {
  type ETask[E, X] = EitherT[Task, E, X]
  def liftE[B, E](f: B => E) = new (ETask[B, ?] ~> ETask[E, ?]) {
    def apply[T](t: ETask[B, T]): ETask[E, T] = t.leftMap(f)
  }

  implicit def ETaskCatchable[E] = new Catchable[ETask[E, ?]] {
    def attempt[A](f: ETask[E, A]) =
      EitherT(f.run.attempt.map(_.fold(
        e => \/-(-\/(e)),
        _.fold(-\/(_), x => \/-(\/-(x))))))

    def fail[A](err: Throwable) = EitherT.right(Task.fail(err))
  }
}
