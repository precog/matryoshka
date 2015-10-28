import quasar.Predef.Vector
import scalaz.{EitherT, WriterT, Writer, NonEmptyList}

package object quasar {
  type SemanticErrors = NonEmptyList[SemanticError]
  type SemanticErrsT[F[_], A] = EitherT[F, SemanticErrors, A]

  type PhaseResults = Vector[PhaseResult]
  type PhaseResultW[A] = Writer[PhaseResults, A]
  type PhaseResultT[F[_], A] = WriterT[F, PhaseResults, A]

  type ExecErrT[F[_], A] = EitherT[F, ExecutionError, A]
}
