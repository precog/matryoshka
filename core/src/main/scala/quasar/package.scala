import quasar.Predef.{Long, String, Vector}
import quasar.fs.FileSystem
import scalaz._

package object quasar {
  type SemanticErrors = NonEmptyList[SemanticError]
  type SemanticErrsT[F[_], A] = EitherT[F, SemanticErrors, A]

  type PhaseResults = Vector[PhaseResult]
  type PhaseResultW[A] = Writer[PhaseResults, A]
  type PhaseResultT[F[_], A] = WriterT[F, PhaseResults, A]

  type ExecErrT[F[_], A] = EitherT[F, ExecutionError, A]
  type EnvErr2T[F[_], A] = EitherT[F, EnvironmentError2, A]

  type SeqNameGeneratorT[F[_], A] = StateT[F, Long, A]
  type SaltedSeqNameGeneratorT[F[_], A] = ReaderT[SeqNameGeneratorT[F, ?], String, A]

  type QueryableFileSystem[A] = Coproduct[ExecutePlan, FileSystem, A]
}
