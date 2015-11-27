import quasar.Predef.{Long, String, Vector}
import quasar.fp._
import quasar.recursionschemes._, Fix._, Recursive.ops._, FunctorT.ops._
import quasar.sql._

import scalaz._
import scalaz.syntax.monad._
import scalaz.syntax.either._
import scalaz.syntax.writer._
import scalaz.syntax.nel._
import scalaz.std.vector._

package object quasar {
  type SemanticErrors = NonEmptyList[SemanticError]
  type SemanticErrsT[F[_], A] = EitherT[F, SemanticErrors, A]

  type PhaseResults = Vector[PhaseResult]
  type PhaseResultW[A] = Writer[PhaseResults, A]
  type PhaseResultT[F[_], A] = WriterT[F, PhaseResults, A]

  type CompileM[A] = SemanticErrsT[PhaseResultW, A]

  type EnvErr2T[F[_], A] = EitherT[F, EnvironmentError2, A]

  type SeqNameGeneratorT[F[_], A] = StateT[F, Long, A]
  type SaltedSeqNameGeneratorT[F[_], A] = ReaderT[SeqNameGeneratorT[F, ?], String, A]

  /** Returns the `LogicalPlan` for the given SQL^2 query. */
  def queryPlan(query: Expr, vars: Variables): CompileM[Fix[LogicalPlan]] = {
    import SemanticAnalysis.AllPhases

    def phase[A: RenderTree](label: String, r: SemanticErrors \/ A): CompileM[A] =
      EitherT(r.point[PhaseResultW]) flatMap { a =>
        val pr = PhaseResult.Tree(label, RenderTree[A].render(a))
        (a.set(Vector(pr)): PhaseResultW[A]).liftM[SemanticErrsT]
      }

    for {
      ast         <- phase("SQL AST", query.right)
      substAst    <- phase("Variables Substituted",
                           ast.cataM[SemanticError \/ ?, Expr](Variables.substVarsƒ(vars)) leftMap (_.wrapNel))
      annTree     <- phase("Annotated Tree", AllPhases(substAst))
      logical     <- phase("Logical Plan", Compiler.compile(annTree) leftMap (_.wrapNel))
      simplified  <- phase("Simplified", logical.transCata(repeatedly(Optimizer.simplifyƒ)).right)
      typechecked <- phase("Typechecked", LogicalPlan.ensureCorrectTypes(simplified).disjunction)
    } yield typechecked
  }
}
