package quasar

import quasar.Predef._
import quasar.fp._
import quasar.fs.{Path => QPath, _}
import quasar.recursionschemes._
import Recursive.ops._

import pathy._, Path._

import scalaz._
import scalaz.syntax.nel._
import scalaz.syntax.monad._
import scalaz.syntax.writer._
import scalaz.syntax.either._
import scalaz.syntax.std.option._
import scalaz.std.vector._
import scalaz.stream.Process

final case class ExecutePlan[A](
  lp: Fix[LogicalPlan],
  out: AbsFile[Sandboxed],
  f: ((PhaseResults, ExecutionError \/ ResultFile)) => A)

object ExecutePlan {
  type CompileM[A] = SemanticErrsT[PhaseResultW, A]

  // TODO: Find a home for this
  def compileQuery(query: sql.Expr, vars: Variables): CompileM[Fix[LogicalPlan]] = {
    import SemanticAnalysis.{tree, AllPhases}

    def phase[A: RenderTree](label: String, r: SemanticErrors \/ A): CompileM[A] =
      EitherT(r.point[PhaseResultW]) flatMap { a =>
        val pr = PhaseResult.Tree(label, RenderTree[A].render(a))
        (a.set(Vector(pr)): PhaseResultW[A]).liftM[SemanticErrsT]
      }

    for {
      ast         <- phase("SQL AST", query.right)
      substAst    <- phase("Variables Substituted",
                           Variables.substVars[Unit](tree(ast), vars) leftMap (_.wrapNel))
      annTree     <- phase("Annotated Tree", AllPhases(substAst).disjunction)
      logical     <- phase("Logical Plan", Compiler.compile(annTree) leftMap (_.wrapNel))
      simplified  <- phase("Simplified", logical.cata(Optimizer.simplify).right)
      typechecked <- phase("Typechecked", LogicalPlan.ensureCorrectTypes(simplified).disjunction)
    } yield typechecked
  }

  final class Ops[S[_]](implicit S0: Functor[S], S1: ExecutePlan :<: S) {
    import ResultFile._

    type F[A] = Free[S, A]

    val transforms = Transforms[F]
    import transforms._

    /** Returns the path to the result of executing the given [[LogicalPlan]],
      * using the provided path if possible.
      *
      * Execution of certain plans may return a result file other than the
      * requested file if it is more efficient to do so (i.e. to avoid copying
      * lots of data for a plan consisting of a single `ReadF(...)`).
      */
    def execute(plan: Fix[LogicalPlan], out: AbsFile[Sandboxed]): ExecM[ResultFile] =
      EitherT(WriterT(lift(ExecutePlan(plan, out, ι))): G[ExecutionError \/ ResultFile])

    /** Returns the path to the result of executing the given [[LogicalPlan]] */
    def execute_(plan: Fix[LogicalPlan])
                (implicit M: ManageFile.Ops[S]): ExecM[ResultFile] = {

      val outFile = plan.foldMap {
        case Fix(LogicalPlan.ReadF(p)) => Vector(p)
        case _                         => Vector.empty
      }.headOption flatMap pathToAbsFile cata (M.tempFileNear, M.anyTempFile)

      for {
        out <- toExec(outFile)
        rf0 <- execute(plan, out)
        rf1 =  rf0.fold(Temp, usr => if (usr == out) Temp(usr) else User(usr))
      } yield rf1
    }

    /** Returns the source of values from the result of executing the given
      * [[LogicalPlan]].
      */
    def evaluate(plan: Fix[LogicalPlan])
                (implicit R: ReadFile.Ops[S], M: ManageFile.Ops[S])
                : Process[FileSystemErrT[ExecM, ?], Data] = {

      type N[A] = FileSystemErrT[ExecM, A]

      val hoistFS: FileSystemErrT[F, ?] ~> N =
        Hoist[FileSystemErrT].hoist(toExec)

      def values(f: AbsFile[Sandboxed]) =
        R.scanAll(f).translate[N](hoistFS)

      def handleTemp(tmp: AbsFile[Sandboxed]) = {
        val cleanup = (hoistFS(M.deleteFile(tmp)): N[Unit])
                        .liftM[Process].drain
        values(tmp) onComplete cleanup
      }

      (execute_(plan).liftM[FileSystemErrT]: N[ResultFile])
        .liftM[Process]
        .flatMap(_.fold(values, handleTemp))
    }

    /** Returns the path to the result of executing the given SQL^2 query. */
    def executeQuery(query: sql.Expr, vars: Variables, out: AbsFile[Sandboxed])
                    : CompExecM[ResultFile] = {

      compileAnd(query, vars)(execute(_, out))
    }

    /** Returns the path to the result of executing the given SQL^2 query
      * using the given output file if possible.
      */
    def executeQuery_(query: sql.Expr, vars: Variables)
                     (implicit M: ManageFile.Ops[S]): CompExecM[ResultFile] = {
      compileAnd(query, vars)(execute_)
    }

    /** Returns the source of values from the result of executing the given
      * SQL^2 query.
      */
    def evaluateQuery(query: sql.Expr, vars: Variables)
                     (implicit R: ReadFile.Ops[S], M: ManageFile.Ops[S])
                     : Process[FileSystemErrT[CompExecM, ?], Data] = {

      type N[A] = FileSystemErrT[CompExecM, A]

      def comp = (compToCompExec(compileQuery(query, vars))
                   .liftM[FileSystemErrT]: N[Fix[LogicalPlan]])
                   .liftM[Process]

      val hoistFS = Hoist[FileSystemErrT].hoist(execToCompExec)

      comp flatMap (lp => evaluate(lp).translate[N](hoistFS))
    }

    ////

    private def compileAnd[A](query: sql.Expr, vars: Variables)
                             (f: Fix[LogicalPlan] => ExecM[A])
                             : CompExecM[A] = {
      compToCompExec(compileQuery(query, vars))
        .flatMap(lp => execToCompExec(f(lp)))
    }

    private def pathToAbsFile(p: QPath): Option[AbsFile[Sandboxed]] =
      p.file map (fn =>
        p.asAbsolute.dir
          .foldLeft(rootDir[Sandboxed])((d, n) => d </> dir(n.value)) </>
          file(fn.value))

    private def lift[A](ep: ExecutePlan[A]): F[A] =
      Free.liftF(S1.inj(ep))
  }

  object Ops {
    implicit def apply[S[_]](implicit S0: Functor[S], S1: ExecutePlan :<: S): Ops[S] =
      new Ops[S]
  }

  class Transforms[F[_]: Monad] {
    type G[A] = PhaseResultT[F, A]
    type H[A] = SemanticErrsT[G, A]

    type ExecM[A]     = ExecErrT[G, A]
    type CompExecM[A] = ExecErrT[H, A]

    val execToCompExec: ExecM ~> CompExecM =
      Hoist[ExecErrT].hoist[G, H](liftMT[G, SemanticErrsT])

    val compToCompExec: CompileM ~> CompExecM = {
      val hoistW: PhaseResultW ~> G = Hoist[PhaseResultT].hoist(pointNT[F])
      val hoistC: CompileM ~> H     = Hoist[SemanticErrsT].hoist(hoistW)
      liftMT[H, ExecErrT] compose hoistC
    }

    val toExec: F ~> ExecM =
      liftMT[G, ExecErrT] compose liftMT[F, PhaseResultT]

    val toCompExec: F ~> CompExecM =
      execToCompExec compose toExec
  }

  object Transforms {
    def apply[F[_]: Monad]: Transforms[F] =
      new Transforms[F]
  }

  implicit def executePlanFunctor: Functor[ExecutePlan] =
    new Functor[ExecutePlan] {
      def map[A, B](fa: ExecutePlan[A])(f: A => B) =
        ExecutePlan(fa.lp, fa.out, f compose fa.f)
    }
}
