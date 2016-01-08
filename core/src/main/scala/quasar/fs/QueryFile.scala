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

package quasar.fs

import quasar.Predef._
import quasar._
import quasar.effect.LiftedOps
import quasar.fp._
import quasar.fs.{Path => QPath}
import quasar.recursionschemes._

import pathy.Path._
import scalaz._, Scalaz._
import scalaz.stream.Process

sealed trait QueryFile[A]

object QueryFile {
  final case class ResultHandle(run: Long) extends scala.AnyVal

  object ResultHandle {
    implicit val resultHandleShow: Show[ResultHandle] =
      Show.showFromToString

    implicit val resultHandleOrder: Order[ResultHandle] =
      Order.orderBy(_.run)
  }

  final case class ExecutePlan(lp: Fix[LogicalPlan], out: AFile)
    extends QueryFile[(PhaseResults, FileSystemError \/ AFile)]

  final case class EvaluatePlan(lp: Fix[LogicalPlan])
    extends QueryFile[(PhaseResults, FileSystemError \/ ResultHandle)]

  final case class More(h: ResultHandle)
    extends QueryFile[FileSystemError \/ Vector[Data]]

  final case class Close(h: ResultHandle)
    extends QueryFile[Unit]

  final case class Explain(lp: Fix[LogicalPlan])
    extends QueryFile[(PhaseResults, FileSystemError \/ ExecutionPlan)]

  /** TODO: While this is a bit better in one dimension here in `QueryFile`,
    *       `@mossprescott` points out it is still a bit of a stretch to include
    *       in this algebra. We need to revisit this and probably add algebras
    *       over multiple dimensions to better organize these (and other)
    *       operations.
    *
    *       For more discussion, see
    *       https://github.com/quasar-analytics/quasar/pull/986#discussion-diff-45081757
    */
  final case class ListContents(dir: ADir)
    extends QueryFile[FileSystemError \/ Set[Node]]

  final case class FileExists(file: AFile)
    extends QueryFile[FileSystemError \/ Boolean]

  @SuppressWarnings(Array("org.brianmckenna.wartremover.warts.NonUnitStatements"))
  final class Ops[S[_]](implicit S0: Functor[S], S1: QueryFileF :<: S)
    extends LiftedOps[QueryFile, S] {

    type M[A] = FileSystemErrT[F, A]

    val unsafe = Unsafe[S]
    val transforms = Transforms[F]
    import transforms._

    /** Returns the path to the result of executing the given [[LogicalPlan]],
      * using the provided path if possible.
      *
      * Execution of certain plans may return a result file other than the
      * requested file if it is more efficient to do so (i.e. to avoid copying
      * lots of data for a plan consisting of a single `ReadF(...)`).
      */
    def execute(plan: Fix[LogicalPlan], out: AFile): ExecM[AFile] =
      EitherT(WriterT(lift(ExecutePlan(plan, out))): G[FileSystemError \/ AFile])

    /** Returns the stream of data resulting from evaluating the given
      * [[LogicalPlan]].
      */
    def evaluate(plan: Fix[LogicalPlan]): Process[ExecM, Data] = {
      val f: M ~> ExecM =
        Hoist[FileSystemErrT].hoist[F, G](liftMT[F, PhaseResultT])

      def moreUntilEmpty(h: ResultHandle): Process[M, Data] =
        Process.await(unsafe.more(h): M[Vector[Data]]) { data =>
          if (data.isEmpty)
            Process.halt
          else
            Process.emitAll(data) ++ moreUntilEmpty(h)
        }

      def close(h: ResultHandle): ExecM[Unit] =
        toExec(unsafe.close(h))

      Process.await(unsafe.eval(plan))(h =>
        moreUntilEmpty(h).translate(f) onComplete Process.eval_(close(h)))
    }

    /** Returns a description of how the the given logical plan will be
      * executed.
      */
    def explain(plan: Fix[LogicalPlan]): ExecM[ExecutionPlan] =
      EitherT(WriterT(lift(Explain(plan))): G[FileSystemError \/ ExecutionPlan])

    /** Returns the path to the result of executing the given SQL^2 query
      * using the given output file if possible.
      */
    def executeQuery(query: sql.Expr, vars: Variables, out: AFile)
                    : CompExecM[AFile] = {

      compileAnd(query, vars)(execute(_, out))
    }

    /** Returns the source of values from the result of executing the given
      * SQL^2 query.
      */
    def evaluateQuery(query: sql.Expr, vars: Variables)
                     : Process[CompExecM, Data] = {

      compToCompExec(queryPlan(query, vars))
        .liftM[Process]
        .flatMap(lp => evaluate(lp).translate[CompExecM](execToCompExec))
    }

    /** Returns a description of how the the given SQL^2 query will be
      * executed.
      */
    def explainQuery(query: sql.Expr, vars: Variables)
                    : CompExecM[ExecutionPlan] = {

      compileAnd(query, vars)(explain)
    }

    /** Returns immediate children of the given directory, fails if the
      * directory does not exist.
      */
    def ls(dir: ADir): M[Set[Node]] =
      EitherT(lift(ListContents(dir)))

    /** The children of the root directory. */
    def ls: M[Set[Node]] =
      ls(rootDir)

    /** Returns all files in this directory and all of it's sub-directories
      * Fails if the directory does not exist.
      */
    def descendantFiles(dir: ADir): M[Set[RFile]] = {
      type S[A] = StreamT[M, A]

      def lsR(desc: RDir): StreamT[M, RFile] =
        StreamT.fromStream[M, Node](ls(dir </> desc) map (_.toStream))
          .flatMap(n => refineType(n.path).fold(
            d => lsR(desc </> d),
            f => (desc </> f).point[S]))

      lsR(currentDir).foldLeft(Set.empty[RFile])(_ + _)
    }

    /** Returns whether the given file exists. */
    def fileExists(file: AFile): M[Boolean] =
      EitherT(lift(FileExists(file)))

    ////

    private def compileAnd[A](query: sql.Expr, vars: Variables)
                             (f: Fix[LogicalPlan] => ExecM[A])
                             : CompExecM[A] = {
      compToCompExec(queryPlan(query, vars))
        .flatMap(lp => execToCompExec(f(lp)))
    }
  }

  object Ops {
    implicit def apply[S[_]](implicit S0: Functor[S], S1: QueryFileF :<: S): Ops[S] =
      new Ops[S]
  }

  /** Low-level, unsafe operations. Clients are responsible for resource-safety
    * when using these.
    */
  @SuppressWarnings(Array("org.brianmckenna.wartremover.warts.NonUnitStatements"))
  final class Unsafe[S[_]](implicit S0: Functor[S], S1: QueryFileF :<: S)
    extends LiftedOps[QueryFile, S] {

    val transforms = Transforms[F]
    import transforms._

    /** Returns a handle to the results of evaluating the given [[LogicalPlan]]
      * that can be used to read chunks of result data.
      *
      * Care must be taken to `close` the returned handle in order to avoid
      * potential resource leaks.
      */
    def eval(lp: Fix[LogicalPlan]): ExecM[ResultHandle] =
      EitherT(WriterT(lift(EvaluatePlan(lp))): G[FileSystemError \/ ResultHandle])

    /** Read the next chunk of data from the result set represented by the given
      * handle.
      *
      * An empty `Vector` signals that all data has been read.
      */
    def more(rh: ResultHandle): FileSystemErrT[F, Vector[Data]] =
      EitherT(lift(More(rh)))

    /** Closes the given result handle, freeing any resources it was using. */
    def close(rh: ResultHandle): F[Unit] =
      lift(Close(rh))
  }

  object Unsafe {
    implicit def apply[S[_]](implicit S0: Functor[S], S1: QueryFileF :<: S): Unsafe[S] =
      new Unsafe[S]
  }

  class Transforms[F[_]: Monad] {
    type G[A] = PhaseResultT[F, A]
    type H[A] = SemanticErrsT[G, A]

    type ExecM[A]     = FileSystemErrT[G, A]
    type CompExecM[A] = FileSystemErrT[H, A]

    val execToCompExec: ExecM ~> CompExecM =
      Hoist[FileSystemErrT].hoist[G, H](liftMT[G, SemanticErrsT])

    val compToCompExec: CompileM ~> CompExecM = {
      val hoistW: PhaseResultW ~> G = Hoist[PhaseResultT].hoist(pointNT[F])
      val hoistC: CompileM ~> H     = Hoist[SemanticErrsT].hoist(hoistW)
      liftMT[H, FileSystemErrT] compose hoistC
    }

    val toExec: F ~> ExecM =
      liftMT[G, FileSystemErrT] compose liftMT[F, PhaseResultT]

    def fsErrToExec: FileSystemErrT[F, ?] ~> ExecM =
      Hoist[FileSystemErrT].hoist[F, PhaseResultT[F, ?]](liftMT[F, PhaseResultT])

    val toCompExec: F ~> CompExecM =
      execToCompExec compose toExec
  }

  object Transforms {
    def apply[F[_]: Monad]: Transforms[F] =
      new Transforms[F]
  }
}
