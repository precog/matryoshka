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

import quasar.Predef.{Long, String, Vector}
import quasar.fp._
import quasar.recursionschemes._, Fix._
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
                        Variables.substVars(ast, vars) leftMap (_.wrapNel))
      annTree     <- phase("Annotated Tree", AllPhases(substAst))
      logical     <- phase("Logical Plan", Compiler.compile(annTree) leftMap (_.wrapNel))
      optimized   <- phase("Optimized", \/-(Optimizer.optimize(logical)))
      typechecked <- phase("Typechecked", LogicalPlan.ensureCorrectTypes(optimized).disjunction)
    } yield typechecked
  }
}
