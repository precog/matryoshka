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

package slamdata.engine

import slamdata.engine.fp._
import slamdata.engine.analysis.fixplate._
import slamdata.engine.sql._

import scalaz.{Node => _, Tree => _, _}
import Scalaz._

trait Planner[PhysicalPlan] {
  import Planner._

  def plan(logical: Term[LogicalPlan]): EitherWriter[PhysicalPlan]

  private val sqlParser = new SQLParser()

  def queryPlanner(showNative: PhysicalPlan => (String, Cord))(implicit RA: RenderTree[PhysicalPlan]):
      QueryRequest => (Vector[slamdata.engine.PhaseResult], slamdata.engine.Error \/ PhysicalPlan) = { req =>
    import SemanticAnalysis._

    // TODO: Factor these things out as individual WriterT functions that can be composed.

    implicit val RU = new RenderTree[Unit] {
      def render(v: Unit) = Terminal(List("Unit"), Some("()"))
    }
    (for {
      select     <- withTree("SQL AST")(\/-(req.query))
      tree       <- withTree("Variables Substituted")(Variables.substVars[Unit](tree(select), req.variables))
      tree       <- withTree("Annotated Tree")(AllPhases(tree).disjunction.leftMap(ManyErrors.apply))
      logical    <- withTree("Logical Plan")(Compiler.compile(tree))
      simplified <- withTree("Simplified")(\/-(logical.cata(Optimizer.simplify)))
      physical   <- plan(simplified)
      _          <- withTree("Physical Plan")(\/-(physical))
      _          <- withString(physical)(showNative)
    } yield physical).run.run
  }
}
object Planner {
  private type WriterResult[A] = Writer[Vector[PhaseResult], A]

  type EitherWriter[A] = EitherT[Writer[Vector[PhaseResult], ?], Error, A]

  def emit[A](log: Vector[PhaseResult], v: Error \/ A): EitherWriter[A] = {
    EitherT[WriterResult, Error, A](
      WriterT.writer(log -> v))
  }

  def withTree[A](name: String)(ea: Error \/ A)(implicit RA: RenderTree[A]): EitherWriter[A] = {
    val result = ea.fold(
      PhaseResult.Error(name, _),
      a => PhaseResult.Tree(name, RA.render(a)))

    emit(Vector(result), ea)
  }

  def withString[A](a: A)(render: A => (String, Cord)): EitherWriter[A] = {
    val (name, plan) = render(a)
    val result = PhaseResult.Detail(name, plan.toString)

    emit(Vector(result), \/-(a))
  }
}
