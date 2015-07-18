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

sealed trait PlannerError {
  def message: String
}
trait BackendSpecificError {
  def message: String
}

object PlannerError {
  final case class NonRepresentableData(data: Data) extends PlannerError {
    def message = "The back-end has no representation for the constant: " + data
  }
  final case class UnsupportedFunction(func: Func, message: String) extends PlannerError
  final case class PlanPathError(error: slamdata.engine.fs.PathError) extends PlannerError {
    def message = error.message
  }
  object UnsupportedFunction extends ((Func, String) => PlannerError) {
    def apply(func: Func): PlannerError =
      new UnsupportedFunction(func, "The function '" + func.name + "' is recognized but not supported by this back-end")
  }
  final case class UnsupportedPlan(plan: LogicalPlan[_], hint: Option[String]) extends PlannerError {
    def message = "The back-end has no or no efficient means of implementing the plan" + hint.map(" (" + _ + ")").getOrElse("")+ ": " + plan
  }
  final case class FuncApply(func: Func, expected: String, actual: String) extends PlannerError {
    def message = "A parameter passed to function " + func.name + " is invalid: Expected " + expected + " but found: " + actual
  }
  final case class FuncArity(func: Func, actual: Int) extends PlannerError {
    def message = "The wrong number of parameters were passed to " + func.name + "; expected " + func.arity + " but found " + actual
  }
  final case class ObjectIdFormatError(str: String) extends PlannerError {
    def message = "Invalid ObjectId string: " + str
  }

  final case class NonRepresentableInJS(value: String) extends PlannerError {
    def message = "Operation/value could not be compiled to JavaScript: " + value
  }
  final case class UnsupportedJS(value: String) extends PlannerError {
    def message = "Conversion of operation/value to JavaScript not implemented: " + value
  }

  final case class InternalError(message: String) extends PlannerError

  final case class BackendError(error: BackendSpecificError) extends PlannerError {
    def message = error.message
  }

  implicit val PlannerErrorRenderTree: RenderTree[PlannerError] = new RenderTree[PlannerError] {
    def render(v: PlannerError) = Terminal(List("Error"), Some(v.message))
  }
}

trait Planner[PhysicalPlan] {
  def plan(logical: Term[LogicalPlan]): PlannerError \/ PhysicalPlan

  private val sqlParser = new SQLParser()

  private type WriterResult[A] = Writer[Vector[PhaseResult], A]

  private type EitherWriter[A] = EitherT[WriterResult, CompilationError, A]

  private def withTree[A](name: String)(ea: CompilationError \/ A)(implicit RA: RenderTree[A]): EitherWriter[A] = {
    val result = ea.fold(
      PhaseResult.Error(name, _),
      a => PhaseResult.Tree(name, RA.render(a)))

    EitherT[WriterResult, CompilationError, A](WriterT.writer((Vector.empty[PhaseResult] :+ result) -> ea))
  }

  private def withString[A](a: A)(render: A => (String, Cord)): EitherWriter[A] = {
    val (name, plan) = render(a)
    val result = PhaseResult.Detail(name, plan.toString)

    EitherT[WriterResult, CompilationError, A](
      WriterT.writer[Vector[PhaseResult], CompilationError \/ A](
        (Vector.empty :+ result) -> \/- (a)))
  }

  def queryPlanner(showNative: PhysicalPlan => (String, Cord))(implicit RA: RenderTree[PhysicalPlan]):
      QueryRequest => (Vector[slamdata.engine.PhaseResult], CompilationError \/ PhysicalPlan) = { req =>
    import SemanticAnalysis._

    // TODO: Factor these things out as individual WriterT functions that can be composed.

    implicit val RU = new RenderTree[Unit] {
      def render(v: Unit) = Terminal(List("Unit"), Some("()"))
    }
    (for {
      select     <- withTree("SQL AST")(\/-(req.query))
      tree       <- withTree("Variables Substituted")(Variables.substVars[Unit](tree(select), req.variables).leftMap(ESemanticError))
      tree       <- withTree("Annotated Tree")(AllPhases(tree).disjunction.leftMap(e => ManyErrors(e.map(ESemanticError))))
      logical    <- withTree("Logical Plan")(Compiler.compile(tree).leftMap(ESemanticError))
      simplified <- withTree("Simplified")(\/-(logical.cata(Optimizer.simplify)))
      physical   <- withTree("Physical Plan")(plan(simplified).leftMap(EPlannerError))
      _          <- withString(physical)(showNative)
    } yield physical).run.run
  }
}
