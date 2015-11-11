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

package quasar

import quasar.Predef._
import quasar.fp._
import quasar.fs.Path._
import quasar.recursionschemes._, FunctorT.ops._

import scalaz.{Tree => _, _}, Scalaz._

trait Planner[PhysicalPlan] {
  import Planner._

  def plan(logical: Fix[LogicalPlan]): EitherWriter[PlannerError, PhysicalPlan]

  private def withString[E, A](a: A)(render: A => (String, Cord)): EitherWriter[E, A] = {
    val (name, plan) = render(a)
    EitherT[WriterResult, E, A]((Vector(PhaseResult.Detail(name, plan.toString)), \/-(a)))
  }

  def queryPlanner(showNative: PhysicalPlan => (String, Cord))(implicit RA: RenderTree[PhysicalPlan]):
      QueryRequest => EitherT[(Vector[quasar.PhaseResult], ?), CompilationError, PhysicalPlan] = { req =>
    import SemanticAnalysis._

    // TODO: Factor these things out as individual WriterT functions that can be composed.

    implicit val RU: RenderTree[Unit] = new RenderTree[Unit] {
      def render(v: Unit) = Terminal(List("Unit"), Some("()"))
    }

    for {
      select     <- withTree("SQL AST")(\/-(req.query))
      tree       <- withTree("Variables Substituted")(Variables.substVars[Unit](tree(select), req.variables).leftMap(CSemanticError(_)))
      tree       <- withTree("Annotated Tree")(AllPhases(tree).disjunction.leftMap(ManyErrors(_)))
      logical    <- withTree("Logical Plan")(Compiler.compile(tree).leftMap(CSemanticError(_)))
      simplified <- withTree("Simplified")(\/-(logical.transCata(repeatedly(Optimizer.simplifyƒ))))
      checked    <- withTree("Typechecked")(LogicalPlan.ensureCorrectTypes(simplified).disjunction.leftMap(ManyErrors(_)))
      physical   <- plan(checked).leftMap(CPlannerError(_))
      _          <- withTree("Physical Plan")(\/-(physical))
      _          <- withString(physical)(showNative)
    } yield physical
  }
}
object Planner {
  private type WriterResult[A] = (Vector[PhaseResult], A)

  type EitherWriter[E, A] = EitherT[(Vector[PhaseResult], ?), E, A]

  def emit[E, A](log: Vector[PhaseResult], v: E \/ A): EitherWriter[E, A] = {
    EitherT[WriterResult, E, A]((log, v))
  }

  def withTree[A](name: String)(ea: CompilationError \/ A)(implicit RA: RenderTree[A]): EitherWriter[CompilationError, A] = {
    emit(Vector.empty, ea).flatMap(a =>
      emit(Vector(PhaseResult.Tree(name, RA.render(a))), ea))
  }

  def withString[A](a: A)(render: A => (String, Cord)): EitherWriter[CompilationError, A] = {
    val (name, plan) = render(a)
    val result = PhaseResult.Detail(name, plan.toString)

    emit(Vector(result), \/-(a))
  }

  sealed trait PlannerError {
    def message: String
  }

  final case class NonRepresentableData(data: Data) extends PlannerError {
    def message = "The back-end has no representation for the constant: " + data
  }
  final case class UnsupportedFunction(func: Func, message: String) extends PlannerError
  final case class PlanPathError(error: PathError) extends PlannerError {
    def message = error.message
  }
  object UnsupportedFunction extends ((Func, String) => PlannerError) {
    def apply(func: Func): PlannerError =
      new UnsupportedFunction(func, "The function '" + func.name + "' is recognized but not supported by this back-end")
  }
  final case class UnsupportedJoinCondition(cond: Fix[LogicalPlan]) extends PlannerError {
    def message = "Joining with " + cond + " is not currently supported"
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

  implicit val PlannerErrorRenderTree: RenderTree[PlannerError] = new RenderTree[PlannerError] {
    def render(v: PlannerError) = Terminal(List("Error"), Some(v.message))
  }

  sealed trait CompilationError {
    def message: String
  }
  object CompilationError {
    final case class CompilePathError(error: PathError)
        extends CompilationError {
      def message = error.message
    }
    final case class CSemanticError(error: SemanticError)
        extends CompilationError {
      def message = error.message
    }
    final case class CPlannerError(error: PlannerError)
        extends CompilationError {
      def message = error.message
    }
    final case class ManyErrors(errors: NonEmptyList[SemanticError])
        extends CompilationError {
      def message = errors.map(_.message).list.mkString("[", "\n", "]")
    }
  }

  object CompilePathError {
    def apply(error: PathError): CompilationError =
      CompilationError.CompilePathError(error)
    def unapply(obj: CompilationError): Option[PathError] = obj match {
      case CompilationError.CompilePathError(error) => Some(error)
      case _                             => None
    }
  }
  object CSemanticError {
    def apply(error: SemanticError): CompilationError = CompilationError.CSemanticError(error)
    def unapply(obj: CompilationError): Option[SemanticError] = obj match {
      case CompilationError.CSemanticError(error) => Some(error)
      case _                             => None
    }
  }
  object CPlannerError {
    def apply(error: PlannerError): CompilationError = CompilationError.CPlannerError(error)
    def unapply(obj: CompilationError): Option[PlannerError] = obj match {
      case CompilationError.CPlannerError(error) => Some(error)
      case _                             => None
    }
  }
  object ManyErrors {
    def apply(error: NonEmptyList[SemanticError]): CompilationError = CompilationError.ManyErrors(error)
    def unapply(obj: CompilationError): Option[NonEmptyList[SemanticError]] = obj match {
      case CompilationError.ManyErrors(error) => Some(error)
      case _                             => None
    }
  }
}
