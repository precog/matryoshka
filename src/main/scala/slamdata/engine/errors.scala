package slamdata.engine

import scalaz._

trait Error extends Throwable {
  def message: String

  override def getMessage = message

  val stackTrace = java.lang.Thread.currentThread.getStackTrace

  def fullMessage = message + "\n" + stackTrace.map(_.toString).mkString("\n")

  override def toString = fullMessage
}
object Error {
  implicit def ShowError[A <: Error] = new Show[A] {
    override def show(v: A): Cord = Cord(v.fullMessage)
  }
}

case class ManyErrors(errors: NonEmptyList[Error]) extends Error {
  def message = errors.map(_.message).list.mkString("[", "\n", "]")
}
case class PhaseError(phases: Vector[PhaseResult], causedBy: Error) extends Error {
  def message = phases.mkString("\n\n") + causedBy.message

  override val stackTrace = causedBy.stackTrace

  override def fullMessage = phases.mkString("\n\n") + causedBy.fullMessage
}

sealed trait ParsingError extends Error
case class GenericParsingError(message: String) extends ParsingError

sealed trait SemanticError extends Error
object SemanticError {
  import slamdata.engine.sql._

  implicit val SemanticErrorShow = new Show[SemanticError] {
    override def show(value: SemanticError) = Cord(value.message)
  }

  case class GenericError(message: String) extends SemanticError

  case class DomainError(data: Data, hint: Option[String] = None) extends SemanticError {
    def message = "The data '" + data + "' did not fall within its expected domain" + hint.map(": " + _)
  }

  case class FunctionNotFound(name: String) extends SemanticError {
    def message = "The function '" + name + "' could not be found in the standard library"
  }
  case class FunctionNotBound(node: Node) extends SemanticError {
    def message = "A function was not bound to the node " + node
  }
  case class TypeError(expected: Type, actual: Type, hint: Option[String] = None) extends SemanticError {
    def message = "Expected type " + expected + " but found " + actual + hint.map(": " + _).getOrElse("")
  }
  case class DuplicateRelationName(defined: String, duplicated: SqlRelation) extends SemanticError {
    private def nameOf(r: SqlRelation) = r match {
      case r @ TableRelationAST(name, aliasOpt) => aliasOpt.getOrElse(name)
      case r @ SubqueryRelationAST(subquery, alias) => alias
      case r @ JoinRelation(left, right, join, clause) => "unknown"
      case r @ CrossRelation(left, right) => "unknown"
    }

    def message = "Found relation with duplicate name '" + defined + "': " + defined
  }
  case class NoTableDefined(node: Node) extends SemanticError {
    def message = "No table was defined in the scope of \'" + node.sql + "\'"
  }
  case class MissingField(name: String) extends SemanticError {
    def message = "No field named '" + name + "' exists"
  }
  case class MissingIndex(index: Int) extends SemanticError {
    def message = "No element exists at array index '" + index
  }
  case class WrongArgumentCount(func: Func, expected: Int, actual: Int) extends SemanticError {
    def message = "Wrong number of arguments for function '" + func.name + "': expected " + expected + " but found " + actual
  }
  case class NonCompilableNode(node: Node) extends SemanticError {
    def message = "The node " + node + " cannot be compiled"
  }
  case class ExpectedLiteral(node: Node) extends SemanticError {
    def message = "Expected literal but found '" + node.sql + "'"
  }
  case class AmbiguousReference(node: Node, relations: List[SqlRelation]) extends SemanticError {
    def message = "The expression '" + node.sql + "' is ambiguous and might refer to any of the tables " + relations.mkString(", ")
  }
  case class UnsupportedJoinCondition(clause: Expr) extends SemanticError {
    def message = "The join clause is not supported: " + clause.sql
  }
  case class ExpectedOneTableInJoin(expr: Expr) extends SemanticError {
    def message = "In a join clause, expected to find an expression with a single table, but found: " + expr.sql
  }
  case object CompiledTableMissing extends SemanticError {
    def message = "Expected the root table to be compiled but found nothing"
  }
  case class CompiledSubtableMissing(name: String) extends SemanticError {
    def message = "Expected to find a compiled subtable with name \"" + name + "\""
  }
}

sealed trait PlannerError extends Error
object PlannerError {
  case class InternalError(message: String) extends PlannerError
  case class NonRepresentableData(data: Data) extends PlannerError {
    def message = "The back-end has no representation for the constant: " + data
  }
  case class UnsupportedFunction(func: Func, message: String) extends PlannerError {
  }
  object UnsupportedFunction extends ((Func, String) => PlannerError) {
    def apply(func: Func): PlannerError = 
      new UnsupportedFunction(func, "The function '" + func.name + "' is recognized but not supported by this back-end")
  }
  case class UnsupportedPlan(plan: LogicalPlan[_], hint: Option[String] = None) extends PlannerError {
    def message = "The back-end has no or no efficient means of implementing the plan" + hint.map(" (" + _ + ")").getOrElse("")+ ": " + plan
  }
  
  implicit val PlannerErrorRenderTree: RenderTree[PlannerError] = new RenderTree[PlannerError] {
    def render(v: PlannerError) = Terminal(v.message)
  }
}