package slamdata.engine

import scalaz._

trait Error extends Throwable {
  def message: String

  override def getMessage = message

  val stackTrace = getStackTrace

  def fullMessage = message + "\n" + slamdata.java.JavaUtil.abbrev(stackTrace)

  override def toString = fullMessage
}
object Error {
  implicit def ShowError[A <: Error] = new Show[A] {
    override def show(v: A): Cord = Cord(v.fullMessage)
  }

  implicit def ErrorRenderTree[A <: Error]= new RenderTree[A] {
    def render(v: A) = Terminal(v.message, List("Error"))
  }
}

case class ManyErrors(errors: NonEmptyList[Error]) extends Error {
  def message = errors.map(_.message).list.mkString("[", "\n", "]")

  override val stackTrace = errors.head.stackTrace

  override def fullMessage = errors.head.fullMessage
}
case class PhaseError(phases: Vector[PhaseResult], causedBy: Error) extends Error {
  def message = phases.mkString("\n\n")

  override val stackTrace = causedBy.stackTrace

  override def fullMessage = phases.mkString("\n\n")
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
  case class VariableTypeError(vari: VarName, expected: Type, actual: VarValue) extends SemanticError {
    def message = "The variable " + vari + " should be convertible to type " + expected + " but found: " + actual
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
  case class UnboundVariable(v: Vari) extends SemanticError {
    def message = "The variable " + v + " is not bound to any value"
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
  case class DateFormatError(func: Func, str: String) extends SemanticError {
    def message = "Date/time string could not be parsed as " + func.name + ": " + str
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
  case class FuncApply(func: Func, expected: String, actual: String) extends PlannerError {
    def message = "A parameter passed to function " + func.name + " is invalid: Expected " + expected + " but found: " + actual
  }
  case class FuncArity(func: Func, actual: Int) extends PlannerError {
    def message = "The wrong number of parameters were passed to " + func.name + "; expected " + func.arity + " but found " + actual
  }
  case class ObjectIdFormatError(str: String) extends PlannerError {
    def message = "Invalid ObjectId string: " + str
  }
  case class NonRepresentableInJS(value: String) extends PlannerError {
    def message = "Operation/value could not be compiled to JavaScript: " + value
  }
  case class UnsupportedJS(value: String) extends PlannerError {
    def message = "Conversion of operation/value to JavaScript not implemented: " + value
  }

  implicit val PlannerErrorRenderTree: RenderTree[PlannerError] = new RenderTree[PlannerError] {
    def render(v: PlannerError) = Terminal(v.message)
  }
}
