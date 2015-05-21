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

  implicit def ErrorRenderTree[A <: Error] = RenderTree.fromToString[A]("Error")
}

final case class ManyErrors(errors: NonEmptyList[Error]) extends Error {
  def message = errors.map(_.message).list.mkString("[", "\n", "]")

  override val stackTrace = errors.head.stackTrace

  override def fullMessage = errors.head.fullMessage
}
final case class PhaseError(phases: Vector[PhaseResult], causedBy: Error) extends Error {
  def message = phases.mkString("\n\n")

  override val stackTrace = causedBy.stackTrace

  override def fullMessage = phases.mkString("\n\n")
}

sealed trait ParsingError extends Error
final case class GenericParsingError(message: String) extends ParsingError

sealed trait SemanticError extends Error
object SemanticError {
  import slamdata.engine.sql._

  implicit val SemanticErrorShow = new Show[SemanticError] {
    override def show(value: SemanticError) = Cord(value.message)
  }

  final case class GenericError(message: String) extends SemanticError

  final case class DomainError(data: Data, hint: Option[String]) extends SemanticError {
    def message = "The data '" + data + "' did not fall within its expected domain" + hint.map(": " + _)
  }

  final case class FunctionNotFound(name: String) extends SemanticError {
    def message = "The function '" + name + "' could not be found in the standard library"
  }
  final case class FunctionNotBound(node: Node) extends SemanticError {
    def message = "A function was not bound to the node " + node
  }
  final case class TypeError(expected: Type, actual: Type, hint: Option[String]) extends SemanticError {
    def message = "Expected type " + expected + " but found " + actual + hint.map(": " + _).getOrElse("")
  }
  final case class VariableTypeError(vari: VarName, expected: Type, actual: VarValue) extends SemanticError {
    def message = "The variable " + vari + " should be convertible to type " + expected + " but found: " + actual
  }
  final case class DuplicateRelationName(defined: String, duplicated: SqlRelation) extends SemanticError {
    private def nameOf(r: SqlRelation) = r match {
      case TableRelationAST(name, aliasOpt) => aliasOpt.getOrElse(name)
      case ExprRelationAST(_, alias)        => alias
      case JoinRelation(_, _, _, _)         => "unknown"
      case CrossRelation(_, _)              => "unknown"
    }

    def message = "Found relation with duplicate name '" + defined + "': " + defined
  }
  final case class NoTableDefined(node: Node) extends SemanticError {
    def message = "No table was defined in the scope of \'" + node.sql + "\'"
  }
  final case class UnboundVariable(v: Vari) extends SemanticError {
    def message = "The variable " + v + " is not bound to any value"
  }
  final case class MissingField(name: String) extends SemanticError {
    def message = "No field named '" + name + "' exists"
  }
  final case class MissingIndex(index: Int) extends SemanticError {
    def message = "No element exists at array index '" + index
  }
  final case class WrongArgumentCount(func: Func, expected: Int, actual: Int) extends SemanticError {
    def message = "Wrong number of arguments for function '" + func.name + "': expected " + expected + " but found " + actual
  }
  final case class NonCompilableNode(node: Node) extends SemanticError {
    def message = "The node " + node + " cannot be compiled"
  }
  final case class ExpectedLiteral(node: Node) extends SemanticError {
    def message = "Expected literal but found '" + node.sql + "'"
  }
  final case class AmbiguousReference(node: Node, relations: List[SqlRelation]) extends SemanticError {
    def message = "The expression '" + node.sql + "' is ambiguous and might refer to any of the tables " + relations.mkString(", ")
  }
  final case class UnsupportedJoinCondition(clause: Expr) extends SemanticError {
    def message = "The join clause is not supported: " + clause.sql
  }
  final case class ExpectedOneTableInJoin(expr: Expr) extends SemanticError {
    def message = "In a join clause, expected to find an expression with a single table, but found: " + expr.sql
  }
  final case object CompiledTableMissing extends SemanticError {
    def message = "Expected the root table to be compiled but found nothing"
  }
  final case class CompiledSubtableMissing(name: String) extends SemanticError {
    def message = "Expected to find a compiled subtable with name \"" + name + "\""
  }
  final case class DateFormatError(func: Func, str: String, hint: Option[String]) extends SemanticError {
    def message = "Date/time string could not be parsed as " + func.name + ": " + str + hint.map(" (" + _ + ")").getOrElse("")
  }
}

sealed trait PlannerError extends Error
object PlannerError {
  final case class InternalError(message: String) extends PlannerError
  final case class NonRepresentableData(data: Data) extends PlannerError {
    def message = "The back-end has no representation for the constant: " + data
  }
  final case class UnsupportedFunction(func: Func, message: String) extends PlannerError {
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

  implicit val PlannerErrorRenderTree: RenderTree[PlannerError] = new RenderTree[PlannerError] {
    def render(v: PlannerError) = Terminal(List("Error"), Some(v.message))
  }
}
