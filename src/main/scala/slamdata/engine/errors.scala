package slamdata.engine

import scalaz._

sealed trait SemanticError {
  def message: String

  val stackTrace = java.lang.Thread.currentThread.getStackTrace

  def fullMessage = message + "\n" + stackTrace.map(_.toString).mkString("\n")
}
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
  case class NoTableDefined(ident: Ident) extends SemanticError {
    def message = "No table was defined in the scope of identifier \'" + ident + "\'"
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
  case class AmbiguousIdentifier(ident: Ident, relations: List[SqlRelation]) extends SemanticError {
    def message = "The identifier '" + ident + "' is ambiguous and might refer to any of the tables " + relations.mkString(", ")
  }
}