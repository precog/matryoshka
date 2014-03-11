package slamdata.engine

import scalaz._

sealed trait SemanticError {
  def message: String
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
  case class IncompilableNode(node: Node) extends SemanticError {
    def message = "The node " + node + " cannot be compiled"
  }
}