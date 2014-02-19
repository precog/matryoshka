package slamdata.engine

import scalaz._

sealed trait SemanticError {
  def message: String
}
object SemanticError {
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
  case class FunctionNotBound(node: slamdata.engine.sql.Node) extends SemanticError {
    def message = "A function was not bound to the node " + node
  }
  case class TypeError(expected: Type, actual: Type, hint: Option[String]) extends SemanticError {
    def message = "Expected type " + expected + " but found " + actual + hint.map(": " + _).getOrElse("")
  }
}