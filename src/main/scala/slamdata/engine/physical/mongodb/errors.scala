package slamdata.engine.physical.mongodb

import slamdata.engine.Error

import scalaz._
import Scalaz._

case class PipelineOpMergeError(left: PipelineOp, right: PipelineOp, hint: Option[String] = None) {
  def message = "The pipeline op " + left + " cannot be merged with the pipeline op " + right + hint.map(": " + _).getOrElse("")

  override lazy val hashCode = Set(left.hashCode, right.hashCode, hint.hashCode).hashCode

  override def equals(that: Any) = that match {
    case PipelineOpMergeError(l2, r2, h2) => (left == l2 && right == r2 || left == r2 && right == l2) && hint == h2
    case _ => false
  }
}

sealed trait MergePatchError extends Error
object MergePatchError {
  case class Op(error: PipelineOpMergeError) extends MergePatchError {
    def message = error.message
  }
  case object NonEmpty extends MergePatchError {
    def message = "merge history not empty"
  }
  case object UnknownShape extends MergePatchError {
    def message = "The shape of a pipeline op is not recognized and merge cannot be completed"
  }
}
