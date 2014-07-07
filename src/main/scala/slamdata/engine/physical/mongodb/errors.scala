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

case class PipelineMergeError(merged: List[PipelineOp], lrest: List[PipelineOp], rrest: List[PipelineOp], hint: Option[String] = None) {
  def message = "The pipeline " + lrest + " cannot be merged with the pipeline " + rrest + hint.map(": " + _).getOrElse("")

  override lazy val hashCode = Set(merged.hashCode, lrest.hashCode, rrest.hashCode, hint.hashCode).hashCode

  override def equals(that: Any) = that match {
    case PipelineMergeError(m2, l2, r2, h2) => merged == m2 && (lrest == l2 && rrest == r2 || lrest == r2 && rrest == l2) && hint == h2
    case _ => false
  }
}

sealed trait MergePatchError extends Error
object MergePatchError {
  case class Pipeline(error: PipelineMergeError) extends MergePatchError {
    def message = error.message
  }
  case class Op(error: PipelineOpMergeError) extends MergePatchError {
    def message = error.message
  }
  case object NonEmpty extends MergePatchError {
    def message = "merge history not empty"
  }
}
