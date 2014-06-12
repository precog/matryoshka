package slamdata.engine.physical.mongodb

import org.specs2.mutable._

import slamdata.engine.DisjunctionMatchers 

class PipelineSpec extends Specification with DisjunctionMatchers {
  def p(ops: PipelineOp*) = Pipeline(ops.toList)

  val empty = p()

  import PipelineOp._
  import ExprOp._

  "Pipeline.merge" should {
    "return left when right is empty" in {
      val l = p(Skip(10), Limit(10))

      l.merge(empty) must (beRightDisj(l))
    }

    "return right when left is empty" in {
      val r = p(Skip(10), Limit(10))

      empty.merge(r) must (beRightDisj(r))
    }
  }
}