package slamdata.engine.physical.mongodb

import scalaz._
import Scalaz._

import slamdata.engine.{RenderTree, Terminal, NonTerminal}
import slamdata.engine.fp._

/**
 * A `PipelineBuilder` consists of a list of pipeline operations in *reverse*
 * order and a patch to be applied to all subsequent additions to the pipeline.
 *
 * This abstraction represents a work-in-progress, where more operations are 
 * expected to be patched and added to the pipeline. At some point, the `build`
 * method will be invoked to convert the `PipelineBuilder` into a `Pipeline`.
 */
case class PipelineBuilder private (buffer: List[PipelineOp], patch: MergePatch) {
  def build: Pipeline = Pipeline(buffer.reverse)

  def schema: PipelineSchema = PipelineSchema(buffer.reverse)

  def + (op: PipelineOp): MergePatchError \/ PipelineBuilder = {
    for {
      t <- patch(op)

      (ops2, patch2) = t
    } yield copy(buffer = ops2.reverse ::: buffer, patch2)
  }

  def ++ (ops: List[PipelineOp]): MergePatchError \/ PipelineBuilder = {
    type EitherE[X] = MergePatchError \/ X

    ops.foldLeftM[EitherE, PipelineBuilder](this) {
      case (pipe, op) => pipe + op
    }
  }

  def patch(patch2: MergePatch)(f: (MergePatch, MergePatch) => MergePatch): PipelineBuilder = copy(patch = f(patch, patch2))

  def patchSeq(patch2: MergePatch) = patch(patch2)(_ >> _)

  def patchPar(patch2: MergePatch) = patch(patch2)(_ && _)

  def fby(that: PipelineBuilder): MergePatchError \/ PipelineBuilder = {
    for {
      t <- patch.applyAll(that.buffer.reverse)

      (thatOps2, thisPatch2) = t
    } yield PipelineBuilder(thatOps2.reverse ::: this.buffer, thisPatch2 >> that.patch)
  }

  def merge0(that: PipelineBuilder): MergePatchError \/ (PipelineBuilder, MergePatch, MergePatch) = {
    for {
      t <- PipelineMerge.mergeOps(Nil, this.buffer.reverse, this.patch, that.buffer.reverse, that.patch).leftMap(MergePatchError.Pipeline.apply)

      (ops, lp, rp) = t
    } yield (PipelineBuilder(ops.reverse, lp && rp), lp, rp)
  }

  def merge(that: PipelineBuilder): MergePatchError \/ PipelineBuilder = merge0(that).map(_._1)
}
object PipelineBuilder {
  def empty = PipelineBuilder(Nil, MergePatch.Id)

  def apply(p: PipelineOp): PipelineBuilder = PipelineBuilder(p :: Nil, MergePatch.Id)

  implicit def PipelineBuilderRenderTree(implicit RO: RenderTree[PipelineOp]) = new RenderTree[PipelineBuilder] {
    override def render(v: PipelineBuilder) = NonTerminal("PipelineBuilder", v.buffer.reverse.map(RO.render(_)))
  }
}
