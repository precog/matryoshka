package slamdata.engine.physical.mongodb

import scalaz._
import Scalaz._

import slamdata.engine.{RenderTree, Terminal, NonTerminal, Error}
import slamdata.engine.fp._

case class ExprPointer(schema: PipelineSchema, ref: ExprOp.DocVar)

/**
 * A `PipelineBuilder` consists of a list of pipeline operations in *reverse*
 * order and a patch to be applied to all subsequent additions to the pipeline.
 *
 * This abstraction represents a work-in-progress, where more operations are 
 * expected to be patched and added to the pipeline. At some point, the `build`
 * method will be invoked to convert the `PipelineBuilder` into a `Pipeline`.
 */
final case class PipelineBuilder private (buffer: List[PipelineOp], patch: MergePatch) {
  import PipelineBuilder._
  import PipelineOp._
  import ExprOp.{DocVar}

  def build: Pipeline = Pipeline(MoveRootToField :: buffer.reverse)

  def schema: PipelineSchema = PipelineSchema(buffer.reverse)

  def unify(that: PipelineBuilder)(f: (ExprPointer, ExprPointer) => Error \/ (ExprOp \/ PipelineBuilder)): Error \/ PipelineBuilder = {
    def up[A <: Error](a: A): Error = a

    val LeftName  = BsonField.Name("__sd_expr_left")
    val RightName = BsonField.Name("__sd_expr_right")

    for {
      left  <- (this.add0(Project(Reshape.Doc(Map(LeftName  -> -\/ (ExprVar)))))).leftMap(up)
      right <- (that.add0(Project(Reshape.Doc(Map(RightName -> -\/ (ExprVar)))))).leftMap(up)
      t     <- left.merge0(right).leftMap(up)

      (merged, lp, rp) = t

      leftVar  = lp.applyVar(DocVar.ROOT(LeftName))
      rightVar = rp.applyVar(DocVar.ROOT(RightName))

      e     <- f(ExprPointer(left.schema, leftVar), ExprPointer(right.schema, rightVar))

      p <- e.fold(
        e => PipelineBuilder(Project(Reshape.Doc(Map(ExprName -> -\/ (e))))),
        p => p.add0(Project(Reshape.Doc(Map(ExprName -> -\/ (DocVar.ROOT()))))).leftMap(up)
      )

      r <- merged.fby(p).leftMap(up)
    } yield r
  }

  private def add0(op: PipelineOp): MergePatchError \/ PipelineBuilder = {
    for {
      t <- patch(op)

      (ops2, patch2) = t
    } yield copy(buffer = ops2.reverse ::: buffer, patch2)
  }

  def + (op: PipelineOp): MergePatchError \/ PipelineBuilder = {
    for {
      t   <- MoveToExprField(op)
      r   <- addAll0(t._1)
      r   <- if (op.isInstanceOf[ShapePreservingOp]) \/- (r) else r.add0(MoveRootToField)
    } yield r
  }

  def ++ (ops: List[PipelineOp]): MergePatchError \/ PipelineBuilder = {
    type EitherE[X] = MergePatchError \/ X

    ops.foldLeftM[EitherE, PipelineBuilder](this) {
      case (pipe, op) => pipe + op
    }
  }

  private def addAll0(ops: List[PipelineOp]): MergePatchError \/ PipelineBuilder = {
    type EitherE[X] = MergePatchError \/ X

    ops.foldLeftM[EitherE, PipelineBuilder](this) {
      case (pipe, op) => pipe.add0(op)
    }
  }

  /**
   * Absorbs the last pipeline op for which the partial function is defined.
   *
   * The specified builder will be merged into the history and its information
   * content available to the partial function (together with the patch for 
   * that side).
   */
  def absorbLast(that: PipelineBuilder)(f: PartialFunction[PipelineOp, MergePatch => PipelineOp]): MergePatchError \/ PipelineBuilder = {
    val rbuffer = buffer.reverse

    val index = (buffer.length - 1) - buffer.indexWhere(f.isDefinedAt _)

    if (index < 0) -\/ (MergePatchError.UnknownShape)
    else {
      val prefix = rbuffer.take(index)
      val op     = rbuffer(index)
      val suffix = rbuffer.drop(index + 1)

      for {
        t <- PipelineMerge.mergeOps(Nil, prefix, MergePatch.Id, that.buffer.reverse, MergePatch.Id).leftMap(MergePatchError.Pipeline.apply)

        (merged, lp, rp) = t

        t <- lp(op)

        (ops, lp) = t

        ops <-  ops.indexWhere(f.isDefinedAt _) match {
                  case i if (i < 0) => -\/ (MergePatchError.UnknownShape)
                  case i => 
                    val pre  = ops.take(i)
                    val op0  = ops(i)
                    val post = ops.drop(i + 1)

                    val op = f(op0)(rp)

                    \/- (pre ::: op :: post)
                }

        t <- lp.applyAll(suffix)

        (suffix, lp) = t
      } yield new PipelineBuilder((prefix ::: ops ::: suffix).reverse, this.patch >> lp)
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

  def merge0(that: PipelineBuilder): MergePatchError \/ (PipelineBuilder, MergePatch, MergePatch) = mergeCustom(that)(_ >> _)

  def mergeCustom(that: PipelineBuilder)(f: (MergePatch, MergePatch) => MergePatch): MergePatchError \/ (PipelineBuilder, MergePatch, MergePatch) = {
    for {
      t <- PipelineMerge.mergeOps(Nil, this.buffer.reverse, this.patch, that.buffer.reverse, that.patch).leftMap(MergePatchError.Pipeline.apply)

      (ops, lp, rp) = t
    } yield (PipelineBuilder(ops.reverse, f(lp, rp)), lp, rp)
  }

  def merge(that: PipelineBuilder): MergePatchError \/ PipelineBuilder = merge0(that).map(_._1)
}
object PipelineBuilder {
  import PipelineOp._
  import ExprOp.{DocVar}

  private val ExprName = BsonField.Name("__sd_expr")
  private val ExprVar  = ExprOp.DocVar.ROOT(ExprName)

  private val MoveToExprField = MergePatch.Rename(ExprOp.DocVar.ROOT(), ExprVar)

  private val MoveRootToField = Project(Reshape.Doc(Map(ExprName -> -\/ (DocVar.ROOT()))))

  def empty = PipelineBuilder(Nil, MergePatch.Id)

  def apply(p: PipelineOp): MergePatchError \/ PipelineBuilder = empty + p

  implicit def PipelineBuilderRenderTree(implicit RO: RenderTree[PipelineOp]) = new RenderTree[PipelineBuilder] {
    override def render(v: PipelineBuilder) = NonTerminal("PipelineBuilder", v.buffer.reverse.map(RO.render(_)))
  }
}
