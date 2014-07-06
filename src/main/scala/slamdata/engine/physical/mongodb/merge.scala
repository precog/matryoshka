package slamdata.engine.physical.mongodb

import slamdata.engine.Error

import com.mongodb.DBObject

import scalaz._
import Scalaz._

import slamdata.engine.{RenderTree, Terminal, NonTerminal}
import slamdata.engine.fp._

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


private[mongodb] sealed trait MergePatch {
  import ExprOp.{And => EAnd, _}
  import PipelineOp._
  import MergePatch._

  /**
   * Combines the patches in sequence.
   */
  def >> (that: MergePatch): MergePatch = (this, that) match {
    case (left, Id) => left
    case (Id, right) => right

    case (x, y) => Then(x, y)
  }

  /**
   * Combines the patches in parallel.
   */
  def && (that: MergePatch): MergePatch = (this, that) match {
    case (x, y) if x == y => x

    case (x, y) => And(x, y)
  }

  def flatten: List[MergePatch.PrimitiveMergePatch]

  def applyVar(v: DocVar): DocVar

  def apply(op: PipelineOp): MergePatchError \/ (List[PipelineOp], MergePatch)

  private def genApply(op: PipelineOp)(applyVar0: PartialFunction[DocVar, DocVar]): MergePatchError \/ (List[PipelineOp], MergePatch) = {
    val applyVar = (f: DocVar) => applyVar0.lift(f).getOrElse(f)

    def applyExprOp(e: ExprOp): ExprOp = e.mapUp {
      case f : DocVar => applyVar(f)
    }

    def applyFieldName(name: BsonField): BsonField = {
      applyVar(DocField(name)).deref.getOrElse(name) // TODO: Delete field if it's transformed away to nothing???
    }

    def applySelector(s: Selector): Selector = s.mapUpFields(PartialFunction(applyFieldName _))

    def applyReshape(shape: Reshape): Reshape = shape match {
      case Reshape.Doc(value) => Reshape.Doc(value.transform {
        case (k, -\/(e)) => -\/(applyExprOp(e))
        case (k, \/-(r)) => \/-(applyReshape(r))
      })

      case Reshape.Arr(value) => Reshape.Arr(value.transform {
        case (k, -\/(e)) => -\/(applyExprOp(e))
        case (k, \/-(r)) => \/-(applyReshape(r))
      })
    }

    def applyGrouped(grouped: Grouped): Grouped = Grouped(grouped.value.transform {
      case (k, groupOp) => applyExprOp(groupOp) match {
        case groupOp : GroupOp => groupOp
        case _ => sys.error("Transformation changed the type -- error!")
      }
    })

    def applyMap[A](m: Map[BsonField, A]): Map[BsonField, A] = m.map(t => applyFieldName(t._1) -> t._2)

    def applyNel[A](m: NonEmptyList[(BsonField, A)]): NonEmptyList[(BsonField, A)] = m.map(t => applyFieldName(t._1) -> t._2)

    def applyFindQuery(q: FindQuery): FindQuery = {
      q.copy(
        query   = applySelector(q.query),
        max     = q.max.map(applyMap _),
        min     = q.min.map(applyMap _),
        orderby = q.orderby.map(applyNel _)
      )
    }

    \/- (op match {
      case Project(shape)     => (Project(applyReshape(shape)) :: Nil) -> Id // Patch is all consumed
      case Group(grouped, by) => (Group(applyGrouped(grouped), by.bimap(applyExprOp _, applyReshape _)) :: Nil) -> Id // Patch is all consumed
    
      case Match(s)       => (Match(applySelector(s)) :: Nil)  -> this // Patch is not consumed
      case Redact(e)      => (Redact(applyExprOp(e)) :: Nil)   -> this
      case v @ Limit(_)   => (v :: Nil)                        -> this
      case v @ Skip(_)    => (v :: Nil)                        -> this
      case v @ Unwind(f)  => (Unwind(applyVar(f)) :: Nil)      -> this
      case v @ Sort(l)    => (Sort(applyNel(l)) :: Nil)        -> this
      case v @ Out(_)     => (v :: Nil)                        -> this
      case g : GeoNear    => (g.copy(distanceField = applyFieldName(g.distanceField), query = g.query.map(applyFindQuery _)) :: Nil) -> this
    })
  }

  def applyAll(l: List[PipelineOp]): MergePatchError \/ (List[PipelineOp], MergePatch) = {
    type ErrorM[X] = MergePatchError \/ X

    (l.headOption map { h0 =>
      for {
        t <- this(h0)

        (hs, patch) = t

        t <-  (l.tail.foldLeftM[ErrorM, (List[PipelineOp], MergePatch)]((hs, patch)) {
                case ((acc, patch), op0) =>
                  for {
                    t <- patch.apply(op0)

                    (ops, patch2) = t
                  } yield (ops.reverse ::: acc) -> patch2
              })

        (acc, patch3) = t
      } yield (acc.reverse, patch3)
    }).getOrElse(\/-(l -> MergePatch.Id))
  }
}
object MergePatch {
  import ExprOp._

  implicit val MergePatchShow = new Show[MergePatch] {
    override def show(v: MergePatch): Cord = Cord(v.toString)
  }

  sealed trait PrimitiveMergePatch extends MergePatch
  case object Id extends PrimitiveMergePatch {
    def flatten: List[PrimitiveMergePatch] = this :: Nil

    def applyVar(v: DocVar): DocVar = v

    def apply(op: PipelineOp): MergePatchError \/ (List[PipelineOp], MergePatch) = \/- ((op :: Nil) -> Id)
  }
  case class Rename(from: DocVar, to: DocVar) extends PrimitiveMergePatch {
    def flatten: List[PrimitiveMergePatch] = this :: Nil

    private lazy val flatFrom = from.deref.toList.flatMap(_.flatten)
    private lazy val flatTo   = to.deref.toList.flatMap(_.flatten)

    private def replaceMatchingPrefix(f: Option[BsonField]): Option[BsonField] = f match {
      case None => None

      case Some(f) =>
        val l = f.flatten

        BsonField(if (l.startsWith(flatFrom)) flatTo ::: l.drop(flatFrom.length) else l)
    }

    def applyVar(v: DocVar): DocVar = v match {
      case DocVar(name, deref) if (name == from.name) => DocVar(to.name, replaceMatchingPrefix(deref))
    }

    def apply(op: PipelineOp): MergePatchError \/ (List[PipelineOp], MergePatch) = genApply(op) {
      case DocVar(name, deref) if (name == from.name) => DocVar(to.name, replaceMatchingPrefix(deref))
    }
  }
  sealed trait CompositeMergePatch extends MergePatch

  case class Then(fst: MergePatch, snd: MergePatch) extends CompositeMergePatch {
    def flatten: List[PrimitiveMergePatch] = fst.flatten ++ snd.flatten

    def applyVar(v: DocVar): DocVar = snd.applyVar(fst.applyVar(v))

    def apply(op: PipelineOp): MergePatchError \/ (List[PipelineOp], MergePatch) = {
      for {
        t <- fst(op)
        
        (ops, patchfst) = t

        t <- snd.applyAll(ops)

        (ops, patchsnd) = t
      } yield (ops, patchfst >> patchsnd)
    }
  }

  case class And(left: MergePatch, right: MergePatch) extends CompositeMergePatch {
    def flatten: List[PrimitiveMergePatch] = left.flatten ++ right.flatten

    def applyVar(v: DocVar): DocVar = {
      val lv = left.applyVar(v)
      var rv = right.applyVar(v)

      if (lv == v) rv
      else if (rv == v) lv
      else if (lv.toString.compareTo(rv.toString) < 0) lv else rv
    }

    def apply(op: PipelineOp): MergePatchError \/ (List[PipelineOp], MergePatch) = {
      for {
        t <- left(op)

        (lops, lp2) = t

        t <- right(op)

        (rops, rp2) = t

        merged0 <- PipelineOp.mergeOps(Nil, lops, lp2, rops, rp2).leftMap(MergePatchError.Pipeline.apply)

        (merged, lp3, rp3) = merged0
      } yield (merged, lp3 && rp3)
    }
  }
}

private[mongodb] sealed trait MergeResult {
  import MergeResult._

  def ops: List[PipelineOp]

  def leftPatch: MergePatch

  def rightPatch: MergePatch  

  def flip: MergeResult = this match {
    case Left (v, lp, rp) => Right(v, rp, lp)
    case Right(v, lp, rp) => Left (v, rp, lp)
    case Both (v, lp, rp) => Both (v, rp, lp)
  }

  def tuple: (List[PipelineOp], MergePatch, MergePatch) = (ops, leftPatch, rightPatch)
}
private[mongodb] object MergeResult {
  case class Left (ops: List[PipelineOp], leftPatch: MergePatch = MergePatch.Id, rightPatch: MergePatch = MergePatch.Id) extends MergeResult
  case class Right(ops: List[PipelineOp], leftPatch: MergePatch = MergePatch.Id, rightPatch: MergePatch = MergePatch.Id) extends MergeResult
  case class Both (ops: List[PipelineOp], leftPatch: MergePatch = MergePatch.Id, rightPatch: MergePatch = MergePatch.Id) extends MergeResult
}

sealed trait PipelineSchema {
  def accum(op: PipelineOp): PipelineSchema = op match {
    case (p @ PipelineOp.Project(_)) => p.schema
    case (g @ PipelineOp.Group(_, _)) => g.schema
    case _ => this
  }
}
object PipelineSchema {
  import ExprOp.DocVar
  import PipelineOp.{Reshape, Project}

  def apply(ops: List[PipelineOp]): PipelineSchema = ops.foldLeft[PipelineSchema](Init)((s, o) => s.accum(o))

  case object Init extends PipelineSchema
  case class Succ(proj: Map[BsonField.Leaf, Unit \/ Succ]) extends PipelineSchema {
    private def toProject0(prefix: DocVar, s: Succ): Project = {
      def rec[A <: BsonField.Leaf](prefix: DocVar, x: A, y: Unit \/ Succ): (A, ExprOp \/ Reshape) = {
        x -> y.fold(_ => -\/ (prefix), s => \/- (s.toProject0(prefix, s).shape))
      }

      val indices = proj.collect {
        case (x : BsonField.Index, y) => 
          rec(prefix \ x, x, y)
      }

      val fields = proj.collect {
        case (x : BsonField.Name, y) =>
          rec(prefix \ x, x, y)
      }

      val arr = Reshape.Arr(indices)
      val doc = Reshape.Doc(fields)

      Project(arr ++ doc)
    }

    def toProject: Project = toProject0(DocVar.ROOT(), this)
  }
}

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
      t <- PipelineOp.mergeOps(Nil, this.buffer.reverse, this.patch, that.buffer.reverse, that.patch).leftMap(MergePatchError.Pipeline.apply)

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

