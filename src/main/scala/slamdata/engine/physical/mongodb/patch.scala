package slamdata.engine.physical.mongodb

import slamdata.engine.Error

import com.mongodb.DBObject

import scalaz._
import Scalaz._

import slamdata.engine.{RenderTree, Terminal, NonTerminal}
import slamdata.engine.fp._

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

  private def genApply(op0: PipelineOp)(applyVar: PartialFunction[DocVar, DocVar]): MergePatchError \/ (List[PipelineOp], MergePatch) = {
    val op = op0.rewriteRefs(applyVar)

    \/- (op0 match {
      case Project(_)   => (op :: Nil) -> Id // Patch is all consumed
      case Group(_, _)  => (op :: Nil) -> Id // Patch is all consumed

      case _ => (op :: Nil) -> this
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

        merged0 <- PipelineMerge.mergeOps(Nil, lops, lp2, rops, rp2).leftMap(MergePatchError.Pipeline.apply)

        (merged, lp3, rp3) = merged0
      } yield (merged, lp3 && rp3)
    }
  }
}
