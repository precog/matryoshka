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

        merged0 <- PipelineMerge.mergeOps(Nil, lops, lp2, rops, rp2).leftMap(MergePatchError.Pipeline.apply)

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

object PipelineMerge {
  import PipelineOp._
  
  private[PipelineMerge] case class Patched(ops: List[PipelineOp], unpatch: PipelineOp) {
    def decon: Option[(PipelineOp, Patched)] = ops.headOption.map(head => head -> copy(ops = ops.tail))
  }

  private[PipelineMerge] case class MergeState(patched0: Option[Patched], unpatched: List[PipelineOp], patch: MergePatch, schema: PipelineSchema) {
    def isEmpty: Boolean = patched0.map(_.ops.isEmpty).getOrElse(true)

    /**
     * Refills the patched op, if possible.
     */
    def refill: MergePatchError \/ MergeState = {
      if (isEmpty) unpatched match {
        case Nil => \/- (this)

        case x :: xs => 
          for {
            t <- patch(x)

            (ops, patch) = t 

            r <- copy(patched0 = Some(Patched(ops, x)), unpatched = xs, patch = patch).refill
          } yield r
      } else \/- (this)
    }

    /**
     * Patches an extracts out a single pipeline op, if such is possible.
     */
    def patchedHead: MergePatchError \/ Option[(PipelineOp, MergeState)] = {
      for {
        side <- refill
      } yield {
        for {
          patched <- side.patched0
          t       <- patched.decon

          (head, patched) = t
        } yield head -> copy(patched0 = if (patched.ops.length == 0) None else Some(patched), schema = schema.accum(head))
      }
    }

    /**
     * Appends the specified patch to this patch, repatching already patched ops (if any).
     */
    def patch(patch2: MergePatch): MergePatchError \/ MergeState = {
      val mpatch = patch >> patch2

      // We have to not only concatenate this patch with the existing patch,
      // but apply this patch to all of the ops already patched.
      patched0.map { patched =>
        for {
          t <- patch2.applyAll(patched.ops)

          (ops, patch2) = t
        } yield copy(Some(patched.copy(ops = ops)), patch = mpatch)
      }.getOrElse(\/- (copy(patch = mpatch)))
    }

    def replacePatch(patch2: MergePatch): MergePatchError \/ MergeState = 
      patched0 match {
        case None => \/- (copy(patch = patch2))
        case _ => -\/ (MergePatchError.NonEmpty)
      }

    def step(that: MergeState): MergePatchError \/ Option[(List[PipelineOp], MergeState, MergeState)] = {
      def mergeHeads(lh: PipelineOp, ls: MergeState, lt: MergeState, rh: PipelineOp, rs: MergeState, rt: MergeState) = {
        def construct(hs: List[PipelineOp]) = (ls: MergeState, rs: MergeState) => Some((hs, ls, rs))

        // One on left & right, merge them:
        for {
          mr <- PipelineMerge.merge(lh, rh).leftMap(MergePatchError.Op.apply)
          r  <- mr match {
                  case MergeResult.Left (hs, lp2, rp2) => (lt.patch(lp2) |@| rs.patch(rp2))(construct(hs))
                  case MergeResult.Right(hs, lp2, rp2) => (ls.patch(lp2) |@| rt.patch(rp2))(construct(hs))
                  case MergeResult.Both (hs, lp2, rp2) => (lt.patch(lp2) |@| rt.patch(rp2))(construct(hs))
                }
        } yield r
      }
      
      def reconstructRight(left: PipelineSchema, right: PipelineSchema, rp: MergePatch): 
          MergePatchError \/ (List[PipelineOp], MergePatch) = {
        def patchUp(proj: Project, rp2: MergePatch) = for {
          t <- rp(proj)
          (proj, rp) = t
        } yield (proj, rp >> rp2)

        right match {
          case s @ PipelineSchema.Succ(_) => patchUp(s.toProject, MergePatch.Id)

          case PipelineSchema.Init => 
            val uniqueField = left match {
              case PipelineSchema.Init => BsonField.genUniqName(Nil)
              case PipelineSchema.Succ(m) => BsonField.genUniqName(m.keys.map(_.toName))
            }
            val proj = Project(Reshape.Doc(Map(uniqueField -> -\/ (ExprOp.DocVar.ROOT()))))

            patchUp(proj, MergePatch.Rename(ExprOp.DocVar.ROOT(), ExprOp.DocField(uniqueField)))
        }
      }
      
      for {
        ls <- this.refill
        rs <- that.refill

        t1 <- ls.patchedHead
        t2 <- rs.patchedHead

        r  <- (t1, t2) match {
                case (None, None) => \/- (None)

                case (Some((out @ Out(_), lt)), None) =>
                  \/- (Some((out :: Nil, lt, rs)))
                
                case (Some((lh, lt)), None) => 
                  for {
                    t <- reconstructRight(this.schema, that.schema, rs.patch)
                    (rhs, rp) = t
                    rh :: Nil = rhs  // HACK!
                    rt <- rs.replacePatch(rp)
                    r <- mergeHeads(lh, ls, lt, rh, rs, rt)
                  } yield r

                case (None, Some((out @ Out(_), rt))) =>
                  \/- (Some((out :: Nil, ls, rt)))
                
                case (None, Some((rh, rt))) => 
                  for {
                    t <- reconstructRight(that.schema, this.schema, ls.patch)
                    (lhs, lp) = t
                    lh :: Nil = lhs  // HACK!
                    lt <- ls.replacePatch(lp)
                    r <- mergeHeads(lh, ls, lt, rh, rs, rt)
                  } yield r

                case (Some((lh, lt)), Some((rh, rt))) => mergeHeads(lh, ls, lt, rh, rs, rt)
              }
      } yield r
    }

    def rest: List[PipelineOp] = patched0.toList.flatMap(_.unpatch :: Nil) ::: unpatched
  }

  def mergeOps(merged: List[PipelineOp], left: List[PipelineOp], lp0: MergePatch, right: List[PipelineOp], rp0: MergePatch): PipelineMergeError \/ (List[PipelineOp], MergePatch, MergePatch) = {
    mergeOpsM[Free.Trampoline](merged, left, lp0, right, rp0).run.run
  }

  def mergeOpsM[F[_]: Monad](merged: List[PipelineOp], left: List[PipelineOp], lp0: MergePatch, right: List[PipelineOp], rp0: MergePatch): 
        EitherT[F, PipelineMergeError, (List[PipelineOp], MergePatch, MergePatch)] = {

    type M[X] = EitherT[F, PipelineMergeError, X]    

    def succeed[A](a: A): M[A] = a.point[M]
    def fail[A](e: PipelineMergeError): M[A] = EitherT((-\/(e): \/[PipelineMergeError, A]).point[F])

    def mergeOpsM0(merged: List[PipelineOp], left: MergeState, right: MergeState): 
          EitherT[F, PipelineMergeError, (List[PipelineOp], MergePatch, MergePatch)] = {

      // FIXME: Loss of information here (.rest) in case one op was patched 
      //        into many and one or more of the many were merged. Need to make
      //        things atomic.
      val convertError = (e: MergePatchError) => PipelineMergeError(merged, left.rest, right.rest)

      for {
        t <-  left.step(right).leftMap(convertError).fold(fail _, succeed _)
        r <-  t match {
                case None => succeed((merged, left.patch, right.patch))
                case Some((ops, left, right)) => mergeOpsM0(ops.reverse ::: merged, left, right)
              }
      } yield r 
    }

    for {
      t <- mergeOpsM0(merged, MergeState(None, left, lp0, PipelineSchema.Init), MergeState(None, right, rp0, PipelineSchema.Init))
      (ops, lp, rp) = t
    } yield (ops.reverse, lp, rp)
  }

  private def merge(left: PipelineOp, right: PipelineOp): PipelineOpMergeError \/ MergeResult = {
    def delegateMerge: PipelineOpMergeError \/ MergeResult = merge(right, left).map(_.flip)
  
    def mergeLeftFirst: PipelineOpMergeError \/ MergeResult        = \/- (MergeResult.Left(left :: Nil))
    def mergeRightFirst: PipelineOpMergeError \/ MergeResult       = \/- (MergeResult.Right(right :: Nil))
    def mergeLeftAndDropRight: PipelineOpMergeError \/ MergeResult = \/- (MergeResult.Both(left :: Nil))
    def error(msg: String) = -\/ (PipelineOpMergeError(left, right, Some(msg)))

    (left, right) match {
      case (Project(lshape), Project(rshape)) => 
        val (merged, patch) = lshape.merge(rshape, ExprOp.DocVar.ROOT())

        \/- (MergeResult.Both(Project(merged) :: Nil, MergePatch.Id, patch))
        
      case (Project(_), Match(_))            => mergeRightFirst
      case (Project(_), Redact(_))           => mergeRightFirst
      case (Project(_), Limit(_))            => mergeRightFirst
      case (Project(_), Skip(_))             => mergeRightFirst
      case (Project(_), Unwind(_))           => mergeRightFirst // TODO:
      case (Project(_), right @ Group(_, _)) => mergeGroupOnRight(ExprOp.DocVar.ROOT())(right)
      case (Project(_), Sort(_))             => mergeRightFirst
      case (Project(_), Out(_))              => mergeLeftFirst
      case (Project(_), _: GeoNear)          => mergeRightFirst
      
      
      case (Match(lsel), Match(rsel)) => \/- (MergeResult.Both(Match(Selector.SelectorAndSemigroup.append(lsel, rsel)) :: Nil))
      case (Match(_), Redact(_))      => mergeLeftFirst
      case (Match(_), Limit(_))       => mergeRightFirst
      case (Match(_), Skip(_))        => mergeRightFirst
      case (Match(_), Unwind(_))      => mergeLeftFirst
      case (Match(_), Group(_, _))    => mergeLeftFirst   // FIXME: Verify logic & test!!!
      case (Match(_), Sort(_))        => mergeLeftFirst
      case (Match(_), Out(_))         => mergeLeftFirst
      case (Match(_), _: GeoNear)     => mergeRightFirst
      case (Match(_), _)              => delegateMerge


      case (Redact(_), Redact(_)) if (left == right)
                                            => mergeLeftAndDropRight
      case (Redact(_), Redact(_))           => error("Cannot merge multiple $redact ops") // FIXME: Verify logic & test!!!

      case (Redact(_), Limit(_))            => mergeRightFirst
      case (Redact(_), Skip(_))             => mergeRightFirst
      case (left @ Redact(_), Unwind(field)) if (left.fields.contains(field))
                                            => error("Cannot merge $redact with $unwind--condition refers to the field being unwound")
      case (Redact(_), Unwind(_))           => mergeLeftFirst
      case (Redact(_), right @ Group(_, _)) => mergeGroupOnRight(ExprOp.DocVar.ROOT())(right) // FIXME: Verify logic & test!!!
      case (Redact(_), Sort(_))             => mergeRightFirst
      case (Redact(_), Out(_))              => mergeLeftFirst
      case (Redact(_), _: GeoNear)          => mergeRightFirst
      case (Redact(_), _)                   => delegateMerge


      case (Limit(lvalue), Limit(rvalue)) => \/- (MergeResult.Both(Limit(lvalue max rvalue) :: nil))
      case (Limit(lvalue), Skip(rvalue))  => \/- (MergeResult.Both(Limit((lvalue - rvalue) max 0) :: right :: nil))
      case (Limit(_), Unwind(_))          => mergeLeftFirst
      case (Limit(_), Group(_, _))        => mergeLeftFirst // FIXME: Verify logic & test!!!
      case (Limit(_), Sort(_))            => mergeLeftFirst
      case (Limit(_), Out(_))             => mergeLeftFirst
      case (Limit(_), _: GeoNear)         => mergeRightFirst
      case (Limit(_), _)                  => delegateMerge


      case (Skip(lvalue), Skip(rvalue)) => \/- (MergeResult.Both(Skip(lvalue min rvalue) :: nil))
      case (Skip(_), Unwind(_))         => mergeLeftFirst
      case (Skip(_), Group(_, _))       => mergeLeftFirst // FIXME: Verify logic & test!!!
      case (Skip(_), Sort(_))           => mergeLeftFirst
      case (Skip(_), Out(_))            => mergeLeftFirst
      case (Skip(_), _: GeoNear)        => mergeRightFirst
      case (Skip(_), _)                 => delegateMerge


      case (Unwind(_), Unwind(_)) if (left == right) => mergeLeftAndDropRight
      case (Unwind(lfield), Unwind(rfield)) if (lfield.field.asText < rfield.field.asText) 
                                                     => mergeLeftFirst 
      case (Unwind(_), Unwind(_))                    => mergeRightFirst
      case (left @ Unwind(_), right @ Group(_, _))   => mergeGroupOnRight(left.field)(right)   // FIXME: Verify logic & test!!!
      case (Unwind(_), Sort(_))                      => mergeRightFirst
      case (Unwind(_), Out(_))                       => mergeLeftFirst
      case (Unwind(_), _: GeoNear)                   => mergeRightFirst
      case (Unwind(_), _)                            => delegateMerge


      case (left @ Group(lgrouped, lby), right @ Group(rgrouped, rby)) => 
        if (left.hashCode <= right.hashCode) {
           // FIXME: Verify logic & test!!!
          val leftKeys = lgrouped.value.keySet
          val rightKeys = rgrouped.value.keySet

          val allKeys = leftKeys union rightKeys

          val overlappingKeys = (leftKeys intersect rightKeys).toList
          val overlappingVars = overlappingKeys.map(ExprOp.DocField.apply)
          val newNames        = BsonField.genUniqNames(overlappingKeys.length, allKeys.map(_.toName))
          val newVars         = newNames.map(ExprOp.DocField.apply)

          val varMapping = overlappingVars.zip(newVars)
          val nameMap    = overlappingKeys.zip(newNames).toMap

          val rightRenamePatch = (varMapping.headOption.map { 
            case (old, new0) =>
              varMapping.tail.foldLeft[MergePatch](MergePatch.Rename(old, new0)) {
                case (patch, (old, new0)) => patch >> MergePatch.Rename(old, new0)
              }
          }).getOrElse(MergePatch.Id)

          val leftByName  = BsonField.Index(0)
          val rightByName = BsonField.Index(1)

          val mergedGroup = Grouped(lgrouped.value ++ rgrouped.value.map {
            case (k, v) => nameMap.get(k).getOrElse(k) -> v
          })

          val mergedBy = if (lby == rby) lby else \/-(Reshape.Arr(Map(leftByName -> lby, rightByName -> rby)))

          val merged = Group(mergedGroup, mergedBy)

          \/- (MergeResult.Both(merged :: Nil, MergePatch.Id, rightRenamePatch))
        } else delegateMerge

      case (left @ Group(_, _), Sort(_)) => mergeGroupOnLeft(ExprOp.DocVar.ROOT())(left) // FIXME: Verify logic & test!!!
      case (Group(_, _), Out(_))         => mergeLeftFirst
      case (Group(_, _), _: GeoNear)     => mergeRightFirst
      case (Group(_, _), _)              => delegateMerge


      case (left @ Sort(_), right @ Sort(_)) if (left == right) 
                                 => mergeLeftAndDropRight
      case (Sort(_), Sort(_))    => error("Cannot merge multiple $sort ops")
      case (Sort(_), Out(_))     => mergeLeftFirst
      case (Sort(_), _: GeoNear) => mergeRightFirst
      case (Sort(_), _)          => delegateMerge


      case (Out(_), Out(_)) if (left == right) => mergeLeftAndDropRight
      case (Out(_), Out(_))                    => error("Cannot merge multiple $out ops")
      case (Out(_), _: GeoNear)                => mergeRightFirst
      case (Out(_), _)                         => delegateMerge


      case (_: GeoNear, _: GeoNear) if (left == right) => mergeLeftAndDropRight
      case (_: GeoNear, _: GeoNear)                    => error("Cannot merge multiple $geoNear ops")
    
      case (_: GeoNear, _)                             => delegateMerge
    }
  }

  private def mergeGroupOnRight(field: ExprOp.DocVar)(right: Group): PipelineOpMergeError \/ MergeResult = {
    val Group(Grouped(m), b) = right

    // FIXME: Verify logic & test!!!
    val tmpName = BsonField.genUniqName(m.keys.map(_.toName))
    val tmpField = ExprOp.DocField(tmpName)

    val right2 = Group(Grouped(m + (tmpName -> ExprOp.Push(field))), b)
    val leftPatch = MergePatch.Rename(field, tmpField)

    \/- (MergeResult.Right(right2 :: Unwind(tmpField) :: Nil, leftPatch, MergePatch.Id))
  }

  private def mergeGroupOnLeft(field: ExprOp.DocVar)(left: Group): PipelineOpMergeError \/ MergeResult = {
    mergeGroupOnRight(field)(left).map(_.flip)
  }
}
