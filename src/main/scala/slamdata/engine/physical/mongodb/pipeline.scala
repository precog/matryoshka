package slamdata.engine.physical.mongodb

import slamdata.engine.Error

import com.mongodb.DBObject

import scalaz._
import Scalaz._

import slamdata.engine.{RenderTree, Terminal, NonTerminal}
import slamdata.engine.fp._

final case class Pipeline(ops: List[PipelineOp]) {
  def repr: java.util.List[DBObject] = ops.foldLeft(new java.util.ArrayList[DBObject](): java.util.List[DBObject]) {
    case (list, op) =>
      list.add(op.bson.repr)

      list
  }

  def reverse: Pipeline = copy(ops = ops.reverse)

  def merge(that: Pipeline): PipelineMergeError \/ Pipeline = mergeM[Free.Trampoline](that).run.map(_._1).map(Pipeline(_))

  private def mergeM[F[_]](that: Pipeline)(implicit F: Monad[F]): F[PipelineMergeError \/ (List[PipelineOp], MergePatch, MergePatch)] = {
    PipelineOp.mergeOpsM[F](Nil, this.ops, MergePatch.Id, that.ops, MergePatch.Id).run
  }
}
object Pipeline {
  implicit def PipelineRenderTree(implicit RO: RenderTree[PipelineOp]) = new RenderTree[Pipeline] {
    override def render(p: Pipeline) = NonTerminal("Pipeline", p.ops.map(RO.render(_)))
  }
}

sealed trait PipelineOp {
  def bson: Bson.Doc

  def isShapePreservingOp: Boolean = this match {
    case x : PipelineOp.ShapePreservingOp => true
    case _ => false
  }

  def isNotShapePreservingOp: Boolean = !isShapePreservingOp

  def commutesWith(that: PipelineOp): Boolean = false

  def merge(that: PipelineOp): PipelineOpMergeError \/ MergeResult

  private def delegateMerge(that: PipelineOp): PipelineOpMergeError \/ MergeResult = that.merge(this).map(_.flip)
  
  private def mergeThisFirst: PipelineOpMergeError \/ MergeResult                   = \/- (MergeResult.Left(this :: Nil))
  private def mergeThatFirst(that: PipelineOp): PipelineOpMergeError \/ MergeResult = \/- (MergeResult.Right(that :: Nil))
  private def mergeThisAndDropThat: PipelineOpMergeError \/ MergeResult             = \/- (MergeResult.Both(this :: Nil))
  private def error(left: PipelineOp, right: PipelineOp, msg: String)               = -\/ (PipelineOpMergeError(left, right, Some(msg)))
}

object PipelineOp {
  def mergeOps(merged: List[PipelineOp], left: List[PipelineOp], lp0: MergePatch, right: List[PipelineOp], rp0: MergePatch): PipelineMergeError \/ (List[PipelineOp], MergePatch, MergePatch) = {
    mergeOpsM[Free.Trampoline](merged, left, lp0, right, rp0).run.run
  }

  private[PipelineOp] case class Patched(ops: List[PipelineOp], unpatch: PipelineOp) {
    def decon: Option[(PipelineOp, Patched)] = ops.headOption.map(head => head -> copy(ops = ops.tail))
  }

  private[PipelineOp] case class MergeState(patched0: Option[Patched], unpatched: List[PipelineOp], patch: MergePatch, schema: PipelineSchema) {
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
          mr <- lh.merge(rh).leftMap(MergePatchError.Op.apply)
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

  private def mergeGroupOnRight(field: ExprOp.DocVar)(right: Group): PipelineOpMergeError \/ MergeResult = {
    val Group(Grouped(m), b) = right

    // FIXME: Verify logic & test!!!
    val tmpName = BsonField.genUniqName(m.keys.map(_.toName))
    val tmpField = ExprOp.DocField(tmpName)

    val right2 = Group(Grouped(m + (tmpName -> ExprOp.Push(field))), b)
    val leftPatch = MergePatch.Rename(field, tmpField)

    \/- (MergeResult.Right(right2 :: Unwind(tmpField) :: Nil, leftPatch, MergePatch.Id))
  }

  sealed trait ShapePreservingOp extends PipelineOp

  private def mergeGroupOnLeft(field: ExprOp.DocVar)(left: Group): PipelineOpMergeError \/ MergeResult = {
    mergeGroupOnRight(field)(left).map(_.flip)
  }

  implicit def PiplineOpRenderTree(implicit RG: RenderTree[Grouped], RS: RenderTree[Selector])  = new RenderTree[PipelineOp] {
    def render(op: PipelineOp) = op match {
      case Project(Reshape.Doc(map)) => NonTerminal("Project", (map.map { case (name, x) => Terminal(name + " -> " + x) } ).toList)
      case Project(Reshape.Arr(map)) => NonTerminal("Project", (map.map { case (index, x) => Terminal(index + " -> " + x) } ).toList)
      case Group(grouped, by)        => NonTerminal("Group", RG.render(grouped) :: Terminal(by.toString) :: Nil)
      case Match(selector)           => NonTerminal("Match", RS.render(selector) :: Nil)
      case Sort(keys)                => NonTerminal("Sort", (keys.map { case (expr, ot) => Terminal(expr + " -> " + ot) } ).toList)
      case _                         => Terminal(op.toString)
    }
  }
  
  implicit def GroupedRenderTree = new RenderTree[Grouped] {
    def render(grouped: Grouped) = NonTerminal("Grouped", (grouped.value.map { case (name, expr) => Terminal(name + " -> " + expr) } ).toList)
  }
  
  private[PipelineOp] abstract sealed class SimpleOp(op: String) extends PipelineOp {
    def rhs: Bson

    def bson = Bson.Doc(Map(op -> rhs))
  }

  sealed trait Reshape {
    def toDoc: Reshape.Doc

    def bson: Bson.Doc

    def schema: PipelineSchema.Succ

    def nestField(name: String): Reshape.Doc = Reshape.Doc(Map(BsonField.Name(name) -> \/-(this)))

    def nestIndex(index: Int): Reshape.Arr = Reshape.Arr(Map(BsonField.Index(index) -> \/-(this)))

    def ++ (that: Reshape): Reshape = {
      implicit val sg = Semigroup.lastSemigroup[ExprOp \/ Reshape]

      (this, that) match {
        case (Reshape.Arr(m1), Reshape.Arr(m2)) => Reshape.Arr(m1 |+| m2)

        case (r1_, r2_) => 
          val r1 = r1_.toDoc 
          val r2 = r2_.toDoc

          Reshape.Doc(r1.value |+| r2.value)
      }
    }

    def merge(that: Reshape, path: ExprOp.DocVar): (Reshape, MergePatch) = {
      def mergeDocShapes(leftShape: Reshape.Doc, rightShape: Reshape.Doc, path: ExprOp.DocVar): (Reshape, MergePatch) = {
        rightShape.value.foldLeft((leftShape, MergePatch.Id: MergePatch)) { 
          case ((Reshape.Doc(shape), patch), (name, right)) => 
            shape.get(name) match {
              case None => (Reshape.Doc(shape + (name -> right)), patch)
      
              case Some(left) => (left, right) match {
                case (l, r) if (r == l) =>
                  (Reshape.Doc(shape), patch)
                
                case (\/- (l), \/- (r)) =>
                  val (mergedShape, innerPatch) = l.merge(r, path \ name)
                  (Reshape.Doc(shape + (name -> \/- (mergedShape))), innerPatch) 

                case _ =>
                  val tmpName = BsonField.genUniqName(shape.keySet)
                  (Reshape.Doc(shape + (tmpName -> right)), patch >> MergePatch.Rename(path \ name, (path \ tmpName)))
              }
            }
        }
      }

      def mergeArrShapes(leftShape: Reshape.Arr, rightShape: Reshape.Arr, path: ExprOp.DocVar): (Reshape.Arr, MergePatch) = {
        rightShape.value.foldLeft((leftShape, MergePatch.Id: MergePatch)) { 
          case ((Reshape.Arr(shape), patch), (name, right)) => 
            shape.get(name) match {
              case None => (Reshape.Arr(shape + (name -> right)), patch)
      
              case Some(left) => (left, right) match {
                case (l, r) if (r == l) =>
                  (Reshape.Arr(shape), patch)
                
                case (\/- (l), \/- (r)) =>
                  val (mergedShape, innerPatch) = l.merge(r, path \ name)
                  (Reshape.Arr(shape + (name -> \/- (mergedShape))), innerPatch) 

                case _ =>
                  val tmpName = BsonField.genUniqIndex(shape.keySet)
                  (Reshape.Arr(shape + (tmpName -> right)), patch >> MergePatch.Rename(path \ name, (path \ tmpName)))
              }
            }
        }
      }

      (this, that) match {
        case (r1 @ Reshape.Arr(_), r2 @ Reshape.Arr(_)) => mergeArrShapes(r1, r2, path)
        case (r1 @ Reshape.Doc(_), r2 @ Reshape.Doc(_)) => mergeDocShapes(r1, r2, path)
        case (r1, r2) => mergeDocShapes(r1.toDoc, r2.toDoc, path)
      }
    }
  }

  object Reshape {
    def unapply(v: Reshape): Option[Reshape] = Some(v)
    
    case class Doc(value: Map[BsonField.Name, ExprOp \/ Reshape]) extends Reshape {
      def schema: PipelineSchema.Succ = PipelineSchema.Succ(value.map {
        case (n, v) => (n: BsonField.Leaf) -> v.fold(_ => -\/ (()), r => \/-(r.schema))
      })

      def bson: Bson.Doc = Bson.Doc(value.map {
        case (field, either) => field.asText -> either.fold(_.bson, _.bson)
      })

      def toDoc = this
    }
    case class Arr(value: Map[BsonField.Index, ExprOp \/ Reshape]) extends Reshape {      
      def schema: PipelineSchema.Succ = PipelineSchema.Succ(value.map {
        case (n, v) => (n: BsonField.Leaf) -> v.fold(_ => -\/ (()), r => \/-(r.schema))
      })

      def bson: Bson.Doc = Bson.Doc(value.map {
        case (field, either) => field.asText -> either.fold(_.bson, _.bson)
      })

      def minIndex: Option[Int] = {
        val keys = value.keys

        keys.headOption.map(_ => keys.map(_.value).min)
      }

      def maxIndex: Option[Int] = {
        val keys = value.keys

        keys.headOption.map(_ => keys.map(_.value).max)
      }

      def offset(i0: Int) = Reshape.Arr(value.map {
        case (BsonField.Index(i), v) => BsonField.Index(i0 + i) -> v
      })

      def toDoc: Doc = Doc(value.map(t => t._1.toName -> t._2))

      // def flatten: (Map[BsonField.Index, ExprOp], Reshape.Arr)
    }

    implicit val ReshapeMonoid = new Monoid[Reshape] {
      def zero = Reshape.Arr(Map())

      def append(v10: Reshape, v20: => Reshape): Reshape = {
        val v1 = v10.toDoc
        val v2 = v20.toDoc

        val m1 = v1.value
        val m2 = v2.value
        val keys = m1.keySet ++ m2.keySet

        Reshape.Doc(keys.foldLeft(Map.empty[BsonField.Name, ExprOp \/ Reshape]) {
          case (map, key) =>
            val left  = m1.get(key)
            val right = m2.get(key)

            val result = ((left |@| right) {
              case (-\/(e1), -\/(e2)) => -\/ (e2)
              case (-\/(e1), \/-(r2)) => \/- (r2)
              case (\/-(r1), \/-(r2)) => \/- (append(r1, r2))
              case (\/-(r1), -\/(e2)) => -\/ (e2)
            }) orElse (left) orElse (right)

            map + (key -> result.get)
        })
      }
    }
  }

  case class Grouped(value: Map[BsonField.Leaf, ExprOp.GroupOp]) {
    def schema: PipelineSchema.Succ = PipelineSchema.Succ(value.mapValues(_ => -\/(())))

    def bson = Bson.Doc(value.map(t => t._1.asText -> t._2.bson))
  }
  case class Project(shape: Reshape) extends SimpleOp("$project") {
    def rhs = shape.bson

    def schema: PipelineSchema = shape.schema

    def id: Project = {
      def loop(prefix: Option[BsonField], p: Project): Project = {
        def nest(child: BsonField): BsonField = prefix.map(_ \ child).getOrElse(child)

        Project(p.shape match {
          case Reshape.Doc(m) => 
            Reshape.Doc(
              m.transform {
                case (k, v) =>
                  v.fold(
                    _ => -\/  (ExprOp.DocVar.ROOT(nest(k))),
                    r =>  \/- (loop(Some(nest(k)), Project(r)).shape)
                  )
              }
            )

          case Reshape.Arr(m) =>
            Reshape.Arr(
              m.transform {
                case (k, v) =>
                  v.fold(
                    _ => -\/  (ExprOp.DocVar.ROOT(nest(k))),
                    r =>  \/- (loop(Some(nest(k)), Project(r)).shape)
                  )
              }
            )
        })
      }

      loop(None, this)
    }

    def nestField(name: String): Project = Project(shape.nestField(name))

    def nestIndex(index: Int): Project = Project(shape.nestIndex(index))

    def ++ (that: Project): Project = Project(this.shape ++ that.shape)

    def merge(that: PipelineOp): PipelineOpMergeError \/ MergeResult = {
      that match {
        case Project(shape2) => 
          val (merged, patch) = this.shape.merge(shape2, ExprOp.DocVar.ROOT())

          \/- (MergeResult.Both(Project(merged) :: Nil, MergePatch.Id, patch))
          
        case Match(_)           => mergeThatFirst(that)
        case Redact(_)          => mergeThatFirst(that)
        case Limit(_)           => mergeThatFirst(that)
        case Skip(_)            => mergeThatFirst(that)
        case Unwind(_)          => mergeThatFirst(that) // TODO:
        case that @ Group(_, _) => mergeGroupOnRight(ExprOp.DocVar.ROOT())(that)
        case Sort(_)            => mergeThatFirst(that)
        case Out(_)             => mergeThisFirst
        case _: GeoNear         => mergeThatFirst(that)
      }
    }

    def field(name: String): Option[ExprOp \/ Project] = shape match {
      case Reshape.Doc(m) => m.get(BsonField.Name(name)).map { _ match {
          case e @ -\/(_) => e
          case     \/-(r) => \/- (Project(r))
        }
      }

      case _ => None
    }

    def index(idx: Int): Option[ExprOp \/ Project] = shape match {
      case Reshape.Arr(m) => m.get(BsonField.Index(idx)).map { _ match {
          case e @ -\/(_) => e
          case     \/-(r) => \/- (Project(r))
        }
      }

      case _ => None
    }
  }
  case class Match(selector: Selector) extends SimpleOp("$match") with ShapePreservingOp {
    def rhs = selector.bson

    private def mergeSelector(that: Match): Selector = 
      Selector.SelectorAndSemigroup.append(selector, that.selector)

    def merge(that: PipelineOp): PipelineOpMergeError \/ MergeResult = that match {
      case that: Match => \/- (MergeResult.Both(Match(mergeSelector(that)) :: Nil))
      case Redact(_)   => mergeThisFirst
      case Limit(_)    => mergeThatFirst(that)
      case Skip(_)     => mergeThatFirst(that)
      case Unwind(_)   => mergeThisFirst
      case Group(_, _) => mergeThisFirst   // FIXME: Verify logic & test!!!
      case Sort(_)     => mergeThisFirst
      case Out(_)      => mergeThisFirst
      case _: GeoNear  => mergeThatFirst(that)

      case _ => delegateMerge(that)
    }
  }
  case class Redact(value: ExprOp) extends SimpleOp("$redact") {
    def rhs = value.bson

    private def fields: List[ExprOp.DocVar] = {
      import scalaz.std.list._

      ExprOp.foldMap({
        case f : ExprOp.DocVar => f :: Nil
      })(value)
    }

    def merge(that: PipelineOp): PipelineOpMergeError \/ MergeResult = that match {
      case Redact(_) if (this == that) => mergeThisAndDropThat
      case Redact(_)                   => error(this, that, "Cannot merge multiple $redact ops") // FIXME: Verify logic & test!!!

      case Limit(_)                    => mergeThatFirst(that)
      case Skip(_)                     => mergeThatFirst(that)
      case Unwind(field) if (fields.contains(field))
                                       => error(this, that, "Cannot merge $redact with $unwind--condition refers to the field being unwound")
      case Unwind(_)                   => mergeThisFirst
      case that @ Group(_, _)          => mergeGroupOnRight(ExprOp.DocVar.ROOT())(that) // FIXME: Verify logic & test!!!
      case Sort(_)                     => mergeThatFirst(that)
      case Out(_)                      => mergeThisFirst
      case _: GeoNear                  => mergeThatFirst(that)
      
      case _ => delegateMerge(that)
    }
  }

  object Redact {
    val DESCEND = ExprOp.DocVar(ExprOp.DocVar.Name("DESCEND"),  None)
    val PRUNE   = ExprOp.DocVar(ExprOp.DocVar.Name("PRUNE"),    None)
    val KEEP    = ExprOp.DocVar(ExprOp.DocVar.Name("KEEP"),     None)
  }
  
case class Limit(value: Long) extends SimpleOp("$limit") with ShapePreservingOp {
    def rhs = Bson.Int64(value)

    def merge(that: PipelineOp): PipelineOpMergeError \/ MergeResult = that match {
      case Limit(value) => \/- (MergeResult.Both(Limit(this.value max value) :: nil))
      case Skip(value)  => \/- (MergeResult.Both(Limit((this.value - value) max 0) :: that :: nil))
      case Unwind(_)   => mergeThisFirst
      case Group(_, _) => mergeThisFirst // FIXME: Verify logic & test!!!
      case Sort(_)     => mergeThisFirst
      case Out(_)      => mergeThisFirst
      case _: GeoNear  => mergeThatFirst(that)
      
      case _ => delegateMerge(that)
    }
  }
  case class Skip(value: Long) extends SimpleOp("$skip") with ShapePreservingOp {
    def rhs = Bson.Int64(value)

    def merge(that: PipelineOp): PipelineOpMergeError \/ MergeResult = that match {
      case Skip(value) => \/- (MergeResult.Both(Skip(this.value min value) :: nil))
      case Unwind(_)   => mergeThisFirst
      case Group(_, _) => mergeThisFirst // FIXME: Verify logic & test!!!
      case Sort(_)     => mergeThisFirst
      case Out(_)      => mergeThisFirst
      case _: GeoNear  => mergeThatFirst(that)
      
      case _ => delegateMerge(that)
    }
  }
  case class Unwind(field: ExprOp.DocVar) extends SimpleOp("$unwind") {
    def rhs = Bson.Text(field.field.asField)

    def merge(that: PipelineOp): PipelineOpMergeError \/ MergeResult = that match {
      case Unwind(_)     if (this == that)                                 => mergeThisAndDropThat
      case Unwind(field) if (this.field.field.asText < field.field.asText) => mergeThisFirst 
      case Unwind(_)                                                       => mergeThatFirst(that)
      
      case that @ Group(_, _) => mergeGroupOnRight(this.field)(that)   // FIXME: Verify logic & test!!!

      case Sort(_)            => mergeThatFirst(that)
      case Out(_)             => mergeThisFirst
      case _: GeoNear         => mergeThatFirst(that)
      
      case _ => delegateMerge(that)
    }
  }
  case class Group(grouped: Grouped, by: ExprOp \/ Reshape) extends SimpleOp("$group") {
    def schema: PipelineSchema = grouped.schema

    def rhs = {
      val Bson.Doc(m) = grouped.bson

      Bson.Doc(m + ("_id" -> by.fold(_.bson, _.bson)))
    }

    def merge(that: PipelineOp): PipelineOpMergeError \/ MergeResult = that match {
      case that @ Group(grouped2, by2) => 
        if (this.hashCode <= that.hashCode) {
           // FIXME: Verify logic & test!!!
          val leftKeys = grouped.value.keySet
          val rightKeys = grouped2.value.keySet

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

          val mergedGroup = Grouped(grouped.value ++ grouped2.value.map {
            case (k, v) => nameMap.get(k).getOrElse(k) -> v
          })

          val mergedBy = if (by == by2) by else \/-(Reshape.Arr(Map(leftByName -> by, rightByName -> by2)))

          val merged = Group(mergedGroup, mergedBy)

          \/- (MergeResult.Both(merged :: Nil, MergePatch.Id, rightRenamePatch))
        } else delegateMerge(that)

      case Sort(_)     => mergeGroupOnLeft(ExprOp.DocVar.ROOT())(this) // FIXME: Verify logic & test!!!
      case Out(_)      => mergeThisFirst
      case _: GeoNear  => mergeThatFirst(that)
      
      case _ => delegateMerge(that)
    }
  }
  case class Sort(value: NonEmptyList[(BsonField, SortType)]) extends SimpleOp("$sort") with ShapePreservingOp {
    // Note: Map doesn't in general preserve the order of entries, which means we need a different representation for Bson.Doc.
    def rhs = Bson.Doc(Map((value.map { case (k, t) => k.asText -> t.bson }).list: _*))

    def merge(that: PipelineOp): PipelineOpMergeError \/ MergeResult = that match {
      case Sort(_) if (this == that) => mergeThisAndDropThat
      case Sort(_)                   => error(this, that, "Cannot merge multiple $sort ops")
      case Out(_)                    => mergeThisFirst
      case _: GeoNear                => mergeThatFirst(that)
      
      case _ => delegateMerge(that)
    }
  }
  case class Out(collection: Collection) extends SimpleOp("$out") with ShapePreservingOp {
    def rhs = Bson.Text(collection.name)

    def merge(that: PipelineOp): PipelineOpMergeError \/ MergeResult = that match {
      case Out(_) if (this == that) => mergeThisAndDropThat
      case Out(_)                   => error(this, that, "Cannot merge multiple $out ops")

      case _: GeoNear               => mergeThatFirst(that)
      
      case _ => delegateMerge(that)
    }
  }
  case class GeoNear(near: (Double, Double), distanceField: BsonField, 
                     limit: Option[Int], maxDistance: Option[Double],
                     query: Option[FindQuery], spherical: Option[Boolean],
                     distanceMultiplier: Option[Double], includeLocs: Option[BsonField],
                     uniqueDocs: Option[Boolean]) extends SimpleOp("$geoNear") {
    def rhs = Bson.Doc(List(
      List("near"           -> Bson.Arr(Bson.Dec(near._1) :: Bson.Dec(near._2) :: Nil)),
      List("distanceField"  -> distanceField.bson),
      limit.toList.map(limit => "limit" -> Bson.Int32(limit)),
      maxDistance.toList.map(maxDistance => "maxDistance" -> Bson.Dec(maxDistance)),
      query.toList.map(query => "query" -> query.bson),
      spherical.toList.map(spherical => "spherical" -> Bson.Bool(spherical)),
      distanceMultiplier.toList.map(distanceMultiplier => "distanceMultiplier" -> Bson.Dec(distanceMultiplier)),
      includeLocs.toList.map(includeLocs => "includeLocs" -> includeLocs.bson),
      uniqueDocs.toList.map(uniqueDocs => "uniqueDocs" -> Bson.Bool(uniqueDocs))
    ).flatten.toMap)

    def merge(that: PipelineOp): PipelineOpMergeError \/ MergeResult = that match {
      case _: GeoNear if (this == that) => mergeThisAndDropThat
      case _: GeoNear                   => error(this, that, "Cannot merge multiple $geoNear ops")
      
      case _ => delegateMerge(that)
    }
  }
}
