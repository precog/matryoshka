package slamdata.engine.physical.mongodb

import slamdata.engine.Error

import com.mongodb.DBObject

import collection.immutable.ListMap

import scalaz._
import Scalaz._

import slamdata.engine.{RenderTree, Terminal, NonTerminal}
import slamdata.engine.fp._

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
          mr <- PipelineMerge.mergeOps(lh, rh).leftMap(MergePatchError.Op.apply)
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
            val proj = Project(Reshape.Doc(ListMap(uniqueField -> -\/ (ExprOp.DocVar.ROOT()))))

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

  private def mergeOps(left: PipelineOp, right: PipelineOp): PipelineOpMergeError \/ MergeResult = {
    def delegateMerge: PipelineOpMergeError \/ MergeResult = mergeOps(right, left).map(_.flip)
  
    def mergeLeftFirst: PipelineOpMergeError \/ MergeResult        = \/- (MergeResult.Left(left :: Nil))
    def mergeRightFirst: PipelineOpMergeError \/ MergeResult       = \/- (MergeResult.Right(right :: Nil))
    def mergeLeftAndDropRight: PipelineOpMergeError \/ MergeResult = \/- (MergeResult.Both(left :: Nil))
    def error(msg: String) = -\/ (PipelineOpMergeError(left, right, Some(msg)))

    def mergeGroupOnRight(field: ExprOp.DocVar)(right: Group): PipelineOpMergeError \/ MergeResult = {
      val Group(Grouped(m), b) = right

      // FIXME: Verify logic & test!!!
      val tmpName = BsonField.genUniqName(m.keys.map(_.toName))
      val tmpField = ExprOp.DocField(tmpName)

      val right2 = Group(Grouped(m + (tmpName -> ExprOp.Push(field))), b)
      val leftPatch = MergePatch.Rename(field, tmpField)

      \/- (MergeResult.Right(right2 :: Unwind(tmpField) :: Nil, leftPatch, MergePatch.Id))
    }

    def mergeGroupOnLeft(field: ExprOp.DocVar)(left: Group): PipelineOpMergeError \/ MergeResult =
      mergeGroupOnRight(field)(left).map(_.flip)
    
    def mergeGroups(left: Group, right: Group) = {
       // FIXME: Verify logic & test!!!
      val leftKeys = left.grouped.value.keySet
      val rightKeys = right.grouped.value.keySet

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

      val mergedGroup = Grouped(left.grouped.value ++ right.grouped.value.map {
        case (k, v) => nameMap.get(k).getOrElse(k) -> v
      })

      val mergedBy = if (left.by == right.by) left.by else \/-(Reshape.Arr(ListMap(leftByName -> left.by, rightByName -> right.by)))

      val merged = Group(mergedGroup, mergedBy)

      \/- (MergeResult.Both(merged :: Nil, MergePatch.Id, rightRenamePatch))
    }

    def mergeShapes(left: Reshape, right: Reshape, path: ExprOp.DocVar): (Reshape, MergePatch) = {
      def mergeDocShapes(leftShape: Reshape.Doc, rightShape: Reshape.Doc, path: ExprOp.DocVar): (Reshape, MergePatch) = {
        rightShape.value.foldLeft((leftShape, MergePatch.Id: MergePatch)) { 
          case ((Reshape.Doc(shape), patch), (name, right)) => 
            shape.get(name) match {
              case None => (Reshape.Doc(shape + (name -> right)), patch)
    
              case Some(left) => (left, right) match {
                case (l, r) if (r == l) =>
                  (Reshape.Doc(shape), patch)
              
                case (\/- (l), \/- (r)) =>
                  val (mergedShape, innerPatch) = mergeShapes(l, r, path \ name)
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
                  val (mergedShape, innerPatch) = mergeShapes(l, r, path \ name)
                  (Reshape.Arr(shape + (name -> \/- (mergedShape))), innerPatch) 

                case _ =>
                  val tmpName = BsonField.genUniqIndex(shape.keySet)
                  (Reshape.Arr(shape + (tmpName -> right)), patch >> MergePatch.Rename(path \ name, (path \ tmpName)))
              }
            }
        }
      }

      (left, right) match {
        case (r1 @ Reshape.Arr(_), r2 @ Reshape.Arr(_)) => mergeArrShapes(r1, r2, path)
        case (r1 @ Reshape.Doc(_), r2 @ Reshape.Doc(_)) => mergeDocShapes(r1, r2, path)
        case (r1, r2) => mergeDocShapes(r1.toDoc, r2.toDoc, path)
      }
    }

    (left, right) match {
      case (Project(lshape), Project(rshape)) => 
        val (merged, patch) = mergeShapes(lshape, rshape, ExprOp.DocVar.ROOT())

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
      case (Unwind(lfield), Unwind(rfield)) if (lfield.toString < rfield.toString) 
                                                     => mergeLeftFirst 
      case (Unwind(_), Unwind(_))                    => mergeRightFirst
      case (left @ Unwind(_), right @ Group(_, _))   => mergeGroupOnRight(left.field)(right)   // FIXME: Verify logic & test!!!
      case (Unwind(_), Sort(_))                      => mergeRightFirst
      case (Unwind(_), Out(_))                       => mergeLeftFirst
      case (Unwind(_), _: GeoNear)                   => mergeRightFirst
      case (Unwind(_), _)                            => delegateMerge


      case (left @ Group(_, _), right @ Group(_, _)) => if (left.hashCode <= right.hashCode) mergeGroups(left, right)
                                                        else mergeGroups(right, left)
      case (left @ Group(_, _), Sort(_))             => mergeGroupOnLeft(ExprOp.DocVar.ROOT())(left) // FIXME: Verify logic & test!!!
      case (Group(_, _), Out(_))                     => mergeLeftFirst
      case (Group(_, _), _: GeoNear)                 => mergeRightFirst
      case (Group(_, _), _)                          => delegateMerge


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
  
}
