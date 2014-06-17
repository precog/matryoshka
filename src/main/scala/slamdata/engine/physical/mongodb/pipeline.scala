package slamdata.engine.physical.mongodb

import com.mongodb.DBObject

import scalaz._
import Scalaz._

case class PipelineOpMergeError(left: PipelineOp, right: PipelineOp, hint: Option[String] = None) {
  def message = "The pipeline op " + left + " cannot be merged with the pipeline op " + right + hint.map(": " + _).getOrElse("")
}

case class PipelineMergeError(merged: List[PipelineOp], lrest: List[PipelineOp], rrest: List[PipelineOp], hint: Option[String] = None) {
  def message = "The pipeline " + lrest + " cannot be merged with the pipeline " + rrest + hint.map(": " + _).getOrElse("")
}

private[mongodb] sealed trait MergePatch {
  import ExprOp._
  import PipelineOp._
  import MergePatch._

  def flatten: List[MergePatch.PrimitiveMergePatch]

  def apply(op: PipelineOp): (PipelineOp, MergePatch)

  private def genApply(applyVar0: PartialFunction[DocVar, DocVar])(op: PipelineOp): (PipelineOp, MergePatch) = {
    val applyVar = (f: DocVar) => applyVar0.lift(f).getOrElse(f)

    def applyExprOp(e: ExprOp): ExprOp = e.mapUp {
      case f : DocVar => applyVar(f)
    }

    def applySelector(s: Selector): Selector = s.mapUp {
      case Selector.Doc(m)       => Selector.Doc(applyMap(m))
      case Selector.FirstElem(f) => Selector.FirstElem(applyVar(DocField(f)).field)
    }

    def applyReshape(shape: Reshape): Reshape = Reshape(shape.value.transform {
      case (k, -\/(e)) => -\/(applyExprOp(e))
      case (k, \/-(r)) => \/-(applyReshape(r))
    })

    def applyGrouped(grouped: Grouped): Grouped = Grouped(grouped.value.transform {
      case (k, groupOp) => applyExprOp(groupOp) match {
        case groupOp : GroupOp => groupOp
        case _ => sys.error("Transformation changed the type -- error!")
      }
    })

    def applyMap[A](m: Map[BsonField, A]): Map[BsonField, A] = m.map(t => applyVar(DocField(t._1)).field -> t._2)

    def applyFindQuery(q: FindQuery): FindQuery = {
      q.copy(
        query   = applySelector(q.query),
        max     = q.max.map(applyMap _),
        min     = q.min.map(applyMap _),
        orderby = q.orderby.map(applyMap _)
      )
    }

    op match {
      case Project(shape)     => Project(applyReshape(shape)) -> Id // Patch is all consumed
      case Group(grouped, by) => Group(applyGrouped(grouped), applyExprOp(by)) -> Id // Patch is all consumed
    
      case Match(s)     => Match(applySelector(s))  -> this // Patch is not consumed
      case Redact(e)    => Redact(applyExprOp(e))   -> this
      case l @ Limit(_) => l                        -> this
      case s @ Skip(_)  => s                        -> this
      case Unwind(f)    => Unwind(applyVar(f))      -> this
      case Sort(l)      => Sort(applyMap(l))        -> this
      case o @ Out(_)   => o                        -> this
      case g : GeoNear  => g.copy(distanceField = applyVar(DocField(g.distanceField)).field, query = g.query.map(applyFindQuery _)) -> this
    }
  }
}
object MergePatch {
  import ExprOp._

  implicit val MergePatchMonoid = new Monoid[MergePatch] {
    def zero = Id

    def append(v1: MergePatch, v2: => MergePatch): MergePatch = (v1, v2) match {
      case (left, Id) => left
      case (Id, right) => right
      case (Nest(f1), Nest(f2)) if f1 nestsWith f2 => Nest((f1 \ f2).get)
      case (Rename(f1, t1), Rename(f2, t2)) if (t1 == f2) => Rename(f1, t2)
      case (x, y) => Then(x, y)
    }
  }

  sealed trait PrimitiveMergePatch extends MergePatch
  case object Id extends PrimitiveMergePatch {
    def flatten: List[PrimitiveMergePatch] = this :: Nil

    def apply(op: PipelineOp): (PipelineOp, MergePatch) = op -> Id
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

    def apply(op: PipelineOp): (PipelineOp, MergePatch) = genApply(
      {
        case DocVar(name, deref) if (name == from.name) => DocVar(to.name, replaceMatchingPrefix(deref))
      }
    )(op)
  }
  case class Nest(var0: DocVar) extends PrimitiveMergePatch {
    def flatten: List[PrimitiveMergePatch] = this :: Nil

    private def combine(deref2: Option[BsonField]): Option[BsonField] = {
      (var0.deref |@| deref2)(_ \ _) orElse (deref2) orElse (var0.deref)
    }

    def apply(op: PipelineOp): (PipelineOp, MergePatch) = genApply(
      {
        case DocVar.ROOT   (tail) => var0.copy(deref = combine(tail))
        case DocVar.CURRENT(tail) => var0.copy(deref = combine(tail))
      }
    )(op)
  }
  sealed trait CompositeMergePatch extends MergePatch
  case class Then(fst: MergePatch, snd: MergePatch) extends CompositeMergePatch {
    def flatten: List[PrimitiveMergePatch] = fst.flatten ++ snd.flatten

    def apply(op: PipelineOp): (PipelineOp, MergePatch) = {
      val l = flatten

      l.headOption.map { headPatch =>
        l.tail.foldLeft(headPatch(op)) {
          case ((op, patch0), patch1) => 
            (patch0 |+| patch1)(op)
        }
      }.getOrElse(op -> Id)
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
    case x => x
  }
}
private[mongodb] object MergeResult {
  case class Left (ops: List[PipelineOp], leftPatch: MergePatch = MergePatch.Id, rightPatch: MergePatch = MergePatch.Id) extends MergeResult
  case class Right(ops: List[PipelineOp], leftPatch: MergePatch = MergePatch.Id, rightPatch: MergePatch = MergePatch.Id) extends MergeResult
  case class Both (ops: List[PipelineOp], leftPatch: MergePatch = MergePatch.Id, rightPatch: MergePatch = MergePatch.Id) extends MergeResult
}

final case class Pipeline(ops: List[PipelineOp]) {
  def repr: java.util.List[DBObject] = ops.foldLeft(new java.util.ArrayList[DBObject](): java.util.List[DBObject]) {
    case (list, op) =>
      list.add(op.bson.repr)

      list
  }

  def merge(that: Pipeline): PipelineMergeError \/ Pipeline = mergeM[Free.Trampoline](that).run

  def mergeStack(that: Pipeline): PipelineMergeError \/ Pipeline = mergeM[Id.Id](that)

  private def mergeM[F[_]](that: Pipeline)(implicit F: Monad[F]): F[PipelineMergeError \/ Pipeline] = {
    type M[X] = EitherT[F, PipelineMergeError, X]

    def succeed[A](a: A): M[A] = EitherT((\/-(a): \/[PipelineMergeError, A]).point[F])
    def fail[A](e: PipelineMergeError): M[A] = EitherT((-\/(e): \/[PipelineMergeError, A]).point[F])

    def applyAll(l: List[PipelineOp], patch: MergePatch): List[PipelineOp] = {
      (l.headOption map { h0 =>
        val (h, patch2) = patch(h0)

        (l.tail.foldLeft[(List[PipelineOp], MergePatch)]((h :: Nil, patch2)) {
          case ((acc, patch), op0) =>
            val (op, patch2) = patch(op0)

            (op :: acc) -> patch2
        })._1.reverse
      }).getOrElse(l)
    }

    def merge0(merged: List[PipelineOp], left: List[PipelineOp], lp0: MergePatch, right: List[PipelineOp], rp0: MergePatch): M[List[PipelineOp]] = {
      (left, right) match {
        case (Nil, Nil) => succeed(merged)

        case (left, Nil)  => succeed(applyAll(left,  lp0).reverse ::: merged)
        case (Nil, right) => succeed(applyAll(right, rp0).reverse ::: merged)

        case (lh0 :: lt, rh0 :: rt) => 
          val (lh, lp1) = lp0(lh0)
          val (rh, rp1) = rp0(rh0)

          for {
            x <-  lh.merge(rh).fold(_ => fail(PipelineMergeError(merged, left, right)), succeed _) // FIXME: Try commuting!!!!
            m <-  x match {
                    case MergeResult.Left (hs, lp2, rp2) => merge0(hs ::: merged, lt,       lp1 |+| lp2, rh :: rt, rp1 |+| rp2)
                    case MergeResult.Right(hs, lp2, rp2) => merge0(hs ::: merged, lh :: lt, lp1 |+| lp2, rt,       rp1 |+| rp2)
                    case MergeResult.Both (hs, lp2, rp2) => merge0(hs ::: merged, lt,       lp1 |+| lp2, rt,       rp1 |+| rp2)
                  }
          } yield m
      }
    }

    merge0(Nil, this.ops, MergePatch.Id, that.ops, MergePatch.Id).map(list => Pipeline(list.reverse)).run
  }
}

sealed trait PipelineOp {
  def bson: Bson.Doc

  def commutesWith(that: PipelineOp): Boolean = false

  def merge(that: PipelineOp): PipelineOpMergeError \/ MergeResult

  private def delegateMerge(that: PipelineOp): PipelineOpMergeError \/ MergeResult = that.merge(this).map(_.flip)
  
  private def mergeThisFirst: PipelineOpMergeError \/ MergeResult = \/- (MergeResult.Left(this :: Nil))
  private def mergeThatFirst(that: PipelineOp): PipelineOpMergeError \/ MergeResult = \/- (MergeResult.Right(that :: Nil))
  private def mergeThisAndDropThat: PipelineOpMergeError \/ MergeResult = \/- (MergeResult.Both(this :: Nil))
}

object PipelineOp {
  implicit val ShowPipelineOp = new Show[PipelineOp] {
    // TODO:
    override def show(v: PipelineOp): Cord = Cord(v.toString)
  }
  private[PipelineOp] abstract sealed class SimpleOp(op: String) extends PipelineOp {
    def rhs: Bson

    def bson = Bson.Doc(Map(op -> rhs))
  }

  case class Reshape(value: Map[BsonField.Name, ExprOp \/ Reshape]) {
    def bson: Bson.Doc = Bson.Doc(value.map {
      case (field, either) => field.asText -> either.fold(_.bson, _.bson)
    })
  }
  object Reshape {
    implicit val ReshapeMonoid = new Monoid[Reshape] {
      def zero = Reshape(Map())

      def append(v1: Reshape, v2: => Reshape): Reshape = {
        val m1 = v1.value
        val m2 = v2.value
        val keys = m1.keySet ++ m2.keySet

        Reshape(keys.foldLeft(Map.empty[BsonField.Name, ExprOp \/ Reshape]) {
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
  case class Grouped(value: Map[BsonField.Name, ExprOp.GroupOp]) {
    def bson = Bson.Doc(value.map(t => t._1.asText -> t._2.bson))
  }
  case class Project(shape: Reshape) extends SimpleOp("$project") {
    def rhs = shape.bson

    def merge(that: PipelineOp): PipelineOpMergeError \/ MergeResult = that match {
      case Project(shape) => \/- (MergeResult.Both(Project(this.shape |+| shape) :: Nil))
      case Match(_)       => mergeThatFirst(that)
      case Redact(_)      => mergeThatFirst(that)
      case Limit(_)       => mergeThatFirst(that)
      case Skip(_)        => mergeThatFirst(that)
      case Unwind(_)      => mergeThatFirst(that) // TODO:
      case Group(Grouped(m), b) => 
        // FIXME: Verify logic & test!!!
        val tmpName = BsonField.genUniqName(m.keys)

        val that2 = Group(Grouped(m + (tmpName -> ExprOp.AddToSet(ExprOp.DocVar.ROOT()))), b)

        // FIXME: Enhance rename so that references to ROOT (implicit or explicit!!!) can be renamed.
        val thisPatch = MergePatch.Rename(???, ExprOp.DocField(tmpName))

        \/- (MergeResult.Right(that2 :: Nil, thisPatch, MergePatch.Id))

      ???
      case Sort(_)        => mergeThatFirst(that)
      case Out(_)         => mergeThisFirst
      case _: GeoNear     => mergeThatFirst(that)
    }
  }
  case class Match(selector: Selector) extends SimpleOp("$match") {
    def rhs = selector.bson

    private def mergeSelector(that: Match): Selector = {
      (this.selector, that.selector) match {
        case (Selector.Doc(value), Selector.Doc(value2)) => 
          implicit val exprAnd = Selector.SelectorAndSemigroup
          Selector.Doc(value |+| value2)
        case (sel1, sel2) => Selector.And(NonEmptyList(sel1, sel2))
      }
    }

    def merge(that: PipelineOp): PipelineOpMergeError \/ MergeResult = that match {
      case that: Match => \/- (MergeResult.Both(Match(mergeSelector(that)) :: Nil))
      case Redact(_)   => mergeThisFirst
      case Limit(_)    => mergeThatFirst(that)
      case Skip(_)     => mergeThatFirst(that)
      case Unwind(_)   => mergeThisFirst
      case Group(_, _) => ???
      case Sort(_)     => mergeThisFirst
      case Out(_)      => mergeThisFirst
      case _: GeoNear  => mergeThatFirst(that)

      case _ => delegateMerge(that)
    }
  }
  case class Redact(value: ExprOp) extends SimpleOp("$redact") {
    def rhs = value.bson

    private def fields: List[BsonField] = {
      import scalaz.std.list._
      val field: PartialFunction[ExprOp, List[BsonField]] = {
        case ExprOp.DocField(f) => f :: Nil
      }
      ExprOp.foldMap(field)(value)
    }

    def merge(that: PipelineOp): PipelineOpMergeError \/ MergeResult = that match {
      case Redact(_) if (this == that) => mergeThisAndDropThat
      case Redact(_)                   => -\/ (PipelineOpMergeError(this, that, Some("Cannot merge multiple $redact ops"))) // FIXME: Verify logic & test!!!
      case Limit(_)                    => mergeThatFirst(that)
      case Skip(_)                     => mergeThatFirst(that)
      case Unwind(field) if (fields.contains(field))
                                       => -\/ (PipelineOpMergeError(this, that, Some("Cannot merge $redact with $unwind--condition refers to the field being unwound")))
      case Unwind(_)                   => mergeThisFirst
      case Group(_, _)                 => ???
      case Sort(_)                     => mergeThatFirst(that)
      case Out(_)                      => mergeThisFirst
      case _: GeoNear                  => mergeThatFirst(that)
      
      case _ => delegateMerge(that)
    }
  }
  case class Limit(value: Long) extends SimpleOp("$limit") {
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
  case class Skip(value: Long) extends SimpleOp("$skip") {
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
      case Unwind(_) if (this == that) => \/- (MergeResult.Both(this :: Nil))
      case Unwind(field)               => if (this.field.field.asText < field.field.asText) mergeThisFirst else mergeThatFirst(that)
      
      case Group(Grouped(m), b) =>
        // FIXME: Verify logic & test!!!
        val tmpName = BsonField.genUniqName(m.keys)

        val that2 = Group(Grouped(m + (tmpName -> ExprOp.AddToSet(this.field))), b)
        val thisPatch = MergePatch.Rename(this.field, ExprOp.DocField(tmpName))

        \/- (MergeResult.Right(that2 :: Nil, thisPatch, MergePatch.Id))

      case Sort(_)     => mergeThatFirst(that)
      case Out(_)      => mergeThisFirst
      case _: GeoNear  => mergeThatFirst(that)
      
      case _ => delegateMerge(that)
    }
  }
  case class Group(grouped: Grouped, by: ExprOp) extends SimpleOp("$group") {
    def rhs = {
      val Bson.Doc(m) = grouped.bson

      Bson.Doc(m + ("_id" -> by.bson))
    }

    def merge(that: PipelineOp): PipelineOpMergeError \/ MergeResult = that match {
      case Group(_, _) => ???
      case Sort(_)     => ???
      case Out(_)      => mergeThisFirst
      case _: GeoNear  => mergeThatFirst(that)
      
      case _ => delegateMerge(that)
    }
  }
  case class Sort(value: Map[BsonField, SortType]) extends SimpleOp("$sort") {
    // TODO: make the value preserve the order of keys
    def rhs = Bson.Doc(value.map(t => t._1.asText -> t._2.bson))

    def merge(that: PipelineOp): PipelineOpMergeError \/ MergeResult = that match {
      case Sort(_) if (this == that) => mergeThisAndDropThat
      case Sort(_)                   => -\/ (PipelineOpMergeError(this, that, Some("Cannot merge multiple $sort ops")))
      case Out(_)                    => mergeThisFirst
      case _: GeoNear                => mergeThatFirst(that)
      
      case _ => delegateMerge(that)
    }
  }
  case class Out(collection: Collection) extends SimpleOp("$out") {
    def rhs = Bson.Text(collection.name)

    def merge(that: PipelineOp): PipelineOpMergeError \/ MergeResult = that match {
      case Out(_) if (this == that) => \/- (MergeResult.Both(this :: Nil))
      case Out(_)                   => -\/ (PipelineOpMergeError(this, that, Some("Cannot merge multiple $out ops")))
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
      case _: GeoNear if (this == that) => \/- (MergeResult.Both(this :: Nil))
      case _: GeoNear                   => -\/(PipelineOpMergeError(this, that, Some("Cannot merge multiple $geoNear ops")))
      
      case _ => delegateMerge(that)
    }
  }
}

sealed trait ExprOp {
  def bson: Bson

  import ExprOp._

  def mapUp(f0: PartialFunction[ExprOp, ExprOp]): ExprOp = {
    (mapUpM[Free.Trampoline](new PartialFunction[ExprOp, Free.Trampoline[ExprOp]] {
      def isDefinedAt(v: ExprOp) = f0.isDefinedAt(v)
      def apply(v: ExprOp) = f0(v).point[Free.Trampoline]
    })).run
  }

  // TODO: Port physical plan to fixplate to eliminate this madness!!!!!!!!!!!!!!!!!!!!!
  def mapUpM[F[_]](f0: PartialFunction[ExprOp, F[ExprOp]])(implicit F: Monad[F]): F[ExprOp] = {
    val f0l = f0.lift
    val f = (e: ExprOp) => f0l(e).getOrElse(e.point[F])

    def mapUp0(v: ExprOp): F[ExprOp] = {
      val rec = (v match {
        case Doc(a)             => 
          type MapBsonFieldName[X] = Map[BsonField.Name, X]

          Traverse[MapBsonFieldName].sequence[F, ExprOp \/ Doc](a.mapValues { e =>
            e.fold(
              (e: ExprOp) => mapUp0(e).map(-\/.apply), 
              (d: Doc)    => mapUp0(d).map(d => \/-(d.asInstanceOf[Doc]))
            )
          }: Map[BsonField.Name, F[ExprOp \/ Doc]]).map(Doc(_))

        case Include            => v.point[F]
        case Exclude            => v.point[F]
        case DocVar(_, _)       => v.point[F]
        case Add(l, r)          => (mapUp0(l) |@| mapUp0(r))(Add(_, _))
        case AddToSet(_)        => v.point[F]
        case And(v)             => v.map(mapUp0 _).sequenceU.map(And(_))
        case SetEquals(l, r)       => (mapUp0(l) |@| mapUp0(r))(SetEquals(_, _))
        case SetIntersection(l, r) => (mapUp0(l) |@| mapUp0(r))(SetIntersection(_, _))
        case SetDifference(l, r)   => (mapUp0(l) |@| mapUp0(r))(SetDifference(_, _))
        case SetUnion(l, r)        => (mapUp0(l) |@| mapUp0(r))(SetUnion(_, _))
        case SetIsSubset(l, r)     => (mapUp0(l) |@| mapUp0(r))(SetIsSubset(_, _))
        case AnyElementTrue(v)     => mapUp0(v).map(AnyElementTrue(_))
        case AllElementsTrue(v)    => mapUp0(v).map(AllElementsTrue(_))
        case ArrayMap(a, b, c)  => (mapUp0(a) |@| mapUp0(c))(ArrayMap(_, b, _))
        case Avg(v)             => mapUp0(v).map(Avg(_))
        case Cmp(l, r)          => (mapUp0(l) |@| mapUp0(r))(Cmp(_, _))
        case Concat(a, b, cs)   => (mapUp0(a) |@| mapUp0(b) |@| cs.map(mapUp0 _).sequenceU)(Concat(_, _, _))
        case Cond(a, b, c)      => (mapUp0(a) |@| mapUp0(b) |@| mapUp0(c))(Cond(_, _, _))
        case DayOfMonth(a)      => mapUp0(a).map(DayOfMonth(_))
        case DayOfWeek(a)       => mapUp0(a).map(DayOfWeek(_))
        case DayOfYear(a)       => mapUp0(a).map(DayOfYear(_))
        case Divide(a, b)       => (mapUp0(a) |@| mapUp0(b))(Divide(_, _))
        case Eq(a, b)           => (mapUp0(a) |@| mapUp0(b))(Eq(_, _))
        case First(a)           => mapUp0(a).map(First(_))
        case Gt(a, b)           => (mapUp0(a) |@| mapUp0(b))(Gt(_, _))
        case Gte(a, b)          => (mapUp0(a) |@| mapUp0(b))(Gte(_, _))
        case Hour(a)            => mapUp0(a).map(Hour(_))
        case Meta                  => v.point[F]
        case Size(a)               => mapUp0(a).map(Size(_))
        case IfNull(a, b)       => (mapUp0(a) |@| mapUp0(b))(IfNull(_, _))
        case Last(a)            => mapUp0(a).map(Last(_))
        case Let(a, b)          => 
          type MapBsonField[X] = Map[BsonField, X]

          (Traverse[MapBsonField].sequence[F, ExprOp](a.map(t => t._1 -> mapUp0(t._2))) |@| mapUp0(b))(Let(_, _))

        case Literal(_)         => v.point[F]
        case Lt(a, b)           => (mapUp0(a) |@| mapUp0(b))(Lt(_, _))
        case Lte(a, b)          => (mapUp0(a) |@| mapUp0(b))(Lte(_, _))
        case Max(a)             => mapUp0(a).map(Max(_))
        case Millisecond(a)     => mapUp0(a).map(Millisecond(_))
        case Min(a)             => mapUp0(a).map(Min(_))
        case Minute(a)          => mapUp0(a).map(Minute(_))
        case Mod(a, b)          => (mapUp0(a) |@| mapUp0(b))(Mod(_, _))
        case Month(a)           => mapUp0(a).map(Month(_))
        case Multiply(a, b)     => (mapUp0(a) |@| mapUp0(b))(Multiply(_, _))
        case Neq(a, b)          => (mapUp0(a) |@| mapUp0(b))(Neq(_, _))
        case Not(a)             => mapUp0(a).map(Not(_))
        case Or(a)              => a.map(mapUp0 _).sequenceU.map(Or(_))
        case Push(a)            => v.point[F]
        case Second(a)          => mapUp0(a).map(Second(_))
        case Strcasecmp(a, b)   => (mapUp0(a) |@| mapUp0(b))(Strcasecmp(_, _))
        case Substr(a, b, c)    => mapUp0(a).map(Substr(_, b, c))
        case Subtract(a, b)     => (mapUp0(a) |@| mapUp0(b))(Subtract(_, _))
        case Sum(a)             => mapUp0(a).map(Sum(_))
        case ToLower(a)         => mapUp0(a).map(ToLower(_))
        case ToUpper(a)         => mapUp0(a).map(ToUpper(_))
        case Week(a)            => mapUp0(a).map(Week(_))
        case Year(a)            => mapUp0(a).map(Year(_))
      }) 

      rec >>= f
    }

    mapUp0(this)
  }
}

object ExprOp {
  implicit val ExprOpShow: Show[ExprOp] = new Show[ExprOp] {
    override def show(v: ExprOp): Cord = Cord(v.toString) // TODO
  }

  def children(expr: ExprOp): List[ExprOp] = expr match {
    case Doc(v)                => v.values.map(_.fold(op => op, doc => doc)).toList
    case Include               => Nil
    case Exclude               => Nil
    case DocVar(_, _)          => Nil
    case Add(l, r)             => l :: r :: Nil
    case AddToSet(_)           => Nil
    case And(v)                => v.toList
    case SetEquals(l, r)       => l :: r :: Nil
    case SetIntersection(l, r) => l :: r :: Nil
    case SetDifference(l, r)   => l :: r :: Nil
    case SetUnion(l, r)        => l :: r :: Nil
    case SetIsSubset(l, r)     => l :: r :: Nil
    case AnyElementTrue(v)     => v :: Nil
    case AllElementsTrue(v)    => v :: Nil
    case ArrayMap(a, _, c)     => a :: c :: Nil
    case Avg(v)                => v :: Nil
    case Cmp(l, r)             => l :: r :: Nil
    case Concat(a, b, cs)      => a :: b :: cs
    case Cond(a, b, c)         => a :: b :: c :: Nil
    case DayOfMonth(a)         => a :: Nil
    case DayOfWeek(a)          => a :: Nil
    case DayOfYear(a)          => a :: Nil
    case Divide(a, b)          => a :: b :: Nil
    case Eq(a, b)              => a :: b :: Nil
    case First(a)              => a :: Nil
    case Gt(a, b)              => a :: b :: Nil
    case Gte(a, b)             => a :: b :: Nil
    case Hour(a)               => a :: Nil
    case Meta                  => Nil
    case Size(a)               => a :: Nil
    case IfNull(a, b)          => a :: b :: Nil
    case Last(a)               => a :: Nil
    case Let(_, b)             => b :: Nil
    case Literal(_)            => Nil
    case Lt(a, b)              => a :: b :: Nil
    case Lte(a, b)             => a :: b :: Nil
    case Max(a)                => a :: Nil
    case Millisecond(a)        => a :: Nil
    case Min(a)                => a :: Nil
    case Minute(a)             => a :: Nil
    case Mod(a, b)             => a :: b :: Nil
    case Month(a)              => a :: Nil
    case Multiply(a, b)        => a :: b :: Nil
    case Neq(a, b)             => a :: b :: Nil
    case Not(a)                => a :: Nil
    case Or(a)                 => a.toList
    case Push(a)               => Nil
    case Second(a)             => a :: Nil
    case Strcasecmp(a, b)      => a :: b :: Nil
    case Substr(a, _, _)       => a :: Nil
    case Subtract(a, b)        => a :: b :: Nil
    case Sum(a)                => a :: Nil
    case ToLower(a)            => a :: Nil
    case ToUpper(a)            => a :: Nil
    case Week(a)               => a :: Nil
    case Year(a)               => a :: Nil
  }

  def foldMap[Z: Monoid](f0: PartialFunction[ExprOp, Z])(v: ExprOp): Z = {
    val f = (e: ExprOp) => f0.lift(e).getOrElse(Monoid[Z].zero)
    Monoid[Z].append(f(v), Foldable[List].foldMap(children(v))(foldMap(f0)))
  }

  private[ExprOp] abstract sealed class SimpleOp(op: String) extends ExprOp {
    def rhs: Bson

    def bson = Bson.Doc(Map(op -> rhs))
  }

  case class Doc(value: Map[BsonField.Name, ExprOp \/ Doc]) extends ExprOp {
    def bson: Bson = Bson.Doc(value.map(t => t._1.asText -> t._2.fold(_.bson, _.bson)))
  }

  sealed trait IncludeExclude extends ExprOp
  case object Include extends IncludeExclude {
    def bson = Bson.Int32(1)
  }
  case object Exclude extends IncludeExclude {
    def bson = Bson.Int32(0)
  }

  sealed trait FieldLike extends ExprOp {
    def field: BsonField
  }
  object DocField {
    def apply(field: BsonField): DocVar = DocVar.ROOT(field)

    def unapply(docVar: DocVar): Option[BsonField] = docVar match {
      case DocVar.ROOT(tail) => tail
      case _ => None
    }
  }
  case class DocVar(name: DocVar.Name, deref: Option[BsonField]) extends FieldLike {
    def field: BsonField = BsonField.Name(name.name) \\ deref.toList.flatMap(_.flatten)

    def bson = this match {
      case DocVar(DocVar.ROOT,    Some(field)) => Bson.Text(field.asField)
      case DocVar(DocVar.CURRENT, Some(field)) => Bson.Text(field.asField)

      case _ => Bson.Text(field.asVar)
    }

    def nestsWith(that: DocVar): Boolean = this.name == that.name

    def \ (that: DocVar): Option[DocVar] = (this, that) match {
      case (DocVar(n1, f1), DocVar(n2, f2)) if (n1 == n2) => 
        val f3 = (f1 |@| f2)(_ \ _) orElse (f1) orElse (f2)

        Some(DocVar(n1, f3))

      case _ => None
    }
  }
  object DocVar {
    case class Name(name: String) {
      def apply() = DocVar(this, None)

      def apply(field: BsonField) = DocVar(this, Some(field))

      def apply(deref: Option[BsonField]) = DocVar(this, deref)

      def apply(leaves: List[BsonField.Leaf]) = DocVar(this, BsonField(leaves))

      def unapply(v: DocVar): Option[Option[BsonField]] = Some(v.deref)
    }
    val ROOT    = Name("ROOT")
    val CURRENT = Name("CURRENT")
    val KEEP    = Name("KEEP")
    val PRUNE   = Name("PRUNE")
    val DESCEND = Name("DESCEND")
  }

  sealed trait GroupOp extends ExprOp
  case class AddToSet(field: FieldLike) extends SimpleOp("$addToSet") with GroupOp {
    def rhs = field.bson
  }
  case class Push(field: FieldLike) extends SimpleOp("$push") with GroupOp {
    def rhs = field.bson
  }
  case class First(value: ExprOp) extends SimpleOp("$first") with GroupOp {
    def rhs = value.bson
  }
  case class Last(value: ExprOp) extends SimpleOp("$last") with GroupOp {
    def rhs = value.bson
  }
  case class Max(value: ExprOp) extends SimpleOp("$max") with GroupOp {
    def rhs = value.bson
  }
  case class Min(value: ExprOp) extends SimpleOp("$min") with GroupOp {
    def rhs = value.bson
  }
  case class Avg(value: ExprOp) extends SimpleOp("$avg") with GroupOp {
    def rhs = value.bson
  }
  case class Sum(value: ExprOp) extends SimpleOp("$sum") with GroupOp {
    def rhs = value.bson
  }
  object Count extends Sum(Literal(Bson.Int32(1)))

  sealed trait BoolOp extends ExprOp
  case class And(values: NonEmptyList[ExprOp]) extends SimpleOp("$and") with BoolOp {
    def rhs = Bson.Arr(values.list.map(_.bson))
  }
  case class Or(values: NonEmptyList[ExprOp]) extends SimpleOp("$or") with BoolOp {
    def rhs = Bson.Arr(values.list.map(_.bson))
  }
  case class Not(value: ExprOp) extends SimpleOp("$not") with BoolOp {
    def rhs = value.bson
  }

  sealed trait BinarySetOp extends ExprOp {
    def left: ExprOp
    def right: ExprOp
    
    def rhs = Bson.Arr(left.bson :: right.bson :: Nil)
  }
  case class SetEquals(left: ExprOp, right: ExprOp) extends SimpleOp("$setEquals") with BinarySetOp
  case class SetIntersection(left: ExprOp, right: ExprOp) extends SimpleOp("$setIntersection") with BinarySetOp
  case class SetDifference(left: ExprOp, right: ExprOp) extends SimpleOp("$setDifference") with BinarySetOp
  case class SetUnion(left: ExprOp, right: ExprOp) extends SimpleOp("$setUnion") with BinarySetOp
  case class SetIsSubset(left: ExprOp, right: ExprOp) extends SimpleOp("$setIsSubset") with BinarySetOp

  sealed trait UnarySetOp extends ExprOp {
    def value: ExprOp
    
    def rhs = value.bson
  }
  case class AnyElementTrue(value: ExprOp) extends SimpleOp("$anyElementTrue") with UnarySetOp
  case class AllElementsTrue(value: ExprOp) extends SimpleOp("$allElementsTrue") with UnarySetOp

  sealed trait CompOp extends ExprOp {
    def left: ExprOp    
    def right: ExprOp

    def rhs = Bson.Arr(left.bson :: right.bson :: Nil)
  }
  case class Cmp(left: ExprOp, right: ExprOp) extends SimpleOp("$cmp") with CompOp
  case class Eq(left: ExprOp, right: ExprOp) extends SimpleOp("$eq") with CompOp
  case class Gt(left: ExprOp, right: ExprOp) extends SimpleOp("$gt") with CompOp
  case class Gte(left: ExprOp, right: ExprOp) extends SimpleOp("$gte") with CompOp
  case class Lt(left: ExprOp, right: ExprOp) extends SimpleOp("$lt") with CompOp
  case class Lte(left: ExprOp, right: ExprOp) extends SimpleOp("$lte") with CompOp
  case class Neq(left: ExprOp, right: ExprOp) extends SimpleOp("$ne") with CompOp

  sealed trait MathOp extends ExprOp {
    def left: ExprOp
    def right: ExprOp

    def rhs = Bson.Arr(left.bson :: right.bson :: Nil)
  }
  case class Add(left: ExprOp, right: ExprOp) extends SimpleOp("$add") with MathOp
  case class Divide(left: ExprOp, right: ExprOp) extends SimpleOp("$divide") with MathOp
  case class Mod(left: ExprOp, right: ExprOp) extends SimpleOp("$mod") with MathOp
  case class Multiply(left: ExprOp, right: ExprOp) extends SimpleOp("$multiply") with MathOp
  case class Subtract(left: ExprOp, right: ExprOp) extends SimpleOp("$subtract") with MathOp

  sealed trait StringOp extends ExprOp
  case class Concat(first: ExprOp, second: ExprOp, others: List[ExprOp]) extends SimpleOp("$concat") with StringOp {
    def rhs = Bson.Arr(first.bson :: second.bson :: others.map(_.bson))
  }
  case class Strcasecmp(left: ExprOp, right: ExprOp) extends SimpleOp("$strcasecmp") with StringOp {
    def rhs = Bson.Arr(left.bson :: right.bson :: Nil)
  }
  case class Substr(value: ExprOp, start: Int, count: Int) extends SimpleOp("$substr") with StringOp {
    def rhs = Bson.Arr(value.bson :: Bson.Int32(start) :: Bson.Int32(count) :: Nil)
  }
  case class ToLower(value: ExprOp) extends SimpleOp("$toLower") with StringOp {
    def rhs = value.bson
  }
  case class ToUpper(value: ExprOp) extends SimpleOp("$toUpper") with StringOp {
    def rhs = value.bson
  }

  sealed trait TextSearchOp extends ExprOp
  case object Meta extends SimpleOp("$meta") with TextSearchOp {
    def rhs = Bson.Text("textScore")
  }

  sealed trait ArrayOp extends ExprOp
  case class Size(array: ExprOp) extends SimpleOp("$size") with ArrayOp {
    def rhs = array.bson
  }

  sealed trait ProjOp extends ExprOp
  case class ArrayMap(input: ExprOp, as: String, in: ExprOp) extends SimpleOp("$map") with ProjOp {
    def rhs = Bson.Doc(Map(
      "input" -> input.bson,
      "as"    -> Bson.Text(as),
      "in"    -> in.bson
    ))
  }
  case class Let(vars: Map[BsonField, ExprOp], in: ExprOp) extends SimpleOp("$let") with ProjOp {
    def rhs = Bson.Doc(Map(
      "vars" -> Bson.Doc(vars.map(t => (t._1.asText, t._2.bson))),
      "in"   -> in.bson
    ))
  }
  case class Literal(value: Bson) extends ProjOp {
    def bson = value match {
      case Bson.Text(str) if (str.startsWith("$")) => Bson.Doc(Map("$literal" -> value))
      
      case Bson.Doc(value)                         => Bson.Doc(value.transform((_, x) => Literal(x).bson))
      case Bson.Arr(value)                         => Bson.Arr(value.map(x => Literal(x).bson))
      
      case _                                       => value
    }
  }

  sealed trait DateOp extends ExprOp {
    def date: ExprOp

    def rhs = date.bson
  }
  case class DayOfYear(date: ExprOp) extends SimpleOp("$dayOfYear") with DateOp
  case class DayOfMonth(date: ExprOp) extends SimpleOp("$dayOfMonth") with DateOp
  case class DayOfWeek(date: ExprOp) extends SimpleOp("$dayOfWeek") with DateOp
  case class Year(date: ExprOp) extends SimpleOp("$year") with DateOp
  case class Month(date: ExprOp) extends SimpleOp("$month") with DateOp
  case class Week(date: ExprOp) extends SimpleOp("$week") with DateOp
  case class Hour(date: ExprOp) extends SimpleOp("$hour") with DateOp
  case class Minute(date: ExprOp) extends SimpleOp("$minute") with DateOp
  case class Second(date: ExprOp) extends SimpleOp("$second") with DateOp
  case class Millisecond(date: ExprOp) extends SimpleOp("$millisecond") with DateOp

  sealed trait CondOp extends ExprOp
  case class Cond(predicate: ExprOp, ifTrue: ExprOp, ifFalse: ExprOp) extends SimpleOp("$cond") with CondOp {
    def rhs = Bson.Arr(predicate.bson :: ifTrue.bson :: ifFalse.bson :: Nil)
  }
  case class IfNull(expr: ExprOp, replacement: ExprOp) extends CondOp {
    def bson = Bson.Arr(expr.bson :: replacement.bson :: Nil)
  }
}

