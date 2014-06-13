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
  def apply(op: PipelineOp): (PipelineOp, MergePatch)
}
object MergePatch {
  case object Id extends MergePatch {
    def apply(op: PipelineOp): (PipelineOp, MergePatch) = op -> Id
  }
  case class Nest(field: BsonField) extends MergePatch {
    import PipelineOp._
    import ExprOp._

    def apply(op: PipelineOp): (PipelineOp, MergePatch) = op match {
      case Project(shape)     => ???
      case Group(grouped, by) => ???

      case x => x -> this
    }
  }

  implicit val MergePatchMonoid = new Monoid[MergePatch] {
    def zero = Id

    def append(v1: MergePatch, v2: => MergePatch): MergePatch = (v1, v2) match {
      case (left, Id) => left
      case (Id, right) => right
      case (Nest(f1), Nest(f2)) => Nest(f1 :+ f2)
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

  def merge(that: Pipeline): PipelineMergeError \/ Pipeline = {
    def merge0(merged: List[PipelineOp], left: List[PipelineOp], right: List[PipelineOp]): PipelineMergeError \/ List[PipelineOp] = {
      (left, right) match {
        case (left, right) if left == right => \/- (left.reverse ::: merged)

        case (left, Nil) => \/- (left.reverse ::: merged)
        case (Nil, right) => \/- (right.reverse ::: merged)

        case (lh :: lt, rh :: rt) => 
          for {
            x <-  lh.merge(rh).leftMap(_ => PipelineMergeError(merged, left, right)) // FIXME: Try commuting!!!!
            m <-  x match {
                    case MergeResult.Left (hs, lp, rp) => merge0(hs ::: merged, lt,   right)
                    case MergeResult.Right(hs, lp, rp) => merge0(hs ::: merged, left, rt)
                    case MergeResult.Both (hs, lp, rp) => merge0(hs ::: merged, lt,   rt)
                  }
          } yield m
      }
    }

    merge0(Nil, this.ops, that.ops).map(list => Pipeline(list.reverse))
  }
}

sealed trait PipelineOp {
  def bson: Bson.Doc

  def commutesWith(that: PipelineOp): Boolean = false

  def merge(that: PipelineOp): PipelineOpMergeError \/ MergeResult

  private def delegateMerge(that: PipelineOp): PipelineOpMergeError \/ MergeResult = that.merge(this).map(_.flip)
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

  case class Reshape(value: Map[String, ExprOp \/ Reshape]) {
    def bson: Bson.Doc = Bson.Doc(value.mapValues(either => either.fold(_.bson, _.bson)))
  }
  object Reshape {
    implicit val ReshapeMonoid = new Monoid[Reshape] {
      def zero = Reshape(Map())

      def append(v1: Reshape, v2: => Reshape): Reshape = {
        val m1 = v1.value
        val m2 = v2.value
        val keys = m1.keySet ++ m2.keySet

        Reshape(keys.foldLeft(Map.empty[String, ExprOp \/ Reshape]) {
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
  case class Grouped(value: Map[String, ExprOp.GroupOp]) {
    def bson = Bson.Doc(value.mapValues(_.bson))
  }
  case class Project(shape: Reshape) extends SimpleOp("$project") {
    def rhs = shape.bson

    def merge(that: PipelineOp): PipelineOpMergeError \/ MergeResult = that match {
      case that @ Project(_)  => \/- (MergeResult.Both(Project(this.shape |+| that.shape) :: Nil))
      case that @ Match(_)    => \/- (MergeResult.Right(that :: Nil))
      case that @ Redact(_)   => \/- (MergeResult.Right(that :: Nil))
      case that @ Limit(_)    => \/- (MergeResult.Right(that :: Nil))
      case that @ Skip(_)     => \/- (MergeResult.Right(that :: Nil))
      case that @ Unwind(_)   => \/- (MergeResult.Right(that :: Nil)) // TODO:
      case that @ Group(_, _) => ???
      case that @ Sort(_)     => \/- (MergeResult.Right(that :: Nil))
      case that @ Out(_)      => \/- (MergeResult.Left(this :: nil))
      case that @ GeoNear(_, _, _, _, _, _, _, _, _) => \/- (MergeResult.Right(that :: nil))
    }
  }
  case class Match(selector: Selector) extends SimpleOp("$match") {
    def rhs = selector.bson

    def merge(that: PipelineOp): PipelineOpMergeError \/ MergeResult = that match {
      // case that @ Project(_)  => delegateMerge(that)
      case that @ Match(_)    => \/- (MergeResult.Left(this :: nil))
      case that @ Redact(_)   => ???
      case that @ Limit(_)    => \/- (MergeResult.Right(that :: nil))
      case that @ Skip(_)     => \/- (MergeResult.Right(that :: nil))
      case that @ Unwind(_)   => \/- (MergeResult.Left(this :: nil))
      case that @ Group(_, _) => ???
      case that @ Sort(_)     => \/- (MergeResult.Left(this :: nil))
      case that @ Out(_)      => \/- (MergeResult.Left(this :: nil))
      case that @ GeoNear(_, _, _, _, _, _, _, _, _) => \/- (MergeResult.Right(that :: nil))
      case _ => delegateMerge(that)
    }
  }
  case class Redact(value: ExprOp) extends SimpleOp("$redact") {
    def rhs = value.bson

    def merge(that: PipelineOp): PipelineOpMergeError \/ MergeResult = that match {
      // case that @ Project(_)  => delegateMerge(that)
      // case that @ Match(_)    => ???
      case that @ Redact(_)   => ???
      case that @ Limit(_)    => \/- (MergeResult.Left(this :: nil))
      case that @ Skip(_)     => \/- (MergeResult.Left(this :: nil))
      case that @ Unwind(_)   => ???
      case that @ Group(_, _) => ???
      case that @ Sort(_)     => \/- (MergeResult.Left(this :: nil))
      case that @ Out(_)      => \/- (MergeResult.Left(this :: nil))
      case that @ GeoNear(_, _, _, _, _, _, _, _, _) => \/- (MergeResult.Right(that :: nil))
      case _ => delegateMerge(that)
    }
  }
  case class Limit(value: Long) extends SimpleOp("$limit") {
    def rhs = Bson.Int64(value)

    def merge(that: PipelineOp): PipelineOpMergeError \/ MergeResult = that match {
      // case that @ Project(_)    => delegateMerge(that)
      // case that @ Match(_)      => \/- (MergeResult.Left(this :: nil))
      // case that @ Redact(_)     => ???
      case that @ Limit(value2) => \/- (MergeResult.Both(Limit(value max value2) :: nil))
      case that @ Skip(value2)  => \/- (MergeResult.Both(Limit((value - value2) max 0) :: that :: nil))
      case that @ Unwind(_)     => \/- (MergeResult.Left(this :: nil))
      case that @ Group(_, _)   => ???
      case that @ Sort(_)       => \/- (MergeResult.Left(this :: nil))
      case that @ Out(_)        => \/- (MergeResult.Left(this :: nil))
      case that @ GeoNear(_, _, _, _, _, _, _, _, _) => \/- (MergeResult.Right(that :: nil))
      case _ => delegateMerge(that)
    }
  }
  case class Skip(value: Long) extends SimpleOp("$skip") {
    def rhs = Bson.Int64(value)

    def merge(that: PipelineOp): PipelineOpMergeError \/ MergeResult = that match {
      // case that @ Project(_)    => delegateMerge(that)
      // case that @ Match(_)      => \/- (MergeResult.Left(this :: nil))
      // case that @ Redact(_)     => ???
      // case that @ Limit(value2) => \/- (MergeResult.Both(Limit((value2 - value) max 0) :: this :: nil))
      case that @ Skip(value2)  => \/- (MergeResult.Both(Skip(value min value2) :: nil))
      case that @ Unwind(_)     => \/- (MergeResult.Left(this :: nil))
      case that @ Group(_, _)   => ???
      case that @ Sort(_)       => \/- (MergeResult.Left(this :: nil))
      case that @ Out(_)        => \/- (MergeResult.Left(this :: nil))
      case that @ GeoNear(_, _, _, _, _, _, _, _, _) => \/- (MergeResult.Right(that :: nil))
      case _ => delegateMerge(that)
    }
  }
  case class Unwind(field: BsonField) extends SimpleOp("$unwind") {
    def rhs = Bson.Text("$" + field.asText)

    def merge(that: PipelineOp): PipelineOpMergeError \/ MergeResult = that match {
      // case that @ Project(_)  => delegateMerge(that)
      // case that @ Match(_)    => ???
      // case that @ Redact(_)   => ???
      // case that @ Limit(_)    => ???
      // case that @ Skip(_)     => ???
      case that @ Unwind(_)   => ???
      case that @ Group(_, _) => ???
      case that @ Sort(_)     => \/- (MergeResult.Right(that :: nil))
      case that @ Out(_)      => \/- (MergeResult.Left(this :: nil))
      case that @ GeoNear(_, _, _, _, _, _, _, _, _) => \/- (MergeResult.Right(that :: nil))
      case _ => delegateMerge(that)
    }
  }
  case class Group(grouped: Grouped, by: ExprOp) extends SimpleOp("$group") {
    def rhs = Bson.Doc(grouped.value.mapValues(_.bson) + ("_id" -> by.bson))

    def merge(that: PipelineOp): PipelineOpMergeError \/ MergeResult = that match {
      // case that @ Project(_)  => ???
      // case that @ Match(_)    => ???
      // case that @ Redact(_)   => ???
      // case that @ Limit(_)    => ???
      // case that @ Skip(_)     => ???
      // case that @ Unwind(_)   => ???
      case that @ Group(_, _) => ???
      case that @ Sort(_)     => ???
      case that @ Out(_)      => \/- (MergeResult.Left(this :: nil))
      case that @ GeoNear(_, _, _, _, _, _, _, _, _) => \/- (MergeResult.Right(that :: nil))
      case _ => delegateMerge(that)
    }
  }
  case class Sort(value: Map[String, SortType]) extends SimpleOp("$sort") {
    def rhs = Bson.Doc(value.mapValues(_.bson))

    def merge(that: PipelineOp): PipelineOpMergeError \/ MergeResult = that match {
      // case that @ Project(_)  => delegateMerge(that)
      // case that @ Match(_)    => \/- (MergeResult.Left(this :: nil))
      // case that @ Redact(_)   => ???
      // case that @ Limit(_)    => \/- (MergeResult.Left(this :: nil))
      // case that @ Skip(_)     => \/- (MergeResult.Left(this :: nil))
      // case that @ Unwind(_)   => ???
      // case that @ Group(_, _) => ???
      case that @ Sort(_)     => \/- (MergeResult.Left(this :: nil))  // ???
      case that @ Out(_)      => \/- (MergeResult.Left(this :: nil))
      case that @ GeoNear(_, _, _, _, _, _, _, _, _) => \/- (MergeResult.Right(that :: nil))
      case _ => delegateMerge(that)
    }
  }
  case class Out(collection: Collection) extends SimpleOp("$out") {
    def rhs = Bson.Text(collection.name)

    def merge(that: PipelineOp): PipelineOpMergeError \/ MergeResult = that match {
      // case that @ Project(_)  => delegateMerge(that)
      // case that @ Match(_)    => \/- (MergeResult.Left(this :: nil))
      // case that @ Redact(_)   => ???
      // case that @ Limit(_)    => \/- (MergeResult.Left(this :: nil))
      // case that @ Skip(_)     => \/- (MergeResult.Left(this :: nil))
      // case that @ Unwind(_)   => ???
      // case that @ Group(_, _) => ???
      // case that @ Sort(_)     => \/- (MergeResult.Left(this :: nil))
      case Out(_)                             => -\/ (PipelineOpMergeError(this, that, Some("Cannot merge multiple $out ops")))
      case GeoNear(_, _, _, _, _, _, _, _, _) => \/- (MergeResult.Right(that :: nil))
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
      // case that @ Project(_)  => delegateMerge(that)
      // case that @ Match(_)    => \/- (MergeResult.Left(this :: nil))
      // case that @ Redact(_)   => ???
      // case that @ Limit(_)    => \/- (MergeResult.Left(this :: nil))
      // case that @ Skip(_)     => \/- (MergeResult.Left(this :: nil))
      // case that @ Unwind(_)   => ???
      // case that @ Group(_, _) => ???
      // case that @ Sort(_)     => \/- (MergeResult.Left(this :: nil))
      // case that @ Out(_)      => delegateMerge(that)
      case GeoNear(_, _, _, _, _, _, _, _, _) => -\/(PipelineOpMergeError(this, that, Some("Cannot merge multiple $geoNear ops")))
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
        case x @ Include            => v.point[F]
        case x @ Exclude            => v.point[F]
        case x @ DocField(_)        => v.point[F]
        case x @ DocVar(_)          => v.point[F]
        case x @ Add(l, r)          => (mapUp0(l) |@| mapUp0(r))(Add(_, _))
        case x @ AddToSet(_)        => v.point[F]
        case x @ And(v)             => v.map(mapUp0 _).sequenceU.map(And(_))
        case x @ ArrayMap(a, b, c)  => (mapUp0(a) |@| mapUp0(c))(ArrayMap(_, b, _))
        case x @ Avg(v)             => mapUp0(v).map(Avg(_))
        case x @ Cmp(l, r)          => (mapUp0(l) |@| mapUp0(r))(Cmp(_, _))
        case x @ Concat(a, b, cs)   => (mapUp0(a) |@| mapUp0(b) |@| cs.map(mapUp0 _).sequenceU)(Concat(_, _, _))
        case x @ Cond(a, b, c)      => (mapUp0(a) |@| mapUp0(b) |@| mapUp0(c))(Cond(_, _, _))
        case x @ DayOfMonth(a)      => mapUp0(a).map(DayOfMonth(_))
        case x @ DayOfWeek(a)       => mapUp0(a).map(DayOfWeek(_))
        case x @ DayOfYear(a)       => mapUp0(a).map(DayOfYear(_))
        case x @ Divide(a, b)       => (mapUp0(a) |@| mapUp0(b))(Divide(_, _))
        case x @ Eq(a, b)           => (mapUp0(a) |@| mapUp0(b))(Eq(_, _))
        case x @ First(a)           => mapUp0(a).map(First(_))
        case x @ Gt(a, b)           => (mapUp0(a) |@| mapUp0(b))(Gt(_, _))
        case x @ Gte(a, b)          => (mapUp0(a) |@| mapUp0(b))(Gte(_, _))
        case x @ Hour(a)            => mapUp0(a).map(Hour(_))
        case x @ IfNull(a, b)       => (mapUp0(a) |@| mapUp0(b))(IfNull(_, _))
        case x @ Last(a)            => mapUp0(a).map(Last(_))
        case x @ Let(a, b)          => 
          type MapBsonField[X] = Map[BsonField, X]

          (Traverse[MapBsonField].sequence[F, ExprOp](a.map(t => t._1 -> mapUp0(t._2))) |@| mapUp0(b))(Let(_, _))

        case x @ Literal(_)         => v.point[F]
        case x @ Lt(a, b)           => (mapUp0(a) |@| mapUp0(b))(Lt(_, _))
        case x @ Lte(a, b)          => (mapUp0(a) |@| mapUp0(b))(Lte(_, _))
        case x @ Max(a)             => mapUp0(a).map(Max(_))
        case x @ Millisecond(a)     => mapUp0(a).map(Millisecond(_))
        case x @ Min(a)             => mapUp0(a).map(Min(_))
        case x @ Minute(a)          => mapUp0(a).map(Minute(_))
        case x @ Mod(a, b)          => (mapUp0(a) |@| mapUp0(b))(Mod(_, _))
        case x @ Month(a)           => mapUp0(a).map(Month(_))
        case x @ Multiply(a, b)     => (mapUp0(a) |@| mapUp0(b))(Multiply(_, _))
        case x @ Neq(a, b)          => (mapUp0(a) |@| mapUp0(b))(Neq(_, _))
        case x @ Not(a)             => mapUp0(a).map(Not(_))
        case x @ Or(a)              => a.map(mapUp0 _).sequenceU.map(Or(_))
        case x @ Push(a)            => v.point[F]
        case x @ Second(a)          => mapUp0(a).map(Second(_))
        case x @ Strcasecmp(a, b)   => (mapUp0(a) |@| mapUp0(b))(Strcasecmp(_, _))
        case x @ Substr(a, b, c)    => mapUp0(a).map(Substr(_, b, c))
        case x @ Subtract(a, b)     => (mapUp0(a) |@| mapUp0(b))(Subtract(_, _))
        case x @ Sum(a)             => mapUp0(a).map(Sum(_))
        case x @ ToLower(a)         => mapUp0(a).map(ToLower(_))
        case x @ ToUpper(a)         => mapUp0(a).map(ToUpper(_))
        case x @ Week(a)            => mapUp0(a).map(Week(_))
        case x @ Year(a)            => mapUp0(a).map(Year(_))
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

  private[ExprOp] abstract sealed class SimpleOp(op: String) extends ExprOp {
    def rhs: Bson

    def bson = Bson.Doc(Map(op -> rhs))
  }

  sealed trait IncludeExclude extends ExprOp
  case object Include extends IncludeExclude {
    def bson = Bson.Int32(1)
  }
  case object Exclude extends IncludeExclude {
    def bson = Bson.Int32(0)
  }

  sealed trait FieldLike extends ExprOp
  case class DocField(field: BsonField) extends FieldLike {
    def bson = Bson.Text("$" + field.asText)
  }
  case class DocVar(field: BsonField) extends FieldLike {
    def bson = Bson.Text("$$" + field.asText)
  }

  sealed trait GroupOp extends ExprOp
  case class AddToSet(field: DocField) extends SimpleOp("$addToSet") with GroupOp {
    def rhs = field.bson
  }
  case class Push(field: DocField) extends SimpleOp("$push") with GroupOp {
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

  sealed trait ProjOp extends ExprOp
  case class ArrayMap(input: ExprOp, as: String, in: ExprOp) extends SimpleOp("$map") {
    def rhs = Bson.Doc(Map(
      "input" -> input.bson,
      "as"    -> Bson.Text(as),
      "in"    -> in.bson
    ))
  }
  case class Let(vars: Map[BsonField, ExprOp], in: ExprOp) extends SimpleOp("$let") {
    def rhs = Bson.Doc(Map(
      "vars" -> Bson.Doc(vars.map(t => (t._1.asText, t._2.bson))),
      "in"   -> in.bson
    ))
  }
  case class Literal(value: Bson) extends SimpleOp("$literal") {
    def rhs = value
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
  case class Cond(predicate: ExprOp, ifTrue: ExprOp, ifFalse: ExprOp) extends CondOp {
    def bson = Bson.Arr(predicate.bson :: ifTrue.bson :: ifFalse.bson :: Nil)
  }
  case class IfNull(expr: ExprOp, replacement: ExprOp) extends CondOp {
    def bson = Bson.Arr(expr.bson :: replacement.bson :: Nil)
  }
}

