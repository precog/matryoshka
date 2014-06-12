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

final case class Pipeline(ops: List[PipelineOp]) {
  def repr: java.util.List[DBObject] = ops.foldLeft(new java.util.ArrayList[DBObject](): java.util.List[DBObject]) {
    case (list, op) =>
      list.add(op.bson.repr)

      list
  }

  def merge(that: Pipeline): PipelineMergeError \/ Pipeline = {
    def merge0(merged: List[PipelineOp], left: List[PipelineOp], right: List[PipelineOp]): PipelineMergeError \/ List[PipelineOp] = {
      (left, right) match {
        case (left, right) if left == right => \/- (merged ++ left)

        case (left, Nil) => \/- (merged ++ left)
        case (Nil, right) => \/- (merged ++ right)

        case (lh :: lt, rh :: rt) => 
          for {
            h <- lh.merge(rh).leftMap(_ => PipelineMergeError(merged, left, right)) // FIXME: Try commuting!!!!
            m <- merge0(merged ++ h, lt, rt)
          } yield m
      }
    }

    merge0(Nil, this.ops, that.ops).map(Pipeline.apply)
  }
}

sealed trait PipelineOp {
  def bson: Bson.Doc

  def commutesWith(that: PipelineOp): Boolean = false

  def merge(that: PipelineOp): PipelineOpMergeError \/ List[PipelineOp]
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

    def merge(that: PipelineOp): PipelineOpMergeError \/ List[PipelineOp] = that match {
      case that @ Project(_)  => \/- (Project(this.shape |+| that.shape) :: Nil)
      case that @ Match(_)    => \/- (that :: this :: Nil)
      case that @ Redact(_)   => \/- (that :: this :: Nil)
      case that @ Limit(_)    => ???
      case that @ Skip(_)     => ???
      case that @ Unwind(_)   => ???
      case that @ Group(_, _) => ???
      case that @ Sort(_)     => ???
      case that @ Out(_)      => ???
      case that @ GeoNear(_, _, _, _, _, _, _, _, _) => ???
    }
  }
  case class Match(selector: Selector) extends SimpleOp("$match") {
    def rhs = selector.bson

    def merge(that: PipelineOp): PipelineOpMergeError \/ List[PipelineOp] = that match {
      case that @ Project(_)  => that.merge(this)
      case that @ Match(_)    => ???
      case that @ Redact(_)   => ???
      case that @ Limit(_)    => ???
      case that @ Skip(_)     => ???
      case that @ Unwind(_)   => ???
      case that @ Group(_, _) => ???
      case that @ Sort(_)     => ???
      case that @ Out(_)      => ???
      case that @ GeoNear(_, _, _, _, _, _, _, _, _) => ???
    }
  }
  case class Redact(value: ExprOp) extends SimpleOp("$redact") {
    def rhs = value.bson

    def merge(that: PipelineOp): PipelineOpMergeError \/ List[PipelineOp] = that match {
      case that @ Project(_)  => that.merge(this)
      case that @ Match(_)    => ???
      case that @ Redact(_)   => ???
      case that @ Limit(_)    => ???
      case that @ Skip(_)     => ???
      case that @ Unwind(_)   => ???
      case that @ Group(_, _) => ???
      case that @ Sort(_)     => ???
      case that @ Out(_)      => ???
      case that @ GeoNear(_, _, _, _, _, _, _, _, _) => ???
    }
  }
  case class Limit(value: Long) extends SimpleOp("$limit") {
    def rhs = Bson.Int64(value)

    def merge(that: PipelineOp): PipelineOpMergeError \/ List[PipelineOp] = that match {
      case that @ Project(_)  => ???
      case that @ Match(_)    => ???
      case that @ Redact(_)   => ???
      case that @ Limit(_)    => ???
      case that @ Skip(_)     => ???
      case that @ Unwind(_)   => ???
      case that @ Group(_, _) => ???
      case that @ Sort(_)     => ???
      case that @ Out(_)      => ???
      case that @ GeoNear(_, _, _, _, _, _, _, _, _) => ???
    }
  }
  case class Skip(value: Long) extends SimpleOp("$skip") {
    def rhs = Bson.Int64(value)

    def merge(that: PipelineOp): PipelineOpMergeError \/ List[PipelineOp] = that match {
      case that @ Project(_)  => ???
      case that @ Match(_)    => ???
      case that @ Redact(_)   => ???
      case that @ Limit(_)    => ???
      case that @ Skip(_)     => ???
      case that @ Unwind(_)   => ???
      case that @ Group(_, _) => ???
      case that @ Sort(_)     => ???
      case that @ Out(_)      => ???
      case that @ GeoNear(_, _, _, _, _, _, _, _, _) => ???
    }
  }
  case class Unwind(field: BsonField) extends SimpleOp("$unwind") {
    def rhs = Bson.Text("$" + field.asText)

    def merge(that: PipelineOp): PipelineOpMergeError \/ List[PipelineOp] = that match {
      case that @ Project(_)  => ???
      case that @ Match(_)    => ???
      case that @ Redact(_)   => ???
      case that @ Limit(_)    => ???
      case that @ Skip(_)     => ???
      case that @ Unwind(_)   => ???
      case that @ Group(_, _) => ???
      case that @ Sort(_)     => ???
      case that @ Out(_)      => ???
      case that @ GeoNear(_, _, _, _, _, _, _, _, _) => ???
    }
  }
  case class Group(grouped: Grouped, by: ExprOp) extends SimpleOp("$group") {
    def rhs = Bson.Doc(grouped.value.mapValues(_.bson) + ("_id" -> by.bson))

    def merge(that: PipelineOp): PipelineOpMergeError \/ List[PipelineOp] = that match {
      case that @ Project(_)  => ???
      case that @ Match(_)    => ???
      case that @ Redact(_)   => ???
      case that @ Limit(_)    => ???
      case that @ Skip(_)     => ???
      case that @ Unwind(_)   => ???
      case that @ Group(_, _) => ???
      case that @ Sort(_)     => ???
      case that @ Out(_)      => ???
      case that @ GeoNear(_, _, _, _, _, _, _, _, _) => ???
    }
  }
  case class Sort(value: Map[String, SortType]) extends SimpleOp("$sort") {
    def rhs = Bson.Doc(value.mapValues(_.bson))

    def merge(that: PipelineOp): PipelineOpMergeError \/ List[PipelineOp] = that match {
      case that @ Project(_)  => ???
      case that @ Match(_)    => ???
      case that @ Redact(_)   => ???
      case that @ Limit(_)    => ???
      case that @ Skip(_)     => ???
      case that @ Unwind(_)   => ???
      case that @ Group(_, _) => ???
      case that @ Sort(_)     => ???
      case that @ Out(_)      => ???
      case that @ GeoNear(_, _, _, _, _, _, _, _, _) => ???
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

    def merge(that: PipelineOp): PipelineOpMergeError \/ List[PipelineOp] = that match {
      case that @ Project(_)  => ???
      case that @ Match(_)    => ???
      case that @ Redact(_)   => ???
      case that @ Limit(_)    => ???
      case that @ Skip(_)     => ???
      case that @ Unwind(_)   => ???
      case that @ Group(_, _) => ???
      case that @ Sort(_)     => ???
      case that @ Out(_)      => ???
      case that @ GeoNear(_, _, _, _, _, _, _, _, _) => ???
    }
  }
  case class Out(collection: Collection) extends SimpleOp("$out") {
    def rhs = Bson.Text(collection.name)

    def merge(that: PipelineOp): PipelineOpMergeError \/ List[PipelineOp] = that match {
      case that @ Project(_)  => ???
      case that @ Match(_)    => ???
      case that @ Redact(_)   => ???
      case that @ Limit(_)    => ???
      case that @ Skip(_)     => ???
      case that @ Unwind(_)   => ???
      case that @ Group(_, _) => ???
      case that @ Sort(_)     => ???
      case that @ Out(_)      => ???
      case that @ GeoNear(_, _, _, _, _, _, _, _, _) => ???
    }
  }
}

sealed trait ExprOp {
  def bson: Bson
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
  case class Concat(first: ExprOp, second: ExprOp, others: ExprOp*) extends SimpleOp("$concat") with StringOp {
    def rhs = Bson.Arr(first.bson :: second.bson :: others.toList.map(_.bson))
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

