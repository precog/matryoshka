package slamdata.engine.physical.mongodb

final case class Pipeline(ops: Seq[PipelineOp])

import scalaz.NonEmptyList
import scalaz.\/

sealed trait PipelineOp

object PipelineOp {
  case class Reshape(value: Map[String, ExprOp \/ Reshape])
  case class Grouped(value: Map[String, ExprOp.GroupOp])

  case class Project(shape: Reshape) extends PipelineOp
  case class Match(query: Query) extends PipelineOp
  case class Limit(value: Int) extends PipelineOp
  case class Skip(value: Int) extends PipelineOp
  case class Unwind(field: String) extends PipelineOp
  case class Group(grouped: Grouped, by: ExprOp) extends PipelineOp
  case class Sort(value: Map[String, SortType]) extends PipelineOp
  case class GeoNear(near: (Double, Double), distanceField: BsonField, 
                     limit: Option[Int], maxDistance: Option[Double],
                     query: Option[Query], spherical: Option[Boolean],
                     distanceMultiplier: Option[Double], includeLocs: Option[String],
                     uniqueDocs: Option[Boolean]) extends PipelineOp
}

sealed trait ExprOp

object ExprOp {
  case class Literal(value: Bson) extends ExprOp

  case class DocField(field: BsonField) extends ExprOp

  sealed trait GroupOp extends ExprOp
  case class AddToSet(field: DocField) extends GroupOp
  case class Push(field: DocField) extends GroupOp
  case class First(value: ExprOp) extends GroupOp
  case class Last(value: ExprOp) extends GroupOp
  case class Max(value: ExprOp) extends GroupOp
  case class Min(value: ExprOp) extends GroupOp
  case class Avg(value: ExprOp) extends GroupOp
  case class Sum(value: ExprOp) extends GroupOp

  sealed trait BoolOp extends ExprOp
  case class And(values: NonEmptyList[ExprOp]) extends BoolOp
  case class Or(values: NonEmptyList[ExprOp]) extends BoolOp
  case class Not(value: ExprOp) extends BoolOp

  sealed trait CompOp extends ExprOp
  case class Cmp(left: ExprOp, right: ExprOp) extends CompOp
  case class Eq(left: ExprOp, right: ExprOp) extends CompOp
  case class Gt(left: ExprOp, right: ExprOp) extends CompOp
  case class Gte(left: ExprOp, right: ExprOp) extends CompOp
  case class Lt(left: ExprOp, right: ExprOp) extends CompOp
  case class Lte(left: ExprOp, right: ExprOp) extends CompOp
  case class Ne(left: ExprOp, right: ExprOp) extends CompOp

  sealed trait MathOp extends ExprOp
  case class Add(left: ExprOp, right: ExprOp) extends MathOp
  case class Divide(left: ExprOp, right: ExprOp) extends MathOp
  case class Mod(left: ExprOp, right: ExprOp) extends MathOp
  case class Multiply(left: ExprOp, right: ExprOp) extends MathOp
  case class Subtract(left: ExprOp, right: ExprOp) extends MathOp

  sealed trait StringOp extends ExprOp
  case class Concat(first: ExprOp, second: ExprOp, others: ExprOp*) extends StringOp
  case class Strcasecmp(left: ExprOp, right: ExprOp) extends StringOp
  case class Substr(value: ExprOp, start: Int, count: Int) extends StringOp
  case class ToLower(value: ExprOp) extends StringOp
  case class ToUpper(value: ExprOp) extends StringOp

  sealed trait DateOp extends ExprOp
  case class DayOfYear(date: ExprOp) extends DateOp
  case class DayOfMonth(date: ExprOp) extends DateOp
  case class DayOfWeek(date: ExprOp) extends DateOp
  case class Year(date: ExprOp) extends DateOp
  case class Month(date: ExprOp) extends DateOp
  case class Week(date: ExprOp) extends DateOp
  case class Hour(date: ExprOp) extends DateOp
  case class Minute(date: ExprOp) extends DateOp
  case class Second(date: ExprOp) extends DateOp
  case class Millisecond(date: ExprOp) extends DateOp

  sealed trait CondOp extends ExprOp
  case class Cond(predicate: ExprOp, ifTrue: ExprOp, ifFalse: ExprOp) extends CondOp
  case class IfNull(expr: ExprOp, replacement: ExprOp) extends CondOp
}

