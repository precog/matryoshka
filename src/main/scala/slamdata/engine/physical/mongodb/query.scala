package slamdata.engine.physical.mongodb

import scalaz.NonEmptyList

final case class Query(filter: Selector)

sealed trait Selector

object Selector {
  final case class Obj(value: Map[String, Selector]) extends Selector

  sealed trait Comparison extends Selector
  case class Gt(value: Bson) extends Comparison
  case class Gte(value: Bson) extends Comparison
  case class In(value: Bson.Arr) extends Comparison
  case class Lt(value: Bson) extends Comparison
  case class Lte(value: Bson) extends Comparison
  case class Ne(value: Bson) extends Comparison
  case class Nin(value: Bson.Arr) extends Comparison

  sealed trait Logical extends Selector
  case class Or(conditions: NonEmptyList[Selector]) extends Logical
  case class And(conditions: NonEmptyList[Selector]) extends Logical
  case class Not(condition: Selector) extends Logical
  case class Nor(conditions: NonEmptyList[Selector]) extends Logical

  sealed trait Element extends Selector
  case class Exists(field: BsonField) extends Element
  case class Type(value: BsonType) extends Element

  sealed trait Evaluation extends Selector
  case class Mod(divisor: Int, remainder: Int) extends Evaluation
  case class Regex(pattern: String) extends Evaluation
  case class Where(code: String) extends Evaluation

  sealed trait Geospatial extends Selector
  case class GeoWithin(geometry: String, coords: List[List[(Double, Double)]]) extends Geospatial
  case class GeoIntersects(geometry: String, coords: List[List[(Double, Double)]]) extends Geospatial
  case class Near(lat: Double, long: Double, maxDistance: Double) extends Geospatial
  case class NearSphere(lat: Double, long: Double, maxDistance: Double) extends Geospatial

  sealed trait Array extends Selector
  case class ContainsAll(value: Seq[Selector]) extends Array
  case class ExistsElemMatch(value: Selector) extends Array
  case class HasSize(value: Int) extends Array

  sealed trait Projection extends Selector
  case class FirstElem(field: BsonField) extends Projection
  case class FirstElemMatch(value: Selector) extends Projection
  case class Slice(skip: Int, limit: Option[Int]) extends Projection
}