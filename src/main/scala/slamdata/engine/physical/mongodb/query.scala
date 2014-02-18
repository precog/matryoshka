package slamdata.engine.physical.mongodb

import scalaz.NonEmptyList

final case class Query(
  query:        Selector,
  comment:      Option[String] = None,
  explain:      Option[Boolean] = None,
  hint:         Option[Bson] = None,
  maxScan:      Option[Long] = None,
  max:          Option[Map[String, Bson]] = None,
  min:          Option[Map[String, Bson]] = None,
  orderby:      Option[Map[String, SortType]] = None,
  returnKey:    Option[Boolean] = None,
  showDiskLoc:  Option[Boolean] = None,
  snapshot:     Option[Boolean] = None
) {
  def bson = Bson.Doc(List[List[(String, Bson)]](
    List("$query" -> query.bson),
    comment.toList.map(comment => ("$comment", Bson.Text(comment))),
    explain.toList.map(explain => ("$explain", if (explain) Bson.Int32(1) else Bson.Int32(0))),
    hint.toList.map(hint => ("$hint", hint)),
    maxScan.toList.map(maxScan => ("$maxScan", Bson.Int64(maxScan))),
    max.toList.map(max => ("$max", Bson.Doc(max))),
    min.toList.map(min => ("$min", Bson.Doc(min))),
    orderby.toList.map(_.mapValues(_.bson)).map(map => ("orderby", Bson.Doc(map))),
    returnKey.toList.map(returnKey => ("$returnKey", if (returnKey) Bson.Int32(1) else Bson.Int32(0))),
    showDiskLoc.toList.map(showDiskLoc => ("$showDiskLoc", if (showDiskLoc) Bson.Int32(1) else Bson.Int32(0))),
    snapshot.toList.map(snapshot => ("$snapshot", if (snapshot) Bson.Int32(1) else Bson.Int32(0)))
  ).flatten.toMap)
}

sealed trait Selector {
  def bson: Bson
}

object Selector {
  final case class Doc(value: Map[String, Selector]) extends Selector {
    def bson = Bson.Doc(value.mapValues(_.bson))
  }

  private[Selector] abstract class SimpleSelector(val op: String) extends Selector {
    protected def rhs: Bson

    def bson = Bson.Doc(Map(op -> rhs))
  }

  sealed trait Comparison extends Selector
  case class Gt(rhs: Bson) extends SimpleSelector("$gt") with Comparison
  case class Gte(rhs: Bson) extends SimpleSelector("$gte") with Comparison
  case class In(rhs: Bson.Arr) extends SimpleSelector("$in") with Comparison
  case class Lt(rhs: Bson) extends SimpleSelector("$lt") with Comparison
  case class Lte(rhs: Bson) extends SimpleSelector("$lte") with Comparison
  case class Ne(rhs: Bson) extends SimpleSelector("$ne") with Comparison
  case class Nin(rhs: Bson.Arr) extends SimpleSelector("$nin") with Comparison

  sealed trait Logical extends Selector
  case class Or(conditions: List[Selector]) extends SimpleSelector("$or") with Logical {
    protected def rhs = Bson.Arr(conditions.map(_.bson))
  }
  case class And(conditions: List[Selector]) extends SimpleSelector("$or") with Logical {
    protected def rhs = Bson.Arr(conditions.map(_.bson))
  }
  case class Not(condition: Selector) extends SimpleSelector("$not") with Logical {
    protected def rhs = condition.bson
  }
  case class Nor(conditions: List[Selector]) extends SimpleSelector("$nor") with Logical {
    protected def rhs = Bson.Arr(conditions.map(_.bson))
  }

  sealed trait Element extends Selector
  case class Exists(exists: Boolean) extends SimpleSelector("$exists") with Element {
    protected def rhs = Bson.Bool(exists)
  }
  case class Type(bsonType: BsonType) extends SimpleSelector("$type") with Element {
    protected def rhs = Bson.Int32(bsonType.ordinal)
  }

  sealed trait Evaluation extends Selector
  case class Mod(divisor: Int, remainder: Int) extends SimpleSelector("$mod") with Evaluation {
    protected def rhs = Bson.Arr(Bson.Int32(divisor) :: Bson.Int32(remainder) :: Nil)
  }
  case class Regex(pattern: String) extends SimpleSelector("$regex") with Evaluation {
    protected def rhs = Bson.Regex(pattern)
  }
  case class Where(code: Js.Expr) extends SimpleSelector("$where") with Evaluation {
    protected def rhs = Bson.JavaScript(code)
  }

  sealed trait Geospatial extends Selector
  case class GeoWithin(geometry: String, coords: List[List[(Double, Double)]]) extends SimpleSelector("$geoWithin") with Geospatial {
    protected def rhs = Bson.Doc(Map(
      "$geometry" -> Bson.Doc(Map(
        "type"        -> Bson.Text(geometry),
        "coordinates" -> Bson.Arr(coords.map(v => Bson.Arr(v.map(t => Bson.Arr(Bson.Dec(t._1) :: Bson.Dec(t._2) :: Nil)))))
      ))
    ))
  }
  case class GeoIntersects(geometry: String, coords: List[List[(Double, Double)]]) extends SimpleSelector("$geoIntersects") with Geospatial {
    protected def rhs = Bson.Doc(Map(
      "$geometry" -> Bson.Doc(Map(
        "type"        -> Bson.Text(geometry),
        "coordinates" -> Bson.Arr(coords.map(v => Bson.Arr(v.map(t => Bson.Arr(Bson.Dec(t._1) :: Bson.Dec(t._2) :: Nil)))))
      ))
    ))
  }
  case class Near(lat: Double, long: Double, maxDistance: Double) extends SimpleSelector("$near") with Geospatial {
    protected def rhs = Bson.Doc(Map(
      "$geometry" -> Bson.Doc(Map(
        "type"        -> Bson.Text("Point"),
        "coordinates" -> Bson.Arr(Bson.Dec(long) :: Bson.Dec(lat) :: Nil)
      ))
    ))
  }
  case class NearSphere(lat: Double, long: Double, maxDistance: Double) extends SimpleSelector("$nearSphere") with Geospatial {
    protected def rhs = Bson.Doc(Map(
      "$geometry" -> Bson.Doc(Map(
        "type"        -> Bson.Text("Point"),
        "coordinates" -> Bson.Arr(Bson.Dec(long) :: Bson.Dec(lat) :: Nil)
      )),
      "$maxDistance" -> Bson.Dec(maxDistance)
    ))
  }

  sealed trait Arr extends Selector
  case class ContainsAll(selectors: Seq[Selector]) extends SimpleSelector("$all") with Arr {
    protected def rhs = Bson.Arr(selectors.map(_.bson))
  }
  case class ExistsElemMatch(selector: Selector) extends SimpleSelector("$elemMatch") with Arr {
    protected def rhs = selector.bson
  }
  case class HasSize(size: Int) extends SimpleSelector("$size") with Arr {
    protected def rhs = Bson.Int32(size)
  }

  sealed trait Proj extends Selector
  case class FirstElem(field: BsonField) extends Proj {
    def bson = Bson.Doc(Map(
      (field.bsonText + ".$") -> Bson.Int32(1)
    ))
  }
  case class FirstElemMatch(selector: Selector) extends SimpleSelector("$elemMatch") with Proj {
    protected def rhs = selector.bson
  }
  case class Slice(skip: Int, limit: Option[Int]) extends SimpleSelector("$") with Proj {
    protected def rhs = (limit.map { limit =>
      Bson.Arr(Bson.Int32(skip) :: Bson.Int32(limit) :: Nil)
    }).getOrElse(Bson.Int32(skip))
  }
}