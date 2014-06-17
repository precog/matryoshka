package slamdata.engine.physical.mongodb

import scalaz.{NonEmptyList, Foldable, Show, Cord, Semigroup}

import scalaz.std.list._

final case class FindQuery(
  query:        Selector,
  comment:      Option[String] = None,
  explain:      Option[Boolean] = None,
  hint:         Option[Bson] = None,
  maxScan:      Option[Long] = None,
  max:          Option[Map[BsonField, Bson]] = None,
  min:          Option[Map[BsonField, Bson]] = None,
  orderby:      Option[NonEmptyList[(BsonField, SortType)]] = None,
  returnKey:    Option[Boolean] = None,
  showDiskLoc:  Option[Boolean] = None,
  snapshot:     Option[Boolean] = None,
  natural:      Option[SortType] = None
) {
  def bson = Bson.Doc(List[List[(String, Bson)]](
    List("$query" -> query.bson),
    comment.toList.map    (comment      => ("$comment",     Bson.Text(comment))),
    explain.toList.map    (explain      => ("$explain",     if (explain) Bson.Int32(1) else Bson.Int32(0))),
    hint.toList.map       (hint         => ("$hint",        hint)),
    maxScan.toList.map    (maxScan      => ("$maxScan",     Bson.Int64(maxScan))),
    max.toList.map        (max          => ("$max",         Bson.Doc(max.map(mapField _)))),
    min.toList.map        (min          => ("$min",         Bson.Doc(min.map(mapField _)))),
    orderby.toList.map    (_.map { case (k, t) => k.asText -> t.bson }).map(ts => ("orderby", Bson.Doc(Map(ts.list: _*)))),
    returnKey.toList.map  (returnKey    => ("$returnKey",   if (returnKey) Bson.Int32(1) else Bson.Int32(0))),
    showDiskLoc.toList.map(showDiskLoc  => ("$showDiskLoc", if (showDiskLoc) Bson.Int32(1) else Bson.Int32(0))),
    snapshot.toList.map   (snapshot     => ("$snapshot",    if (snapshot) Bson.Int32(1) else Bson.Int32(0))),
    natural.toList.map    (natural      => "$natural" -> natural.bson)
  ).flatten.toMap)

  private def mapField[A](t: (BsonField, A)): (String, A) = t._1.asText -> t._2
}

sealed trait Selector {
  def bson: Bson

  import Selector._



  // TODO: Replace this with fixplate!!!
  def mapUp(f0: PartialFunction[Selector, Selector]): Selector = {
    val f0l = f0.lift

    mapUp0(s => f0l(s).getOrElse(s))
  }

  private def mapUp0(f: Selector => Selector): Selector = {
    f(this match {
      case s @ Doc(a)               => Doc(a.mapValues(_.mapUp0(f)))
      case s @ And(a)               => And(a.map(_.mapUp0(f)))
      case s @ ContainsAll(a)       => ContainsAll(a.map(_.mapUp0(f)))
      case s @ Eq(_)                => s
      case s @ Exists(_)            => s
      case s @ ExistsElemMatch(a)   => ExistsElemMatch(a.mapUp0(f))
      case s @ FirstElem(_)         => s
      case s @ FirstElemMatch(a)    => FirstElemMatch(a.mapUp0(f))
      case s @ GeoIntersects(_, _)  => s
      case s @ GeoWithin(_, _)      => s
      case s @ Gt(_)                => s
      case s @ Gte(_)               => s
      case s @ HasSize(_)           => s
      case s @ In(_)                => s
      case s @ Literal(_)           => s
      case s @ Lt(_)                => s
      case s @ Lte(_)               => s
      case s @ Mod(_, _)            => s
      case s @ Near(_, _, _)        => s
      case s @ NearSphere(_, _, _)  => s
      case s @ Neq(_)               => s
      case s @ Nin(_)               => s
      case s @ Nor(a)               => Nor(a.map(_.mapUp0(f)))
      case s @ Not(a)               => Not(a.mapUp0(f))
      case s @ Or(a)                => Or(a.map(_.mapUp0(f)))
      case s @ Regex(_)             => s
      case s @ Slice(_, _)          => s
      case s @ Type(_)              => s
      case s @ Where(_)             => s
    })
  }
}

object Selector {
  implicit val SelectorShow: Show[Selector] = new Show[Selector] {
    override def show(v: Selector): Cord = Cord(v.toString) // TODO
  }

  final case class Doc(value: Map[BsonField, Selector]) extends Selector {
    def bson = Bson.Doc(value.map(t => (t._1.asText, t._2.bson)))
  }

  private[Selector] abstract sealed class SimpleSelector(val op: String) extends Selector {
    protected def rhs: Bson

    def bson = Bson.Doc(Map(op -> rhs))
  }

  // This does not appear to be valid as literals may only occur on the right-hand side of 
  // relational operators. Nonetheless....
  case class Literal(bson: Bson) extends Selector 

  sealed trait Comparison extends Selector
  case class Eq(rhs: Bson) extends SimpleSelector("$eq") with Comparison
  case class Gt(rhs: Bson) extends SimpleSelector("$gt") with Comparison
  case class Gte(rhs: Bson) extends SimpleSelector("$gte") with Comparison
  case class In(rhs: Bson) extends SimpleSelector("$in") with Comparison
  case class Lt(rhs: Bson) extends SimpleSelector("$lt") with Comparison
  case class Lte(rhs: Bson) extends SimpleSelector("$lte") with Comparison
  case class Neq(rhs: Bson) extends SimpleSelector("$ne") with Comparison
  case class Nin(rhs: Bson) extends SimpleSelector("$nin") with Comparison

  sealed trait Logical extends Selector
  case class Or(conditions: NonEmptyList[Selector]) extends SimpleSelector("$or") with Logical {
    protected def rhs = Bson.Arr(conditions.list.map(_.bson))
  }
  case class And(conditions: NonEmptyList[Selector]) extends SimpleSelector("$and") with Logical {
    protected def rhs = Bson.Arr(conditions.list.map(_.bson))
  }
  case class Not(condition: Selector) extends SimpleSelector("$not") with Logical {
    protected def rhs = condition.bson
  }
  case class Nor(conditions: NonEmptyList[Selector]) extends SimpleSelector("$nor") with Logical {
    protected def rhs = Bson.Arr(conditions.list.map(_.bson))
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
      (field.asText + ".$") -> Bson.Int32(1)
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
  
  val SelectorAndSemigroup: Semigroup[Selector] = new Semigroup[Selector] {
    def append(s1: Selector, s2: => Selector): Selector = 
      if (s1 == s2) s1 
      else And(NonEmptyList(s1, s2))
  }
}