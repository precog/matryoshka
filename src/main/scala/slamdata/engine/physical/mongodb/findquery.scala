package slamdata.engine.physical.mongodb

import scala.collection.immutable.ListMap

import scalaz._
import Scalaz._

import slamdata.engine.{RenderTree, Terminal, NonTerminal}
import slamdata.engine.fp._

final case class FindQuery(
  query:        Selector,
  comment:      Option[String] = None,
  explain:      Option[Boolean] = None,
  hint:         Option[Bson] = None,
  maxScan:      Option[Long] = None,
  max:          Option[ListMap[BsonField, Bson]] = None,
  min:          Option[ListMap[BsonField, Bson]] = None,
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
    orderby.toList.map    (_.map { case (k, t) => k.asText -> t.bson }).map(ts => ("orderby", Bson.Doc(ListMap(ts.list: _*)))),
    returnKey.toList.map  (returnKey    => ("$returnKey",   if (returnKey) Bson.Int32(1) else Bson.Int32(0))),
    showDiskLoc.toList.map(showDiskLoc  => ("$showDiskLoc", if (showDiskLoc) Bson.Int32(1) else Bson.Int32(0))),
    snapshot.toList.map   (snapshot     => ("$snapshot",    if (snapshot) Bson.Int32(1) else Bson.Int32(0))),
    natural.toList.map    (natural      => "$natural" -> natural.bson)
  ).flatten.toListMap)

  private def mapField[A](t: (BsonField, A)): (String, A) = t._1.asText -> t._2
}

sealed trait Selector {
  def bson: Bson.Doc

  import Selector._

  // // TODO: Replace this with fixplate!!!
  
  def mapUpFields(f0: PartialFunction[BsonField, BsonField]): Selector = {
    val f0l = f0.lift

    mapUp0(s => f0l(s).getOrElse(s))
  }

  private def mapUp0(f: BsonField => BsonField): Selector = {
    this match {
      case Doc(pairs)          => Doc(pairs.map { case (field, expr) => f(field) -> expr })
      case And(left, right)    => And(left.mapUp0(f), right.mapUp0(f))
      case Or(left, right)     => Or(left.mapUp0(f), right.mapUp0(f))
      case Nor(left, right)    => Nor(left.mapUp0(f), right.mapUp0(f))
      case Where(_)            => this  // TODO: need to rename fields referenced in the JS?
    }
  }
}

object Selector {
  implicit def SelectorRenderTree[S <: Selector] = new RenderTree[Selector] {
    val SelectorNodeType = List("Selector")
    
    override def render(sel: Selector) = sel match {
      case and: And     => NonTerminal("And", and.flatten.map(render), SelectorNodeType)
      case or: Or       => NonTerminal("Or", or.flatten.map(render), SelectorNodeType)
      case nor: Nor     => NonTerminal("Nor", nor.flatten.map(render), SelectorNodeType)
      case where: Where => Terminal(where.bson.repr.toString, SelectorNodeType)
      case Doc(pairs)   => {
        val children = pairs.map {
          case (field, Expr(expr)) => Terminal(field + " -> " + expr, SelectorNodeType)
          case (field, notExpr @ NotExpr(_)) => Terminal(field + " -> " + notExpr, SelectorNodeType)
        }
        NonTerminal("Doc", children.toList, SelectorNodeType)
      }
    }
  }

  sealed trait Condition {
    def bson: Bson
  }

  private[Selector] abstract sealed class SimpleCondition(val op: String) extends Condition {
    protected def rhs: Bson

    def bson = Bson.Doc(ListMap(op -> rhs))
  }

  case class Literal(bson: Bson) extends Condition

  sealed trait Comparison extends Condition
  case class Eq(rhs: Bson) extends SimpleCondition("$eq") with Comparison {
    override def toString = s"Selector.Eq($rhs)"
  }
  case class Gt(rhs: Bson) extends SimpleCondition("$gt") with Comparison {
    override def toString = s"Selector.Gt($rhs)"
  }
  case class Gte(rhs: Bson) extends SimpleCondition("$gte") with Comparison {
    override def toString = s"Selector.Gte($rhs)"
  }
  case class In(rhs: Bson) extends SimpleCondition("$in") with Comparison {
    override def toString = s"Selector.In($rhs)"
  }
  case class Lt(rhs: Bson) extends SimpleCondition("$lt") with Comparison {
    override def toString = s"Selector.Lt($rhs)"
  }
  case class Lte(rhs: Bson) extends SimpleCondition("$lte") with Comparison {
    override def toString = s"Selector.Lte($rhs)"
  }
  case class Neq(rhs: Bson) extends SimpleCondition("$ne") with Comparison {
    override def toString = s"Selector.Neq($rhs)"
  }
  case class Nin(rhs: Bson) extends SimpleCondition("$nin") with Comparison {
    override def toString = s"Selector.Nin($rhs)"
  }

  sealed trait Element extends Condition
  case class Exists(exists: Boolean) extends SimpleCondition("$exists") with Element {
    protected def rhs = Bson.Bool(exists)

    override def toString = s"Selector.Exists($exists)"
  }
  case class Type(bsonType: BsonType) extends SimpleCondition("$type") with Element {
    protected def rhs = Bson.Int32(bsonType.ordinal)

    override def toString = s"Selector.Type($bsonType)"
  }

  sealed trait Evaluation extends Condition
  case class Mod(divisor: Int, remainder: Int) extends SimpleCondition("$mod") with Evaluation {
    protected def rhs = Bson.Arr(Bson.Int32(divisor) :: Bson.Int32(remainder) :: Nil)

    override def toString = s"Selector.Mod($divisor, $remainder)"
  }
  case class Regex(pattern: String, caseInsensitive: Boolean, multiLine: Boolean, extended: Boolean, dotAll: Boolean) extends Evaluation {
    def bson = Bson.Doc(ListMap(
                  "$regex" -> Bson.Text(pattern),  // Note: _not_ a Bson regex. Those can be use without the $regex operator, but we don't model that
                  "$options" -> 
                    Bson.Text(
                      (if (caseInsensitive) "i" else "") + 
                      (if (multiLine)       "m" else "") + 
                      (if (extended)        "x" else "") + 
                      (if (dotAll)          "s" else "")
                    )
                  ))

    override def toString = s"Selector.Regex($pattern, $caseInsensitive, $multiLine, $extended, $dotAll)"
  }
  // Note: $where can actually appear within a Doc (as in
  //     {foo: 1, $where: "this.bar < this.baz"}),
  // but the same thing can be accomplished with $and, so we always wrap $where
  // in its own Bson.Doc.
  case class Where(code: Js.Expr) extends Selector {
    def bson = Bson.Doc(ListMap("$where" -> Bson.JavaScript(code)))

    override def toString = s"Selector.Where($code)"
  }

  sealed trait Geospatial extends Condition
  case class GeoWithin(geometry: String, coords: List[List[(Double, Double)]]) extends SimpleCondition("$geoWithin") with Geospatial {
    protected def rhs = Bson.Doc(ListMap(
      "$geometry" -> Bson.Doc(ListMap(
        "type"        -> Bson.Text(geometry),
        "coordinates" -> Bson.Arr(coords.map(v => Bson.Arr(v.map(t => Bson.Arr(Bson.Dec(t._1) :: Bson.Dec(t._2) :: Nil)))))
      ))
    ))

    override def toString = s"Selector.GeoWithin($geometry, $coords)"
  }
  case class GeoIntersects(geometry: String, coords: List[List[(Double, Double)]]) extends SimpleCondition("$geoIntersects") with Geospatial {
    protected def rhs = Bson.Doc(ListMap(
      "$geometry" -> Bson.Doc(ListMap(
        "type"        -> Bson.Text(geometry),
        "coordinates" -> Bson.Arr(coords.map(v => Bson.Arr(v.map(t => Bson.Arr(Bson.Dec(t._1) :: Bson.Dec(t._2) :: Nil)))))
      ))
    ))

    override def toString = s"Selector.GeoIntersects($geometry, $coords)"
  }
  case class Near(lat: Double, long: Double, maxDistance: Double) extends SimpleCondition("$near") with Geospatial {
    protected def rhs = Bson.Doc(ListMap(
      "$geometry" -> Bson.Doc(ListMap(
        "type"        -> Bson.Text("Point"),
        "coordinates" -> Bson.Arr(Bson.Dec(long) :: Bson.Dec(lat) :: Nil)
      ))
    ))

    override def toString = s"Selector.Near($lat, $long, $maxDistance)"
  }
  case class NearSphere(lat: Double, long: Double, maxDistance: Double) extends SimpleCondition("$nearSphere") with Geospatial {
    protected def rhs = Bson.Doc(ListMap(
      "$geometry" -> Bson.Doc(ListMap(
        "type"        -> Bson.Text("Point"),
        "coordinates" -> Bson.Arr(Bson.Dec(long) :: Bson.Dec(lat) :: Nil)
      )),
      "$maxDistance" -> Bson.Dec(maxDistance)
    ))

    override def toString = s"Selector.NearSphere($lat, $long, $maxDistance)"
  }

  sealed trait Arr extends Condition
  case class All(selectors: Seq[Selector]) extends SimpleCondition("$all") with Arr {
    protected def rhs = Bson.Arr(selectors.map(_.bson))

    override def toString = s"Selector.All($selectors)"
  }
  case class ElemMatch(selector: Selector) extends SimpleCondition("$elemMatch") with Arr {
    protected def rhs = selector.bson

    override def toString = s"Selector.ElemMatch($selector)"
  }
  case class Size(size: Int) extends SimpleCondition("$size") with Arr {
    protected def rhs = Bson.Int32(size)

    override def toString = s"Selector.Size($size)"
  }
  
  sealed trait SelectorExpr {
    def bson: Bson
  }
 
  case class Expr(value: Condition) extends SelectorExpr {
    def bson = value.bson

    override def toString = s"Selector.Expr($value)"
  }
  
  case class NotExpr(value: Condition) extends SelectorExpr {
    def bson = Bson.Doc(ListMap("$not" -> value.bson))

    override def toString = s"Selector.NotExpr($value)"
  }
  
  case class Doc(pairs: ListMap[BsonField, SelectorExpr]) extends Selector {
    def bson = Bson.Doc(pairs.map { case (f, e) => f.asText -> e.bson })

    override def toString = {
      val children = pairs.map {
        case (field, Expr(expr)) => field + " -> " + expr
        case (field, notExpr @ NotExpr(_)) => field + " -> " + notExpr
      }
      "Selector.Doc(" + children.mkString(", ") + ")"
    }
  }
  object Doc {
    def apply(pairs: (BsonField, Condition)*): Doc =
      Doc(ListMap(pairs.map(t => t._1 -> Expr(t._2)): _*))
  }
    
  sealed trait CompoundSelector extends Selector {
    protected def op: String
    def left: Selector
    def right: Selector
    
    def flatten: List[Selector] = {
      def loop(sel: Selector) = sel match {
        case sel: CompoundSelector if (this.op == sel.op) => sel.flatten
        case _ => sel :: Nil
      }
      loop(left) ++ loop(right)
    }
  }
  
  private[Selector] abstract sealed class Abstract(val op: String) extends CompoundSelector {
    def bson = Bson.Doc(ListMap(op -> Bson.Arr(flatten.map(_.bson))))
  }
  
  case class And(left: Selector, right: Selector) extends Abstract("$and") {
    override def toString = s"Selector.And($left, $right)"
  }
  case class Or(left: Selector, right: Selector) extends Abstract("$or") {
    override def toString = s"Selector.Or($left, $right)"
  }
  case class Nor(left: Selector, right: Selector) extends Abstract("$nor") {
    override def toString = s"Selector.Nor($left, $right)"
  }
  
  implicit val SelectorAndSemigroup: Semigroup[Selector] = new Semigroup[Selector] {
    def append(s1: Selector, s2: => Selector): Selector = {
      def overlapping[A](s1: Set[A], s2: Set[A]) = !(s1 & s2).isEmpty
      
      (s1, s2) match {
        case _ if (s1 == s2)  => s1 
        case (Doc(pairs1), Doc(pairs2)) if (!overlapping(pairs1.keySet, pairs2.keySet)) 
                              => Doc(pairs1 ++ pairs2)
        case _                => And(s1, s2)
      }
    }
  }
}
