package slamdata.engine.sql

import scalaz.{Show, Cord}

sealed trait Node {
  def sql: String

  def children: List[Node]

  protected def _q(s: String): String = "\"" + s + "\""
}
trait NodeInstances {
  implicit val NodeShow = new Show[Node] {
    override def show(v: Node) = Cord(v.toString)
  }
}
object Node extends NodeInstances

final case class SelectStmt(projections:  List[Proj],
                            relations:    List[SqlRelation],
                            filter:       Option[Expr],
                            groupBy:      Option[GroupBy],
                            orderBy:      Option[OrderBy],
                            limit:        Option[Int],
                            offset:       Option[Int]) extends Node {
  def sql =
    List(Some("select"),
        Some(projections.map(_.sql).mkString(", ")),
        relations.headOption.map(_ => "from " + relations.map(_.sql).mkString(", ")),
        filter.map(x => "where " + x.sql),
        groupBy.map(_.sql),
        orderBy.map(_.sql),
        limit.map(x => "limit " + x.toString),
        offset.map(x => "offset " + x.toString)).flatten.mkString(" ")

  def children: List[Node] = projections.toList ++ relations ++ filter.toList ++ groupBy.toList ++ orderBy.toList

  def namedProjections: List[(String, Expr)] = projections.toList.zipWithIndex.map {
    case (Proj(expr, None), index) => index.toString -> expr
    case (Proj(expr, Some(name)), index) => name -> expr
  }
}

case class Proj(expr: Expr, alias: Option[String]) extends Node {  
  def sql = List(Some(expr.sql), alias).flatten.mkString(" as ")

  def children = expr :: Nil
}

sealed trait Expr extends Node

sealed trait SetExpr extends Expr

final case class Subselect(select: SelectStmt) extends SetExpr {
  def sql = List("(", select.sql, ")") mkString ""

  def children = select :: Nil
}

final case class SetLiteral(set: List[Expr]) extends SetExpr {
  def sql = set.map(_.sql).mkString("(", ", ", ")")

  def children = set.toList
}

case object Wildcard extends Expr {
  def sql = "*"

  def children = Nil
}

final case class Binop(lhs: Expr, rhs: Expr, op: BinaryOperator) extends Expr {
  def sql = op match {
    case FieldDeref => rhs match {
      case StringLiteral(str) => List("(", lhs.sql, ").", str) mkString ""
      case _ => List("(", lhs.sql, "){", rhs.sql, "}") mkString ""
    }
    case IndexDeref => List("(", lhs.sql, ")[", rhs.sql, "]") mkString ""
    case _ => List("(" + lhs.sql + ")", op.sql, "(" + rhs.sql + ")") mkString " "
  }

  def children = lhs :: rhs :: Nil
}

sealed abstract class BinaryOperator(val sql: String) extends Node with ((Expr, Expr) => Binop) {
  def apply(lhs: Expr, rhs: Expr): Binop = Binop(lhs, rhs, this)

  val name = "(" + sql + ")"

  def children = Nil
}

case object Or      extends BinaryOperator("or")
case object And     extends BinaryOperator("and")
case object Eq      extends BinaryOperator("=")
case object Neq     extends BinaryOperator("<>")
case object Ge      extends BinaryOperator(">=")
case object Gt      extends BinaryOperator(">")
case object Le      extends BinaryOperator("<=")
case object Lt      extends BinaryOperator("<")
case object Like    extends BinaryOperator("like")
case object NotLike extends BinaryOperator("not like")
case object Plus    extends BinaryOperator("+")
case object Minus   extends BinaryOperator("-")
case object Mult    extends BinaryOperator("*")
case object Div     extends BinaryOperator("/")
case object In      extends BinaryOperator("in")
case object NotIn   extends BinaryOperator("not in")
case object Between extends BinaryOperator("between")
case object FieldDeref extends BinaryOperator("{}")
case object IndexDeref extends BinaryOperator("[]")

final case class Unop(expr: Expr, op: UnaryOperator) extends Expr {
  def sql = List(op.sql, "(", expr.sql, ")") mkString " "

  def children = expr :: Nil
}

sealed abstract class UnaryOperator(val sql: String) extends Node with (Expr => Unop) {
  def apply(expr: Expr): Unop = Unop(expr, this)

  val name = sql

  def children = Nil
}

case object Not         extends UnaryOperator("not")
case object Exists      extends UnaryOperator("exists")
case object Positive    extends UnaryOperator("+")
case object Negative    extends UnaryOperator("-")
case object Distinct    extends UnaryOperator("distinct")
case object YearFrom    extends UnaryOperator("year from")
case object MonthFrom   extends UnaryOperator("month from")
case object DayFrom     extends UnaryOperator("day from")
case object HourFrom    extends UnaryOperator("hour from")
case object MinuteFrom  extends UnaryOperator("minute from")
case object SecondFrom  extends UnaryOperator("second from")
case object ToDate      extends UnaryOperator("date")
case object ToInterval  extends UnaryOperator("interval")

final case class Ident(name: String) extends Expr {
  def sql = name

  def children = Nil
}

final case class InvokeFunction(name: String, args: List[Expr]) extends Expr {
  def sql = List(name, "(", args.map(_.sql) mkString ", ", ")") mkString ""

  def children = args.toList
}

final case class Case(cond: Expr, expr: Expr) extends Node {
  def sql = List("when", cond.sql, "then", expr.sql) mkString " "

  def children = cond :: expr :: Nil
}

final case class Match(expr: Expr, cases: List[Case], default: Option[Expr]) extends Expr {
  def sql = List(Some("case"), Some(expr.sql), Some(cases.map(_.sql) mkString " "), default.map(d => "else " + d.sql), Some("end")).flatten.mkString(" ")

  def children = expr :: cases.toList ++ default.toList
}

final case class Switch(cases: List[Case], default: Option[Expr]) extends Expr {
  def sql = List(Some("case"), Some(cases.map(_.sql) mkString " "), default.map(d => "else " + d.sql), Some("end")).flatten.mkString(" ")

  def children = cases.toList ++ default.toList
}

sealed trait LiteralExpr extends Expr {
  def children = Nil
}

final case class IntLiteral(v: Long) extends LiteralExpr {
  def sql = v.toString
}
final case class FloatLiteral(v: Double) extends LiteralExpr {
  def sql = v.toString
}
final case class StringLiteral(v: String) extends LiteralExpr {
  def sql = _q(v)
}

final case class NullLiteral() extends LiteralExpr {
  def sql = "null"
}

sealed trait SqlRelation extends Node

final case class TableRelationAST(name: String, alias: Option[String]) extends SqlRelation {
  def sql = List(Some(name), alias).flatten.mkString(" ")

  def children = Nil
}

final case class SubqueryRelationAST(subquery: SelectStmt, alias: String) extends SqlRelation {
  def sql = List("(", subquery.sql, ")", "as", alias) mkString " "

  def children = subquery :: Nil
}

sealed abstract class JoinType(val sql: String)
case object LeftJoin extends JoinType("left join")
case object RightJoin extends JoinType("right join")
case object InnerJoin extends JoinType("inner join")
case object FullJoin extends JoinType("full join")

final case class JoinRelation(left: SqlRelation, right: SqlRelation, tpe: JoinType, clause: Expr) extends SqlRelation {
  def sql = List(left.sql, tpe.sql, right.sql, "on", "(", clause.sql, ")") mkString " "

  def children = left :: right :: clause :: Nil
}

sealed trait OrderType
case object ASC extends OrderType
case object DESC extends OrderType

final case class GroupBy(keys: List[Expr], having: Option[Expr]) extends Node {
  def sql = List(Some("group by"), Some(keys.map(_.sql).mkString(", ")), having.map(e => "having " + e.sql)).flatten.mkString(" ")

  def children = keys.toList ++ having.toList
}

final case class OrderBy(keys: List[(Expr, OrderType)]) extends Node {
  def sql = List("order by", keys map (x => x._1.sql + " " + x._2.toString) mkString ", ") mkString " "

  def children = keys.map(_._1).toList
}
