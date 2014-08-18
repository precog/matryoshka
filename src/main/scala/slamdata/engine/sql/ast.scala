package slamdata.engine.sql

import scalaz._
import Scalaz._

import slamdata.engine.{RenderTree, Terminal, NonTerminal}

sealed trait Node {
  def sql: String

  def children: List[Node]

  protected def _q(s: String): String = "\"" + s + "\""
}
trait NodeInstances {
  implicit def NodeRenderTree[A <: Node]: RenderTree[A] = new RenderTree[A] {
    override def render(n: A) = {
      n match {
        case SelectStmt(projections, relations, filter, groupBy, orderBy, limit, offset) => 
          NonTerminal("",
                      projections.map(p => NodeRenderTree.render(p)) ++
                        (relations.map(r => NodeRenderTree.render(r)) ::
                          filter.map(f => NodeRenderTree.render(f)) ::
                          groupBy.map(g => NodeRenderTree.render(g)) ::
                          orderBy.map(o => NodeRenderTree.render(o)) ::
                          limit.map(l => Terminal(l.toString, List("AST", "Limit"))) ::
                          offset.map(o => Terminal(o.toString, List("AST", "Offset"))) ::
                          Nil).flatten,
                    List("AST", "Select"))

        case Proj(expr, Some(alias)) => NonTerminal(alias, NodeRenderTree.render(expr) :: Nil, List("AST", "Proj"))
        case Proj(expr, None)        => NonTerminal("", NodeRenderTree.render(expr) :: Nil, List("AST", "Proj"))

        case SubqueryRelationAST(select, alias) => NonTerminal("Subquery as " + alias, NodeRenderTree.render(select) :: Nil, List("AST", "SubqueryRelation"))

        case TableRelationAST(name, Some(alias)) => Terminal(name + " as " + alias, List("AST", "TableRelation"))
        case TableRelationAST(name, None)        => Terminal(name, List("AST", "TableRelation"))

        case CrossRelation(left, right) => NonTerminal("Cross", NodeRenderTree.render(left) :: NodeRenderTree.render(right) :: Nil, List("AST", "CrossRelation"))

        case JoinRelation(left, right, jt, clause) => NonTerminal(s"Join ($jt)",
          NodeRenderTree.render(left) :: NodeRenderTree.render(right) :: NodeRenderTree.render(clause) :: Nil,
          List("AST", "JoinRelation"))

        case OrderBy(keys) => NonTerminal("", keys.map { case (x, t) => NonTerminal(t.toString, NodeRenderTree.render(x) :: Nil, List("AST", "OrderType"))}, List("AST", "OrderBy"))

        case GroupBy(keys, Some(having)) => NonTerminal("", keys.map(NodeRenderTree.render(_)) :+ NodeRenderTree.render(having), List("AST", "GroupBy"))
        case GroupBy(keys, None)         => NonTerminal("", keys.map(NodeRenderTree.render(_)), List("AST", "GroupBy"))

        case Subselect(select) => NodeRenderTree.render(select)

        case SetLiteral(exprs) => NonTerminal("", exprs.map(NodeRenderTree.render(_)), List("AST", "Set"))

        case InvokeFunction(name, args) => NonTerminal(name, args.map(NodeRenderTree.render(_)), List("AST", "InvokeFunction"))

        case Case(cond, expr) => NonTerminal("", NodeRenderTree.render(cond) :: NodeRenderTree.render(expr) :: Nil, List("AST", "Case"))

        case Match(expr, cases, Some(default)) => NonTerminal("", NodeRenderTree.render(expr) :: (cases.map(NodeRenderTree.render(_)) :+ NodeRenderTree.render(default)), List("AST", "Match"))
        case Match(expr, cases, None)          => NonTerminal("", NodeRenderTree.render(expr) :: cases.map(NodeRenderTree.render(_)), List("AST", "Match"))

        case Switch(cases, Some(default)) => NonTerminal("", cases.map(NodeRenderTree.render(_)) :+ NodeRenderTree.render(default), List("AST", "Switch"))
        case Switch(cases, None)          => NonTerminal("", cases.map(NodeRenderTree.render(_)), List("AST", "Switch"))

        case Binop(lhs, rhs, op) => NonTerminal(op.toString, NodeRenderTree.render(lhs) :: NodeRenderTree.render(rhs) :: Nil, List("AST", "Binop"))

        case Unop(expr, op) => NonTerminal(op.sql, NodeRenderTree.render(expr) :: Nil, List("AST", "Unop"))

        case Wildcard => Terminal("*", List("AST", "Wildcard"))

        case Ident(name) => Terminal(name, List("AST", "Ident"))

        case x: LiteralExpr => Terminal(x.sql, List("AST", "LiteralExpr"))
      }
    }
  }
}

object Node extends NodeInstances

final case class SelectStmt(projections:  List[Proj],
                            relations:    Option[SqlRelation],
                            filter:       Option[Expr],
                            groupBy:      Option[GroupBy],
                            orderBy:      Option[OrderBy],
                            limit:        Option[Long],
                            offset:       Option[Long]) extends Node {
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

  // TODO: move this logic to another file where it can be used by both the type checker and compiler?
  def namedProjections: List[(String, Expr)] = {
    def extractName(expr: Expr): Option[String] = expr match {
      case Ident(name)                               => Some(name)
      case Binop(_, StringLiteral(name), FieldDeref) => Some(name)
      case Binop(Binop(_, StringLiteral(name), FieldDeref), Wildcard, FieldDeref) => Some(name)
      case Binop(Binop(_, StringLiteral(name), FieldDeref), Wildcard, IndexDeref) => Some(name)
      case Binop(Ident(name), Wildcard, FieldDeref)  => Some(name)
      case Binop(Ident(name), Wildcard, IndexDeref)  => Some(name)
      case _                                         => None
    }
    projections.toList.zipWithIndex.map {
      case (Proj(expr, Some(name)), _) => name -> expr
      case (Proj(expr, None), index)   => extractName(expr).getOrElse(index.toString()) -> expr
    }
  }

  def mapUpM[F[_]: Monad](select: SelectStmt => F[SelectStmt],
                          proj: Proj => F[Proj],
                          relation: SqlRelation => F[SqlRelation],
                          expr: Expr => F[Expr],
                          groupBy: GroupBy => F[GroupBy],
                          orderBy: OrderBy => F[OrderBy]): F[SelectStmt] = {

    def selectLoop(node: SelectStmt): F[SelectStmt] = node match {
      case SelectStmt(p, r, f, g, o, limit, offset) => (for {
        p2 <- p.map(projLoop).sequence
        r2 <- r.map(relationLoop).sequence
        f2 <- f.map(exprLoop).sequence
        g2 <- g.map(groupByLoop).sequence
        o2 <- o.map(orderByLoop).sequence
      } yield SelectStmt(p2, r2, f2, g2, o2, limit, offset)).flatMap(select)
    }

    def projLoop(node: Proj): F[Proj] = (for {
      x2 <- exprLoop(node.expr)
    } yield Proj(x2, node.alias)).flatMap(proj)

    def relationLoop(node: SqlRelation): F[SqlRelation] = node match {
      case t @ TableRelationAST(_, _) => relation(t)

      case SubqueryRelationAST(s, alias) =>
        (for {
          s2 <- selectLoop(s)
        } yield SubqueryRelationAST(s2, alias)).flatMap(relation)

      case CrossRelation(left, right) =>
        (for {
          l2 <- relationLoop(left)
          r2 <- relationLoop(right)
        } yield CrossRelation(l2, r2)).flatMap(relation)

      case JoinRelation(left, right, jt, expr) =>
        (for {
          l2 <- relationLoop(left)
          r2 <- relationLoop(right)
          x2 <- exprLoop(expr)
        } yield JoinRelation(l2, r2, jt, x2)).flatMap(relation)
    }

    def exprLoop(node: Expr): F[Expr] = node match {
      case Unop(x, op) =>
        (for {
          x2 <- exprLoop(x)
        } yield Unop(x2, op)).flatMap(expr)

      case Binop(left, right, op) =>
        (for {
          l2 <- exprLoop(left)
          r2 <- exprLoop(right)
        } yield Binop(l2, r2, op)).flatMap(expr)

      case InvokeFunction(name, args) =>
        (for {
          a2 <- args.map(exprLoop).sequence
        } yield InvokeFunction(name, a2)).flatMap(expr)

      case SetLiteral(set) =>
        (for {
          s2 <- set.map(exprLoop).sequence
        } yield SetLiteral(s2)).flatMap(expr)

      case Match(x, cases, default) =>
        (for {
          x2 <- exprLoop(x)
          c2 <- cases.map(c => Case(exprLoop(c.cond), exprLoop(c.expr))).sequence
          d2 <- default.map(exprLoop).sequence
        } yield Match(x2, c2, d2)).flatMap(expr)

      case Switch(cases, default) =>
        (for {
          c2 <- cases.map(c => Case(exprLoop(c.cond), exprLoop(c.expr))).sequence
          d2 <- default.map(exprLoop).sequence
        } yield Switch(c2, d2)).flatMap(expr)

      case Subselect(sel) =>
        (for {
          s2 <- selectLoop(sel)
        } yield Subselect(s2)).flatMap(expr)

      case i @ Ident(_)         => expr(i)
      case w @ Wildcard         => expr(w)
      case l @ IntLiteral(_)    => expr(l)
      case l @ FloatLiteral(_)  => expr(l)
      case l @ StringLiteral(_) => expr(l)
      case l @ NullLiteral()    => expr(l)
    }

    def groupByLoop(node: GroupBy): F[GroupBy] = (for {
      k2 <- node.keys.map(exprLoop).sequence
      h2 <- node.having.map(exprLoop).sequence
    } yield GroupBy(k2, h2)).flatMap(groupBy)

    def orderByLoop(node: OrderBy): F[OrderBy] = (for {
      k2 <- node.keys.map { case (key, orderType) => exprLoop(key).map(_ -> orderType) }.sequence
    } yield OrderBy(k2)).flatMap(orderBy)

    selectLoop(this)
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

  override def equals(that: Any) = that match {
    case x : BinaryOperator if (sql == x.sql) => true
    case _ => false
  }

  override def hashCode = sql.hashCode

  override def toString = sql
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
case object Mod     extends BinaryOperator("%")
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

sealed trait SqlRelation extends Node {
  def namedRelations: Map[String, List[NamedRelation]] = {
    def collect(n: SqlRelation): List[(String, NamedRelation)] = n match {
      case t @ TableRelationAST(_, _) => (t.aliasName -> t) :: Nil
      case t @ SubqueryRelationAST(_, _) => (t.aliasName -> t) :: Nil
      case CrossRelation(left, right) => collect(left) ++ collect(right)
      case JoinRelation(left, right, _, _) => collect(left) ++ collect(right)
    }

    collect(this).groupBy(_._1).mapValues(_.map(_._2))
  }
}

sealed trait NamedRelation extends SqlRelation {
  def aliasName: String
}

final case class TableRelationAST(name: String, alias: Option[String]) extends NamedRelation {
  def sql = List(Some(name), alias).flatten.mkString(" ")

  def aliasName = alias.getOrElse(name)

  def children = Nil
}

final case class SubqueryRelationAST(subquery: SelectStmt, aliasName: String) extends NamedRelation {
  def sql = List("(", subquery.sql, ")", "as", aliasName) mkString " "

  def children = subquery :: Nil
}

final case class CrossRelation(left: SqlRelation, right: SqlRelation) extends SqlRelation {
  def sql = List(left.sql, "CROSS JOIN", right.sql).mkString(" ")

  def children = left :: right :: Nil
}

final case class JoinRelation(left: SqlRelation, right: SqlRelation, tpe: JoinType, clause: Expr) extends SqlRelation {
  def sql = List(left.sql, tpe.sql, right.sql, "on", "(", clause.sql, ")") mkString " "

  def children = left :: right :: clause :: Nil
}

sealed abstract class JoinType(val sql: String)
case object LeftJoin extends JoinType("left join")
case object RightJoin extends JoinType("right join")
case object InnerJoin extends JoinType("inner join")
case object FullJoin extends JoinType("full join")

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
