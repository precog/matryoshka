package slamdata.engine.sql

import scalaz._
import Scalaz._

import slamdata.engine.analysis.fixplate._
import slamdata.engine.fp._
import slamdata.engine.{RenderTree, RenderedTree, Terminal, NonTerminal}

trait IsDistinct
case object SelectDistinct extends IsDistinct
case object SelectAll extends IsDistinct

case class Proj[+A](expr: A, alias: Option[String])

sealed trait Expr[+A]

final case class Select[A](isDistinct:   IsDistinct,
                           projections:  List[Proj[A]],
                           relations:    Option[SqlRelation[A]],
                           filter:       Option[A],
                           groupBy:      Option[GroupBy[A]],
                           orderBy:      Option[OrderBy[A]],
                           limit:        Option[Long],
                           offset:       Option[Long])
    extends Expr[A]

final case class Vari(symbol: String) extends Expr[Nothing]
final case class SetLiteral[A](exprs: List[A]) extends Expr[A]
final case class ArrayLiteral[A](exprs: List[A]) extends Expr[A]
case class Splice[A](expr: Option[A]) extends Expr[A]

final case class Binop[A](lhs: A, rhs: A, op: BinaryOperator) extends Expr[A]

sealed abstract class BinaryOperator(val sql: String)
    extends ((Term[Expr], Term[Expr]) => Term[Expr]) {
  def apply(lhs: Term[Expr], rhs: Term[Expr]): Term[Expr] =
    Term(Binop(lhs, rhs, this))

  val name = "(" + sql + ")"

  override def equals(that: Any) = that match {
    case x : BinaryOperator if (sql == x.sql) => true
    case _                                    => false
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
case object Concat  extends BinaryOperator("||")
case object Plus    extends BinaryOperator("+")
case object Minus   extends BinaryOperator("-")
case object Mult    extends BinaryOperator("*")
case object Div     extends BinaryOperator("/")
case object Mod     extends BinaryOperator("%")
case object In      extends BinaryOperator("in")
case object FieldDeref extends BinaryOperator("{}")
case object IndexDeref extends BinaryOperator("[]")

final case class Unop[A](expr: A, op: UnaryOperator) extends Expr[A]

sealed abstract class UnaryOperator(val sql: String)
    extends (Term[Expr] => Term[Expr]) {
  def apply(expr: Term[Expr]): Term[Expr] = Term(Unop(expr, this))
  val name = sql
}

case object Not           extends UnaryOperator("not")
case object IsNull        extends UnaryOperator("is_null")
case object Exists        extends UnaryOperator("exists")
case object Positive      extends UnaryOperator("+")
case object Negative      extends UnaryOperator("-")
case object Distinct      extends UnaryOperator("distinct")
case object ToDate        extends UnaryOperator("date")
case object ToTime        extends UnaryOperator("time")
case object ToTimestamp   extends UnaryOperator("timestamp")
case object ToInterval    extends UnaryOperator("interval")
case object ToId          extends UnaryOperator("oid")
case object ObjectFlatten extends UnaryOperator("flatten_object")
case object ArrayFlatten  extends UnaryOperator("flatten_array")

final case class Ident(name: String) extends Expr[Nothing]
final case class InvokeFunction[A](name: String, args: List[A]) extends Expr[A]

final case class Case[+A](cond: A, expr: A)

final case class Match[A](expr: A, cases: List[Case[A]], default: Option[A])
    extends Expr[A]
final case class Switch[A](cases: List[Case[A]], default: Option[A])
    extends Expr[A]

sealed trait LiteralExpr extends Expr[Nothing]
final case class IntLiteral(v: Long) extends LiteralExpr
final case class FloatLiteral(v: Double) extends LiteralExpr
final case class StringLiteral(v: String) extends LiteralExpr
final case object NullLiteral extends LiteralExpr
final case class BoolLiteral(value: Boolean) extends LiteralExpr

sealed trait SqlRelation[+A] {
  def namedRelations: Map[String, List[NamedRelation[A]]] = {
    def collect(n: SqlRelation[A]): List[(String, NamedRelation[A])] = n match {
      case t @ TableRelationAST(_, _) => List(t.aliasName -> t)
      case t @ ExprRelationAST(_, _) => List(t.aliasName -> t)
      case CrossRelation(left, right) => collect(left) ++ collect(right)
      case JoinRelation(left, right, _, _) => collect(left) ++ collect(right)
    }

    collect(this).groupBy(_._1).mapValues(_.map(_._2))
  }
}

sealed trait NamedRelation[+A] extends SqlRelation[A] {
  def aliasName: String
}
final case class TableRelationAST(name: String, alias: Option[String])
    extends NamedRelation[Nothing] {
  def aliasName = alias.getOrElse(name)
}
final case class ExprRelationAST[A](expr: A, aliasName: String)
    extends NamedRelation[A]
final case class CrossRelation[A](left: SqlRelation[A], right: SqlRelation[A])
    extends SqlRelation[A]
final case class JoinRelation[A](left: SqlRelation[A], right: SqlRelation[A], tpe: JoinType, clause: A)
    extends SqlRelation[A]

sealed abstract class JoinType(val sql: String)
case object LeftJoin extends JoinType("left join")
case object RightJoin extends JoinType("right join")
case object InnerJoin extends JoinType("inner join")
case object FullJoin extends JoinType("full join")

sealed trait OrderType
case object ASC extends OrderType
case object DESC extends OrderType

final case class GroupBy[+A](keys: List[A], having: Option[A])
final case class OrderBy[+A](keys: List[(OrderType, A)])

object Expr {
  protected def _q(s: String): String = "'" + s.replace("'", "''") + "'"

  val SimpleNamePattern = "[_a-zA-Z][_a-zA-Z0-9$]*".r

  protected def _qq(s: String): String = s match {
    case SimpleNamePattern() => s
    case _                   => "\"" + s.replace("\"", "\"\"") + "\""
  }

  implicit def ExprTraverse = new Traverse[Expr] {
    def traverseImpl[G[_], A, B](fa: Expr[A])(f: A => G[B])(implicit G: Applicative[G]) = {
      def caseT(c: Case[A]) = G.apply2(f(c.cond), f(c.expr))(Case(_, _))
      def projT(p: Proj[A]) = G.apply(f(p.expr))(Proj(_, p.alias))
      def relT(r: SqlRelation[A]): G[SqlRelation[B]] = r match {
        case x @ TableRelationAST(_, _) => G.point(x)
        case ExprRelationAST(expr, alias) =>
          G.map(f(expr))(ExprRelationAST(_, alias))
        case CrossRelation(left, right) =>
          G.apply2(relT(left), relT(right))(CrossRelation(_, _))
        case JoinRelation(left, right, tpe, clause) =>
          G.apply3(relT(left), relT(right), f(clause))(JoinRelation(_, _, tpe, _))
      }
      def groupT(g: GroupBy[A]) =
        G.apply2(g.keys.map(f).sequence, g.having.map(f).sequence)(GroupBy(_, _))
      def orderT(o: OrderBy[A]): G[OrderBy[B]] =
        G.apply(o.keys.map(f.second(_).sequence).sequence)(OrderBy(_))

      fa match {
        case Select(distinct, proj, rel, filter, group, order, limit, offset) =>
          G.apply5(proj.map(projT).sequence, rel.map(relT).sequence, filter.map(f).sequence, group.map(groupT).sequence, order.map(orderT).sequence)(Select(distinct, _, _, _, _, _, limit, offset))
        case x @ Vari(_) => G.point(x)
        case SetLiteral(exprs) => G.apply(exprs.map(f).sequence)(SetLiteral(_))
        case ArrayLiteral(exprs) =>
          G.map(exprs.map(f).sequence)(ArrayLiteral(_))
        case Splice(expr) => G.apply(expr.map(f).sequence)(Splice(_))
        case Binop(lhs, rhs, op) => G.apply2(f(lhs), f(rhs))(Binop(_, _, op))
        case Unop(expr, op) => G.apply(f(expr))(Unop(_, op))
        case x @ Ident(name) => G.point(x)
        case InvokeFunction(name, args) =>
          G.map(args.map(f).sequence)(InvokeFunction(name, _))
        case Match(expr, cases, default) =>
          G.apply3(f(expr), cases.map(caseT).sequence, default.map(f).sequence)(Match(_, _, _))
        case Switch(cases, default) =>
          G.apply2(cases.map(caseT).sequence, default.map(f).sequence)(Switch(_, _))
        case x @ IntLiteral(_) => G.point(x)
        case x @ FloatLiteral(_) => G.point(x)
        case x @ StringLiteral(_) => G.point(x)
        case x @ NullLiteral => G.point(x)
        case x @ BoolLiteral(_) => G.point(x)
      }
    }
  }

  def exprNameƒ: Expr[(Term[Expr], Option[String])] => Option[String] = {
    case Ident(name)                                          => Some(name)
    case Binop(_, (Term(StringLiteral(name)), _), FieldDeref) => Some(name)
    case Unop(expr, ObjectFlatten)                            => expr._2
    case Unop(expr, ArrayFlatten)                             => expr._2
    case _                                                    => None
  }

  def namedProjections(expr: Term[Expr], relName: Option[String]): Option[List[(String, Term[Expr])]] =
    expr.unFix match {
      case Select(_, proj, _, _, _, _, _, _) =>
        Some(proj.zipWithIndex.map {
          case (Proj(expr, alias), index) =>
            // FIXME: kill if exprNameƒ == relName
            (alias <+> expr.para(exprNameƒ)).getOrElse(index.toString()) -> expr
        })
      case _ => None
  }

  def sqlƒ: Expr[(Term[Expr], String)] => String = {
    type Para = (Term[Expr], String)
    def caseSql(c: Case[Para]): String =
      List("when", c.cond._2, "then", c.expr._2) mkString " "
    def projSql(p: Proj[Para]): String =
      p.alias.foldLeft(p.expr._2)(_ + " as " + _qq(_))
    def relSql(r: SqlRelation[Para]): String = r match {
      case TableRelationAST(name, alias) =>
        List(Some(_qq(name)), alias).flatten.mkString(" ")
      case ExprRelationAST(expr, alias) =>
        List(expr._2, "as", alias) mkString " "
      case CrossRelation(left, right) =>
        List("(", relSql(left), "cross join", relSql(right), ")").mkString(" ")
      case JoinRelation(left, right, tpe, clause) =>
        List("(", relSql(left), tpe.sql, relSql(right), "on", clause._2, ")") mkString " "
    }
    def groupSql(g: GroupBy[Para]): String =
      List(
        Some("group by"),
        Some(g.keys.map(_._2).mkString(", ")),
        g.having.map("having " + _._2)).flatten.mkString(" ")
    def orderSql(o: OrderBy[Para]): String =
      List(
        "order by",
        o.keys map (x => x._2._2 + " " + x._1.toString) mkString ", ") mkString " "

    _ match {
      case Select(distinct, proj, rel, filter, group, order, limit, offset) =>
        List(
          Some("select"),
          distinct match { case `SelectDistinct` => Some("distinct"); case _ => None },
          Some(proj.map(projSql).mkString(", ")),
          rel.headOption.map(κ("from " + rel.map(relSql).mkString(", "))),
          filter.map("where " + _._2),
          group.map(groupSql),
          order.map(orderSql),
          limit.map(x => "limit " + x.toString),
          offset.map(x => "offset " + x.toString)).flatten.mkString("(", " ", ")")
      case Vari(symbol) => ":" + symbol
      case SetLiteral(exprs) => exprs.map(_._2).mkString("(", ", ", ")")
      case ArrayLiteral(exprs) => exprs.map(_._2).mkString("[", ", ", "]")
      case Splice(expr) => expr.fold("*")("(" + _._2 + ").*")
      case Binop(lhs, rhs, op) => op match {
        case FieldDeref => rhs._1.unFix match {
          case StringLiteral(str) => List("(", lhs._2, ").", str) mkString ""
          case _ => List("(", lhs._2, "){", rhs._2, "}") mkString ""
        }
        case IndexDeref => List("(", lhs._2, ")[", rhs._2, "]") mkString ""
        case _ => List("(" + lhs._2 + ")", op.sql, "(" + rhs._2 + ")") mkString " "
      }
      case Unop(expr, op) => op match {
        case ObjectFlatten => "(" + expr._2 + "){*}"
        case ArrayFlatten  => "(" + expr._2 + ")[*]"
        case IsNull        => "(" + expr._2 + ") is null"
        case _ =>
          val s = List(op.sql, "(", expr._2, ")") mkString " "
          // Note: dis-ambiguates the query in case this is the leading projection
          if (op == Distinct) "(" + s + ")" else s
      }
      case Ident(name) => _qq(name)
      case InvokeFunction(name, args) =>
        import slamdata.engine.std.StdLib.string
          (name, args) match {
          case (string.Like.name, value :: pattern :: esc :: Nil) =>
            esc._1.unFix match {
              case StringLiteral("") => "(" + value + ") like (" + pattern + ")"
              case _ => "(" + value._2 + ") like (" + pattern._2 + ") escape (" + esc._2 + ")"
            }
          case _ => List(name, "(", args.map(_._2) mkString ", ", ")") mkString ""
        }
      case Match(expr, cases, default) =>
        List(
          Some("case"),
          Some(expr._2),
          Some(cases.map(caseSql) mkString " "),
          default.map("else " + _._2),
          Some("end")).flatten.mkString(" ")
      case Switch(cases, default) =>
        List(
          Some("case"),
          Some(cases.map(caseSql) mkString " "),
          default.map("else " + _._2),
          Some("end")).flatten.mkString(" ")
      case IntLiteral(v) => v.toString
      case FloatLiteral(v) => v.toString
      case StringLiteral(v) => _q(v)
      case NullLiteral => "null"
      case BoolLiteral(value) => if (value) "true" else "false"
    }
  }

  implicit def ExprRenderTree = new RenderTree[Expr[_]] {
    override def render(n: Expr[_]) = {
      def projRT(p: Proj[_]) =
        Terminal(p.alias.getOrElse(""), List("AST", "Proj"))
      def relRT(r: SqlRelation[_]): RenderedTree = r match {
        case ExprRelationAST(_, alias) =>
          Terminal("Expr as " + alias, List("AST", "ExprRelation"))
        case TableRelationAST(name, alias) =>
          Terminal(alias.foldLeft(name)(_ + " as " + _), List("AST", "TableRelation"))
        case CrossRelation(left, right) =>
          NonTerminal("", relRT(left) :: relRT(right) :: Nil,
            List("AST", "CrossRelation"))
        case JoinRelation(left, right, jt, _) =>
          NonTerminal(s"($jt)", relRT(left) :: relRT(right) :: Nil,
          List("AST", "JoinRelation"))
      }
      def orderRT(o: OrderBy[_]) =
        NonTerminal("", o.keys.map { case (t, _) =>
          Terminal(t.toString, List("AST", "OrderType"))}, List("AST", "OrderBy"))
      def groupRT(g: GroupBy[_]) = Terminal("", List("AST", "GroupBy"))
      def caseRT(c: Case[_]) = Terminal("", List("AST", "Case"))

      n match {
        case Select(isDistinct, projections, relations, _, groupBy, orderBy, limit, offset) =>
          NonTerminal(isDistinct match { case `SelectDistinct` =>  "distinct"; case _ => "" },
                      projections.map(projRT) ++
                        (relations.map(relRT) ::
                          groupBy.map(groupRT) ::
                          orderBy.map(orderRT) ::
                          limit.map(l => Terminal(l.toString, List("AST", "Limit"))) ::
                          offset.map(o => Terminal(o.toString, List("AST", "Offset"))) ::
                          Nil).flatten,
                    List("AST", "Select"))

        case SetLiteral(_) => Terminal("", List("AST", "Set"))
        case ArrayLiteral(_) => Terminal("", List("AST", "Array"))
        case InvokeFunction(name, _) =>
          Terminal(name, List("AST", "InvokeFunction"))
        case Match(_, cases, _) =>
          NonTerminal("", cases.map(caseRT), List("AST", "Match"))
        case Switch(cases, _) =>
          NonTerminal("", cases.map(caseRT), List("AST", "Switch"))
        case Binop(_, _, op) => Terminal(op.toString, List("AST", "Binop"))
        case Unop(_, op) => Terminal(op.sql, List("AST", "Unop"))
        case Splice(_) => Terminal("", List("AST", "Splice"))
        case Ident(name) => Terminal(name, List("AST", "Ident"))
        case Vari(name) => Terminal(":" + name, List("AST", "Variable"))
        case x: LiteralExpr => Terminal(sqlƒ(x), List("AST", "LiteralExpr"))
      }
    }
  }
}
