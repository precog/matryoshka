/*
 * Copyright 2014 - 2015 SlamData Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package slamdata.engine.sql

import scalaz._
import Scalaz._

import slamdata.engine.fp._
import slamdata.engine.{RenderTree, Terminal, NonTerminal}

sealed trait Node {
  def sql: String

  def children: List[Node]

  protected def _q(s: String): String = "'" + s.replace("'", "''") + "'"

  val SimpleNamePattern = "[_a-zA-Z][_a-zA-Z0-9$]*".r

  protected def _qq(s: String): String = s match {
    case SimpleNamePattern() => s
    case _                   => "\"" + s.replace("\"", "\"\"") + "\""
  }

  // TODO: Remove this once AnnotatedTree is dead (or kill Node first).
  def mapUpM0[F[_]: Monad](proj:     ((Proj, Proj)) => F[Proj],
                           relation: ((SqlRelation, SqlRelation)) => F[SqlRelation],
                           expr:     ((Expr, Expr)) => F[Expr],
                           groupBy:  ((GroupBy, GroupBy)) => F[GroupBy],
                           orderBy:  ((OrderBy, OrderBy)) => F[OrderBy],
                           case0:    ((Case, Case)) => F[Case]): F[Expr] =
    sys.error("Called mapUpM0 on a non-Expr.")
}

trait NodeInstances {
  implicit def NodeRenderTree[A <: Node]: RenderTree[A] = new RenderTree[A] {
    val astType = "AST" :: Nil

    override def render(n: A) = {
      n match {
        case Select(isDistinct, projections, relations, filter, groupBy, orderBy, limit, offset) =>
          val nt = "Select" :: astType
          NonTerminal(nt,
            isDistinct match { case `SelectDistinct` => Some("distinct"); case _ => None },
            projections.map(p => NodeRenderTree.render(p)) ++
              (relations.map(r => NodeRenderTree.render(r)) ::
                filter.map(f => NodeRenderTree.render(f)) ::
                groupBy.map(g => NodeRenderTree.render(g)) ::
                orderBy.map(o => NodeRenderTree.render(o)) ::
                limit.map(l => Terminal("Limit" :: nt, Some(l.toString))) ::
                offset.map(o => Terminal("Offset" :: nt, Some(o.toString))) ::
                Nil).flatten)

        case Proj(expr, alias) => NonTerminal("Proj" :: astType, alias, NodeRenderTree.render(expr) :: Nil)

        case ExprRelationAST(select, alias) => NonTerminal("ExprRelation" :: astType, Some("Expr as " + alias), NodeRenderTree.render(select) :: Nil)

        case TableRelationAST(name, Some(alias)) => Terminal("TableRelation" :: astType, Some(name + " as " + alias))
        case TableRelationAST(name, None)        => Terminal("TableRelation" :: astType, Some(name))

        case CrossRelation(left, right) => NonTerminal("CrossRelation" :: astType, None, NodeRenderTree.render(left) :: NodeRenderTree.render(right) :: Nil)

        case JoinRelation(left, right, jt, clause) =>
          NonTerminal("JoinRelation" :: astType, Some(jt.toString),
            NodeRenderTree.render(left) :: NodeRenderTree.render(right) :: NodeRenderTree.render(clause) :: Nil)

        case OrderBy(keys) =>
          val nt = "OrderBy" :: astType
          NonTerminal(nt, None,
            keys.map { case (x, t) => NonTerminal("OrderType" :: nt, Some(t.toString), NodeRenderTree.render(x) :: Nil)})

        case GroupBy(keys, Some(having)) => NonTerminal("GroupBy" :: astType, None, keys.map(NodeRenderTree.render(_)) :+ NodeRenderTree.render(having))
        case GroupBy(keys, None)         => NonTerminal("GroupBy" :: astType, None, keys.map(NodeRenderTree.render(_)))

        case SetLiteral(exprs) => NonTerminal("Set" :: astType, None, exprs.map(NodeRenderTree.render(_)))
        case ArrayLiteral(exprs) => NonTerminal("Array" :: astType, None, exprs.map(NodeRenderTree.render(_)))

        case InvokeFunction(name, args) => NonTerminal("InvokeFunction" :: astType, Some(name), args.map(NodeRenderTree.render(_)))

        case Case(cond, expr) => NonTerminal("Case" :: astType, None, NodeRenderTree.render(cond) :: NodeRenderTree.render(expr) :: Nil)

        case Match(expr, cases, Some(default)) => NonTerminal("Match" :: astType, None, NodeRenderTree.render(expr) :: (cases.map(NodeRenderTree.render(_)) :+ NodeRenderTree.render(default)))
        case Match(expr, cases, None)          => NonTerminal("Match" :: astType, None, NodeRenderTree.render(expr) :: cases.map(NodeRenderTree.render(_)))

        case Switch(cases, Some(default)) => NonTerminal("Switch" :: astType, None, cases.map(NodeRenderTree.render(_)) :+ NodeRenderTree.render(default))
        case Switch(cases, None)          => NonTerminal("Switch" :: astType, None, cases.map(NodeRenderTree.render(_)))

        case Binop(lhs, rhs, op) => NonTerminal("Binop" :: astType, Some(op.toString), NodeRenderTree.render(lhs) :: NodeRenderTree.render(rhs) :: Nil)

        case Unop(expr, op) => NonTerminal("Unop" :: astType, Some(op.sql), NodeRenderTree.render(expr) :: Nil)

        case Splice(expr) => NonTerminal("Splice" :: astType, None, expr.toList.map(NodeRenderTree.render(_)))

        case Ident(name) => Terminal("Ident" :: astType, Some(name))

        case Vari(name) => Terminal("Variable" :: astType, Some(":" + name))

        case x: LiteralExpr => Terminal("LiteralExpr" :: astType, Some(x.sql))
      }
    }
  }
}

object Node extends NodeInstances

trait IsDistinct
final case object SelectDistinct extends IsDistinct
final case object SelectAll extends IsDistinct

final case class Proj(expr: Expr, alias: Option[String]) extends Node {
  def children = expr :: Nil
  def sql = alias.foldLeft(expr.sql)(_ + " as " + _qq(_))
}

sealed trait Expr extends Node {
  def mapUpM[F[_]: Monad](proj:     Proj => F[Proj],
                          relation: SqlRelation => F[SqlRelation],
                          expr:     Expr => F[Expr],
                          groupBy:  GroupBy => F[GroupBy],
                          orderBy:  OrderBy => F[OrderBy],
                          case0:    Case => F[Case]): F[Expr] = {
    mapUpM0[F](
      v => proj(v._2),
      v => relation(v._2),
      v => expr(v._2),
      v => groupBy(v._2),
      v => orderBy(v._2),
      v => case0(v._2))
  }

  override def mapUpM0[F[_]: Monad](
    proj:     ((Proj, Proj)) => F[Proj],
    relation: ((SqlRelation, SqlRelation)) => F[SqlRelation],
    expr:     ((Expr, Expr)) => F[Expr],
    groupBy:  ((GroupBy, GroupBy)) => F[GroupBy],
    orderBy:  ((OrderBy, OrderBy)) => F[OrderBy],
    case0:    ((Case, Case)) => F[Case]):
      F[Expr] = {

    def caseLoop(node: Case): F[Case] = (for {
      cond <- exprLoop(node.cond)
      expr <- exprLoop(node.expr)
    } yield node -> Case(cond, expr)).flatMap(case0)

    def projLoop(node: Proj): F[Proj] = (for {
      x2 <- exprLoop(node.expr)
    } yield node -> (node match {
      case Proj(_, alias) => Proj(x2, alias)
    })).flatMap(proj)

    def relationLoop(node: SqlRelation): F[SqlRelation] = node match {
      case t @ TableRelationAST(_, _) => relation(t -> t)

      case r @ ExprRelationAST(expr, alias) =>
        (for {
          expr2 <- exprLoop(expr)
        } yield r -> ExprRelationAST(expr2, alias)).flatMap(relation)

      case r @ CrossRelation(left, right) =>
        (for {
          l2 <- relationLoop(left)
          r2 <- relationLoop(right)
        } yield r -> CrossRelation(l2, r2)).flatMap(relation)

      case r @ JoinRelation(left, right, jt, expr) =>
        (for {
          l2 <- relationLoop(left)
          r2 <- relationLoop(right)
          x2 <- exprLoop(expr)
        } yield r -> JoinRelation(l2, r2, jt, x2)).flatMap(relation)
    }

    def exprLoop(node: Expr): F[Expr] = node match {
      case e @ Unop(x, op) =>
        (for {
          x2 <- exprLoop(x)
        } yield e -> Unop(x2, op)).flatMap(expr)

      case e @ Binop(left, right, op) =>
        (for {
          l2 <- exprLoop(left)
          r2 <- exprLoop(right)
        } yield e -> Binop(l2, r2, op)).flatMap(expr)

      case e @ InvokeFunction(name, args) =>
        (for {
          a2 <- args.map(exprLoop).sequence
        } yield e -> InvokeFunction(name, a2)).flatMap(expr)

      case e @ SetLiteral(exprs) =>
        (for {
          exprs2 <- exprs.map(exprLoop).sequence
        } yield e -> SetLiteral(exprs2)).flatMap(expr)

      case e @ ArrayLiteral(exprs) =>
        (for {
          exprs2 <- exprs.map(exprLoop).sequence
        } yield e -> ArrayLiteral(exprs2)).flatMap(expr)

      case e @ Match(x, cases, default) =>
        (for {
          x2 <- exprLoop(x)
          c2 <- cases.map(caseLoop _).sequence
          d2 <- default.map(exprLoop).sequence
        } yield e -> Match(x2, c2, d2)).flatMap(expr)

      case e @ Switch(cases, default) =>
        (for {
          c2 <- cases.map(caseLoop _).sequence
          d2 <- default.map(exprLoop).sequence
        } yield e -> Switch(c2, d2)).flatMap(expr)

      case select0 @ Select(d, p, r, f, g, o, limit, offset) => (for {
        p2 <- p.map(projLoop).sequence
        r2 <- r.map(relationLoop).sequence
        f2 <- f.map(exprLoop).sequence
        g2 <- g.map(groupByLoop).sequence
        o2 <- o.map(orderByLoop).sequence
      } yield select0 -> Select(d, p2, r2, f2, g2, o2, limit, offset)).flatMap(expr)

      case e @ Splice(Some(x)) =>
        (for {
          x2 <- exprLoop(x)
        } yield e -> Splice(Some(x2))).flatMap(expr)
      case l @ Splice(None) => expr(l -> l)
      case l @ Ident(_)         => expr(l -> l)
      case l @ IntLiteral(_)    => expr(l -> l)
      case l @ FloatLiteral(_)  => expr(l -> l)
      case l @ StringLiteral(_) => expr(l -> l)
      case l @ NullLiteral      => expr(l -> l)
      case l @ BoolLiteral(_)   => expr(l -> l)
      case l @ Vari(_)          => expr(l -> l)
    }

    def groupByLoop(node: GroupBy): F[GroupBy] = (for {
      k2 <- node.keys.map(exprLoop).sequence
      h2 <- node.having.map(exprLoop).sequence
    } yield node -> GroupBy(k2, h2)).flatMap(groupBy)

    def orderByLoop(node: OrderBy): F[OrderBy] = (for {
      k2 <- node.keys.map { case (key, orderType) => exprLoop(key).map(_ -> orderType) }.sequence
    } yield node -> OrderBy(k2)).flatMap(orderBy)

    exprLoop(this)
  }
}

final case class Select(isDistinct:   IsDistinct,
                        projections:  List[Proj],
                        relations:    Option[SqlRelation],
                        filter:       Option[Expr],
                        groupBy:      Option[GroupBy],
                        orderBy:      Option[OrderBy],
                        limit:        Option[Long],
                        offset:       Option[Long]) extends Expr {
  def sql =
    "(" +
      List(Some("select"),
           isDistinct match { case `SelectDistinct` => Some("distinct"); case _ => None },
           Some(projections.map(_.sql).mkString(", ")),
           relations.headOption.map(κ("from " + relations.map(_.sql).mkString(", "))),
           filter.map(x => "where " + x.sql),
           groupBy.map(_.sql),
           orderBy.map(_.sql),
           limit.map(x => "limit " + x.toString),
           offset.map(x => "offset " + x.toString)).flatten.mkString(" ") +
      ")"

  def children: List[Node] = projections.toList ++ relations ++ filter.toList ++ groupBy.toList ++ orderBy.toList

  // TODO: move this logic to another file where it can be used by both the type checker and compiler?
  def namedProjections(relName: Option[String]): List[(String, Expr)] = {
    def extractName(expr: Expr): Option[String] = expr match {
      case Ident(name) if Some(name) != relName      => Some(name)
      case Binop(_, StringLiteral(name), FieldDeref) => Some(name)
      case Unop(expr, ObjectFlatten)                 => extractName(expr)
      case Unop(expr, ArrayFlatten)                  => extractName(expr)
      case _                                         => None
    }
    projections.toList.zipWithIndex.map {
      case (Proj(expr, alias), index) =>
        (alias <+> extractName(expr)).getOrElse(index.toString()) -> expr
    }
  }
}

final case class Vari(symbol: String) extends Expr {
  def sql = ":" + symbol

  def children = Nil
}

final case class SetLiteral(exprs: List[Expr]) extends Expr {
  def sql = exprs.map(_.sql).mkString("(", ", ", ")")

  def children = exprs.toList
}

final case class ArrayLiteral(exprs: List[Expr]) extends Expr {
  def sql = exprs.map(_.sql).mkString("[", ", ", "]")

  def children = exprs.toList
}

final case class Splice(expr: Option[Expr]) extends Expr {
  def sql = expr.fold("*")(x => "(" + x.sql + ").*")

  def children = expr.toList
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
    case x: BinaryOperator => sql == x.sql
    case _                 => false
  }

  override def hashCode = sql.hashCode

  override def toString = sql
}

final case object Or      extends BinaryOperator("or")
final case object And     extends BinaryOperator("and")
final case object Eq      extends BinaryOperator("=")
final case object Neq     extends BinaryOperator("<>")
final case object Ge      extends BinaryOperator(">=")
final case object Gt      extends BinaryOperator(">")
final case object Le      extends BinaryOperator("<=")
final case object Lt      extends BinaryOperator("<")
final case object Concat  extends BinaryOperator("||")
final case object Plus    extends BinaryOperator("+")
final case object Minus   extends BinaryOperator("-")
final case object Mult    extends BinaryOperator("*")
final case object Div     extends BinaryOperator("/")
final case object Mod     extends BinaryOperator("%")
final case object In      extends BinaryOperator("in")
final case object FieldDeref extends BinaryOperator("{}")
final case object IndexDeref extends BinaryOperator("[]")

final case class Unop(expr: Expr, op: UnaryOperator) extends Expr {
  def sql = op match {
    case ObjectFlatten => "(" + expr.sql + "){*}"
    case ArrayFlatten  => "(" + expr.sql + ")[*]"
    case IsNull        => "(" + expr.sql + ") is null"
    case _ =>
      val s = List(op.sql, "(", expr.sql, ")") mkString " "
      if (op == Distinct) "(" + s + ")" else s  // Note: dis-ambiguates the query in case this is the leading projection
  }

  def children = expr :: Nil
}

sealed abstract class UnaryOperator(val sql: String) extends Node with (Expr => Unop) {
  def apply(expr: Expr): Unop = Unop(expr, this)

  val name = sql

  def children = Nil

  override def equals(that: Any) = that match {
    case x: UnaryOperator => sql == x.sql
    case _                => false
  }

  override def hashCode = sql.hashCode

  override def toString = sql
}

final case object Not           extends UnaryOperator("not")
final case object IsNull        extends UnaryOperator("is_null")
final case object Exists        extends UnaryOperator("exists")
final case object Positive      extends UnaryOperator("+")
final case object Negative      extends UnaryOperator("-")
final case object Distinct      extends UnaryOperator("distinct")
final case object ToDate        extends UnaryOperator("date")
final case object ToTime        extends UnaryOperator("time")
final case object ToTimestamp   extends UnaryOperator("timestamp")
final case object ToInterval    extends UnaryOperator("interval")
final case object ToId          extends UnaryOperator("oid")
final case object ObjectFlatten extends UnaryOperator("flatten_object")
final case object ArrayFlatten  extends UnaryOperator("flatten_array")

final case class Ident(name: String) extends Expr {
  def sql = _qq(name)

  def children = Nil
}

final case class InvokeFunction(name: String, args: List[Expr]) extends Expr {
  import slamdata.engine.std.StdLib.string

  def sql = (name, args) match {
    case (string.Like.name, value :: pattern :: StringLiteral("") :: Nil) => "(" + value.sql + ") like (" + pattern.sql + ")"
    case (string.Like.name, value :: pattern :: esc :: Nil) => "(" + value.sql + ") like (" + pattern.sql + ") escape (" + esc + ")"
    case _ => List(name, "(", args.map(_.sql) mkString ", ", ")") mkString ""
  }

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

final case object NullLiteral extends LiteralExpr {
  def sql = "null"
}

final case class BoolLiteral(value: Boolean) extends LiteralExpr {
  def sql = if (value) "true" else "false"
}

sealed trait SqlRelation extends Node {
  def namedRelations: Map[String, List[NamedRelation]] = {
    def collect(n: SqlRelation): List[(String, NamedRelation)] = n match {
      case t @ TableRelationAST(_, _) => (t.aliasName -> t) :: Nil
      case t @ ExprRelationAST(_, _) => (t.aliasName -> t) :: Nil
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
  def sql = List(Some(_qq(name)), alias).flatten.mkString(" ")

  def aliasName = alias.getOrElse(name)

  def children = Nil
}

final case class ExprRelationAST(expr: Expr, aliasName: String)
    extends NamedRelation {
  def sql = List(expr.sql, "as", aliasName) mkString " "

  def children = expr :: Nil
}

final case class CrossRelation(left: SqlRelation, right: SqlRelation) extends SqlRelation {
  def sql = List("(", left.sql, "cross join", right.sql, ")").mkString(" ")

  def children = left :: right :: Nil
}

final case class JoinRelation(left: SqlRelation, right: SqlRelation, tpe: JoinType, clause: Expr) extends SqlRelation {
  def sql = List("(", left.sql, tpe.sql, right.sql, "on", clause.sql, ")") mkString " "

  def children = left :: right :: clause :: Nil
}

sealed abstract class JoinType(val sql: String)
final case object LeftJoin extends JoinType("left join")
final case object RightJoin extends JoinType("right join")
final case object InnerJoin extends JoinType("inner join")
final case object FullJoin extends JoinType("full join")

sealed trait OrderType
final case object ASC extends OrderType
final case object DESC extends OrderType

final case class GroupBy(keys: List[Expr], having: Option[Expr]) extends Node {
  def sql = List(Some("group by"), Some(keys.map(_.sql).mkString(", ")), having.map(e => "having " + e.sql)).flatten.mkString(" ")

  def children = keys.toList ++ having.toList
}

final case class OrderBy(keys: List[(Expr, OrderType)]) extends Node {
  def sql = List("order by", keys map (x => x._1.sql + " " + x._2.toString) mkString ", ") mkString " "

  def children = keys.map(_._1).toList
}
