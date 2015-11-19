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

package quasar.sql

import quasar.Predef._

import scala.Any

trait IsDistinct
final case object SelectDistinct extends IsDistinct
final case object SelectAll extends IsDistinct

final case class Proj[A](expr: A, alias: Option[String])

sealed trait ExprF[A]
object ExprF {
  final case class SelectF[A](
    isDistinct:  IsDistinct,
    projections: List[Proj[A]],
    relations:   Option[SqlRelation[A]],
    filter:      Option[A],
    groupBy:     Option[GroupBy[A]],
    orderBy:     Option[OrderBy[A]])
      extends ExprF[A]
  final case class VariF[A](symbol: String) extends ExprF[A]
  final case class SetLiteralF[A](exprs: List[A]) extends ExprF[A]
  final case class ArrayLiteralF[A](exprs: List[A]) extends ExprF[A]
  final case class SpliceF[A](expr: Option[A]) extends ExprF[A]
  final case class BinopF[A](lhs: A, rhs: A, op: BinaryOperator)
      extends ExprF[A]
  final case class UnopF[A](expr: A, op: UnaryOperator) extends ExprF[A]
  final case class IdentF[A](name: String) extends ExprF[A]
  final case class InvokeFunctionF[A](name: String, args: List[A])
      extends ExprF[A]
  final case class MatchF[A](expr: A, cases: List[Case[A]], default: Option[A])
      extends ExprF[A]
  final case class SwitchF[A](cases: List[Case[A]], default: Option[A])
      extends ExprF[A]
  final case class IntLiteralF[A](v: Long) extends ExprF[A]
  final case class FloatLiteralF[A](v: Double) extends ExprF[A]
  final case class StringLiteralF[A](v: String) extends ExprF[A]
  final case class NullLiteralF[A]() extends ExprF[A]
  final case class BoolLiteralF[A](value: Boolean) extends ExprF[A]
}

// TODO: Change to extend `(A, A) => ExprF[A]`
sealed abstract class BinaryOperator(val sql: String)
    extends ((Expr, Expr) => Expr) {
  def apply(lhs: Expr, rhs: Expr): Expr = Binop(lhs, rhs, this)

  val name = "(" + sql + ")"

  override def equals(that: Any) = that match {
    case x: BinaryOperator => sql == x.sql
    case _                 => false
  }

  override def hashCode = sql.hashCode

  override def toString = sql
}

final case object Or           extends BinaryOperator("or")
final case object And          extends BinaryOperator("and")
final case object Eq           extends BinaryOperator("=")
final case object Neq          extends BinaryOperator("<>")
final case object Ge           extends BinaryOperator(">=")
final case object Gt           extends BinaryOperator(">")
final case object Le           extends BinaryOperator("<=")
final case object Lt           extends BinaryOperator("<")
final case object Concat       extends BinaryOperator("||")
final case object Plus         extends BinaryOperator("+")
final case object Minus        extends BinaryOperator("-")
final case object Mult         extends BinaryOperator("*")
final case object Div          extends BinaryOperator("/")
final case object Mod          extends BinaryOperator("%")
final case object In           extends BinaryOperator("in")
final case object FieldDeref   extends BinaryOperator("{}")
final case object IndexDeref   extends BinaryOperator("[]")
final case object Limit        extends BinaryOperator("limit")
final case object Offset       extends BinaryOperator("offset")
final case object Union        extends BinaryOperator("union")
final case object UnionAll     extends BinaryOperator("union all")
final case object Intersect    extends BinaryOperator("intersect")
final case object IntersectAll extends BinaryOperator("intersect all")
final case object Except       extends BinaryOperator("except")

// TODO: Change to extend `A => ExprF[A]`
sealed abstract class UnaryOperator(val sql: String) extends (Expr => Expr) {
  def apply(expr: Expr): Expr = Unop(expr, this)

  val name = sql

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

final case class Case[A](cond: A, expr: A)

sealed trait SqlRelation[A] {
  def namedRelations: Map[String, List[NamedRelation[A]]] = {
    def collect(n: SqlRelation[A]): List[(String, NamedRelation[A])] =
      n match {
        case t @ TableRelationAST(_, _) => (t.aliasName -> t) :: Nil
        case t @ ExprRelationAST(_, _) => (t.aliasName -> t) :: Nil
        case JoinRelation(left, right, _, _) => collect(left) ++ collect(right)
    }

    collect(this).groupBy(_._1).mapValues(_.map(_._2))
  }
}

sealed trait NamedRelation[A] extends SqlRelation[A] {
  def aliasName: String
}

final case class TableRelationAST[A](name: String, alias: Option[String])
    extends NamedRelation[A] {
  def aliasName = alias.getOrElse(name)
}
final case class ExprRelationAST[A](expr: A, aliasName: String)
    extends NamedRelation[A]
final case class JoinRelation[A](left: SqlRelation[A], right: SqlRelation[A], tpe: JoinType, clause: A)
    extends SqlRelation[A]

sealed abstract class JoinType(val sql: String)
final case object LeftJoin extends JoinType("left join")
final case object RightJoin extends JoinType("right join")
final case object InnerJoin extends JoinType("inner join")
final case object FullJoin extends JoinType("full join")

sealed trait OrderType
final case object ASC extends OrderType
final case object DESC extends OrderType

final case class GroupBy[A](keys: List[A], having: Option[A])

final case class OrderBy[A](keys: List[(OrderType, A)])

object SelectF {
  def apply[A](
    isDistinct:  IsDistinct,
    projections: List[Proj[A]],
    relations:   Option[SqlRelation[A]],
    filter:      Option[A],
    groupBy:     Option[GroupBy[A]],
    orderBy:     Option[OrderBy[A]]):
      ExprF[A] =
    ExprF.SelectF(isDistinct, projections, relations, filter, groupBy, orderBy)
  def unapply[A](obj: ExprF[A]):
      Option[(
        IsDistinct,
        List[Proj[A]],
        Option[SqlRelation[A]],
        Option[A],
        Option[GroupBy[A]],
        Option[OrderBy[A]])] =
    obj match {
      case ExprF.SelectF(
        isDistinct,
        projections,
        relations,
        filter,
        groupBy,
        orderBy) =>
        Some((isDistinct, projections, relations, filter, groupBy, orderBy))
      case _                         => None
    }
}
object VariF {
  def apply[A](symbol: String): ExprF[A] = ExprF.VariF(symbol)
  def unapply[A](obj: ExprF[A]): Option[String] = obj match {
    case ExprF.VariF(symbol) => Some(symbol)
    case _                   => None
  }
}
object SetLiteralF {
  def apply[A](exprs: List[A]): ExprF[A] = ExprF.SetLiteralF(exprs)
  def unapply[A](obj: ExprF[A]): Option[List[A]] = obj match {
    case ExprF.SetLiteralF(exprs) => Some(exprs)
    case _                        => None
  }
}
object ArrayLiteralF {
  def apply[A](exprs: List[A]): ExprF[A] = ExprF.ArrayLiteralF(exprs)
  def unapply[A](obj: ExprF[A]): Option[List[A]] = obj match {
    case ExprF.ArrayLiteralF(exprs) => Some(exprs)
    case _                          => None
  }
}
object SpliceF {
  def apply[A](expr: Option[A]): ExprF[A] = ExprF.SpliceF(expr)
  def unapply[A](obj: ExprF[A]): Option[Option[A]] = obj match {
    case ExprF.SpliceF(expr) => Some(expr)
    case _                   => None
  }
}
object BinopF {
  def apply[A](lhs: A, rhs: A, op: BinaryOperator): ExprF[A] =
    ExprF.BinopF(lhs, rhs, op)
  def unapply[A](obj: ExprF[A]): Option[(A, A, BinaryOperator)] =
    obj match {
      case ExprF.BinopF(lhs, rhs, op) => Some((lhs, rhs, op))
      case _                          => None
  }
}
object UnopF {
  def apply[A](expr: A, op: UnaryOperator): ExprF[A] = ExprF.UnopF(expr, op)
  def unapply[A](obj: ExprF[A]): Option[(A, UnaryOperator)] = obj match {
    case ExprF.UnopF(expr, op) => Some((expr, op))
    case _                     => None
  }
}
object IdentF {
  def apply[A](name: String): ExprF[A] = ExprF.IdentF(name)
  def unapply[A](obj: ExprF[A]): Option[String] = obj match {
    case ExprF.IdentF(name) => Some(name)
    case _                  => None
  }
}
object InvokeFunctionF {
  def apply[A](name: String, args: List[A]): ExprF[A] =
    ExprF.InvokeFunctionF(name, args)
  def unapply[A](obj: ExprF[A]): Option[(String, List[A])] = obj match {
    case ExprF.InvokeFunctionF(name, args) => Some((name, args))
    case _                                 => None
  }
}
object MatchF {
  def apply[A](expr: A, cases: List[Case[A]], default: Option[A]): ExprF[A] =
    ExprF.MatchF(expr, cases, default)
  def unapply[A](obj: ExprF[A]): Option[(A, List[Case[A]], Option[A])] =
    obj match {
      case ExprF.MatchF(expr, cases, default) => Some((expr, cases, default))
      case _                                  => None
    }
}
object SwitchF {
  def apply[A](cases: List[Case[A]], default: Option[A]): ExprF[A] =
    ExprF.SwitchF(cases, default)
  def unapply[A](obj: ExprF[A]): Option[(List[Case[A]], Option[A])] =
    obj match {
      case ExprF.SwitchF(cases, default) => Some((cases, default))
      case _                             => None
  }
}
object IntLiteralF {
  def apply[A](v: Long): ExprF[A] = ExprF.IntLiteralF(v)
  def unapply[A](obj: ExprF[A]): Option[Long] = obj match {
    case ExprF.IntLiteralF(v) => Some(v)
    case _                    => None
  }
}
object FloatLiteralF {
  def apply[A](v: Double): ExprF[A] = ExprF.FloatLiteralF(v)
  def unapply[A](obj: ExprF[A]): Option[Double] = obj match {
    case ExprF.FloatLiteralF(v) => Some(v)
    case _                      => None
  }
}
object StringLiteralF {
  def apply[A](v: String): ExprF[A] = ExprF.StringLiteralF(v)
  def unapply[A](obj: ExprF[A]): Option[String] = obj match {
    case ExprF.StringLiteralF(v) => Some(v)
    case _                       => None
  }
}
object NullLiteralF {
  def apply[A](): ExprF[A] = ExprF.NullLiteralF()
  def unapply[A](obj: ExprF[A]): Boolean = obj match {
    case ExprF.NullLiteralF() => true
    case _                    => false
  }
}
object BoolLiteralF {
  def apply[A](value: Boolean): ExprF[A] = ExprF.BoolLiteralF(value)
  def unapply[A](obj: ExprF[A]): Option[Boolean] = obj match {
    case ExprF.BoolLiteralF(value) => Some(value)
    case _                         => None
  }
}
