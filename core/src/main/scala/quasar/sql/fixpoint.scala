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
import quasar.recursionschemes._

object Select {
  def apply(
    isDistinct:   IsDistinct,
    projections:  List[Proj[Expr]],
    relations:    Option[SqlRelation[Expr]],
    filter:       Option[Expr],
    groupBy:      Option[GroupBy[Expr]],
    orderBy:      Option[OrderBy[Expr]],
    limit:        Option[Long],
    offset:       Option[Long]):
      Expr =
    Fix[ExprF](SelectF(
      isDistinct,
      projections,
      relations,
      filter,
      groupBy,
      orderBy,
      limit,
      offset))
  def unapply(obj: Expr):
      Option[(
        IsDistinct,
        List[Proj[Expr]],
        Option[SqlRelation[Expr]],
        Option[Expr],
        Option[GroupBy[Expr]],
        Option[OrderBy[Expr]],
        Option[Long],
        Option[Long])] =
    SelectF.unapply(obj.unFix)
}

object Vari {
  def apply(symbol: String): Expr = Fix[ExprF](VariF(symbol))
  def unapply(obj: Expr): Option[String] = VariF.unapply(obj.unFix)
}

object SetLiteral {
  def apply(exprs: List[Expr]): Expr = Fix[ExprF](SetLiteralF(exprs))
  def unapply(obj: Expr): Option[List[Expr]] = SetLiteralF.unapply(obj.unFix)
}

object ArrayLiteral {
  def apply(exprs: List[Expr]): Expr = Fix[ExprF](ArrayLiteralF(exprs))
  def unapply(obj: Expr): Option[List[Expr]] =
    ArrayLiteralF.unapply(obj.unFix)
}

object Splice {
  def apply(expr: Option[Expr]): Expr = Fix[ExprF](SpliceF(expr))
  def unapply(obj: Expr): Option[Option[Expr]] = SpliceF.unapply(obj.unFix)
}

object Binop {
  def apply(lhs: Expr, rhs: Expr, op: BinaryOperator): Expr =
    Fix[ExprF](BinopF(lhs, rhs, op))
  def unapply(obj: Expr): Option[(Expr, Expr, BinaryOperator)] =
    BinopF.unapply(obj.unFix)
}

object Unop {
  def apply(expr: Expr, op: UnaryOperator): Expr =
    Fix[ExprF](UnopF(expr, op))
  def unapply(obj: Expr): Option[(Expr, UnaryOperator)] =
    UnopF.unapply(obj.unFix)
}

object Ident {
  def apply(name: String): Expr = Fix[ExprF](IdentF(name))
  def unapply(obj: Expr): Option[String] = IdentF.unapply(obj.unFix)
}

object InvokeFunction {
  def apply(name: String, args: List[Expr]): Expr =
    Fix[ExprF](InvokeFunctionF(name, args))
  def unapply(obj: Expr): Option[(String, List[Expr])] =
    InvokeFunctionF.unapply(obj.unFix)
}

object Match {
  def apply(expr: Expr, cases: List[Case[Expr]], default: Option[Expr]):
      Expr =
    Fix[ExprF](MatchF(expr, cases, default))
  def unapply(obj: Expr): Option[(Expr, List[Case[Expr]], Option[Expr])] =
    MatchF.unapply(obj.unFix)
}

object Switch {
  def apply(cases: List[Case[Expr]], default: Option[Expr]): Expr =
    Fix[ExprF](SwitchF(cases, default))
  def unapply(obj: Expr): Option[(List[Case[Expr]], Option[Expr])] =
    SwitchF.unapply(obj.unFix)
}

object IntLiteral {
  def apply(v: Long): Expr = Fix[ExprF](IntLiteralF(v))
  def unapply(obj: Expr): Option[Long] = IntLiteralF.unapply(obj.unFix)
}

object FloatLiteral {
  def apply(v: Double): Expr = Fix[ExprF](FloatLiteralF(v))
  def unapply(obj: Expr): Option[Double] = FloatLiteralF.unapply(obj.unFix)
}

object StringLiteral {
  def apply(v: String): Expr = Fix[ExprF](StringLiteralF(v))
  def unapply(obj: Expr): Option[String] = StringLiteralF.unapply(obj.unFix)
}

object NullLiteral {
  def apply(): Expr = Fix[ExprF](NullLiteralF())
  def unapply(obj: Expr): Boolean = NullLiteralF.unapply(obj.unFix)
}

object BoolLiteral {
  def apply(value: Boolean): Expr = Fix[ExprF](BoolLiteralF(value))
  def unapply(obj: Expr): Option[Boolean] = BoolLiteralF.unapply(obj.unFix)
}
