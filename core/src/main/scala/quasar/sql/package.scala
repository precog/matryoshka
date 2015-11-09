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

package quasar

import quasar.Predef._
import RenderTree.ops._
import quasar.recursionschemes._, Recursive.ops._
import quasar.fs._

import scalaz._, Scalaz._

package object sql {
  type Expr = Fix[ExprF]

  def CrossRelation(left: SqlRelation[Expr], right: SqlRelation[Expr]) =
    JoinRelation(left, right, InnerJoin, BoolLiteral(true))

  def namedProjections(e: Expr, relName: Option[String]):
      List[(String, Expr)] = {
    def extractName(expr: Expr): Option[String] = expr match {
      case Ident(name) if Some(name) != relName      => Some(name)
      case Binop(_, StringLiteral(name), FieldDeref) => Some(name)
      case Unop(expr, ObjectFlatten)                 => extractName(expr)
      case Unop(expr, ArrayFlatten)                  => extractName(expr)
      case _                                         => None
    }

    e.unFix match {
      case SelectF(_, projections, _, _, _, _) =>
        projections.zipWithIndex.map {
          case (Proj(expr, alias), index) =>
            (alias <+> extractName(expr)).getOrElse(index.toString()) -> expr
        }
    }
  }

  def mapRelationPathsM[F[_]: Monad, A](r: SqlRelation[A])(f: Path => F[Path]):
      F[SqlRelation[A]] =
    r match {
      case JoinRelation(l, r, t, c) =>
        (mapRelationPathsM(l)(f) |@| mapRelationPathsM(r)(f))(
          JoinRelation(_, _, t, c))
      case ExprRelationAST(_, _) => r.point[F]
      case TableRelationAST(path, alias) => for {
        p <- f(Path(path))
      } yield TableRelationAST(p.pathname, alias)
  }

  def mapPathsMƒ[F[_]: Monad](f: Path => F[Path]): ExprF[Expr] => F[Expr] = {
    case SelectF(d, p, rel, filter, g, order) =>
      rel.map(mapRelationPathsM(_)(f)).sequence.map(
        Select(d, p, _, filter, g, order))
    case x => Fix(x).point[F]
  }

  val mapPathsEƒ = mapPathsMƒ[Path.PathError \/ ?] _

  def relativizePaths(q: Expr, basePath: Path): Path.PathError \/ Expr =
    q.cataM[Path.PathError \/ ?, Expr](mapPathsEƒ(_.from(basePath)))

  def pprint(sql: Expr) = sql.para(sqlƒ)

  val sqlƒ: ExprF[(Expr, String)] => String = {
    val SimpleNamePattern = "[_a-zA-Z][_a-zA-Z0-9$]*".r

    def _q(s: String): String = "'" + s.replace("'", "''") + "'"

    def _qq(s: String): String = s match {
      case SimpleNamePattern() => s
      case _                   => "\"" + s.replace("\"", "\"\"") + "\""
    }

    def caseSql(c: Case[(Expr, String)]): String =
      List("when", c.cond._2, "then", c.expr._2) mkString " "
    def relationSql(r: SqlRelation[(Expr, String)]): String = (r match {
      case TableRelationAST(name, alias) => _qq(name) :: alias.toList
      case ExprRelationAST(expr, aliasName) =>
        List(expr._2, "as", aliasName)
      case JoinRelation(left, right, tpe, clause) =>
        (tpe, clause._1) match {
          case (InnerJoin, BoolLiteral(true)) =>
            List("(", relationSql(left), "cross join", relationSql(right), ")")
          case (_, _) =>
            List("(", relationSql(left), tpe.sql, relationSql(right), "on", clause._2, ")")
        }
    }).mkString(" ")

    {
      case SelectF(
        isDistinct,
        projections,
        relations,
        filter,
        groupBy,
        orderBy) =>
        "(" +
        List(
          Some("select"),
          isDistinct match { case `SelectDistinct` => Some("distinct"); case _ => None },
          Some(projections.map(p => p.alias.foldLeft(p.expr._2)(_ + " as " + _qq(_))).mkString(", ")),
          relations.map(r => "from " + relationSql(r)),
          filter.map("where " + _._2),
          groupBy.map(g =>
            ("group by" ::
              g.keys.map(_._2).mkString(", ") ::
              g.having.map("having " + _._2).toList).mkString(" ")),
          orderBy.map(o => List("order by", o.keys.map(x => x._2._2 + " " + x._1.toString) mkString(", ")).mkString(" "))).foldMap(_.toList).mkString(" ") +
        ")"
      case VariF(symbol) => ":" + symbol
      case SetLiteralF(exprs) => exprs.map(_._2).mkString("(", ", ", ")")
      case ArrayLiteralF(exprs) => exprs.map(_._2).mkString("[", ", ", "]")
      case SpliceF(expr) => expr.fold("*")("(" + _._2 + ").*")
      case BinopF(lhs, rhs, op) => op match {
        case FieldDeref => rhs._1 match {
          case StringLiteral(str) => "(" + lhs._2 + ")." + str
          case _ => "(" + lhs._2 + "){" + rhs._2 + "}"
        }
        case IndexDeref => "(" + lhs._2 + ")[" + rhs._2 + "]"
        case _ => List("(" + lhs._2 + ")", op.sql, "(" + rhs._2 + ")").mkString(" ")
      }
      case UnopF(expr, op) => op match {
        case ObjectFlatten => "(" + expr._2 + "){*}"
        case ArrayFlatten  => "(" + expr._2 + ")[*]"
        case IsNull        => "(" + expr._2 + ") is null"
        case _ =>
          val s = List(op.sql, "(", expr._2, ")") mkString " "
          // NB: dis-ambiguates the query in case this is the leading projection
          if (op == Distinct) "(" + s + ")" else s
      }
      case IdentF(name) => _qq(name)
      case InvokeFunctionF(name, args) =>
        import quasar.std.StdLib.string
        (name, args) match {
          case (string.Like.name, (_, value) :: (_, pattern) :: (StringLiteral("\\"), _) :: Nil) =>
            "(" + value + ") like (" + pattern + ")"
          case (string.Like.name, (_, value) :: (_, pattern) :: (_, esc) :: Nil) =>
            "(" + value + ") like (" + pattern + ") escape (" + esc + ")"
          case _ => name + "(" + args.map(_._2).mkString(", ") + ")"
        }
      case MatchF(expr, cases, default) =>
        ("case" ::
          expr._2 ::
          ((cases.map(caseSql) ++ default.map("else " + _._2).toList) :+
            "end")).mkString(" ")
      case SwitchF(cases, default) =>
        ("case" ::
          ((cases.map(caseSql) ++ default.map("else " + _._2).toList) :+
            "end")).mkString(" ")
      case IntLiteralF(v) => v.toString
      case FloatLiteralF(v) => v.toString
      case StringLiteralF(v) => _q(v)
      case NullLiteralF() => "null"
      case BoolLiteralF(v) => if (v) "true" else "false"
    }
  }

  def exprMapUpM0[F[_]: Monad](
    e: Expr)(
    proj:     ((Proj[Expr], Proj[Expr])) => F[Proj[Expr]],
    relation: ((SqlRelation[Expr], SqlRelation[Expr])) => F[SqlRelation[Expr]],
    expr:     ((Expr, Expr)) => F[Expr],
    groupBy:  ((GroupBy[Expr], GroupBy[Expr])) => F[GroupBy[Expr]],
    orderBy:  ((OrderBy[Expr], OrderBy[Expr])) => F[OrderBy[Expr]],
    case0:    ((Case[Expr], Case[Expr])) => F[Case[Expr]]):
      F[Expr] = {

    def caseLoop(node: Case[Expr]): F[Case[Expr]] =
      (exprLoop(node.cond) ⊛ exprLoop(node.expr))(
        node -> Case(_, _)).flatMap(case0)

    def projLoop(node: Proj[Expr]): F[Proj[Expr]] =
      exprLoop(node.expr).flatMap(x2 => proj(node -> Proj(x2, node. alias)))

    def relationLoop(node: SqlRelation[Expr]): F[SqlRelation[Expr]] = node match {
      case t @ TableRelationAST(_, _) => relation(t -> t)

      case r @ ExprRelationAST(expr, alias) =>
        exprLoop(expr).flatMap(expr2 =>
          relation((r, ExprRelationAST(expr2, alias))))

      case r @ JoinRelation(left, right, jt, expr) =>
        (relationLoop(left) ⊛ relationLoop(right) ⊛ exprLoop(expr))(
          r -> JoinRelation(_, _, jt, _)).flatMap(relation)
    }

    def exprLoop(node: Expr): F[Expr] = node match {
      case e @ Unop(x, op) =>
        exprLoop(x).flatMap(x2 => expr((e, Unop(x2, op))))

      case e @ Binop(left, right, op) =>
        (exprLoop(left) ⊛ exprLoop(right))(
          e -> Binop(_, _, op)).flatMap(expr)

      case e @ InvokeFunction(name, args) =>
        args.traverse(exprLoop).flatMap(a2 =>
          expr((e, InvokeFunction(name, a2))))

      case e @ SetLiteral(exprs) =>
        exprs.traverse(exprLoop).flatMap(exprs2 =>
          expr((e, SetLiteral(exprs2))))

      case e @ ArrayLiteral(exprs) =>
        exprs.traverse(exprLoop).flatMap(exprs2 =>
          expr((e, ArrayLiteral(exprs2))))

      case e @ Match(x, cases, default) =>
        (exprLoop(x) ⊛ cases.traverse(caseLoop) ⊛ default.traverse(exprLoop))(
          e -> Match(_, _, _)).flatMap(expr)

      case e @ Switch(cases, default) =>
        (cases.traverse(caseLoop) ⊛ default.traverse(exprLoop))(
          e -> Switch(_, _)).flatMap(expr)

      case select0 @ Select(d, p, r, f, g, o) =>
        (p.traverse(projLoop) ⊛
          r.traverse(relationLoop) ⊛
          f.traverse(exprLoop) ⊛
          g.traverse(groupByLoop) ⊛
          o.traverse(orderByLoop))(
          select0 -> Select(d, _, _, _, _, _)).flatMap(expr)

      case e @ Splice(Some(x)) =>
        exprLoop(x).flatMap(x2 => expr((e, Splice(Some(x2)))))
      case l @ Splice(None)     => expr(l -> l)
      case l @ Ident(_)         => expr(l -> l)
      case l @ IntLiteral(_)    => expr(l -> l)
      case l @ FloatLiteral(_)  => expr(l -> l)
      case l @ StringLiteral(_) => expr(l -> l)
      case l @ NullLiteral()    => expr(l -> l)
      case l @ BoolLiteral(_)   => expr(l -> l)
      case l @ Vari(_)          => expr(l -> l)
    }

    def groupByLoop(node: GroupBy[Expr]): F[GroupBy[Expr]] =
      (node.keys.traverse(exprLoop) ⊛ node.having.traverse(exprLoop))((k2, h2) =>
        node -> GroupBy(k2, h2)).flatMap(groupBy)

    def orderByLoop(node: OrderBy[Expr]): F[OrderBy[Expr]] =
      node.keys.traverse {
        case (orderType, key) => exprLoop(key).map((orderType, _))
      }.flatMap(k2 => orderBy((node, OrderBy(k2))))

    exprLoop(e)
  }


  implicit val ExprFTraverse: Traverse[ExprF] = new Traverse[ExprF] {
    def traverseImpl[G[_], A, B](
      fa: ExprF[A])(
      f: A => G[B])(
      implicit G: Applicative[G]):
        G[ExprF[B]] = {
      def traverseCase(c: Case[A]): G[Case[B]] =
        G.apply2(f(c.cond), f(c.expr))(Case(_, _))
      def traverseRelation(r: SqlRelation[A]): G[SqlRelation[B]] = r match {
        case TableRelationAST(name, alias) =>
          G.point(TableRelationAST(name, alias))
        case ExprRelationAST(expr, aliasName) =>
          G.apply(f(expr))(ExprRelationAST(_, aliasName))
        case JoinRelation(left, right, tpe, clause) =>
          G.apply3(traverseRelation(left), traverseRelation(right), f(clause))(
            JoinRelation(_, _, tpe, _))
      }

      fa match {
        case SelectF(dist, proj, rel, filter, group, order) =>
          G.apply5(
            Traverse[List].sequence(proj.map(p => f(p.expr).map(Proj(_, p.alias)))),
            Traverse[Option].sequence(rel.map(traverseRelation)),
            Traverse[Option].sequence(filter.map(f)),
            Traverse[Option].sequence(group.map(g =>
              G.apply2(
                Traverse[List].sequence(g.keys.map(f)),
                Traverse[Option].sequence(g.having.map(f)))(
                GroupBy(_, _)))),
            Traverse[Option].sequence(order.map(o =>
              G.apply(Traverse[List].sequence(o.keys.map(p =>
                Traverse[(OrderType, ?)].sequence(p.map(f)))))(
                OrderBy(_)))))(
            SelectF(dist, _, _, _, _, _))
        case VariF(symbol) => G.point(VariF(symbol))
        case SetLiteralF(exprs) =>
          G.map(Traverse[List].sequence(exprs.map(f)))(SetLiteralF(_))
        case ArrayLiteralF(exprs) =>
          G.map(Traverse[List].sequence(exprs.map(f)))(ArrayLiteralF(_))
        case SpliceF(expr) =>
          G.map(Traverse[Option].sequence(expr.map(f))) (SpliceF(_))
        case BinopF(lhs, rhs, op) =>
          G.apply2(f(lhs), f(rhs))(BinopF(_, _, op))
        case UnopF(expr, op) =>
          G.apply(f(expr))(UnopF(_, op))
        case IdentF(name) => G.point(IdentF(name))
        case InvokeFunctionF(name, args) =>
          G.map(Traverse[List].sequence(args.map(f)))(InvokeFunctionF(name, _))
        case MatchF(expr, cases, default) => G.apply3(
          f(expr),
          Traverse[List].sequence(cases.map(traverseCase)),
          Traverse[Option].sequence(default.map(f)))(
          MatchF(_, _, _))
        case SwitchF(cases, default) => G.apply2(
          Traverse[List].sequence(cases.map(traverseCase)),
          Traverse[Option].sequence(default.map(f)))(
          SwitchF(_, _))
        case IntLiteralF(v) => G.point(IntLiteralF(v))
        case FloatLiteralF(v) => G.point(FloatLiteralF(v))
        case StringLiteralF(v) => G.point(StringLiteralF(v))
        case NullLiteralF() => G.point(NullLiteralF())
        case BoolLiteralF(v) => G.point(BoolLiteralF(v))
      }
    }
  }

  private val astType = "AST" :: Nil

  implicit def SqlRelationRenderTree[A: RenderTree]: RenderTree[SqlRelation[A]] =
    new RenderTree[SqlRelation[A]] {
      def render(r: SqlRelation[A]): RenderedTree = r match {
        case ExprRelationAST(select, alias) => NonTerminal("ExprRelation" :: astType, Some("Expr as " + alias), select.render :: Nil)
        case TableRelationAST(name, Some(alias)) => Terminal("TableRelation" :: astType, Some(name + " as " + alias))
        case TableRelationAST(name, None)        => Terminal("TableRelation" :: astType, Some(name))
        case JoinRelation(left, right, jt, clause) =>
          NonTerminal("JoinRelation" :: astType, Some(jt.toString),
            List(render(left), render(right), clause.render))
      }
    }

  implicit val ExprRenderTree: RenderTree[Expr] = new RenderTree[Expr] {
    def renderCase(c: Case[Expr]): RenderedTree =
      NonTerminal("Case" :: astType, None, render(c.cond) :: render(c.expr) :: Nil)

    def render(n: Expr) = n match {
      case Select(isDistinct, projections, relations, filter, groupBy, orderBy) =>
        val nt = "Select" :: astType
        NonTerminal(nt,
          isDistinct match { case `SelectDistinct` => Some("distinct"); case _ => None },
          projections.map { p =>
            NonTerminal("Proj" :: astType, p.alias, render(p.expr) :: Nil)
          } ⊹
            (relations.map(_.render) ::
              filter.map(f => render(f)) ::
              groupBy.map {
                case GroupBy(keys, Some(having)) => NonTerminal("GroupBy" :: astType, None, keys.map(render(_)) :+ render(having))
                case GroupBy(keys, None)         => NonTerminal("GroupBy" :: astType, None, keys.map(render(_)))
              } ::
              orderBy.map {
                case OrderBy(keys) =>
                  val nt = "OrderBy" :: astType
                  NonTerminal(nt, None,
                    keys.map { case (t, x) => NonTerminal("OrderType" :: nt, Some(t.toString), render(x) :: Nil)})
              } ::
              Nil).foldMap(_.toList))

      case SetLiteral(exprs) => NonTerminal("Set" :: astType, None, exprs.map(render(_)))
      case ArrayLiteral(exprs) => NonTerminal("Array" :: astType, None, exprs.map(render(_)))

      case InvokeFunction(name, args) => NonTerminal("InvokeFunction" :: astType, Some(name), args.map(render(_)))

      case Match(expr, cases, Some(default)) => NonTerminal("Match" :: astType, None, render(expr) :: (cases.map(renderCase) :+ render(default)))
      case Match(expr, cases, None)          => NonTerminal("Match" :: astType, None, render(expr) :: cases.map(renderCase))

      case Switch(cases, Some(default)) => NonTerminal("Switch" :: astType, None, cases.map(renderCase) :+ render(default))
      case Switch(cases, None)          => NonTerminal("Switch" :: astType, None, cases.map(renderCase))

      case Binop(lhs, rhs, op) => NonTerminal("Binop" :: astType, Some(op.toString), render(lhs) :: render(rhs) :: Nil)

      case Unop(expr, op) => NonTerminal("Unop" :: astType, Some(op.sql), render(expr) :: Nil)

      case Splice(expr) => NonTerminal("Splice" :: astType, None, expr.toList.map(render(_)))

      case Ident(name) => Terminal("Ident" :: astType, Some(name))

      case Vari(name) => Terminal("Variable" :: astType, Some(":" + name))

      case x => Terminal("LiteralExpr" :: astType, Some(pprint(x)))
    }
  }
}
