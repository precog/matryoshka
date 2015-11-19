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
import quasar.recursionschemes._, Recursive.ops._, FunctorT.ops._
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

  def rewriteRelations(q: Expr)(f: SqlRelation[Expr] => Option[SqlRelation[Expr]]): Expr = {
    def rewrite(r: SqlRelation[Expr]): Option[SqlRelation[Expr]] =
      f(r).orElse(r match {
        case JoinRelation(left, right, tpe, clause) =>
          (rewrite(left) |@| rewrite(right))((l,r) => sql.JoinRelation(l, r, tpe, clause))
        case _ => None
      })
    q.transAnaT(x => x match {
      case Fix(sel @ ExprF.SelectF(_, _, Some(rel), _, _, _)) =>
        rewrite(rel).fold(x)(r => Fix(sel.copy(relations = Some(r))))
      case _ => x
    })
  }

  def pprint(sql: Expr) = sql.para(pprintƒ)

  private val SimpleNamePattern = "[_a-zA-Z][_a-zA-Z0-9$]*".r

  private def _q(s: String): String = "'" + s.replace("'", "''") + "'"

  private def _qq(s: String): String = s match {
    case SimpleNamePattern() => s
    case _                   => "\"" + s.replace("\"", "\"\"") + "\""
  }

  private def pprintRelationƒ(r: SqlRelation[(Expr, String)]): String = (r match {
    case TableRelationAST(name, alias) => _qq(name) :: alias.toList
    case ExprRelationAST(expr, aliasName) =>
      List(expr._2, aliasName)
    case JoinRelation(left, right, tpe, clause) =>
      (tpe, clause._1) match {
        case (InnerJoin, BoolLiteral(true)) =>
          List("(", pprintRelationƒ(left), "cross join", pprintRelationƒ(right), ")")
        case (_, _) =>
          List("(", pprintRelationƒ(left), tpe.sql, pprintRelationƒ(right), "on", clause._2, ")")
      }
  }).mkString(" ")

  def pprintRelation(r: SqlRelation[Expr]) =
    pprintRelationƒ(traverseRelation[Id, Expr, (Expr, String)](r, x => (x, pprint(x))))

  val pprintƒ: ExprF[(Expr, String)] => String = {
    def caseSql(c: Case[(Expr, String)]): String =
      List("when", c.cond._2, "then", c.expr._2) mkString " "

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
          relations.map(r => "from " + pprintRelationƒ(r)),
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

  def normalizeƒ[T[_[_]]: Corecursive]:
      ExprF[T[ExprF]] => Option[ExprF[T[ExprF]]] = {
    case BinopF(l, r, Union) =>
      UnopF(Corecursive[T].embed(BinopF(l, r, UnionAll)), Distinct).some
    case BinopF(l, r, Intersect) =>
      UnopF(Corecursive[T].embed(BinopF(l, r, IntersectAll)), Distinct).some
    case _ => None
  }

  private def traverseRelation[G[_], A, B](r: SqlRelation[A], f: A => G[B])(
      implicit G: Applicative[G]): G[SqlRelation[B]] = r match {
    case TableRelationAST(name, alias) =>
      G.point(TableRelationAST(name, alias))
    case ExprRelationAST(expr, aliasName) =>
      G.apply(f(expr))(ExprRelationAST(_, aliasName))
    case JoinRelation(left, right, tpe, clause) =>
      G.apply3(traverseRelation(left, f), traverseRelation(right, f), f(clause))(
        JoinRelation(_, _, tpe, _))
  }

  implicit val ExprFTraverse: Traverse[ExprF] = new Traverse[ExprF] {
    def traverseImpl[G[_], A, B](
      fa: ExprF[A])(
      f: A => G[B])(
      implicit G: Applicative[G]):
        G[ExprF[B]] = {
      def traverseCase(c: Case[A]): G[Case[B]] =
        G.apply2(f(c.cond), f(c.expr))(Case(_, _))

      fa match {
        case SelectF(dist, proj, rel, filter, group, order) =>
          G.apply5(
            Traverse[List].sequence(proj.map(p => f(p.expr).map(Proj(_, p.alias)))),
            Traverse[Option].sequence(rel.map(traverseRelation(_, f))),
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

  implicit def SqlRelationRenderTree: RenderTree ~> λ[α => RenderTree[SqlRelation[α]]] =
    new (RenderTree ~> λ[α => RenderTree[SqlRelation[α]]]) {
      def apply[α](ra: RenderTree[α]) = new RenderTree[SqlRelation[α]] {
        def render(r: SqlRelation[α]): RenderedTree = r match {
          case ExprRelationAST(select, alias) => NonTerminal("ExprRelation" :: astType, Some("Expr as " + alias), ra.render(select) :: Nil)
          case TableRelationAST(name, Some(alias)) => Terminal("TableRelation" :: astType, Some(name + " as " + alias))
          case TableRelationAST(name, None)        => Terminal("TableRelation" :: astType, Some(name))
          case JoinRelation(left, right, jt, clause) =>
            NonTerminal("JoinRelation" :: astType, Some(jt.toString),
              List(render(left), render(right), ra.render(clause)))
        }
      }
    }

  implicit val ExprFRenderTree: RenderTree ~> λ[α => RenderTree[ExprF[α]]] =
    new (RenderTree ~> λ[α => RenderTree[ExprF[α]]]) {
      def apply[α](ra: RenderTree[α]): RenderTree[ExprF[α]] = new RenderTree[ExprF[α]] {
        def renderCase(c: Case[α]): RenderedTree =
          NonTerminal("Case" :: astType, None, ra.render(c.cond) :: ra.render(c.expr) :: Nil)

        def render(n: ExprF[α]) = n match {
          case SelectF(isDistinct, projections, relations, filter, groupBy, orderBy) =>
            val nt = "Select" :: astType
            NonTerminal(nt,
              isDistinct match { case `SelectDistinct` => Some("distinct"); case _ => None },
              projections.map { p =>
                NonTerminal("Proj" :: astType, p.alias, ra.render(p.expr) :: Nil)
              } ⊹
                (relations.map(SqlRelationRenderTree(ra).render) ::
                  filter.map(ra.render) ::
                  groupBy.map {
                    case GroupBy(keys, Some(having)) => NonTerminal("GroupBy" :: astType, None, keys.map(ra.render) :+ ra.render(having))
                    case GroupBy(keys, None)         => NonTerminal("GroupBy" :: astType, None, keys.map(ra.render))
                  } ::
                  orderBy.map {
                    case OrderBy(keys) =>
                      val nt = "OrderBy" :: astType
                      NonTerminal(nt, None,
                        keys.map { case (t, x) => NonTerminal("OrderType" :: nt, Some(t.toString), ra.render(x) :: Nil)})
                  } ::
                  Nil).foldMap(_.toList))

          case SetLiteralF(exprs) => NonTerminal("Set" :: astType, None, exprs.map(ra.render))
          case ArrayLiteralF(exprs) => NonTerminal("Array" :: astType, None, exprs.map(ra.render))

          case InvokeFunctionF(name, args) => NonTerminal("InvokeFunction" :: astType, Some(name), args.map(ra.render))

          case MatchF(expr, cases, Some(default)) => NonTerminal("Match" :: astType, None, ra.render(expr) :: (cases.map(renderCase) :+ ra.render(default)))
          case MatchF(expr, cases, None)          => NonTerminal("Match" :: astType, None, ra.render(expr) :: cases.map(renderCase))

          case SwitchF(cases, Some(default)) => NonTerminal("Switch" :: astType, None, cases.map(renderCase) :+ ra.render(default))
          case SwitchF(cases, None)          => NonTerminal("Switch" :: astType, None, cases.map(renderCase))

          case BinopF(lhs, rhs, op) => NonTerminal("Binop" :: astType, Some(op.toString), ra.render(lhs) :: ra.render(rhs) :: Nil)

          case UnopF(expr, op) => NonTerminal("Unop" :: astType, Some(op.sql), ra.render(expr) :: Nil)

          case SpliceF(expr) => NonTerminal("Splice" :: astType, None, expr.toList.map(ra.render))

          case IdentF(name) => Terminal("Ident" :: astType, Some(name))

          case VariF(name) => Terminal("Variable" :: astType, Some(":" + name))

          case IntLiteralF(v) => Terminal("LiteralExpr" :: astType, Some(v.shows))
          case FloatLiteralF(v) => Terminal("LiteralExpr" :: astType, Some(v.shows))
          case StringLiteralF(v) => Terminal("LiteralExpr" :: astType, Some(v.shows))
          case NullLiteralF() => Terminal("LiteralExpr" :: astType, None)
          case BoolLiteralF(v) => Terminal("LiteralExpr" :: astType, Some(v.shows))
        }
      }
    }
}
