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
import quasar.fp._
import quasar.recursionschemes.Fix
import quasar.fs._; import Path._
import quasar.std._

import scala.Any
import scala.util.matching.Regex
import scala.util.parsing.combinator.lexical._
import scala.util.parsing.combinator.syntactical._
import scala.util.parsing.input.CharArrayReader.EofCh

import scalaz._
import Scalaz._

sealed trait ParsingError { def message: String}
final case class GenericParsingError(message: String) extends ParsingError
final case class ParsingPathError(error: PathError) extends ParsingError {
  def message = error.message
}

final case class Query(value: String)

class SQLParser extends StandardTokenParsers {
  class SqlLexical extends StdLexical {
    case class FloatLit(chars: String) extends Token {
      override def toString = chars
    }
    case class Variable(chars: String) extends Token {
      override def toString = ":" + chars
    }

    override def token: Parser[Token] = variParser | numLitParser | stringLitParser | quotedIdentParser | super.token

    override protected def processIdent(name: String) =
      if (reserved contains name.toLowerCase) Keyword(name.toLowerCase) else Identifier(name)

    def identifierString: Parser[String] =
      ((letter | elem('_')) ~ rep(digit | letter | elem('_'))) ^^ {
        case x ~ xs => x.toString + xs.mkString
      }

    def variParser: Parser[Token] = ':' ~> identifierString ^^ (Variable(_))

    def numLitParser: Parser[Token] = rep1(digit) ~ opt('.' ~> rep(digit)) ~ opt((elem('e') | 'E') ~> opt(elem('-') | '+') ~ rep(digit)) ^^ {
      case i ~ None ~ None     => NumericLit(i mkString "")
      case i ~ Some(d) ~ None  => FloatLit(i.mkString("") + "." + d.mkString(""))
      case i ~ d ~ Some(s ~ e) => FloatLit(i.mkString("") + "." + d.map(_.mkString("")).getOrElse("0") + "e" + s.getOrElse("") + e.mkString(""))
    }

    def stringLitParser: Parser[Token] =
      '\'' ~> rep(chrExcept('\'') | ('\'' ~ '\'') ^^ κ('\'')) <~ '\'' ^^ ( chars => StringLit(chars.mkString) )

    def quotedIdentParser: Parser[Token] =
      '"' ~> rep(chrExcept('"') | ('"' ~ '"') ^^ κ('"')) <~ '"' ^^ (chars => Identifier(chars.mkString))

    override def whitespace: Parser[Any] = rep(
      whitespaceChar |
      '/' ~ '*' ~ comment |
      '-' ~ '-' ~ rep(chrExcept(EofCh, '\n')) |
      '/' ~ '*' ~ failure("unclosed comment")
    )

    override protected def comment: Parser[Any] = (
      '*' ~ '/'  ^^ κ(' ') |
      chrExcept(EofCh) ~ comment
    )
  }

  override val lexical = new SqlLexical

  def floatLit: Parser[String] = elem("decimal", _.isInstanceOf[lexical.FloatLit]) ^^ (_.chars)

  ignore(lexical.reserved += (
    "and", "as", "asc", "between", "by", "case", "cross", "date", "desc", "distinct",
    "else", "end", "escape", "exists", "false", "for", "from", "full", "group", "having", "in",
    "inner", "interval", "is", "join", "left", "like", "limit", "not", "null",
    "offset", "oid", "on", "or", "order", "outer", "right", "select", "then", "time",
    "timestamp", "true", "when", "where"
  ))

  ignore(lexical.delimiters += (
    "*", "+", "-", "%", "~", "||", "<", "=", "<>", "!=", "<=", ">=", ">", "/", "(", ")", ",", ".", ";", "[", "]", "{", "}"
  ))

  override def keyword(name: String): Parser[String] =
    if (lexical.reserved.contains(name))
      elem("keyword '" + name + "'", v => v.chars == name && v.isInstanceOf[lexical.Keyword]) ^^ (_.chars)
    else failure("You are trying to parse \""+name+"\" as a keyword, but it is not contained in the reserved keywords list")

  def op(op : String): Parser[String] =
    if (lexical.delimiters.contains(op)) elem("operator '" + op + "'", v => v.chars == op && v.isInstanceOf[lexical.Keyword]) ^^ (_.chars)
    else failure("You are trying to parse \""+op+"\" as an operator, but it is not contained in the operators list")

  def select: Parser[Expr] =
    keyword("select") ~> opt(keyword("distinct")) ~ projections ~
      opt(relations) ~ opt(filter) ~
      opt(group_by) ~ opt(order_by) ~ opt(limit) ~ opt(offset) <~ opt(op(";")) ^^ {
        case d ~ p ~ r ~ f ~ g ~ o ~ l ~ off =>
          Select(d.map(κ(SelectDistinct)).getOrElse(SelectAll), p, r.join, f, g, o, l, off)
      }

  def projections: Parser[List[Proj[Expr]]] = repsep(projection, op(",")).map(_.toList)

  def projection: Parser[Proj[Expr]] = expr ~ opt(keyword("as") ~> ident) ^^ {
    case expr ~ ident => Proj(expr, ident)
  }

  def variable: Parser[Expr] = elem("variable", _.isInstanceOf[lexical.Variable]) ^^ (token => Vari(token.chars))

  def expr: Parser[Expr] = or_expr

  def or_expr: Parser[Expr] =
    and_expr * ( keyword("or") ^^^ { (a: Expr, b: Expr) => Or(a, b) } )

  def and_expr: Parser[Expr] =
    cmp_expr * ( keyword("and") ^^^ { (a: Expr, b: Expr) => And(a, b) } )

  def relationalOp: Parser[BinaryOperator] =
    op("=")  ^^^ Eq  |
    op("<>") ^^^ Neq |
    op("!=") ^^^ Neq |
    op("<")  ^^^ Lt  |
    op("<=") ^^^ Le  |
    op(">")  ^^^ Gt  |
    op(">=") ^^^ Ge

  def relational_suffix: Parser[Expr => Expr] =
    relationalOp ~ default_expr ^^ {
      case op ~ rhs => Binop(_, rhs, op)
    }

  def between_suffix: Parser[Expr => Expr] =
    keyword("between") ~ default_expr ~ keyword("and") ~ default_expr ^^ {
      case _ ~ lower ~ _ ~ upper =>
        lhs => InvokeFunction(StdLib.relations.Between.name,
          List(lhs, lower, upper))
    }

  def in_suffix: Parser[Expr => Expr] =
    keyword("in") ~ default_expr ^^ { case _ ~ a => In(_, a) }

  def like_suffix: Parser[Expr => Expr] =
    keyword("like") ~ default_expr ~ opt(keyword("escape") ~> default_expr) ^^ {
      case _ ~ a ~ esc =>
        lhs => InvokeFunction(StdLib.string.Like.name,
          List(lhs, a, esc.getOrElse(StringLiteral(""))))
      }

  def is_suffix: Parser[Expr => Expr] =
    (keyword("is") ~ opt(keyword("not")) ~ (
      keyword("null")    ^^^ (IsNull(_))
      | keyword("true")  ^^^ ((x: Expr) => Eq(x, BoolLiteral(true)))
      | keyword("false") ^^^ ((x: Expr) => Eq(x, BoolLiteral(false)))
    )) ^^ { case _ ~ n ~ f => (x: Expr) => val u = f(x); n.fold(u)(κ(Not(u))) }

  def negatable_suffix: Parser[Expr => Expr] = {
    opt(keyword("not")) ~ (between_suffix | in_suffix | like_suffix) ^^ {
      case inv ~ suffix =>
        inv.fold(suffix)(κ(lhs => Not(suffix(lhs))))
    }
  }

  def rep2sep[T, U](p: => Parser[T], s: => Parser[U]) =
    p ~ rep1(s ~> p) ^^ { case x ~ y => x :: y }

  def set_literal: Parser[Expr] =
    (op("(") ~> rep2sep(expr, op(",")) <~ op(")")) ^^ (SetLiteral(_))

  def array_literal: Parser[Expr] =
    (op("[") ~> repsep(expr, op(",")) <~ op("]")) ^^ (ArrayLiteral(_))

  def set_expr: Parser[Expr] = select | set_literal

  def cmp_expr: Parser[Expr] =
    default_expr ~ rep(relational_suffix | negatable_suffix | is_suffix) ^^ {
      case lhs ~ suffixes => suffixes.foldLeft(lhs)((lhs, op) => op(lhs))
    }

  /** The default precedence level, for some built-ins, and all user-defined */
  def default_expr: Parser[Expr] =
    concat_expr * (op("~") ^^^ ((l: Expr, r: Expr) =>
      InvokeFunction(StdLib.string.Search.name, List(l, r))))

  def concat_expr: Parser[Expr] =
    add_expr * (op("||") ^^^ Concat)

  def add_expr: Parser[Expr] =
    mult_expr * (op("+") ^^^ Plus | op("-") ^^^ Minus)

  def mult_expr: Parser[Expr] =
    deref_expr * (op("*") ^^^ Mult | op("/") ^^^ Div | op("%") ^^^ Mod)

  sealed trait DerefType
  case class ObjectDeref(expr: Expr) extends DerefType
  case class ArrayDeref(expr: Expr) extends DerefType

  def deref_expr: Parser[Expr] = primary_expr ~ (rep(
    (op(".") ~> ((ident ^^ (StringLiteral(_))) ^^ (ObjectDeref(_)))) |
      (op("{") ~> (expr ^^ (ObjectDeref(_))) <~ op("}")) |
      (op("[") ~> (expr ^^ (ArrayDeref(_))) <~ op("]"))
    ): Parser[List[DerefType]]) ~ opt(op(".") ~> wildcard) ^^ {
    case lhs ~ derefs ~ wild =>
      wild.foldLeft(derefs.foldLeft[Expr](lhs) {
        case (lhs, ObjectDeref(Splice(None))) => ObjectFlatten(lhs)
        case (lhs, ObjectDeref(rhs))          => FieldDeref(lhs, rhs)
        case (lhs, ArrayDeref(Splice(None)))  => ArrayFlatten(lhs)
        case (lhs, ArrayDeref(rhs))           => IndexDeref(lhs, rhs)
      })((lhs, rhs) => Splice(Some(lhs)))
  }

  def unary_operator: Parser[UnaryOperator] =
    op("+")              ^^^ Positive |
    op("-")              ^^^ Negative |
    keyword("distinct")  ^^^ Distinct |
    keyword("date")      ^^^ ToDate |
    keyword("time")      ^^^ ToTime |
    keyword("timestamp") ^^^ ToTimestamp |
    keyword("interval")  ^^^ ToInterval |
    keyword("oid")       ^^^ ToId

  def wildcard: Parser[Expr] = op("*") ^^^ Splice(None)

  def primary_expr: Parser[Expr] =
    variable |
    literal |
    wildcard |
    ident ~ (op("(") ~> repsep(expr, op(",")) <~ op(")")) ^^ {
      case a ~ xs => InvokeFunction(a, xs)
    } |
    ident ^^ (Ident(_)) |
    array_literal |
    set_expr |
    op("(") ~> (expr | select) <~ op(")") |
    unary_operator ~ primary_expr ^^ {
      case op ~ expr => op(expr)
    } |
    keyword("not")        ~> cmp_expr ^^ Not |
    keyword("exists")     ~> cmp_expr ^^ Exists |
    case_expr

  def case_expr: Parser[Expr] =
    keyword("case") ~>
      opt(expr) ~ rep1(keyword("when") ~> expr ~ keyword("then") ~ expr ^^ { case a ~ _ ~ b => Case(a, b) }) ~
      opt(keyword("else") ~> expr) <~ keyword("end") ^^ {
      case Some(e) ~ cases ~ default => Match(e, cases, default)
      case None ~ cases ~ default => Switch(cases, default)
    }

  def literal: Parser[Expr] =
    numericLit ^^ { case i => IntLiteral(i.toLong) } |
    floatLit ^^ { case f => FloatLiteral(f.toDouble) } |
    stringLit ^^ { case s => StringLiteral(s) } |
    keyword("null") ^^^ NullLiteral() |
    keyword("true") ^^^ BoolLiteral(true) |
    keyword("false") ^^^ BoolLiteral(false)

  def relations: Parser[Option[SqlRelation[Expr]]] =
    keyword("from") ~> rep1sep(relation, op(",")).map(_.foldLeft[Option[SqlRelation[Expr]]](None) {
      case (None, traverse) => Some(traverse)
      case (Some(acc), traverse) => Some(CrossRelation(acc, traverse))
    })

  def std_join_relation: Parser[SqlRelation[Expr] => SqlRelation[Expr]] =
    opt(join_type) ~ keyword("join") ~ simple_relation ~ keyword("on") ~ expr ^^
      { case tpe ~ _ ~ r2 ~ _ ~ e => r1 => JoinRelation(r1, r2, tpe.getOrElse(InnerJoin), e) }

  def cross_join_relation: Parser[SqlRelation[Expr] => SqlRelation[Expr]] =
    keyword("cross") ~> keyword("join") ~> simple_relation ^^ {
      case r2 => r1 => CrossRelation(r1, r2)
    }

  def relation: Parser[SqlRelation[Expr]] =
    simple_relation ~ rep(std_join_relation | cross_join_relation) ^^ {
      case r ~ fs => fs.foldLeft(r) { case (r, f) => f(r) }
    }

  def join_type: Parser[JoinType] =
    (keyword("left") | keyword("right") | keyword("full")) ~ opt(keyword("outer")) ^^ {
      case "left" ~ o  => LeftJoin
      case "right" ~ o => RightJoin
      case "full" ~ o => FullJoin
    } | keyword("inner") ^^^ (InnerJoin)

  def simple_relation: Parser[SqlRelation[Expr]] =
    ident ~ opt(keyword("as")) ~ opt(ident) ^^ {
      case ident ~ _ ~ alias => TableRelationAST[Expr](ident, alias)
    } |
    op("(") ~> (
      (select ~ op(")") ~ opt(keyword("as")) ~ ident ^^ {
        case select ~ _ ~ _ ~ alias => ExprRelationAST(select, alias)
      }) |
      relation <~ op(")"))

  def filter: Parser[Expr] = keyword("where") ~> expr

  def group_by: Parser[GroupBy[Expr]] =
    keyword("group") ~> keyword("by") ~> rep1sep(expr, op(",")) ~ opt(keyword("having") ~> expr) ^^ {
      case k ~ h => GroupBy(k, h)
    }

  def order_by: Parser[OrderBy[Expr]] =
    keyword("order") ~> keyword("by") ~> rep1sep( expr ~ opt(keyword("asc") | keyword("desc")) ^^ {
      case i ~ (Some("asc") | None) => (ASC, i)
      case i ~ Some("desc") => (DESC, i)
    }, op(",")) ^^ (OrderBy(_))

  def limit: Parser[Long] = keyword("limit") ~> numericLit ^^ (_.toLong)

  def offset: Parser[Long] = keyword("offset") ~> numericLit ^^ (_.toLong)

  private def stripQuotes(s:String) = s.substring(1, s.length-1)

  def parseExpr(exprSql: String): ParsingError \/ Expr = {
    phrase(expr)(new lexical.Scanner(exprSql)) match {
      case Success(r, q)        => \/.right(r)
      case Error(msg, input)    => \/.left(GenericParsingError(msg))
      case Failure(msg, input)  => \/.left(GenericParsingError(msg))
    }
  }

  def parse(sql: Query): ParsingError \/ Expr = {
    phrase(expr)(new lexical.Scanner(sql.value)) match {
      case Success(r, q)        => \/.right(r)
      case Error(msg, input)    => \/.left(GenericParsingError(msg))
      case Failure(msg, input)  => \/.left(GenericParsingError(msg + "; " + input.first))
    }
  }
}

object SQLParser {
  def parseInContext(sql: Query, basePath: Path):
      ParsingError \/ Expr =
    new SQLParser().parse(sql).flatMap(relativizePaths(_, basePath).leftMap(ParsingPathError))
}
