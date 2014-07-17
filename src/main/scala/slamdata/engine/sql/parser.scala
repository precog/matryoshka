package slamdata.engine.sql

import slamdata.engine.{ParsingError, GenericParsingError}

import scala.util.matching.Regex

import scala.util.parsing.combinator._
import scala.util.parsing.combinator.lexical._
import scala.util.parsing.combinator.syntactical._
import scala.util.parsing.combinator.token._

import scala.util.parsing.input.CharArrayReader.EofCh

import scalaz._
import scalaz.std.option._
import scalaz.syntax.bind._

case class Query(value: String)

class SQLParser extends StandardTokenParsers {
  class SqlLexical extends StdLexical {
    case class FloatLit(chars: String) extends Token {
      override def toString = chars
    }

    override def token: Parser[Token] = numLitParser | super.token

    override protected def processIdent(name: String) = 
      if (reserved contains name.toLowerCase) Keyword(name.toLowerCase) else Identifier(name)

    def numLitParser: Parser[Token] = rep1(digit) ~ opt('.' ~> rep(digit)) ^^ {
      case i ~ None    => NumericLit(i mkString "")
      case i ~ Some(d) => FloatLit(i.mkString("") + "." + d.mkString(""))
    }

    override def whitespace: Parser[Any] = rep(
      whitespaceChar |
      '/' ~ '*' ~ comment |
      '-' ~ '-' ~ rep(chrExcept(EofCh, '\n')) |
      '/' ~ '*' ~ failure("unclosed comment")
    )

    override protected def comment: Parser[Any] = (
      '*' ~ '/'  ^^ { case _ => ' '  } | 
      chrExcept(EofCh) ~ comment
    )
  }

  override val lexical = new SqlLexical

  def floatLit: Parser[String] = elem("decimal", _.isInstanceOf[lexical.FloatLit]) ^^ (_.chars)

  lexical.reserved += (
    "select", "as", "or", "and", "group", "order", "by", "where", "limit", "offset",
    "join", "asc", "desc", "from", "on", "not", "having", "distinct",
    "case", "when", "then", "else", "end", "for", "from", "exists", "between", "like", "in",
    "year", "month", "day", "hour", "second", "null", "is", "date", "interval", "group", "order",
    "date", "left", "right", "outer", "inner", "full", "cross" 
  )

  lexical.delimiters += (
    "*", "+", "-", "%", "<", "=", "<>", "!=", "<=", ">=", ">", "/", "(", ")", ",", ".", ";", "[", "]", "{", "}"
  )

  override def keyword(name: String): Parser[String] =
    if (lexical.reserved.contains(name)) elem("keyword '" + name + "'", v => v.chars == name && v.isInstanceOf[lexical.Keyword]) ^^ (_.chars)
    else failure("You are trying to parse \""+name+"\" as a keyword, but it is not contained in the reserved keywords list")

  def op(op : String): Parser[String] =
    if (lexical.delimiters.contains(op)) elem("operator '" + op + "'", v => v.chars == op && v.isInstanceOf[lexical.Keyword]) ^^ (_.chars)
    else failure("You are trying to parse \""+op+"\" as an operator, but it is not contained in the operators list")

  def select: Parser[SelectStmt] =
    keyword("select") ~> projections ~
      opt(relations) ~ opt(filter) ~
      opt(group_by) ~ opt(order_by) ~ opt(limit) ~ opt(offset) <~ opt(op(";")) ^^ {
    case p ~ r ~ f ~ g ~ o ~ l ~ off => SelectStmt(p, r.join, f, g, o, l, off)
  }

  def projections: Parser[List[Proj]] = repsep(projection, op(",")).map(_.toList)

  def projection: Parser[Proj] = expr ~ opt(keyword("as") ~> ident) ^^ {
    case expr ~ ident => Proj(expr, ident)
  }

  def expr: Parser[Expr] = or_expr

  def or_expr: Parser[Expr] =
    and_expr * ( keyword("or") ^^^ { (a: Expr, b: Expr) => Binop(a, b, Or) } )

  def and_expr: Parser[Expr] =
    cmp_expr * ( keyword("and") ^^^ { (a: Expr, b: Expr) => Binop(a, b, And) } )

  def relationalOp: Parser[BinaryOperator] = 
    op("=")  ^^^ Eq  | 
    op("<>") ^^^ Neq | 
    op("!=") ^^^ Neq |
    op("<")  ^^^ Lt  | 
    op("<=") ^^^ Le  | 
    op(">")  ^^^ Gt  | 
    op(">=") ^^^ Ge

  def relational_suffix: Parser[(BinaryOperator, Expr)] =
    relationalOp ~ add_expr ^^ {
      case op ~ rhs => (op, rhs)
    }

  def datetime_op: Parser[UnaryOperator] = 
    (keyword("year")    ^^^ YearFrom | 
     keyword("month")   ^^^ MonthFrom | 
     keyword("day")     ^^^ DayFrom | 
     keyword("hour")    ^^^ HourFrom | 
     keyword("minute")  ^^^ MinuteFrom | 
     keyword("second")  ^^^ SecondFrom) <~ keyword("from")

  def between_op: Parser[BinaryOperator] = 
    keyword("between") ^^^ Between

  def between_suffix: Parser[(BinaryOperator, Expr)] =
    between_op ~ add_expr ~ keyword("and") ~ add_expr ^^ {
      case op ~ lower ~ _ ~ upper => (op, InvokeFunction("RANGE", lower :: upper :: Nil))
    }

  def in_suffix: Parser[(BinaryOperator, Expr)] =
    opt(keyword("not")) ~ keyword("in") ~ add_expr ^^ {
      case Some("not") ~ op ~ a => (NotIn, a)
      case None        ~ op ~ a => (In, a)
    }

  def like_suffix: Parser[(BinaryOperator, Expr)] =
    opt(keyword("not")) ~ keyword("like") ~ add_expr ^^ { 
      case Some("not") ~ _ ~ a => (NotLike, a)
      case None        ~ _ ~ a => (Like, a)
    }

  def set_literal: Parser[Expr] =
    (op("(") ~> rep1sep(expr, op(",")) <~ op(")")) ^^ SetLiteral

  def set_expr: Parser[Expr] = 
    (select ^^ Subselect) | set_literal

  def cmp_expr: Parser[Expr] =
    add_expr ~ rep(relational_suffix | between_suffix | in_suffix | like_suffix) ^^ {
      case lhs ~ suffixes =>
        suffixes.foldLeft(lhs) {
          case (lhs, (Between, (InvokeFunction("RANGE", lower :: upper :: Nil)))) => InvokeFunction("(BETWEEN)", lhs :: lower :: upper :: Nil)
          case (lhs, (op, rhs)) => op(lhs, rhs)
        }
    }

  def add_expr: Parser[Expr] = mult_expr * (op("+") ^^^ Plus | op("-") ^^^ Minus)

  def mult_expr: Parser[Expr] = deref_expr * (op("*") ^^^ Mult | op("/") ^^^ Div | op("%") ^^^ Mod)

  sealed trait DerefType
  case class ObjectDeref(expr: Expr) extends DerefType
  case class ArrayDeref(expr: Expr) extends DerefType

  def deref_expr: Parser[Expr] = primary_expr ~ (rep(
      (op(".") ~> ((ident ^^ StringLiteral) ^^ ObjectDeref)) |
      (op("{") ~> (add_expr ^^ ObjectDeref) <~ op("}")) |
      (op("[") ~> (add_expr ^^ ArrayDeref) <~ op("]"))
    ): Parser[List[DerefType]]) ~ opt(op(".") ~> wildcard) ^^ {
    case lhs ~ derefs ~ wild =>
      wild.foldLeft(derefs.foldLeft[Expr](lhs) {
        case (lhs, ObjectDeref(rhs)) => FieldDeref(lhs, rhs)
        case (lhs, ArrayDeref(rhs))  => IndexDeref(lhs, rhs)
      })(FieldDeref)
  }

  def unary_operator: Parser[UnaryOperator] = op("+") ^^^ Positive | op("-") ^^^ Negative

  def wildcard: Parser[Expr] = op("*") ^^^ Wildcard

  def primary_expr: Parser[Expr] =
    literal |
    wildcard |
    ident ~ (op("(") ~> repsep(expr, op(",")) <~ op(")")) ^^ {      
      case a ~ xs => InvokeFunction(a, xs)
    } |
    ident ^^ Ident |
    set_expr |
    op("(") ~> (expr | select ^^ (Subselect(_))) <~ op(")") |
    unary_operator ~ primary_expr ^^ {
      case op ~ expr => op(expr)
    } |
    keyword("distinct")   ~> cmp_expr ^^ Distinct |
    keyword("not")        ~> cmp_expr ^^ Not |
    keyword("exists")     ~> cmp_expr ^^ Exists |
    keyword("date")       ~> cmp_expr ^^ ToDate |
    keyword("interval")   ~> cmp_expr ^^ ToInterval |
    datetime_op ~ cmp_expr ^^ { case op ~ expr => op(expr) } |
    case_expr

  def case_expr: Parser[Expr] =
    keyword("case") ~>
      opt(expr) ~ rep1(keyword("when") ~> expr ~ keyword("then") ~ expr ^^ { case a ~ _ ~ b => Case(a, b) }) ~
      opt(keyword("else") ~> expr) <~ keyword("end") ^^ {
      case Some(e) ~ cases ~ default => Match(e, cases, default)
      case None ~ cases ~ default => Switch(cases, default)
    }

  def literal: Parser[Expr] =
    numericLit ^^ { case i => IntLiteral(i.toInt) } |
    floatLit ^^ { case f => FloatLiteral(f.toDouble) } |
    stringLit ^^ { case s => StringLiteral(s) } |
    keyword("null") ^^^ NullLiteral.apply

  def relations: Parser[Option[SqlRelation]] =
    keyword("from") ~> rep1sep(relation, op(",")).map(_.foldLeft[Option[SqlRelation]](None) {
      case (None, traverse) => Some(traverse)
      case (Some(acc), traverse) => Some(CrossRelation(acc, traverse))
    })

  def std_join_relation: Parser[SqlRelation => SqlRelation] = 
    opt(join_type) ~ keyword("join") ~ simple_relation ~ keyword("on") ~ expr ^^
      { case tpe ~ _ ~ r2 ~ _ ~ e => r1 => JoinRelation(r1, r2, tpe.getOrElse(InnerJoin), e) }

  def cross_join_relation: Parser[SqlRelation => SqlRelation] = 
    keyword("cross") ~> keyword("join") ~> simple_relation ^^ {
      case r2 => r1 => CrossRelation(r1, r2)
    }

  def relation: Parser[SqlRelation] =
    simple_relation ~ rep(std_join_relation | cross_join_relation) ^^ {
      case r ~ fs => fs.foldLeft(r) { case (r, f) => f(r) }
    }

  def join_type: Parser[JoinType] =
    (keyword("left") | keyword("right") | keyword("full")) ~ opt(keyword("outer")) ^^ {
      case "left" ~ o  => LeftJoin
      case "right" ~ o => RightJoin
      case "full" ~ o => FullJoin
    } | keyword("inner") ^^^ (InnerJoin)

  def simple_relation: Parser[SqlRelation] =
    ident ~ opt(keyword("as")) ~ opt(ident) ^^ {
      case ident ~ _ ~ alias => TableRelationAST(ident, alias)
    } |
    op("(") ~ select ~ op(")") ~ opt(keyword("as")) ~ ident ^^ {
      case _ ~ select ~ _ ~ _ ~ alias => SubqueryRelationAST(select, alias)
    }

  def filter: Parser[Expr] = keyword("where") ~> expr

  def group_by: Parser[GroupBy] =
    keyword("group") ~> keyword("by") ~> rep1sep(expr, op(",")) ~ opt(keyword("having") ~> expr) ^^ {
      case k ~ h => GroupBy(k, h)
    }

  def order_by: Parser[OrderBy] =
    keyword("order") ~> keyword("by") ~> rep1sep( expr ~ opt(keyword("asc") | keyword("desc")) ^^ {
      case i ~ (Some("asc") | None) => (i, ASC)
      case i ~ Some("desc") => (i, DESC)
    }, op(",")) ^^ (OrderBy(_))

  def limit: Parser[Long] = keyword("limit") ~> numericLit ^^ (_.toLong)

  def offset: Parser[Long] = keyword("offset") ~> numericLit ^^ (_.toLong)  

  private def stripQuotes(s:String) = s.substring(1, s.length-1)

  def parse(sql: Query): ParsingError \/ SelectStmt = {
    phrase(select)(new lexical.Scanner(sql.value)) match {
      case Success(r, q)        => \/.right(r)
      case Error(msg, input)    => \/.left(GenericParsingError(msg))
      case Failure(msg, input)  => \/.left(GenericParsingError(msg))
    }
  }
}
