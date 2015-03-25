package slamdata.engine.sql

import slamdata.engine.{ParsingError, GenericParsingError}
import slamdata.engine.analysis.fixplate._
import slamdata.engine.fp._
import slamdata.engine.std._

import scala.util.matching.Regex

import scala.util.parsing.combinator._
import scala.util.parsing.combinator.lexical._
import scala.util.parsing.combinator.syntactical._
import scala.util.parsing.combinator.token._

import scala.util.parsing.input.CharArrayReader.EofCh

import scalaz._
import Scalaz._

case class Query(value: String)

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

  lexical.reserved += (
    "and", "as", "asc", "between", "by", "case", "cross", "date", "desc", "distinct",
    "else", "end", "escape", "exists", "false", "for", "from", "full", "group", "having", "in",
    "inner", "interval", "is", "join", "left", "like", "limit", "not", "null",
    "offset", "oid", "on", "or", "order", "outer", "right", "select", "then", "time",
    "timestamp", "true", "when", "where"
  )

  lexical.delimiters += (
    "*", "+", "-", "%", "~", "||", "<", "=", "<>", "!=", "<=", ">=", ">", "/", "(", ")", ",", ".", ";", "[", "]", "{", "}"
  )

  override def keyword(name: String): Parser[String] =
    if (lexical.reserved.contains(name))
      elem("keyword '" + name + "'", v => v.chars == name && v.isInstanceOf[lexical.Keyword]) ^^ (_.chars)
    else failure("You are trying to parse \""+name+"\" as a keyword, but it is not contained in the reserved keywords list")

  def op(op : String): Parser[String] =
    if (lexical.delimiters.contains(op)) elem("operator '" + op + "'", v => v.chars == op && v.isInstanceOf[lexical.Keyword]) ^^ (_.chars)
    else failure("You are trying to parse \""+op+"\" as an operator, but it is not contained in the operators list")

  def select: Parser[Term[Expr]] =
    keyword("select") ~> opt(keyword("distinct")) ~ projections ~
      opt(relations) ~ opt(filter) ~
      opt(group_by) ~ opt(order_by) ~ opt(limit) ~ opt(offset) <~ opt(op(";")) ^^ {
        case d ~ p ~ r ~ f ~ g ~ o ~ l ~ off =>
          Term(Select(d.map(κ(SelectDistinct)).getOrElse(SelectAll), p, r.join, f, g, o, l, off))
      }

  def projections: Parser[List[Proj[Term[Expr]]]] =
    repsep(projection, op(",")).map(_.toList)

  def projection: Parser[Proj[Term[Expr]]] =
    expr ~ opt(keyword("as") ~> ident) ^^ {
      case expr ~ ident => Proj(expr, ident)
    }

  def variable: Parser[Term[Expr]] = elem("variable", _.isInstanceOf[lexical.Variable]) ^^ (token => Term[Expr](Vari(token.chars)))

  def expr: Parser[Term[Expr]] = or_expr

  def or_expr: Parser[Term[Expr]] =
    and_expr * ( keyword("or") ^^^ { (a: Term[Expr], b: Term[Expr]) => Term(Binop(a, b, Or)) } )

  def and_expr: Parser[Term[Expr]] =
    cmp_expr * ( keyword("and") ^^^ { (a: Term[Expr], b: Term[Expr]) => Term(Binop(a, b, And)) } )

  def relationalOp: Parser[BinaryOperator] =
    op("=")  ^^^ Eq  |
    op("<>") ^^^ Neq |
    op("!=") ^^^ Neq |
    op("<")  ^^^ Lt  |
    op("<=") ^^^ Le  |
    op(">")  ^^^ Gt  |
    op(">=") ^^^ Ge

  def relational_suffix: Parser[Term[Expr] => Term[Expr]] =
    relationalOp ~ default_expr ^^ { case op ~ rhs => lhs =>
      Term(Binop(lhs, rhs, op))
    }

  def between_suffix: Parser[Term[Expr] => Term[Expr]] =
    keyword("between") ~ default_expr ~ keyword("and") ~ default_expr ^^ {
      case _ ~ lower ~ _ ~ upper =>
        lhs => Term(InvokeFunction(StdLib.relations.Between.name,
          List(lhs, lower, upper)))
    }

  def in_suffix: Parser[Term[Expr] => Term[Expr]] =
    keyword("in") ~ default_expr ^^ { case _ ~ a => In(_, a) }

  def like_suffix: Parser[Term[Expr] => Term[Expr]] =
    keyword("like") ~ default_expr ~ opt(keyword("escape") ~> default_expr) ^^ {
      case _ ~ a ~ esc =>
        lhs => Term(InvokeFunction(StdLib.string.Like.name,
          List(lhs, a, esc.getOrElse(Term[Expr](StringLiteral(""))))))
      }

  def is_suffix: Parser[Term[Expr] => Term[Expr]] =
    (keyword("is") ~ opt(keyword("not")) ~ (
      keyword("null")    ^^^ (IsNull(_))
        | keyword("true")  ^^^ ((x: Term[Expr]) => Eq(x, Term[Expr](BoolLiteral(true))))
        | keyword("false") ^^^ ((x: Term[Expr]) => Eq(x, Term[Expr](BoolLiteral(false))))
    )) ^^ { case _ ~ n ~ f => (x: Term[Expr]) => val u = f(x); n.fold(u)(κ(Not(u))) }

  def negatable_suffix: Parser[Term[Expr] => Term[Expr]] = {
    opt(keyword("not")) ~ (between_suffix | in_suffix | like_suffix) ^^ {
      case inv ~ suffix =>
        inv.fold(suffix)(κ(lhs => Not(suffix(lhs))))
    }
  }

  def rep2sep[T, U](p: => Parser[T], s: => Parser[U]) =
    p ~ rep1(s ~> p) ^^ { case x ~ y => x :: y }

  def set_literal: Parser[Term[Expr]] =
    (op("(") ~> rep2sep(expr, op(",")) <~ op(")")) ^^ (x => Term(SetLiteral(x)))

  def array_literal: Parser[Term[Expr]] =
    (op("[") ~> repsep(expr, op(",")) <~ op("]")) ^^ (x => Term(ArrayLiteral(x)))

  def set_expr: Parser[Term[Expr]] = select | set_literal

  def cmp_expr: Parser[Term[Expr]] =
    default_expr ~ rep(relational_suffix | negatable_suffix | is_suffix) ^^ {
      case lhs ~ suffixes => suffixes.foldLeft(lhs)((lhs, op) => op(lhs))
    }

  /** The default precedence level, for some built-ins, and all user-defined */
  def default_expr: Parser[Term[Expr]] =
    concat_expr * (op("~") ^^^ ((l: Term[Expr], r: Term[Expr]) =>
      Term(InvokeFunction(StdLib.string.Search.name, List(l, r)))))

  def concat_expr: Parser[Term[Expr]] =
    add_expr * (op("||") ^^^ Concat)

  def add_expr: Parser[Term[Expr]] =
    mult_expr * (op("+") ^^^ Plus | op("-") ^^^ Minus)

  def mult_expr: Parser[Term[Expr]] =
    deref_expr * (op("*") ^^^ Mult | op("/") ^^^ Div | op("%") ^^^ Mod)

  sealed trait DerefType
  case class ObjectDeref(expr: Term[Expr]) extends DerefType
  case class ArrayDeref(expr: Term[Expr]) extends DerefType

  def deref_expr: Parser[Term[Expr]] = primary_expr ~ (rep(
    (op(".") ~> ((ident ^^ (x => Term[Expr](StringLiteral(x)))) ^^ ObjectDeref)) |
      (op("{") ~> (expr ^^ ObjectDeref) <~ op("}")) |
      (op("[") ~> (expr ^^ ArrayDeref) <~ op("]"))
  ): Parser[List[DerefType]]) ~ opt(op(".") ~> wildcard) ^^ {
    case lhs ~ derefs ~ wild =>
      wild.foldLeft(derefs.foldLeft[Term[Expr]](lhs) {
        case (lhs, ObjectDeref(Term(Splice(None)))) => ObjectFlatten(lhs)
        case (lhs, ObjectDeref(rhs))                => FieldDeref(lhs, rhs)
        case (lhs, ArrayDeref(Term(Splice(None))))  => ArrayFlatten(lhs)
        case (lhs, ArrayDeref(rhs))                 => IndexDeref(lhs, rhs)
      })((lhs, rhs) => Term(Splice(Some(lhs))))
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

  def wildcard: Parser[Term[Expr]] = op("*") ^^^ Term[Expr](Splice(None))

  def primary_expr: Parser[Term[Expr]] =
    variable |
    literal |
    wildcard |
    ident ~ (op("(") ~> repsep(expr, op(",")) <~ op(")")) ^^ {
      case a ~ xs => Term(InvokeFunction(a, xs))
    } |
    ident ^^ (x => Term[Expr](Ident(x))) |
    array_literal |
    set_expr |
    op("(") ~> (expr | select) <~ op(")") |
    unary_operator ~ primary_expr ^^ {
      case op ~ expr => op(expr)
    } |
    keyword("not")        ~> cmp_expr ^^ Not |
    keyword("exists")     ~> cmp_expr ^^ Exists |
    case_expr

  def case_expr: Parser[Term[Expr]] =
    keyword("case") ~>
      opt(expr) ~ rep1(keyword("when") ~> expr ~ keyword("then") ~ expr ^^ { case a ~ _ ~ b => Case(a, b) }) ~
      opt(keyword("else") ~> expr) <~ keyword("end") ^^ {
        case Some(e) ~ cases ~ default => Term(Match(e, cases, default))
        case None ~ cases ~ default => Term (Switch(cases, default))
      }

  def literal: Parser[Term[Expr]] =
    (numericLit ^^ { case i => IntLiteral(i.toLong) } |
      floatLit ^^ { case f => FloatLiteral(f.toDouble) } |
      stringLit ^^ { case s => StringLiteral(s) } |
      keyword("null") ^^^ NullLiteral |
      keyword("true") ^^^ BoolLiteral(true) |
      keyword("false") ^^^ BoolLiteral(false)).map(Term[Expr](_))

  def relations: Parser[Option[SqlRelation[Term[Expr]]]] =
    keyword("from") ~> rep1sep(relation, op(",")).map(_.foldLeft[Option[SqlRelation[Term[Expr]]]](None) {
      case (None, traverse) => Some(traverse)
      case (Some(acc), traverse) => Some(CrossRelation(acc, traverse))
    })

  def std_join_relation: Parser[SqlRelation[Term[Expr]] => SqlRelation[Term[Expr]]] =
    opt(join_type) ~ keyword("join") ~ simple_relation ~ keyword("on") ~ expr ^^
      { case tpe ~ _ ~ r2 ~ _ ~ e => r1 => JoinRelation(r1, r2, tpe.getOrElse(InnerJoin), e) }

  def cross_join_relation: Parser[SqlRelation[Term[Expr]] => SqlRelation[Term[Expr]]] =
    keyword("cross") ~> keyword("join") ~> simple_relation ^^ {
      case r2 => r1 => CrossRelation(r1, r2)
    }

  def relation: Parser[SqlRelation[Term[Expr]]] =
    simple_relation ~ rep(std_join_relation | cross_join_relation) ^^ {
      case r ~ fs => fs.foldLeft(r) { case (r, f) => f(r) }
    }

  def join_type: Parser[JoinType] =
    (keyword("left") | keyword("right") | keyword("full")) ~ opt(keyword("outer")) ^^ {
      case "left" ~ o  => LeftJoin
      case "right" ~ o => RightJoin
      case "full" ~ o => FullJoin
    } | keyword("inner") ^^^ (InnerJoin)

  def simple_relation: Parser[SqlRelation[Term[Expr]]] =
    ident ~ opt(keyword("as")) ~ opt(ident) ^^ {
      case ident ~ _ ~ alias => TableRelationAST(ident, alias)
    } |
    op("(") ~> (
      (select ~ op(")") ~ opt(keyword("as")) ~ ident ^^ {
        case select ~ _ ~ _ ~ alias => ExprRelationAST(select, alias)
      }) |
      relation <~ op(")"))

  def filter: Parser[Term[Expr]] = keyword("where") ~> expr

  def group_by: Parser[GroupBy[Term[Expr]]] =
    keyword("group") ~> keyword("by") ~> rep1sep(expr, op(",")) ~ opt(keyword("having") ~> expr) ^^ {
      case k ~ h => GroupBy(k, h)
    }

  def order_by: Parser[OrderBy[Term[Expr]]] =
    keyword("order") ~> keyword("by") ~> rep1sep( expr ~ opt(keyword("asc") | keyword("desc")) ^^ {
      case i ~ (Some("asc") | None) => (ASC, i)
      case i ~ Some("desc") => (DESC, i)
    }, op(",")) ^^ (OrderBy(_))

  def limit: Parser[Long] = keyword("limit") ~> numericLit ^^ (_.toLong)

  def offset: Parser[Long] = keyword("offset") ~> numericLit ^^ (_.toLong)

  private def stripQuotes(s:String) = s.substring(1, s.length-1)

  def parseExpr(exprSql: String): ParsingError \/ Term[Expr] = {
    phrase(expr)(new lexical.Scanner(exprSql)) match {
      case Success(r, q)        => \/.right(r)
      case Error(msg, input)    => \/.left(GenericParsingError(msg))
      case Failure(msg, input)  => \/.left(GenericParsingError(msg))
    }
  }

  def parse(sql: Query): ParsingError \/ Term[Expr] = {
    phrase(expr)(new lexical.Scanner(sql.value)) match {
      case Success(r, q)        => \/.right(r)
      case Error(msg, input)    => \/.left(GenericParsingError(msg))
      case Failure(msg, input)  => \/.left(GenericParsingError(msg + "; " + input.first))
    }
  }
}

object SQLParser {
  import slamdata.engine.fs._

  def interpretPaths(expr: Term[Expr], mountPath: Path, basePath: Path):
      PathError \/ Term[Expr] = {
    val interpretPath: (SqlRelation ~> ({ type lam[X] = PathError \/ SqlRelation[X] })#lam) =
      new (SqlRelation ~> ({ type lam[X] = PathError \/ SqlRelation[X] })#lam) {
        def apply[A](rel: SqlRelation[A]) = rel match {
          case TableRelationAST(path, alias) =>
            Path(path).interpret(mountPath, basePath).map(p =>
              TableRelationAST(p.pathname, alias))
          case ExprRelationAST(_, _) => \/-(rel)
          case CrossRelation(l, r) =>
            (interpretPath(l) |@| interpretPath(r))(CrossRelation(_, _))
          case JoinRelation(l, r, t, c) =>
            (interpretPath(l) |@| interpretPath(r))(JoinRelation(_, _, t, c))
        }
      }

    expr.cataM[({ type lam[X] = PathError \/ X })#lam, Term[Expr]]({
      case s @ Select(d, p, rel, f, g, or, l, of) =>
        rel.fold[PathError \/ Term[Expr]](
          \/-(Term(s)))(
          interpretPath(_).map(r => Term(Select(d, p, Some(r), f, g, or, l, of))))
      case e => \/-(Term(e))
    })
  }
}
