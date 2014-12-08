package slamdata.engine.sql

import org.specs2.mutable._
import org.specs2.ScalaCheck

import slamdata.engine._
import slamdata.engine.fp._

import scalaz._
import Scalaz._

class SQLParserSpec extends Specification with ScalaCheck with DisjunctionMatchers {
  import SqlQueries._

  implicit def stringToQuery(s: String): Query = Query(s)
  
  "SQLParser" should {
    "parse query1" in {
      val parser = new SQLParser
      val r = parser.parse(q1).toOption
      r should beSome
    }

    "parse query2" in {
      val parser = new SQLParser
      val r = parser.parse(q2).toOption
      r should beSome
    }

    "parse query3" in {
      val parser = new SQLParser
      val r = parser.parse(q3).toOption 
      r should beSome
    }

    "parse query4" in {
      val parser = new SQLParser
      val r = parser.parse(q4).toOption
      r should beSome
    }

    "parse query5" in {
      val parser = new SQLParser
      val r = parser.parse(q5).toOption
      r should beSome
    }

    "parse query6" in {
      val parser = new SQLParser
      val r = parser.parse(q6).toOption
      r should beSome
    }

    "parse query7" in {
      val parser = new SQLParser
      val r = parser.parse(q7).toOption
      r should beSome
    }

    "parse query8" in {
      val parser = new SQLParser
      val r = parser.parse(q8).toOption
      r should beSome
    }

    "parse query9" in {
      val parser = new SQLParser
      val r = parser.parse(q9).toOption
      r should beSome
    }

    "parse query10" in {
      val parser = new SQLParser
      val r = parser.parse(q10).toOption
      r should beSome
    }

    "parse query11" in {
      val parser = new SQLParser
      val r = parser.parse(q11).toOption
      r should beSome
    }

    "parse query12" in {
      val parser = new SQLParser
      val r = parser.parse(q12).toOption
      r should beSome
    }

    "parse query13" in {
      val parser = new SQLParser
      val r = parser.parse(q13).toOption
      r should beSome
    }

    "parse query14" in {
      val parser = new SQLParser
      val r = parser.parse(q14).toOption
      r should beSome
    }

    "parse query16" in {
      val parser = new SQLParser
      val r = parser.parse(q16).toOption
      r should beSome
    }

    "parse query17" in {
      val parser = new SQLParser
      val r = parser.parse(q17).toOption
      r should beSome
    }

    "parse query18" in {
      val parser = new SQLParser
      val r = parser.parse(q18).toOption
      r should beSome
    }

    "parse query19" in {
      val parser = new SQLParser
      val r = parser.parse(q19).toOption
      r should beSome
    }

    "parse query20" in {
      val parser = new SQLParser
      val r = parser.parse(q20).toOption
      r should beSome
    }

    "parse query21" in {
      val parser = new SQLParser
      val r = parser.parse(q21).toOption
      r should beSome
    }

    "parse query22" in {
      val parser = new SQLParser
      val r = parser.parse(q22).toOption
      r should beSome
    }

    "parse quoted literal" in {
      val parser = new SQLParser
      parser.parse("select * from foo where bar = 'abc'").toOption should beSome
    }

    "parse quoted literal with escaped quote" in {
      val parser = new SQLParser
      parser.parse("select * from foo where bar = 'that''s it!'").toOption should beSome
    }

    "parse quoted identifier" in {
      val parser = new SQLParser
      parser.parse("""select * from "tmp/foo" """).toOption should beSome
    }

    "parse quoted identifier with escaped quote" in {
      val parser = new SQLParser
      parser.parse("""select * from "tmp/foo[""bar""]" """).toOption should beSome
    }

    "parse simple query with two variables" in {
      val parser = new SQLParser
      parser.parse("""SELECT * FROM zips WHERE zips.dt > :start_time AND zips.dt <= :end_time """).toOption should beSome
    }    

    "parse true and false literals" in {
      val parser = new SQLParser

      parser.parse("""SELECT * FROM zips WHERE zips.isNormalized = TRUE AND zips.isFruityFlavored = FALSE""").toOption should beSome
    }
    
    "parse date, time, and timestamp literals" in {
      val q = """select * from foo 
                  where dt < date '2014-11-16'
                  and tm < time '03:00:00'
                  and ts < timestamp '2014-11-16T03:00:00Z' + interval 'PT1H'"""
      
      (new SQLParser).parse(q) must beAnyRightDisj
    }

    "parse IS and IS NOT" in {
      val q = """select * from foo 
                  where a IS NULL
                  and b IS NOT NULL
                  and c IS TRUE
                  and d IS NOT FALSE"""
      
      (new SQLParser).parse(q) must beAnyRightDisj
    }

    "round-trip to SQL and back" ! prop { (node: Node) =>
      val parser = new SQLParser
      val parsed = parser.parse(node.sql)

      parsed.fold(
        _ => println(node.sql + "\n" + node.show),
        p => if (p != node) println(p.sql))

      parsed must beRightDisjOrDiff(node)
    }
  }
  
  import org.scalacheck._
  import Gen._
  import org.threeten.bp.{Duration,Instant}
  import slamdata.engine.sql._
  
  implicit def arbitraryNode: Arbitrary[Node] = Arbitrary { selectGen(4) }
  
  def selectGen(depth: Int): Gen[SelectStmt] = for {
    isDistinct <- Gen.oneOf(SelectDistinct, SelectAll)
    projs      <- smallNonEmptyListOf(projGen)
    relations  <- Gen.option(relationGen(depth-1))
    filter     <- Gen.option(exprGen(depth-1))
    groupBy    <- Gen.option(groupByGen(depth-1))
    orderBy    <- Gen.option(orderByGen(depth-1))
    limit      <- Gen.option(choose(1L, 100L))
    offset     <- Gen.option(choose(1L, 100L))
  } yield SelectStmt(isDistinct, projs, relations, filter, groupBy, orderBy, limit, offset)
  
  def projGen: Gen[Proj] =
    exprGen(1).flatMap(x => 
      Gen.oneOf(
        Gen.const(Proj.Anon(x)), 
        for {
          n <- Gen.alphaChar.map(_.toString)  // TODO: generate names requiring quotes, etc.
        } yield Proj.Named(x, n)))
  
  def relationGen(depth: Int): Gen[SqlRelation] = {
    val simple = for {
        n <- Gen.alphaChar.map(_.toString)  // TODO: paths with '/', '.', etc.
        a <- Gen.option(Gen.alphaChar.map(_.toString))
      } yield TableRelationAST(n, a)
    if (depth <= 0) simple
    else Gen.frequency(
      10 -> simple,
      1 -> (for {
        s <- selectGen(2)
        c <- Gen.alphaChar
      } yield SubqueryRelationAST(s, c.toString)),
      1 -> (for {
        l <- relationGen(0)  // Note: we do not parenthesize nested joins, and the parser get confused
        r <- relationGen(0)
      } yield CrossRelation(l, r)),
      1 -> (for {
        l <- relationGen(0)  // Note: we do not parenthesize nested joins, and the parser get confused
        r <- relationGen(0)
        t <- Gen.oneOf(LeftJoin, RightJoin, InnerJoin, FullJoin)
        x <- exprGen(1)
      } yield JoinRelation(l, r, t, x))
    )
  }

  def groupByGen(depth: Int): Gen[GroupBy] = for {
    keys   <- smallNonEmptyListOf(exprGen(depth))
    having <- Gen.option(exprGen(depth))
  } yield GroupBy(keys, having)

  def orderByGen(depth: Int): Gen[OrderBy] = smallNonEmptyListOf(for {
    expr <- exprGen(depth)
    ot   <- Gen.oneOf(ASC, DESC)
  } yield (expr, ot)).map(OrderBy(_))

  def exprGen(depth: Int): Gen[Expr] = Gen.lzy {
    if (depth <= 0) simpleExprGen
    else complexExprGen(depth-1)
  }
  
  def simpleExprGen: Gen[Expr] =
    Gen.frequency(
      2 -> (for {
        n <- Gen.alphaChar.map(_.toString)
      } yield Vari(n)),
      1 -> (for {
        n  <- Gen.chooseNum(2, 5)  // Note: at least two, to be valid set syntax
        cs <- Gen.listOfN(n, constGen)
      } yield SetLiteral(cs)),
      10 -> (for {
        n <- Gen.alphaChar.map(_.toString)  // TODO: generate names requiring quotes, etc.
      } yield Ident(n)),
      1 -> Unop(StringLiteral(Instant.now.toString), ToTimestamp),
      1 -> Gen.choose(0L, 10000000000L).map(millis => Unop(StringLiteral(Duration.ofMillis(millis).toString), ToInterval)),
      1 -> Unop(StringLiteral("2014-11-17"), ToDate),
      1 -> Unop(StringLiteral("12:00:00"), ToTime)
    )

  def complexExprGen(depth: Int): Gen[Expr] =
    Gen.frequency(
      5 -> simpleExprGen,
      1 -> Gen.lzy(selectGen(depth-1).flatMap(s => Subselect(s))),
      1 -> (for {
        expr <- Gen.option(exprGen(depth))
      } yield Splice(expr)),
      3 -> (for {
        l  <- exprGen(depth)
        r  <- exprGen(depth)
        op <- Gen.oneOf(
          Or, And, Eq, Neq, Ge, Gt, Le, Lt,
          Plus, Minus, Mult, Div, Mod,
          In)
      } yield Binop(l, r, op)),
      1 -> (for {
        l <- exprGen(depth)
        n <- Gen.alphaChar
      } yield Binop(l, StringLiteral(n.toString), FieldDeref)),  // parser has trouble with complex "{...}" syntax 
      1 -> (for {
        l <- exprGen(depth)
        i <- Gen.choose(1, 100)
      } yield Binop(l, IntLiteral(i), IndexDeref)),  // parser has trouble with complex expressions inside "[...]" 
      2 -> (for {
        x  <- exprGen(depth)
        op <- Gen.oneOf(
          Not, Exists, Positive, Negative, Distinct,
          //YearFrom, MonthFrom, DayFrom, HourFrom, MinuteFrom, SecondFrom,  // FIXME: all generate wrong SQL
          ToDate, ToInterval,
          ObjectFlatten, ArrayFlatten, 
          IsNull)
      } yield Unop(x, op)),
      2 -> (for {
        fn  <- Gen.oneOf("sum", "count", "avg", "length")
        arg <- exprGen(depth)
      } yield InvokeFunction(fn, List(arg))),
      1 -> (for {
        expr  <- exprGen(depth)
        cases <- casesGen(depth)
        dflt  <- Gen.option(exprGen(depth))
      } yield Match(expr, cases, dflt)),
      1 -> (for {
        cases <- casesGen(depth)
        dflt  <- Gen.option(exprGen(depth))
      } yield Switch(cases, dflt))
    )
    
  def casesGen(depth: Int): Gen[List[Case]] =
    smallNonEmptyListOf(for {
        cond <- exprGen(depth)
        expr <- exprGen(depth)
      } yield Case(cond, expr))
    
  def constGen: Gen[LiteralExpr] = 
    Gen.oneOf(
      Gen.chooseNum(0, 100).flatMap(IntLiteral(_)),       // Note: negative numbers are parsed as Unop(-, _)
      Gen.chooseNum(0.0, 10.0).flatMap(FloatLiteral(_)),  // Note: negative numbers are parsed as Unop(-, _)
      Gen.alphaStr.flatMap(StringLiteral(_)),  // TODO: strings with quotes, etc.
      Gen.const(NullLiteral()),
      Gen.const(BoolLiteral(true)),
      Gen.const(BoolLiteral(false)))
  
  /**
   Generates non-empty lists which grow based on the `size` parameter, but 
   slowly (log), so that trees built out of the lists don't get
   exponentially big.
   */
  def smallNonEmptyListOf[A](gen: Gen[A]): Gen[List[A]] = {
    def log2(x: Int): Int = (Math.log(x)/Math.log(2)).toInt
    for {
      sz <- Gen.size
      n  <- Gen.choose(1, log2(sz))
      l  <- Gen.listOfN(n, gen)
    } yield l
  }
}
