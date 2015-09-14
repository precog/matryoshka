package quasar.sql

import quasar.Predef._
import quasar.RenderTree.ops._
import quasar.fp._
import quasar.recursionschemes._
import quasar.specs2._

import org.specs2.mutable._
import org.specs2.ScalaCheck
import scalaz._, Scalaz._

class SQLParserSpec extends Specification with ScalaCheck with DisjunctionMatchers {
  import SqlQueries._

  implicit def stringToQuery(s: String): Query = Query(s)

  val parser = new SQLParser

  "SQLParser" should {
    "parse query1" in {
      val r = parser.parse(q1).toOption
      r should beSome
    }

    "parse query2" in {
      val r = parser.parse(q2).toOption
      r should beSome
    }

    "parse query3" in {
      val r = parser.parse(q3).toOption
      r should beSome
    }

    "parse query4" in {
      val r = parser.parse(q4).toOption
      r should beSome
    }

    "parse query5" in {
      val r = parser.parse(q5).toOption
      r should beSome
    }

    "parse query6" in {
      val r = parser.parse(q6).toOption
      r should beSome
    }

    "parse query7" in {
      val r = parser.parse(q7).toOption
      r should beSome
    }

    "parse query8" in {
      val r = parser.parse(q8).toOption
      r should beSome
    }

    "parse query9" in {
      val r = parser.parse(q9).toOption
      r should beSome
    }

    "parse query10" in {
      val r = parser.parse(q10).toOption
      r should beSome
    }

    "parse query11" in {
      val r = parser.parse(q11).toOption
      r should beSome
    }

    "parse query12" in {
      val r = parser.parse(q12).toOption
      r should beSome
    }

    "parse query13" in {
      val r = parser.parse(q13).toOption
      r should beSome
    }

    "parse query14" in {
      val r = parser.parse(q14).toOption
      r should beSome
    }

    "parse query16" in {
      val r = parser.parse(q16).toOption
      r should beSome
    }

    "parse query17" in {
      val r = parser.parse(q17).toOption
      r should beSome
    }

    "parse query18" in {
      val r = parser.parse(q18).toOption
      r should beSome
    }

    "parse query19" in {
      val r = parser.parse(q19).toOption
      r should beSome
    }

    "parse query20" in {
      val r = parser.parse(q20).toOption
      r should beSome
    }

    "parse query21" in {
      val r = parser.parse(q21).toOption
      r should beSome
    }

    "parse query22" in {
      val r = parser.parse(q22).toOption
      r should beSome
    }

    "parse quoted literal" in {
      parser.parse("select * from foo where bar = 'abc'").toOption should beSome
    }

    "parse quoted literal with escaped quote" in {
      parser.parse("select * from foo where bar = 'that''s it!'").toOption should beSome
    }

    "parse literal that’s too big for an Int" in {
      parser.parse("select * from users where add_date > 1425460451000") should
        beRightDisjOrDiff(
          Select(
            SelectAll,
            List(Proj(Splice(None), None)),
            Some(TableRelationAST("users",None)),
            Some(Binop(Ident("add_date"),IntLiteral(1425460451000L), Gt)),
            None,None,None,None))
    }

    "parse quoted identifier" in {
      parser.parse("""select * from "tmp/foo" """).toOption should beSome
    }

    "parse quoted identifier with escaped quote" in {
      parser.parse("""select * from "tmp/foo[""bar""]" """).toOption should beSome
    }

    "parse simple query with two variables" in {
      parser.parse("""SELECT * FROM zips WHERE zips.dt > :start_time AND zips.dt <= :end_time """).toOption should beSome
    }

    "parse true and false literals" in {
      parser.parse("""SELECT * FROM zips WHERE zips.isNormalized = TRUE AND zips.isFruityFlavored = FALSE""").toOption should beSome
    }

    "parse numeric literals" in {
      parser.parse("select 1, 2.0, 3000000, 2.998e8, -1.602E-19, 1e+6") should beRightDisjunction
    }

    "parse date, time, timestamp, and id literals" in {
      val q = """select * from foo
                  where dt < date '2014-11-16'
                  and tm < time '03:00:00'
                  and ts < timestamp '2014-11-16T03:00:00Z' + interval 'PT1H'
                  and _id != oid 'abc123'"""

      parser.parse(q) must beRightDisjunction
    }

    "parse IS and IS NOT" in {
      val q = """select * from foo
                  where a IS NULL
                  and b IS NOT NULL
                  and c IS TRUE
                  and d IS NOT FALSE"""

      parser.parse(q) must beRightDisjunction
    }

    "parse nested joins left to right" in {
      val q1 = "select * from a cross join b cross join c"
      val q2 = "select * from (a cross join b) cross join c"
      parser.parse(q1) must_== parser.parse(q2)
    }

    "parse nested joins with parens" in {
      val q = "select * from a cross join (b cross join c)"
      parser.parse(q) must beRightDisjunction(
        Select(
          SelectAll,
          List(Proj(Splice(None), None)),
          Some(
            CrossRelation(
              TableRelationAST("a", None),
              CrossRelation(
                TableRelationAST("b", None),
                TableRelationAST("c", None)))),
          None, None, None, None, None))
    }

    "parse array constructor and concat op" in {
      parser.parse("select loc || [ pop ] from zips") must beRightDisjunction(
        Select(SelectAll,
          List(
            Proj(
              Binop(Ident("loc"),
                ArrayLiteral(List(
                  Ident("pop"))),
                Concat),
              None)),
          Some(TableRelationAST("zips", None)),
          None, None, None, None, None))
    }

    "round-trip to SQL and back" ! prop { (node: Expr) =>
      val parsed = parser.parse(pprint(node))

      parsed.fold(
        _ => println(node.shows + "\n" + pprint(node)),
        p => if (p != node) println(pprint(p) + "\n" + (node.render diff p.render).show))

      parsed must beRightDisjOrDiff(node)
    }
  }

  import org.scalacheck._
  import Gen._
  import org.threeten.bp.{Duration,Instant}
  import quasar.sql._

  implicit def arbitraryExpr: Arbitrary[Expr] = Arbitrary { selectGen(4) }

  def selectGen(depth: Int): Gen[Expr] = for {
    isDistinct <- Gen.oneOf(SelectDistinct, SelectAll)
    projs      <- smallNonEmptyListOf(projGen)
    relations  <- Gen.option(relationGen(depth-1))
    filter     <- Gen.option(exprGen(depth-1))
    groupBy    <- Gen.option(groupByGen(depth-1))
    orderBy    <- Gen.option(orderByGen(depth-1))
    limit      <- Gen.option(choose(1L, 100L))
    offset     <- Gen.option(choose(1L, 100L))
  } yield Select(isDistinct, projs, relations, filter, groupBy, orderBy, limit, offset)

  def projGen: Gen[Proj[Expr]] =
    Gen.oneOf(
      Gen.const(Proj(Splice(None), None)),
      exprGen(1).flatMap(x =>
        Gen.oneOf(
          Gen.const(Proj(x, None)),
          for {
            n <- Gen.oneOf(
              Gen.alphaChar.map(_.toString),
              Gen.const("public enemy #1"),
              Gen.const("I quote: \"foo\""))
          } yield Proj(x, Some(n)))))

  def relationGen(depth: Int): Gen[SqlRelation[Expr]] = {
    val simple = for {
        p <- Gen.oneOf(Nil, "" :: Nil, "." :: Nil)
        s <- Gen.choose(1, 3)
        n <- Gen.listOfN(s, Gen.alphaChar.map(_.toString)).map(ns => (p ++ ns).mkString("/"))
        a <- Gen.option(Gen.alphaChar.map(_.toString))
      } yield TableRelationAST[Expr](n, a)
    if (depth <= 0) simple
    else Gen.frequency(
      5 -> simple,
      1 -> (for {
        s <- selectGen(2)
        c <- Gen.alphaChar
      } yield ExprRelationAST(s, c.toString)),
      1 -> (for {
        l <- relationGen(depth-1)
        r <- relationGen(depth-1)
      } yield CrossRelation(l, r)),
      1 -> (for {
        l <- relationGen(depth-1)
        r <- relationGen(depth-1)
        t <- Gen.oneOf(LeftJoin, RightJoin, InnerJoin, FullJoin)
        x <- exprGen(1)
      } yield JoinRelation(l, r, t, x))
    )
  }

  def groupByGen(depth: Int): Gen[GroupBy[Expr]] = for {
    keys   <- smallNonEmptyListOf(exprGen(depth))
    having <- Gen.option(exprGen(depth))
  } yield GroupBy(keys, having)

  def orderByGen(depth: Int): Gen[OrderBy[Expr]] = smallNonEmptyListOf(for {
    expr <- exprGen(depth)
    ot   <- Gen.oneOf(ASC, DESC)
  } yield (ot, expr)).map(OrderBy(_))

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
        n <- Gen.oneOf(
          Gen.alphaChar.map(_.toString),
          Gen.const("name, address"),
          Gen.const("q: \"a\""))
      } yield Ident(n)),
      1 -> Unop(StringLiteral(Instant.now.toString), ToTimestamp),
      1 -> Gen.choose(0L, 10000000000L).map(millis => Unop(StringLiteral(Duration.ofMillis(millis).toString), ToInterval)),
      1 -> Unop(StringLiteral("2014-11-17"), ToDate),
      1 -> Unop(StringLiteral("12:00:00"), ToTime),
      1 -> Unop(StringLiteral("123456"), ToId)
    )

  import quasar.std.StdLib._

  def complexExprGen(depth: Int): Gen[Expr] =
    Gen.frequency(
      5 -> simpleExprGen,
      1 -> Gen.lzy(selectGen(depth-1)),
      1 -> (for {
        expr <- exprGen(depth)
      } yield Splice(Some(expr))),
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
        n <- exprGen(depth)
      } yield Binop(l, n, FieldDeref)),
      1 -> (for {
        l <- exprGen(depth)
        i <- exprGen(depth)
      } yield Binop(l, i, IndexDeref)),
      2 -> (for {
        x  <- exprGen(depth)
        op <- Gen.oneOf(
          Not, Exists, Positive, Negative, Distinct,
          ToDate, ToInterval,
          ObjectFlatten, ArrayFlatten,
          IsNull)
      } yield Unop(x, op)),
      2 -> (for {
        fn  <- Gen.oneOf(agg.Sum, agg.Count, agg.Avg, string.Length, structural.MakeArray)
        arg <- exprGen(depth)
      } yield InvokeFunction(fn.name, List(arg))),
      1 -> (for {
        arg <- exprGen(depth)
      } yield InvokeFunction(string.Like.name, List(arg, StringLiteral("B%"), StringLiteral("")))),
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

  def casesGen(depth: Int): Gen[List[Case[Expr]]] =
    smallNonEmptyListOf(for {
        cond <- exprGen(depth)
        expr <- exprGen(depth)
      } yield Case(cond, expr))

  def constGen: Gen[Expr] =
    Gen.oneOf(
      Gen.chooseNum(0, 100).flatMap(IntLiteral(_)),       // Note: negative numbers are parsed as Unop(-, _)
      Gen.chooseNum(0.0, 10.0).flatMap(FloatLiteral(_)),  // Note: negative numbers are parsed as Unop(-, _)
      Gen.alphaStr.flatMap(StringLiteral(_)),
      // Note: only `'` gets special encoding; the rest should be accepted as is.
      for {
        s  <- Gen.choose(1, 5)
        cs <- Gen.listOfN(s, Gen.oneOf("'", "\\", " ", "\n", "\t", "a", "b", "c"))
      } yield StringLiteral(cs.mkString),
      Gen.const(NullLiteral()),
      Gen.const(BoolLiteral(true)),
      Gen.const(BoolLiteral(false)))

  /**
   Generates non-empty lists which grow based on the `size` parameter, but
   slowly (log), so that trees built out of the lists don't get
   exponentially big.
   */
  def smallNonEmptyListOf[A](gen: Gen[A]): Gen[List[A]] = {
    def log2(x: Int): Int = (java.lang.Math.log(x)/java.lang.Math.log(2)).toInt
    for {
      sz <- Gen.size
      n  <- Gen.choose(1, log2(sz))
      l  <- Gen.listOfN(n, gen)
    } yield l
  }
}
