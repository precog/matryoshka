package slamdata.engine

import slamdata.engine.analysis.fixplate._
import slamdata.engine.analysis._
import slamdata.engine.sql.SQLParser
import slamdata.engine.std._

import scalaz._

import org.specs2.mutable._
import org.specs2.matcher.{Matcher, Expectable}

class CompilerSpec extends Specification {
  import StdLib._
  import structural._
  import math._
  import LogicalPlan._
  import SemanticAnalysis._

  val c: String => Option[Term[LogicalPlan]] = query => {
    for {
      ast   <- (new SQLParser().parse(query)).toOption
      attr  <- SemanticAnalysis.AllPhases(tree(ast)).toOption
      cld   <- Compiler.compile(attr).toOption
    } yield cld
  }

  case class equalPlan(expected: Term[LogicalPlan]) extends Matcher[Term[LogicalPlan]] {
    val equal = Equal[Term[LogicalPlan]].equal _

    def apply[S <: Term[LogicalPlan]](s: Expectable[S]) = {
      result(equal(expected, s.value),
             s.description + " is not equal to " + expected,
             s.description + " is unexpectedly equal to " + expected,
             s)
    }
  }

  def succeed(query: String, expected: Term[LogicalPlan]) = {
    c(query) must beSome(equalPlan(expected))
  }

  "compiler" should {
    "compile simple constant example 1" in {
      succeed(
        "select 1", 
        MakeObject(constant(Data.Str("0")), constant(Data.Int(1)))
      )
    }

    "compile simple constant example 2" in {
      succeed(
        "select 1 * 1",
        MakeObject(
          constant(Data.Str("0")), 
          Multiply(constant(Data.Int(1)), constant(Data.Int(1)))
        )
      )
    }

    "compile simple 1-table projection when root identifier is also a projection" in {
      // 'foo' must be interpreted as a projection because only this interpretation is possible
      succeed(
        "select foo.bar from baz",
        let(
          Map('tmp0 -> read("baz")),
          MakeObject(
            constant(Data.Str("0")), 
            ObjectProject(
              ObjectProject(free('tmp0), constant(Data.Str("foo"))), 
              constant(Data.Str("bar"))
            )
          )
        )
      )
    }

    "compile simple 1-table projection when root identifier is also a table ref" in {
      // 'foo' must be interpreted as a table reference because this interpretation is possible
      // and consistent with ANSI SQL.
      succeed(
        "select foo.bar from foo",
        let(
          Map('tmp0 -> read("foo")),
          MakeObject(
            constant(Data.Str("0")), 
            ObjectProject(free('tmp0), constant(Data.Str("bar")))
          )
        )
      )
    }
  }
}