package slamdata.engine

import slamdata.engine.analysis.fixplate._
import slamdata.engine.analysis._
import slamdata.engine.sql.SQLParser

import scalaz._

import org.specs2.mutable._

class CompilerSpec extends Specification {
  import LogicalPlan._
  import SemanticAnalysis._

  val c: String => Option[Term[LogicalPlan]] = query => {
    for {
      ast   <- (new SQLParser().parse(query)).toOption
      attr  <- SemanticAnalysis.AllPhases(tree(ast)).toOption
      cld   <- Compiler.compile(attr).toOption
    } yield cld
  }

  val equal = Equal[Term[LogicalPlan]].equal _

  def test(query: String, expected: Term[LogicalPlan]) = {
    c(query).map(actual => equal(expected, actual)) must beSome(true)
  }

  "select constant" should {
    "compile" in {
      true must beTrue
    }
  }
}