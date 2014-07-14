package slamdata.engine

import slamdata.engine.analysis.fixplate._
import slamdata.engine.analysis._
import slamdata.engine.sql.{SQLParser, Query}
import slamdata.engine.std._

import scalaz._

import org.specs2.mutable._
import org.specs2.matcher.{Matcher, Expectable}

trait CompilerHelpers extends Specification with TermLogicalPlanMatchers {
  import StdLib._
  import structural._
  import math._
  import LogicalPlan._
  import SemanticAnalysis._

  val compile: String => String \/ Term[LogicalPlan] = query => {
    for {
      ast   <- (new SQLParser().parse(Query(query))).leftMap(e => e.toString())
      attr  <- SemanticAnalysis.AllPhases(tree(ast)).leftMap(e => e.toString()).disjunction
      cld   <- Compiler.compile(attr).leftMap(e => e.toString())
    } yield cld
  }

  def compileExp(query: String): Term[LogicalPlan] =
    compile(query).fold(e => throw new RuntimeException("could not compile query for expected value: " + query + "; " + e), v => v)

  def testLogicalPlanCompile(query: String, expected: Term[LogicalPlan]) = {
    compile(query).toEither must beRight(equalToPlan(expected))
  }
  
  def read(name: String): Term[LogicalPlan] = LogicalPlan.Read(fs.Path(name))

  def makeObj(ts: (String, Term[LogicalPlan])*): Term[LogicalPlan] = 
    MakeObjectN(ts.map(t => Constant(Data.Str(t._1)) -> t._2): _*)

}
