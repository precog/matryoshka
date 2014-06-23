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

  def testLogicalPlanCompile(query: String, expected: Term[LogicalPlan]) = {
    compile(query).toEither must beRight(equalToPlan(expected))
  }
  
  def read(name: String): Term[LogicalPlan] = LogicalPlan.read(fs.Path(name))

  def letOne(s: Symbol, t: Term[LogicalPlan], expr: Term[LogicalPlan]) = 
    let(Map(s -> t), expr)

  def makeObj(ts: (String, Term[LogicalPlan])*): Term[LogicalPlan] = {
    val objs = ts.map { case (label, term) => MakeObject(constant(Data.Str(label)), term) }
    if (objs.length == 1) objs(0) else ObjectConcat(objs: _*)
  }

  def makeArray(ts: Term[LogicalPlan]*): Term[LogicalPlan] = {
    val objs = ts.map(MakeArray(_))
    if (objs.length == 1) objs(0) else ArrayConcat(objs: _*)
  }

}