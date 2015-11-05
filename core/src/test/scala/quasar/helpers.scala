package quasar

import quasar.Predef._

import quasar.recursionschemes._, FunctorT.ops._
import quasar.sql.{SQLParser, Query}
import quasar.std._
import quasar.fs._

import scalaz._

import org.specs2.mutable._
import org.specs2.matcher.{Matcher, Expectable}

trait CompilerHelpers extends Specification with TermLogicalPlanMatchers {
  import StdLib._
  import structural._
  import LogicalPlan._
  import SemanticAnalysis._

  val compile: String => String \/ Fix[LogicalPlan] = query => {
    for {
      select <- SQLParser.parseInContext(Query(query), Path("./")).leftMap(_.toString)
      attr   <- AllPhases(tree(select)).leftMap(_.toString).disjunction
      cld    <- Compiler.compile(attr).leftMap(_.toString)
    } yield cld
  }

  def compileExp(query: String): Fix[LogicalPlan] =
    compile(query).fold(
      e => throw new RuntimeException("could not compile query for expected value: " + query + "; " + e),
      _.transCata(repeatedly(Optimizer.simplifyƒ)))

  def testLogicalPlanCompile(query: String, expected: Fix[LogicalPlan]) = {
    compile(query).toEither must beRight(equalToPlan(expected))
  }

  def read(name: String): Fix[LogicalPlan] = LogicalPlan.Read(fs.Path(name))

  def makeObj(ts: (String, Fix[LogicalPlan])*): Fix[LogicalPlan] =
    MakeObjectN(ts.map(t => Constant(Data.Str(t._1)) -> t._2): _*)

}
