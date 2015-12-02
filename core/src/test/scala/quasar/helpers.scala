package quasar

import quasar.Predef._
import quasar.recursionschemes._
import quasar.sql.{SQLParser, Query}
import quasar.std._
import quasar.fs._

import org.specs2.mutable._
import org.specs2.matcher.{Matcher, Expectable}
import scalaz._, Scalaz._

trait CompilerHelpers extends Specification with TermLogicalPlanMatchers {
  import StdLib._
  import structural._
  import LogicalPlan._
  import SemanticAnalysis._

  val compile: String => String \/ Fix[LogicalPlan] = query => {
    for {
      select <- SQLParser.parseInContext(Query(query), Path("./")).leftMap(_.toString)
      attr   <- AllPhases(select).leftMap(_.toString)
      cld    <- Compiler.compile(attr).leftMap(_.toString)
    } yield cld
  }

  // NB: this plan is simplified and normalized, but not optimized. That allows
  // the expected result to be controlled more precisley. Assuming you know
  // what plan the compiler produces for a reference query, you can demand that
  // `optimize` produces the same plan given a query in some more deviant form.
  def compileExp(query: String): Fix[LogicalPlan] =
    compile(query).fold(
      e => throw new RuntimeException("could not compile query for expected value: " + query + "; " + e),
      lp => (LogicalPlan.normalizeLets _ >>> LogicalPlan.normalizeTempNames _)(Optimizer.simplify(lp)))

  def testLogicalPlanCompile(query: String, expected: Fix[LogicalPlan]) = {
    compile(query).map(Optimizer.optimize).toEither must beRight(equalToPlan(expected))
  }

  def read(name: String): Fix[LogicalPlan] = LogicalPlan.Read(fs.Path(name))

  type FLP = Fix[LogicalPlan]
  implicit def toFix[F[_]](unFixed: F[Fix[F]]): Fix[F] = Fix(unFixed)

  def makeObj(ts: (String, Fix[LogicalPlan])*): Fix[LogicalPlan] =
    Fix(MakeObjectN(ts.map(t => Constant(Data.Str(t._1)) -> t._2): _*))

}
