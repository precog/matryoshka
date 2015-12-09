package quasar

import quasar.regression._
import quasar.sql._

class StreamingQueryRegressionSpec
  extends QueryRegressionTest[FileSystemIO](QueryRegressionTest.externalFS) {

  val suiteName = "Streaming Queries"

  def queryResults(expr: Expr, vars: Variables) =
    query.evaluateQuery(expr, vars)
}
