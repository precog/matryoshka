package slamdata.engine

import slamdata.engine.sql.Expr
import slamdata.engine.fs.Path

final case class QueryRequest(
  query:      Expr,
  out:        Option[Path],
  variables:  Variables)
