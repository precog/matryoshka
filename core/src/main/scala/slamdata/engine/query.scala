package slamdata.engine

import slamdata.engine.sql.Query
import slamdata.engine.fs.Path

final case class QueryRequest(
  query:      Query,
  out:        Option[Path],
  mountPath:  Path,
  basePath:   Path,
  variables:  Variables) // Variables(Map())
