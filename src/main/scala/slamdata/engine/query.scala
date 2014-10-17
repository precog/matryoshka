package slamdata.engine

import slamdata.engine.sql.Query
import slamdata.engine.fs.Path

case class QueryRequest(
  query:      Query, 
  out:        Option[Path],
  mountPath:  Path = Path.Root, 
  basePath:   Path = Path.Root, 
  variables:  Variables = Variables(Map()))