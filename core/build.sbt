name := "SlamEngine"

mainClass in Compile := Some("slamdata.engine.repl.Repl")

fork in run := true

connectInput in run := true

outputStrategy := Some(StdoutOutput)

import ScoverageSbtPlugin._

ScoverageKeys.coverageExcludedPackages := "slamdata.engine.repl;.*RenderTree;.*MongoDbExecutor;.*MongoWrapper"

ScoverageKeys.coverageMinimum := 66

ScoverageKeys.coverageFailOnMinimum := true

ScoverageKeys.coverageHighlighting := true

