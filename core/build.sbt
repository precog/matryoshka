name := "Core"

mainClass in Compile := Some("slamdata.engine.repl.Repl")

fork in run := true

connectInput in run := true

outputStrategy := Some(StdoutOutput)

import ScoverageSbtPlugin._

ScoverageKeys.coverageExcludedPackages := "slamdata.engine.repl;.*RenderTree"

ScoverageKeys.coverageMinimum := 75

ScoverageKeys.coverageFailOnMinimum := true

ScoverageKeys.coverageHighlighting := true
