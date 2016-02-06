fork in run := true

connectInput in run := true

outputStrategy := Some(StdoutOutput)

import scoverage._

ScoverageKeys.coverageMinimum := 57

ScoverageKeys.coverageFailOnMinimum := true
