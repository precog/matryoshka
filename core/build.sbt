name := "Core"

mainClass in Compile := Some("quasar.repl.Repl")

fork in run := true

connectInput in run := true

outputStrategy := Some(StdoutOutput)

import scoverage._

ScoverageKeys.coverageExcludedPackages := "quasar.repl;.*RenderTree"

ScoverageKeys.coverageMinimum := 80

ScoverageKeys.coverageFailOnMinimum := true

ScoverageKeys.coverageHighlighting := true

sbtbuildinfo.BuildInfoPlugin.projectSettings

buildInfoKeys := Seq[BuildInfoKey](version)

buildInfoPackage := "quasar.build"
