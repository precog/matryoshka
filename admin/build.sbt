name := "Admin"

mainClass in Compile := Some("slamdata.engine.admin.Main")
fork := true

libraryDependencies ++= Seq(
  "com.github.benhutchison" %% "scalaswingcontrib" % "1.5",
  "com.github.tototoshi"    %% "scala-csv"         % "1.1.2"
)
