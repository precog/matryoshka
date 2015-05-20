name := "Web"

mainClass in Compile := Some("slamdata.engine.api.Server")

libraryDependencies ++= Seq(
  "com.github.tototoshi"    %% "scala-csv"         % "1.1.2"
)
