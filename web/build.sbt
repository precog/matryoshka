name := "Web"

mainClass in Compile := Some("quasar.api.Server")

val http4sVersion     = "0.8.6"

libraryDependencies ++= Seq(
  "org.http4s"           %% "http4s-dsl"       % http4sVersion % "compile, test",
  "org.http4s"           %% "http4s-argonaut"  % http4sVersion % "compile, test",
  "org.http4s"           %% "http4s-blazeserver" % http4sVersion % "compile, test",
  "com.github.tototoshi" %% "scala-csv"        % "1.1.2",
  "org.scodec"           %% "scodec-scalaz"    % "1.1.0"
)
