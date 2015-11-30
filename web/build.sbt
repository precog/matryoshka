import CustomKeys._

name := "Web"

mainClass in Compile := Some("quasar.api.Server")

libraryDependencies ++= Seq(
  "org.http4s"           %% "http4s-dsl"          % http4sVersion.value % "compile, test",
  "org.http4s"           %% "http4s-argonaut"     % http4sVersion.value % "compile, test"
    // TODO: remove once jawn-streamz is in agreement with http4s on scalaz-stream version
    exclude("org.scalaz.stream", "scalaz-stream_2.11"),
  "org.http4s"           %% "http4s-blaze-server" % http4sVersion.value % "compile, test",
  "com.github.tototoshi" %% "scala-csv"           % "1.1.2",
  "org.scodec"           %% "scodec-scalaz"       % "1.1.0"
)
