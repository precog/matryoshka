import com.github.retronym.SbtOneJar.oneJar

organization := "com.slamdata.admin"
name := "admin"

mainClass in Compile := Some("slamdata.engine.admin.Main")
fork := true

libraryDependencies ++= Seq(
  // "org.scala-lang"          %  "scala-swing"       % "2.11+",
  "com.github.benhutchison" %% "scalaswingcontrib" % "1.5"
)
